#!/usr/bin/env python3
"""TCP-to-HTTP/1.1 packet-up CXH Client Shim for OfficialHQ.

Splits bidirectional SSH traffic into:
  - 1 streaming GET response:  GET  <path>/<sid>
  - Sequenced POST uploads:    POST <path>/<sid>/<seq> (Content-Length + keep-alive)

Cloud Run example (Termux):
    python3 tools/xh_client.py \
        --host servicename-000xxx0xx00.xx-region.run.app \
        --connect firebaseremoteconfigrealtime.googleapis.com \
        --sni firebaseremoteconfigrealtime.googleapis.com \
        --path /kyouji/open-cxh \
        --listen 127.0.0.1:10022
"""

import argparse
import asyncio
import secrets
import ssl

READ_SIZE = 32768
HEADER_LIMIT = 16384
CHUNK_LINE_LIMIT = 8192
MAX_POST_RETRIES = 3


async def read_headers(reader):
    raw = await reader.readuntil(b"\r\n\r\n")
    if len(raw) > HEADER_LIMIT:
        raise ValueError("HTTP response headers too large")

    lines = raw[:-4].split(b"\r\n")
    first = lines[0].decode("latin-1").split()
    if len(first) < 2:
        raise ValueError("Malformed HTTP response status line")

    try:
        status = int(first[1])
    except ValueError as exc:
        raise ValueError("Invalid HTTP response status") from exc

    headers = {}
    for line in lines[1:]:
        if b":" not in line:
            raise ValueError("Malformed HTTP response header")
        name, value = line.split(b":", 1)
        key = name.decode("latin-1").strip().lower()
        val = value.decode("latin-1").strip()
        if key in headers:
            headers[key] += ", " + val
        else:
            headers[key] = val

    return status, headers, lines[0].decode("latin-1")


def _build_tls_context(use_tls, insecure):
    if not use_tls:
        return None
    ctx = ssl.create_default_context()
    ctx.set_alpn_protocols(["http/1.1"])
    if insecure:
        ctx.check_hostname = False
        ctx.verify_mode = ssl.CERT_NONE
    return ctx


async def _open_http_conn(connect_host, port, sni, tls_context):
    opts = {
        "host": connect_host,
        "port": port,
        "limit": 262144,
    }
    if tls_context is not None:
        opts["ssl"] = tls_context
        opts["server_hostname"] = sni
    return await asyncio.open_connection(**opts)


class PostConnectionPool:
    """Bounded HTTP/1.1 keep-alive connection pool for packet-up POSTs."""

    def __init__(self, connect_host, port, http_host, sni, tls_context, max_inflight):
        self.connect_host = connect_host
        self.port = port
        self.http_host = http_host
        self.sni = sni
        self.tls_context = tls_context
        self.sem = asyncio.Semaphore(max_inflight)
        self._idle = []
        self._closed = False

    async def _acquire_conn(self):
        while self._idle:
            reader, writer = self._idle.pop()
            if not writer.is_closing() and not reader.at_eof():
                return reader, writer
            writer.close()
        return await _open_http_conn(
            self.connect_host, self.port, self.sni, self.tls_context
        )

    def _release_conn(self, reader, writer):
        if self._closed or writer.is_closing() or reader.at_eof():
            writer.close()
            return
        self._idle.append((reader, writer))

    async def send_packet(self, path, seq, payload, is_eof=False):
        async with self.sem:
            fin_hdr = "X-CXH-Fin: 1\r\n" if is_eof else ""
            conn_val = "close" if is_eof else "keep-alive"
            req_head = (
                f"POST {path}/{seq} HTTP/1.1\r\n"
                f"Host: {self.http_host}\r\n"
                "User-Agent: OfficialHQ-XH/0.2\r\n"
                "Content-Type: application/octet-stream\r\n"
                f"Content-Length: {len(payload)}\r\n"
                f"{fin_hdr}"
                f"Connection: {conn_val}\r\n"
                "\r\n"
            ).encode("ascii")

            last_err = None
            for _attempt in range(MAX_POST_RETRIES):
                reader = writer = None
                try:
                    reader, writer = await self._acquire_conn()
                    writer.write(req_head)
                    if payload:
                        writer.write(payload)
                    await writer.drain()

                    status, headers, status_line = await read_headers(reader)
                    cl = int(headers.get("content-length", "0") or "0")
                    if cl > 0:
                        await reader.readexactly(cl)

                    if status != 200:
                        writer.close()
                        raise ConnectionError(
                            f"POST seq={seq} rejected: {status_line}"
                        )

                    conn_tokens = {
                        t.strip().lower()
                        for t in headers.get("connection", "").split(",")
                    }
                    if is_eof or "close" in conn_tokens:
                        writer.close()
                    else:
                        self._release_conn(reader, writer)
                    return

                except asyncio.CancelledError:
                    if writer is not None:
                        writer.close()
                    raise
                except Exception as exc:
                    last_err = exc
                    if writer is not None:
                        writer.close()

            raise ConnectionError(
                f"POST seq={seq} failed after {MAX_POST_RETRIES} attempts: {last_err}"
            )

    async def close_all(self):
        self._closed = True
        conns, self._idle = self._idle, []
        for _reader, writer in conns:
            writer.close()
        for _reader, writer in conns:
            try:
                await writer.wait_closed()
            except Exception:
                pass


async def local_to_http_packets(local_reader, pool, session_path):
    """Read local TCP stream and dispatch sequenced POST requests."""
    seq = 0
    inflight_tasks = set()

    try:
        while True:
            data = await local_reader.read(READ_SIZE)
            if not data:
                break

            if seq == 0:
                print(
                    f"[XH-CLIENT] First upload packet seq=0 ({len(data)}B): {data[:30]!r}",
                    flush=True,
                )

            # Wait if inflight tasks hit pool limit so backpressure applies to local_reader.
            async with pool.sem:
                pass

            task = asyncio.create_task(
                pool.send_packet(session_path, seq, data, is_eof=False)
            )
            inflight_tasks.add(task)
            task.add_done_callback(inflight_tasks.discard)
            seq += 1

            # Surface any completed task exceptions promptly.
            for done_task in list(inflight_tasks):
                if done_task.done() and not done_task.cancelled():
                    err = done_task.exception()
                    if err is not None:
                        raise err

        if inflight_tasks:
            await asyncio.gather(*inflight_tasks)

        # Send final ordered EOF packet (0-length POST + X-CXH-Fin: 1).
        await pool.send_packet(session_path, seq, b"", is_eof=True)
        print(f"[XH-CLIENT] Sent upload EOF packet seq={seq}", flush=True)

    finally:
        for task in inflight_tasks:
            if not task.done():
                task.cancel()
        if inflight_tasks:
            await asyncio.gather(*inflight_tasks, return_exceptions=True)


async def http_to_local_stream(
    connect_host, port, http_host, sni, tls_context, session_path, local_writer
):
    """Open streaming GET and decode chunked response into local TCP socket."""
    http_reader, http_writer = await _open_http_conn(
        connect_host, port, sni, tls_context
    )
    try:
        req = (
            f"GET {session_path} HTTP/1.1\r\n"
            f"Host: {http_host}\r\n"
            "User-Agent: OfficialHQ-XH/0.2\r\n"
            "Accept: application/octet-stream\r\n"
            "Cache-Control: no-store\r\n"
            "Connection: close\r\n"
            "\r\n"
        ).encode("ascii")

        http_writer.write(req)
        await http_writer.drain()

        status, headers, status_line = await read_headers(http_reader)
        if status != 200:
            raise ConnectionError(f"GET stream returned HTTP {status} ({status_line})")

        if "chunked" not in headers.get("transfer-encoding", "").lower():
            raise ValueError("GET stream response is not chunked")

        first_resp = True
        while True:
            line = await http_reader.readline()
            if (
                not line
                or len(line) > CHUNK_LINE_LIMIT
                or not line.endswith(b"\r\n")
            ):
                raise ValueError(f"Invalid response chunk header: {line[:50]!r}")

            token = line[:-2].split(b";", 1)[0].strip()
            try:
                size = int(token, 16)
            except ValueError as exc:
                raise ValueError(f"Invalid response chunk size: {token!r}") from exc

            if size < 0:
                raise ValueError("Negative HTTP chunk size")

            if size == 0:
                trailer_bytes = 0
                while True:
                    trailer = await http_reader.readline()
                    if not trailer:
                        raise ConnectionError("EOF in HTTP response trailers")
                    trailer_bytes += len(trailer)
                    if trailer_bytes > HEADER_LIMIT:
                        raise ValueError("HTTP response trailers too large")
                    if trailer == b"\r\n":
                        if (
                            not local_writer.is_closing()
                            and local_writer.can_write_eof()
                        ):
                            local_writer.write_eof()
                        return
                    if b":" not in trailer:
                        raise ValueError("Invalid HTTP response trailer")

            remaining = size
            while remaining:
                data = await http_reader.readexactly(min(remaining, READ_SIZE))
                if first_resp:
                    print(
                        f"[XH-CLIENT] First downstream chunk ({len(data)}B): {data[:30]!r}",
                        flush=True,
                    )
                    first_resp = False
                local_writer.write(data)
                await local_writer.drain()
                remaining -= len(data)

            if await http_reader.readexactly(2) != b"\r\n":
                raise ValueError("Invalid HTTP response chunk terminator")

    finally:
        http_writer.close()
        try:
            await http_writer.wait_closed()
        except Exception:
            pass


async def handle_local(
    local_reader,
    local_writer,
    connect_host,
    port,
    http_host,
    sni,
    base_path,
    use_tls,
    insecure=False,
    max_inflight=2,
):
    sid = secrets.token_urlsafe(16)
    clean_base = base_path.rstrip("/")
    session_path = f"{clean_base}/{sid}"
    tls_context = _build_tls_context(use_tls, insecure)

    pool = PostConnectionPool(
        connect_host, port, http_host, sni, tls_context, max_inflight
    )
    upload_task = None
    download_task = None

    try:
        print(
            f"[XH-CLIENT] New session sid={sid} -> {connect_host}:{port}{session_path}",
            flush=True,
        )
        download_task = asyncio.create_task(
            http_to_local_stream(
                connect_host,
                port,
                http_host,
                sni,
                tls_context,
                session_path,
                local_writer,
            ),
            name=f"xh-client-down-{sid[:8]}",
        )
        upload_task = asyncio.create_task(
            local_to_http_packets(local_reader, pool, session_path),
            name=f"xh-client-up-{sid[:8]}",
        )

        while True:
            active = {
                t
                for t in (upload_task, download_task)
                if t is not None and not t.done()
            }
            if not active:
                break

            done, _ = await asyncio.wait(
                active, return_when=asyncio.FIRST_COMPLETED
            )
            for task in done:
                err = task.exception()
                if err is not None:
                    raise err

            if download_task in done:
                break

    except asyncio.CancelledError:
        raise
    except Exception as exc:
        print(f"[XH-CLIENT] {type(exc).__name__}: {exc}", flush=True)
    finally:
        for task in (upload_task, download_task):
            if task is not None and not task.done():
                task.cancel()
        tasks = [t for t in (upload_task, download_task) if t is not None]
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)

        await pool.close_all()

        if not local_writer.is_closing():
            local_writer.close()
        try:
            await local_writer.wait_closed()
        except Exception:
            pass


def parse_endpoint(value):
    host, separator, port_text = value.rpartition(":")
    if not separator or not host:
        raise argparse.ArgumentTypeError("Expected HOST:PORT")
    try:
        port = int(port_text)
    except ValueError as exc:
        raise argparse.ArgumentTypeError("Invalid port") from exc
    if not 1 <= port <= 65535:
        raise argparse.ArgumentTypeError("Port out of range")
    return host, port


async def main_async(args):
    http_host = args.host.strip()
    connect_host = args.connect.strip() if args.connect else http_host
    sni = args.sni.strip() if args.sni else connect_host

    for label, val in (("--host", http_host), ("--connect", connect_host), ("--sni", sni)):
        if not val or any(c in val for c in "/\\:\r\n "):
            raise ValueError(f"{label} must be a hostname or IPv4 without scheme or port")

    if not args.path.startswith("/") or any(c in args.path for c in "\r\n "):
        raise ValueError("--path must be an absolute HTTP path")

    listen_host, listen_port = args.listen
    port = args.port if args.port is not None else (80 if args.no_tls else 443)

    async def callback(reader, writer):
        await handle_local(
            reader,
            writer,
            connect_host,
            port,
            http_host,
            sni,
            args.path,
            not args.no_tls,
            insecure=args.insecure,
            max_inflight=args.inflight,
        )

    server = await asyncio.start_server(callback, listen_host, listen_port)
    scheme = "http" if args.no_tls else "https"
    print(
        f"[XH-CLIENT] Listening on {listen_host}:{listen_port}\n"
        f"  -> Upstream : {scheme}://{connect_host}:{port}{args.path}/<sid>/<seq>\n"
        f"  -> Host Hdr : {http_host}\n"
        f"  -> TLS SNI  : {sni}\n"
        f"  -> Inflight : {args.inflight}",
        flush=True,
    )
    async with server:
        await server.serve_forever()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--host", required=True, help="Cloud Run hostname (HTTP Host header)"
    )
    parser.add_argument(
        "--connect", default=None, help="Connect hostname/IP (Bughost / Edge)"
    )
    parser.add_argument(
        "--sni", default=None, help="TLS SNI hostname (defaults to --connect)"
    )
    parser.add_argument(
        "--port", type=int, default=None, help="Target port (defaults to 443 or 80)"
    )
    parser.add_argument(
        "--no-tls", action="store_true", help="Use plain HTTP instead of HTTPS"
    )
    parser.add_argument(
        "--insecure", action="store_true", help="Skip TLS certificate verification"
    )
    parser.add_argument(
        "--path", default="/kyouji/open-cxh", help="CXH endpoint base path"
    )
    parser.add_argument(
        "--inflight", type=int, default=2, help="Max concurrent in-flight POSTs (1-8)"
    )
    parser.add_argument(
        "--listen",
        type=parse_endpoint,
        default=parse_endpoint("127.0.0.1:10022"),
        help="Local TCP listener HOST:PORT",
    )

    args = parser.parse_args()
    if args.port is not None and not 1 <= args.port <= 65535:
        parser.error("--port must be between 1 and 65535")
    if not 1 <= args.inflight <= 8:
        parser.error("--inflight must be between 1 and 8")

    try:
        asyncio.run(main_async(args))
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()
