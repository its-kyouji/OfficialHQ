#!/usr/bin/env python3
"""Upgraded TCP-to-HTTP/1.1 chunked POST CXH Client Shim.
Includes Domain Fronting support, ALPN http/1.1 enforcement, and proper half-close task loop.
"""

import argparse
import asyncio
import ssl

READ_SIZE = 65536
HEADER_LIMIT = 16384
CHUNK_LINE_LIMIT = 8192


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


async def local_to_http(local_reader, http_writer):
    """Encode local TCP bytes as HTTP chunked request data."""
    first_chunk = True
    while True:
        data = await local_reader.read(READ_SIZE)
        if not data:
            break

        if first_chunk:
            print(f"[XH-CLIENT] Sending first SSH chunk ({len(data)} bytes): {data[:30]!r}", flush=True)
            first_chunk = False

        http_writer.write(
            f"{len(data):X}\r\n".encode("ascii") + data + b"\r\n"
        )
        await http_writer.drain()

    print("[XH-CLIENT] Local TCP EOF reached, sending terminal 0 chunk.", flush=True)
    http_writer.write(b"0\r\n\r\n")
    await http_writer.drain()


async def http_to_local(http_reader, local_writer):
    """Decode the chunked HTTP response into local TCP bytes."""
    status, headers, status_line = await read_headers(http_reader)
    print(f"[XH-CLIENT] Server Response: {status_line} | Headers: {headers}", flush=True)

    if status != 200:
        raise ConnectionError(f"XH server returned HTTP {status} ({status_line})")

    if "chunked" not in headers.get("transfer-encoding", "").lower():
        raise ValueError("XH response is not chunked")

    first_resp = True
    while True:
        line = await http_reader.readline()
        if not line or len(line) > CHUNK_LINE_LIMIT or not line.endswith(b"\r\n"):
            raise ValueError(f"Invalid HTTP response chunk header: {line[:50]!r}")

        token = line[:-2].split(b";", 1)[0].strip()
        try:
            size = int(token, 16)
        except ValueError as exc:
            raise ValueError(f"Invalid HTTP response chunk size: {token!r}") from exc

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
                    return
                if b":" not in trailer:
                    raise ValueError("Invalid HTTP response trailer")

        remaining = size
        while remaining:
            data = await http_reader.readexactly(min(remaining, READ_SIZE))
            if first_resp:
                print(f"[XH-CLIENT] Received first downstream chunk ({len(data)} bytes): {data[:30]!r}", flush=True)
                first_resp = False
            local_writer.write(data)
            await local_writer.drain()
            remaining -= len(data)

        if await http_reader.readexactly(2) != b"\r\n":
            raise ValueError("Invalid HTTP response chunk terminator")


async def handle_local(
    local_reader, local_writer, connect_host, port, http_host, sni, path, use_tls, insecure
):
    http_writer = None
    upload_task = None
    download_task = None

    try:
        tls_context = None
        if use_tls:
            tls_context = ssl.create_default_context()
            tls_context.set_alpn_protocols(["http/1.1"])
            if insecure:
                tls_context.check_hostname = False
                tls_context.verify_mode = ssl.CERT_NONE

        connect_options = {
            "host": connect_host,
            "port": port,
            "limit": 262144,
        }

        if tls_context is not None:
            connect_options["ssl"] = tls_context
            connect_options["server_hostname"] = sni

        print(f"[XH-CLIENT] Connecting to {connect_host}:{port} (SNI: {sni})...", flush=True)
        http_reader, http_writer = await asyncio.open_connection(**connect_options)

        request = (
            f"POST {path} HTTP/1.1\r\n"
            f"Host: {http_host}\r\n"
            "User-Agent: OfficialHQ-XH/0.1\r\n"
            "Content-Type: application/octet-stream\r\n"
            "Transfer-Encoding: chunked\r\n"
            "Connection: close\r\n"
            "\r\n"
        )

        http_writer.write(request.encode("ascii"))
        await http_writer.drain()

        upload_task = asyncio.create_task(
            local_to_http(local_reader, http_writer),
            name="xh-client-upload",
        )
        download_task = asyncio.create_task(
            http_to_local(http_reader, local_writer),
            name="xh-client-download",
        )

        # Retained proper task lifecycle loop from original OfficialHQ xh_client.py
        while True:
            active = {
                task
                for task in (upload_task, download_task)
                if task is not None and not task.done()
            }
            if not active:
                break

            done, _ = await asyncio.wait(
                active,
                return_when=asyncio.FIRST_COMPLETED,
            )

            for task in done:
                error = task.exception()
                if error is not None:
                    raise error

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

        tasks = [task for task in (upload_task, download_task) if task is not None]
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)

        if http_writer is not None:
            http_writer.close()
            try:
                await http_writer.wait_closed()
            except Exception:
                pass

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
    listen_host, listen_port = args.listen
    port = args.port if args.port is not None else (80 if args.no_tls else 443)

    async def callback(reader, writer):
        await handle_local(
            reader, writer, connect_host, port, http_host, sni, args.path, not args.no_tls, args.insecure
        )

    server = await asyncio.start_server(callback, listen_host, listen_port)
    scheme = "http" if args.no_tls else "https"
    print(
        f"[XH-CLIENT] Listening on {listen_host}:{listen_port}\n"
        f"  -> Upstream : {scheme}://{connect_host}:{port}{args.path}\n"
        f"  -> Host Hdr : {http_host}\n"
        f"  -> TLS SNI  : {sni}",
        flush=True,
    )
    async with server:
        await server.serve_forever()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--host", required=True, help="Cloud Run Host header (no scheme/port)")
    parser.add_argument("--connect", default=None, help="Connect hostname/IP (Bughost)")
    parser.add_argument("--sni", default=None, help="TLS SNI hostname (defaults to --connect)")
    parser.add_argument("--port", type=int, default=None, help="Target port (443 or 80)")
    parser.add_argument("--no-tls", action="store_true", help="Use plain HTTP")
    parser.add_argument("--insecure", action="store_true", help="Disable TLS cert verification")
    parser.add_argument("--path", default="/kyouji/open-cxh", help="CXH endpoint path")
    parser.add_argument("--listen", type=parse_endpoint, default=parse_endpoint("127.0.0.1:10022"))
    args = parser.parse_args()

    try:
        asyncio.run(main_async(args))
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()
