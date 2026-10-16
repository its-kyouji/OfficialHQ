#!/usr/bin/env python3
"""Experimental TCP-to-HTTP/1.1 chunked POST XH adapter.

Cloud Run example:
    python3 tools/xh_client.py \
        --host SERVICE-HASH.us-central1.run.app \
        --path /kyouji/open-xh \
        --listen 127.0.0.1:10022

Local HTTP bridge example:
    python3 tools/xh_client.py \
        --host 127.0.0.1 \
        --port 2223 \
        --no-tls \
        --path /kyouji/open-xh \
        --listen 127.0.0.1:10022

Full-duplex behavior must be verified through the actual Cloud Run path.
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
        raise ValueError(
            "Invalid HTTP response status"
        ) from exc

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

    return status, headers


async def local_to_http(local_reader, http_writer):
    """Encode local TCP bytes as HTTP chunked request data."""
    while True:
        data = await local_reader.read(READ_SIZE)

        if not data:
            break

        http_writer.write(
            f"{len(data):X}\r\n".encode("ascii")
            + data
            + b"\r\n"
        )

        await http_writer.drain()

    # Finish the request body. The response reader remains active.
    http_writer.write(b"0\r\n\r\n")
    await http_writer.drain()


async def http_to_local(http_reader, local_writer):
    """Decode the chunked HTTP response into local TCP bytes."""
    status, headers = await read_headers(http_reader)

    if status != 200:
        raise ConnectionError(
            f"XH server returned HTTP {status}"
        )

    if "chunked" not in headers.get(
        "transfer-encoding",
        "",
    ).lower():
        raise ValueError("XH response is not chunked")

    while True:
        line = await http_reader.readline()

        if (
            not line
            or len(line) > CHUNK_LINE_LIMIT
            or not line.endswith(b"\r\n")
        ):
            raise ValueError(
                "Invalid HTTP response chunk header"
            )

        token = line[:-2].split(b";", 1)[0].strip()

        try:
            size = int(token, 16)
        except ValueError as exc:
            raise ValueError(
                "Invalid HTTP response chunk size"
            ) from exc

        if size < 0:
            raise ValueError("Negative HTTP chunk size")

        if size == 0:
            # Consume response trailers.
            trailer_bytes = 0

            while True:
                trailer = await http_reader.readline()

                if not trailer:
                    raise ConnectionError(
                        "EOF in HTTP response trailers"
                    )

                trailer_bytes += len(trailer)

                if trailer_bytes > HEADER_LIMIT:
                    raise ValueError(
                        "HTTP response trailers too large"
                    )

                if trailer == b"\r\n":
                    return

                if b":" not in trailer:
                    raise ValueError(
                        "Invalid HTTP response trailer"
                    )

        remaining = size

        while remaining:
            data = await http_reader.readexactly(
                min(remaining, READ_SIZE)
            )

            local_writer.write(data)
            await local_writer.drain()

            remaining -= len(data)

        if await http_reader.readexactly(2) != b"\r\n":
            raise ValueError(
                "Invalid HTTP response chunk terminator"
            )


async def handle_local(
    local_reader,
    local_writer,
    host,
    port,
    path,
    use_tls,
):
    http_writer = None
    upload_task = None
    download_task = None

    try:
        tls_context = (
            ssl.create_default_context()
            if use_tls
            else None
        )

        connect_options = {
            "host": host,
            "port": port,
            "limit": 262144,
        }

        if tls_context is not None:
            connect_options["ssl"] = tls_context
            connect_options["server_hostname"] = host

        http_reader, http_writer = await asyncio.open_connection(
            **connect_options
        )

        request = (
            f"POST {path} HTTP/1.1\r\n"
            f"Host: {host}\r\n"
            "User-Agent: OfficialHQ-XH/0.1\r\n"
            "Content-Type: application/octet-stream\r\n"
            "Transfer-Encoding: chunked\r\n"
            "Connection: close\r\n"
            "\r\n"
        )

        http_writer.write(request.encode("ascii"))
        await http_writer.drain()

        # Upload and download run concurrently.
        upload_task = asyncio.create_task(
            local_to_http(local_reader, http_writer),
            name="xh-client-upload",
        )

        download_task = asyncio.create_task(
            http_to_local(http_reader, local_writer),
            name="xh-client-download",
        )

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

            # End of the HTTP response means the tunnel has closed.
            if download_task in done:
                break

    except asyncio.CancelledError:
        raise

    except Exception as exc:
        print(
            f"[XH-CLIENT] {type(exc).__name__}: {exc}",
            flush=True,
        )

    finally:
        for task in (upload_task, download_task):
            if task is not None and not task.done():
                task.cancel()

        tasks = [
            task
            for task in (upload_task, download_task)
            if task is not None
        ]

        if tasks:
            await asyncio.gather(
                *tasks,
                return_exceptions=True,
            )

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
        raise argparse.ArgumentTypeError(
            "Expected HOST:PORT"
        )

    try:
        port = int(port_text)
    except ValueError as exc:
        raise argparse.ArgumentTypeError(
            "Invalid port"
        ) from exc

    if not 1 <= port <= 65535:
        raise argparse.ArgumentTypeError(
            "Port out of range"
        )

    return host, port


async def main_async(args):
    host = args.host.strip()

    if not host or any(
        char in host
        for char in "/\\:\r\n "
    ):
        raise ValueError(
            "--host must be a hostname or IPv4 address, "
            "without scheme or port"
        )

    if (
        not args.path.startswith("/")
        or any(char in args.path for char in "\r\n ")
    ):
        raise ValueError(
            "--path must be an absolute HTTP path "
            "without spaces or newlines"
        )

    listen_host, listen_port = args.listen

    port = (
        args.port
        if args.port is not None
        else (80 if args.no_tls else 443)
    )

    async def callback(reader, writer):
        await handle_local(
            reader,
            writer,
            host,
            port,
            args.path,
            not args.no_tls,
        )

    server = await asyncio.start_server(
        callback,
        listen_host,
        listen_port,
    )

    scheme = "http" if args.no_tls else "https"

    print(
        f"[XH-CLIENT] Listening on "
        f"{listen_host}:{listen_port}; "
        f"upstream={scheme}://{host}:{port}{args.path}",
        flush=True,
    )

    async with server:
        await server.serve_forever()


def main():
    parser = argparse.ArgumentParser(
        description=__doc__
    )

    parser.add_argument(
        "--host",
        required=True,
        help="Hostname/IP only, no scheme or port",
    )

    parser.add_argument(
        "--port",
        type=int,
        default=None,
        help="HTTP port; defaults to 443 with TLS or 80 without TLS",
    )

    parser.add_argument(
        "--no-tls",
        action="store_true",
        help="Use plain HTTP instead of HTTPS",
    )

    parser.add_argument(
        "--path",
        default="/kyouji/open-xh",
        help="XH endpoint path",
    )

    parser.add_argument(
        "--listen",
        type=parse_endpoint,
        default=parse_endpoint("127.0.0.1:10022"),
        help="Local TCP listener HOST:PORT",
    )

    args = parser.parse_args()

    if args.port is not None and not 1 <= args.port <= 65535:
        parser.error(
            "--port must be between 1 and 65535"
        )

    try:
        asyncio.run(main_async(args))
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()
