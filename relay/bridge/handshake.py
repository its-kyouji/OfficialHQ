import asyncio
from email.utils import formatdate

from config import HANDSHAKE_TIMEOUT, HEADER_LIMIT


# ==========================
# HTTP REQUEST HEADER PARSER
# ==========================
async def read_request_headers(reader):
    """
    Read only the HTTP header block.

    StreamReader.readuntil() preserves any request-body bytes
    buffered after the header terminator.
    """
    raw = await asyncio.wait_for(
        reader.readuntil(b"\r\n\r\n"),
        timeout=HANDSHAKE_TIMEOUT,
    )

    if len(raw) > HEADER_LIMIT:
        raise ValueError("HTTP headers exceed HEADER_LIMIT")

    return raw


def parse_request_headers(raw):
    """
    Return:
        method, path, version, headers

    Header names are normalized to lowercase.
    """
    header_end = raw.find(b"\r\n\r\n")

    if header_end < 0:
        raise ValueError("Incomplete HTTP headers")

    header_block = raw[:header_end]
    lines = header_block.split(b"\r\n")

    if not lines:
        raise ValueError("Missing HTTP request line")

    try:
        request_line = lines[0].decode("ascii").split()

        if len(request_line) != 3:
            raise ValueError("Invalid HTTP request line")

        method, path, version = request_line

        if not version.startswith("HTTP/"):
            raise ValueError("Invalid HTTP version")

        headers = {}

        for line in lines[1:]:
            if not line:
                continue

            if b":" not in line:
                raise ValueError("Malformed HTTP header")

            name, value = line.split(b":", 1)

            name = name.decode("ascii").strip().lower()
            value = value.decode("latin-1").strip()

            if not name:
                raise ValueError("Empty HTTP header name")

            # Preserve duplicate values rather than silently
            # overwriting an earlier header.
            if name in headers:
                headers[name] += ", " + value
            else:
                headers[name] = value

        return method, path, version, headers

    except UnicodeDecodeError as exc:
        raise ValueError("Invalid HTTP header encoding") from exc


# ==========================
# HTTP HANDSHAKE
# ==========================
async def handle_handshake(
    client_reader,
    client_writer,
    label,
    raw=None,
):
    try:
        if raw is None:
            raw = await read_request_headers(client_reader)

        method, path, version, headers = parse_request_headers(raw)

        clean_path = path.split("?", 1)[0]

        is_xh = clean_path.endswith("-xh")

        # Parse the Upgrade header instead of relying on a
        # case-sensitive spacing pattern in the raw request.
        upgrade = headers.get("upgrade", "").strip().lower()

        connection_tokens = {
            token.strip().lower()
            for token in headers.get("connection", "").split(",")
        }

        has_upgrade = (
            bool(upgrade)
            and "upgrade" in connection_tokens
        )

        date_str = formatdate(
            timeval=None,
            localtime=False,
            usegmt=True,
        ).encode("ascii")

        # ==========================
        # LEGACY FAKE XH
        # ==========================
        # Preserve the existing non-Upgrade fake-XH response.
        # Real chunked POST XH requests should already have been
        # dispatched to handle_xh() by bridge.py.
        if is_xh and not has_upgrade:
            client_writer.write(
                b"HTTP/1.1 200 OK\r\n"
                b"Content-Type: application/octet-stream\r\n"
                b"Cache-Control: no-store\r\n"
                b"Date: " + date_str + b"\r\n"
                b"\r\n"
            )

            await client_writer.drain()

            # The request body, if already buffered, remains in
            # client_reader because readuntil() consumed only headers.
            return b""

        # ==========================
        # EXISTING UPGRADE HANDSHAKE
        # ==========================
        if has_upgrade or is_xh:
            client_writer.write(
                b"HTTP/1.1 101 Switching Protocols\r\n"
                b"Upgrade: websocket\r\n"
                b"Connection: keep-alive, Upgrade\r\n"
                b"Date: " + date_str + b"\r\n"
                b"Server: Google Frontend\r\n"
                b"\r\n"
            )

            await client_writer.drain()
            return b""

        # ==========================
        # REJECT UNKNOWN REQUESTS
        # ==========================
        client_writer.write(
            b"HTTP/1.1 403 Forbidden\r\n"
            b"Connection: close\r\n"
            b"Content-Length: 0\r\n"
            b"\r\n"
        )

        await client_writer.drain()
        client_writer.close()

        try:
            await client_writer.wait_closed()
        except Exception:
            pass

        return None

    except asyncio.CancelledError:
        raise

    except Exception as exc:
        print(
            f"[BRIDGE:{label}] Handshake fail: "
            f"{type(exc).__name__}: {exc}",
            flush=True,
        )

        if not client_writer.is_closing():
            client_writer.close()

        try:
            await client_writer.wait_closed()
        except Exception:
            pass

        return None
