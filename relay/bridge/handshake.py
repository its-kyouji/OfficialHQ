import asyncio
from email.utils import formatdate

from config import HANDSHAKE_TIMEOUT, HEADER_LIMIT

# =================
# HTTP HANDSHAKE
# =================
async def handle_handshake(client_reader, client_writer, label):
    raw = b""

    try:
        while b"\r\n\r\n" not in raw:
            chunk = await asyncio.wait_for(
                client_reader.read(4096),
                timeout=HANDSHAKE_TIMEOUT
            )

            if not chunk:
                client_writer.close()
                return None

            raw += chunk

            if len(raw) > HEADER_LIMIT:
                client_writer.close()
                return None

        header_end = raw.index(b"\r\n\r\n") + 4
        leftover = raw[header_end:]

        # Custom XH path detection
        req_line = raw.split(b"\r\n", 1)[0].split(b" ")
        req_path = (
            req_line[1].split(b"?", 1)[0]
            if len(req_line) > 1 else b""
        )
        is_xh = req_path.endswith(b"-xh")

        has_upgrade = (
            b"upgrade: websocket" in raw.lower()
            or b"upgrade: http/1.1" in raw.lower()
        )

        date_str = formatdate(
            timeval=None,
            localtime=False,
            usegmt=True
        )

        # Custom XH: HTTP 200 without Upgrade
        if is_xh and not has_upgrade:
            client_writer.write(
                b"HTTP/1.1 200 OK\r\n"
                b"Content-Type: application/octet-stream\r\n"
                b"Cache-Control: no-store\r\n"
                b"Date: " + date_str.encode() + b"\r\n"
                b"\r\n"
            )

            await client_writer.drain()
            return leftover

        # Existing Upgrade handshake
        if has_upgrade or is_xh:
            client_writer.write(
                b"HTTP/1.1 101 Switching Protocols\r\n"
                b"Upgrade: websocket\r\n"
                b"Connection: keep-alive, Upgrade\r\n"
                b"Date: " + date_str.encode() + b"\r\n"
                b"Server: Google Frontend\r\n"
                b"\r\n"
            )

            await client_writer.drain()
            return leftover

        client_writer.write(
            b"HTTP/1.1 403 Forbidden\r\n"
            b"Connection: close\r\n"
            b"\r\n"
        )

        await client_writer.drain()
        client_writer.close()
        return None

    except Exception as e:
        print(f"[BRIDGE:{label}] Handshake fail: {e}", flush=True)
        client_writer.close()
        return None
