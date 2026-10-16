"""Custom POST/chunked SSH tunnel; not standard Xray XHTTP/SplitHTTP."""

import asyncio
import re

from config import BUF_SIZE, HANDSHAKE_TIMEOUT, HEADER_LIMIT, IDLE_TIMEOUT
from socket_utils import optimize_socket

CHUNK_LINE_LIMIT = 8192
MAX_CHUNK_SIZE = 16 * 1024 * 1024
_HEADER_NAME = re.compile(rb"[!#$%&'*+\-.^_`|~0-9A-Za-z]+\Z")
_FORBIDDEN_TRAILERS = {
    b"connection", b"content-length", b"host", b"trailer",
    b"transfer-encoding", b"upgrade",
}
# ======================
# HTTP RESPONSE HELPERS
# ======================
def _response(status, reason, extra_headers=b""):
    return (
        f"HTTP/1.1 {status} {reason}\r\n".encode("ascii")
        + extra_headers
        + b"Connection: close\r\nContent-Length: 0\r\n\r\n"
    )


async def _send_error(client_writer, status, reason, extra_headers=b""):
    if client_writer.is_closing():
        return
    try:
        client_writer.write(_response(status, reason, extra_headers))
        await asyncio.wait_for(client_writer.drain(), timeout=HANDSHAKE_TIMEOUT)
    except Exception:
        pass
# ========================
# CHUNKED REQUEST DECODER
# ========================
async def _copy_chunked_request_body(client_reader, ssh_writer):
    """Strip HTTP chunk framing and forward payload bytes to SSH."""
    trailer_bytes = 0

    while True:
        line = await asyncio.wait_for(
            client_reader.readline(), timeout=IDLE_TIMEOUT
        )
        if not line:
            raise ConnectionError("EOF while reading HTTP chunk size")
        if len(line) > CHUNK_LINE_LIMIT or not line.endswith(b"\r\n"):
            raise ValueError("Invalid or oversized HTTP chunk-size line")

        size_token = line[:-2].split(b";", 1)[0]
        if not size_token or any(
            c not in b"0123456789abcdefABCDEF" for c in size_token
        ):
            raise ValueError("Invalid HTTP chunk size")

        size = int(size_token, 16)
        if size > MAX_CHUNK_SIZE:
            raise ValueError("HTTP chunk size exceeds limit")

        # End of upload: validate trailers and half-close SSH upload.
        if size == 0:
            while True:
                trailer = await asyncio.wait_for(
                    client_reader.readline(), timeout=IDLE_TIMEOUT
                )
                if not trailer:
                    raise ConnectionError("EOF while reading HTTP trailers")

                trailer_bytes += len(trailer)
                if trailer_bytes > HEADER_LIMIT or not trailer.endswith(b"\r\n"):
                    raise ValueError("Invalid or oversized HTTP trailers")
                if trailer == b"\r\n":
                    if ssh_writer.is_closing():
                        return
                    if not ssh_writer.can_write_eof():
                        raise RuntimeError(
                            "SSH transport does not support TCP half-close"
                        )
                    ssh_writer.write_eof()
                    await asyncio.wait_for(
                        ssh_writer.drain(), timeout=IDLE_TIMEOUT
                    )
                    return

                name, sep, _value = trailer[:-2].partition(b":")
                if not sep or not _HEADER_NAME.fullmatch(name):
                    raise ValueError("Malformed HTTP trailer")
                if name.lower() in _FORBIDDEN_TRAILERS:
                    raise ValueError("Forbidden HTTP trailer field")

        # Forward payload only; HTTP chunk framing is discarded.
        remaining = size
        while remaining:
            data = await asyncio.wait_for(
                client_reader.readexactly(min(remaining, BUF_SIZE)),
                timeout=IDLE_TIMEOUT,
            )
            if ssh_writer.is_closing():
                raise ConnectionError("SSH upstream closed during upload")

            ssh_writer.write(data)
            await asyncio.wait_for(ssh_writer.drain(), timeout=IDLE_TIMEOUT)
            remaining -= len(data)

        ending = await asyncio.wait_for(
            client_reader.readexactly(2), timeout=IDLE_TIMEOUT
        )
        if ending != b"\r\n":
            raise ValueError("Invalid HTTP chunk terminator")


# === SSH RESPONSE ENCODER ===
async def _stream_ssh_response(ssh_reader, client_writer):
    """Encode SSH response bytes as HTTP chunked response data."""
    while True:
        data = await asyncio.wait_for(
            ssh_reader.read(BUF_SIZE), timeout=IDLE_TIMEOUT
        )
        if not data:
            break
        if client_writer.is_closing():
            return

        client_writer.write(f"{len(data):X}\r\n".encode("ascii") + data + b"\r\n")
        await asyncio.wait_for(client_writer.drain(), timeout=IDLE_TIMEOUT)

    if not client_writer.is_closing():
        client_writer.write(b"0\r\n\r\n")
        await asyncio.wait_for(client_writer.drain(), timeout=IDLE_TIMEOUT)


# === CUSTOM POST-BASED XH TRANSPORT ===
async def handle_xh(client_reader, client_writer, method, path, headers, cfg):
    """Handle one chunked POST tunnel to an SSH TCP upstream."""
    label = cfg.get("label", "XH")
    ssh_reader = ssh_writer = None
    upload_task = download_task = None
    response_started = False

    try:
        # Validate request framing before opening the upstream.
        if method.upper() != "POST":
            await _send_error(
                client_writer, 405, "Method Not Allowed", b"Allow: POST\r\n"
            )
            return
        if "content-length" in headers:
            await _send_error(client_writer, 400, "Bad Request")
            return
        if headers.get("transfer-encoding", "").strip().lower() != "chunked":
            await _send_error(client_writer, 411, "Length Required")
            return
        if not cfg.get("target_host"):
            await _send_error(client_writer, 502, "Bad Gateway")
            return

        # Connect to SSH upstream.
        try:
            ssh_reader, ssh_writer = await asyncio.wait_for(
                asyncio.open_connection(
                    cfg["target_host"], cfg["target_port"], limit=BUF_SIZE
                ),
                timeout=HANDSHAKE_TIMEOUT,
            )
        except Exception as exc:
            print(
                f"[BRIDGE:{label}] XH upstream connect failed: "
                f"{type(exc).__name__}: {exc}",
                flush=True,
            )
            await _send_error(client_writer, 502, "Bad Gateway")
            return

        optimize_socket(client_writer.get_extra_info("socket"))
        optimize_socket(ssh_writer.get_extra_info("socket"))

        # Mark response started before drain to prevent a second response.
        client_writer.write(
            b"HTTP/1.1 200 OK\r\n"
            b"Content-Type: application/octet-stream\r\n"
            b"Cache-Control: no-store\r\n"
            b"Transfer-Encoding: chunked\r\n"
            b"Connection: close\r\n\r\n"
        )
        response_started = True
        await asyncio.wait_for(client_writer.drain(), timeout=IDLE_TIMEOUT)

        print(
            f"[BRIDGE:{label}] XH {path} connected -> "
            f"{cfg['target_host']}:{cfg['target_port']}",
            flush=True,
        )

        # Full-duplex upload and download.
        upload_task = asyncio.create_task(
            _copy_chunked_request_body(client_reader, ssh_writer),
            name=f"xh-upload-{label}",
        )
        download_task = asyncio.create_task(
            _stream_ssh_response(ssh_reader, client_writer),
            name=f"xh-download-{label}",
        )

        while True:
            active = {
                task for task in (upload_task, download_task) if not task.done()
            }
            if not active:
                break

            done, _ = await asyncio.wait(
                active, return_when=asyncio.FIRST_COMPLETED
            )
            for task in done:
                if task.cancelled():
                    continue
                error = task.exception()
                if error is not None:
                    raise error

            # If SSH closes its response stream, stop the upload too.
            if download_task in done:
                break

    except asyncio.CancelledError:
        raise
    except Exception as exc:
        print(
            f"[BRIDGE:{label}] XH error: {type(exc).__name__}: {exc}",
            flush=True,
        )
        if not response_started:
            await _send_error(client_writer, 500, "Internal Server Error")

    finally:
        # Cancel remaining tasks before closing either connection.
        tasks = [task for task in (upload_task, download_task) if task is not None]
        for task in tasks:
            if not task.done():
                task.cancel()
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)

        # Close SSH and HTTP connections.
        for writer in (ssh_writer, client_writer):
            if writer is not None and not writer.is_closing():
                writer.close()

        await asyncio.gather(
            *(
                writer.wait_closed()
                for writer in (ssh_writer, client_writer)
                if writer is not None
            ),
            return_exceptions=True,
                    )
