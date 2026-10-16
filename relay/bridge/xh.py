"""
Experimental custom POST-based XH transport for an SSH TCP upstream.

Uses HTTP/1.1 chunked request and response bodies.

IMPORTANT:
Full-duplex behavior must be verified through the actual Cloud Run
ingress before production use.
"""

import asyncio

from config import BUF_SIZE, HEADER_LIMIT, IDLE_TIMEOUT
from socket_utils import optimize_socket


CHUNK_LINE_LIMIT = 8192
MAX_CHUNK_SIZE = 16 * 1024 * 1024


# ==========================
# HTTP RESPONSE HELPERS
# ==========================
def _response(status, reason, extra_headers=b""):
    return (
        f"HTTP/1.1 {status} {reason}\r\n".encode("ascii")
        + extra_headers
        + b"Connection: close\r\n"
        + b"Content-Length: 0\r\n"
        + b"\r\n"
    )


async def _send_error(client_writer, status, reason, extra_headers=b""):
    if client_writer.is_closing():
        return

    client_writer.write(
        _response(status, reason, extra_headers)
    )

    try:
        await asyncio.wait_for(
            client_writer.drain(),
            timeout=IDLE_TIMEOUT,
        )
    except Exception:
        pass


# ==========================
# CHUNKED REQUEST DECODER
# ==========================
async def _copy_chunked_request_body(
    client_reader,
    ssh_writer,
):
    """
    Decode HTTP chunked request framing and forward only payload
    bytes to the SSH upstream.

    Chunk-size lines, CRLF delimiters, and trailers are not forwarded.
    """

    trailer_bytes = 0

    while True:
        line = await asyncio.wait_for(
            client_reader.readline(),
            timeout=IDLE_TIMEOUT,
        )

        if not line:
            raise ConnectionError(
                "EOF while reading HTTP chunk size"
            )

        if len(line) > CHUNK_LINE_LIMIT:
            raise ValueError(
                "HTTP chunk-size line too large"
            )

        if not line.endswith(b"\r\n"):
            raise ValueError(
                "Malformed HTTP chunk-size line"
            )

        size_token = line[:-2].split(b";", 1)[0].strip()

        if not size_token:
            raise ValueError(
                "Empty HTTP chunk size"
            )

        try:
            size = int(size_token, 16)
        except ValueError as exc:
            raise ValueError(
                "Invalid HTTP chunk size"
            ) from exc

        if size < 0 or size > MAX_CHUNK_SIZE:
            raise ValueError(
                "HTTP chunk size out of allowed range"
            )

        # ==========================
        # END OF REQUEST BODY
        # ==========================
        if size == 0:
            while True:
                trailer = await asyncio.wait_for(
                    client_reader.readline(),
                    timeout=IDLE_TIMEOUT,
                )

                if not trailer:
                    raise ConnectionError(
                        "EOF while reading HTTP trailers"
                    )

                trailer_bytes += len(trailer)

                if trailer_bytes > HEADER_LIMIT:
                    raise ValueError(
                        "HTTP trailers too large"
                    )

                if not trailer.endswith(b"\r\n"):
                    raise ValueError(
                        "Malformed HTTP trailer"
                    )

                if trailer == b"\r\n":
                    # Signal upload EOF to the SSH server while
                    # allowing the SSH response stream to continue.
                    if not ssh_writer.is_closing():
                        try:
                            if ssh_writer.can_write_eof():
                                ssh_writer.write_eof()

                                await asyncio.wait_for(
                                    ssh_writer.drain(),
                                    timeout=IDLE_TIMEOUT,
                                )
                        except (
                            OSError,
                            RuntimeError,
                            AttributeError,
                            asyncio.TimeoutError,
                        ):
                            pass

                    return

                if b":" not in trailer:
                    raise ValueError(
                        "Malformed HTTP trailer"
                    )

        # ==========================
        # FORWARD CHUNK PAYLOAD
        # ==========================
        remaining = size

        while remaining > 0:
            data = await asyncio.wait_for(
                client_reader.readexactly(
                    min(remaining, BUF_SIZE)
                ),
                timeout=IDLE_TIMEOUT,
            )

            if ssh_writer.is_closing():
                raise ConnectionError(
                    "SSH upstream closed during upload"
                )

            ssh_writer.write(data)

            await asyncio.wait_for(
                ssh_writer.drain(),
                timeout=IDLE_TIMEOUT,
            )

            remaining -= len(data)

        # Each non-final chunk must end with CRLF.
        ending = await asyncio.wait_for(
            client_reader.readexactly(2),
            timeout=IDLE_TIMEOUT,
        )

        if ending != b"\r\n":
            raise ValueError(
                "Invalid HTTP chunk terminator"
            )


# ==========================
# SSH RESPONSE ENCODER
# ==========================
async def _stream_ssh_response(
    ssh_reader,
    client_writer,
):
    """
    Encode SSH response bytes as HTTP/1.1 chunked response data.
    """

    while True:
        data = await asyncio.wait_for(
            ssh_reader.read(BUF_SIZE),
            timeout=IDLE_TIMEOUT,
        )

        if not data:
            break

        if client_writer.is_closing():
            return

        client_writer.write(
            f"{len(data):X}\r\n".encode("ascii")
            + data
            + b"\r\n"
        )

        await asyncio.wait_for(
            client_writer.drain(),
            timeout=IDLE_TIMEOUT,
        )

    # HTTP terminating chunk.
    if not client_writer.is_closing():
        client_writer.write(b"0\r\n\r\n")

        await asyncio.wait_for(
            client_writer.drain(),
            timeout=IDLE_TIMEOUT,
        )


# ==========================
# CUSTOM XH TRANSPORT
# ==========================
async def handle_xh(
    client_reader,
    client_writer,
    method,
    path,
    headers,
    cfg,
):
    """
    Handle a chunked POST request to an -xh endpoint.

    This handler is independent of the existing WS/HU raw relay.
    """

    label = cfg.get("label", "XH")

    ssh_reader = None
    ssh_writer = None

    upload_task = None
    download_task = None

    response_started = False

    try:
        # ==========================
        # VALIDATE REQUEST
        # ==========================
        if method.upper() != "POST":
            await _send_error(
                client_writer,
                405,
                "Method Not Allowed",
                b"Allow: POST\r\n",
            )
            return

        transfer_encoding = headers.get(
            "transfer-encoding",
            "",
        ).strip().lower()

        content_length = headers.get("content-length")

        # Do not accept ambiguous HTTP request framing.
        if content_length is not None:
            await _send_error(
                client_writer,
                400,
                "Bad Request",
            )
            return

        if transfer_encoding != "chunked":
            await _send_error(
                client_writer,
                411,
                "Length Required",
            )
            return

        if not cfg.get("target_host"):
            await _send_error(
                client_writer,
                502,
                "Bad Gateway",
            )
            return

        # ==========================
        # CONNECT TO SSH UPSTREAM
        # ==========================
        try:
            ssh_reader, ssh_writer = await asyncio.wait_for(
                asyncio.open_connection(
                    cfg["target_host"],
                    cfg["target_port"],
                    limit=BUF_SIZE,
                ),
                timeout=IDLE_TIMEOUT,
            )

        except Exception as exc:
            print(
                f"[BRIDGE:{label}] XH upstream connect failed: "
                f"{type(exc).__name__}: {exc}",
                flush=True,
            )

            await _send_error(
                client_writer,
                502,
                "Bad Gateway",
            )
            return

        # Match the socket optimization used by the existing bridge.
        optimize_socket(
            client_writer.get_extra_info("socket")
        )

        optimize_socket(
            ssh_writer.get_extra_info("socket")
        )

        # ==========================
        # START HTTP RESPONSE
        # ==========================
        client_writer.write(
            b"HTTP/1.1 200 OK\r\n"
            b"Content-Type: application/octet-stream\r\n"
            b"Cache-Control: no-store\r\n"
            b"Transfer-Encoding: chunked\r\n"
            b"Connection: close\r\n"
            b"\r\n"
        )

        await asyncio.wait_for(
            client_writer.drain(),
            timeout=IDLE_TIMEOUT,
        )

        response_started = True

        print(
            f"[BRIDGE:{label}] XH {path} connected -> "
            f"{cfg['target_host']}:{cfg['target_port']}",
            flush=True,
        )

        # ==========================
        # BIDIRECTIONAL STREAMING
        # ==========================
        upload_task = asyncio.create_task(
            _copy_chunked_request_body(
                client_reader,
                ssh_writer,
            ),
            name=f"xh-upload-{label}",
        )

        download_task = asyncio.create_task(
            _stream_ssh_response(
                ssh_reader,
                client_writer,
            ),
            name=f"xh-download-{label}",
        )

        # Upload completion does not end the response stream.
        # Keep downloading SSH output until the upstream closes,
        # an error occurs, or the configured idle timeout expires.
        while True:
            active = {
                task
                for task in (
                    upload_task,
                    download_task,
                )
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

            # SSH output has ended. The tunnel is no longer useful.
            if download_task in done:
                break

    except asyncio.CancelledError:
        raise

    except Exception as exc:
        print(
            f"[BRIDGE:{label}] XH error: "
            f"{type(exc).__name__}: {exc}",
            flush=True,
        )

        # Do not attempt a second HTTP response after the 200 headers
        # have already been sent.
        if not response_started:
            await _send_error(
                client_writer,
                400,
                "Bad Request",
            )

    finally:
        # ==========================
        # CANCEL STREAM TASKS
        # ==========================
        for task in (
            upload_task,
            download_task,
        ):
            if task is not None and not task.done():
                task.cancel()

        tasks = [
            task
            for task in (
                upload_task,
                download_task,
            )
            if task is not None
        ]

        if tasks:
            await asyncio.gather(
                *tasks,
                return_exceptions=True,
            )

        # ==========================
        # CLOSE SSH CONNECTION
        # ==========================
        if ssh_writer is not None:
            if not ssh_writer.is_closing():
                ssh_writer.close()

            try:
                await ssh_writer.wait_closed()
            except Exception:
                pass

        # ==========================
        # CLOSE HTTP CONNECTION
        # ==========================
        if not client_writer.is_closing():
            client_writer.close()

        try:
            await client_writer.wait_closed()
        except Exception:
            pass
