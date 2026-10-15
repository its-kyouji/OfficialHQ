#!/usr/bin/env python3

import asyncio

from config import (
    LISTEN_PORT,
    BACKEND_HOST,
    BACKEND_PORT,
    BUF_SIZE,
    HEADER_LIMIT,
    HEADER_TIMEOUT,
    READ_TIMEOUT,
    BACKEND_TIMEOUT,
    H2C_PREFACE,
    BASE_PREFIX,
    BAN_THRESHOLD,
    BAN_TIME,
)

from routing import (
    validate_path,
    resolve_backend,
)

from security import (
    get_client_ip,
    is_banned,
)

from socket_utils import optimize_socket
from relay import relay_stream

# ==========================
# CLIENT CONNECTION HANDLER
# ==========================
async def handle_client(reader, writer):
    # Get remote peer address
    peer = writer.get_extra_info("peername")

    # Set fallback client IP
    fallback_ip = (
        peer[0]
        if peer
        else "127.0.0.1"
    )

    client_ip = fallback_ip
    backend_reader = None
    backend_writer = None
    target_port = BACKEND_PORT

    try:
        # Read initial client payload
        raw = await asyncio.wait_for(
            reader.read(65536),
            timeout=HEADER_TIMEOUT
        )

        if not raw:
            raise ValueError("Empty payload")

        # ===============
        # H2C CONNECTION
        # ===============
        if raw.startswith(H2C_PREFACE):
            client_ip = fallback_ip

        # ================
        # HTTP CONNECTION
        # ================
        else:
            # Read until the HTTP headers are complete
            while b"\r\n\r\n" not in raw:
                more = await asyncio.wait_for(
                    reader.read(65536),
                    timeout=READ_TIMEOUT
                )

                if not more:
                    raise ValueError("Incomplete header")

                raw += more

                if len(raw) > HEADER_LIMIT:
                    raise ValueError("Header too large")

            header_end = raw.find(b"\r\n\r\n")
            lines = raw[:header_end].split(b"\r\n")

            if not lines:
                raise ValueError("Malformed request")

            # ==========
            # CLIENT IP
            # ==========
            client_ip = get_client_ip(
                lines[1:],
                fallback_ip
            )

            if is_banned(
                client_ip,
                BAN_THRESHOLD,
                BAN_TIME
            ):
                writer.close()
                return

            # =================
            # PATH VALIDATION
            # =================
            request_line = lines[0].split(b" ")

            if len(request_line) < 2:
                raise ValueError("Malformed request line")

            request_path = request_line[1].split(b"?", 1)[0]

            if not validate_path(request_path):
                print(
                    "[FORTRESS] Rejected Path: "
                    f"{request_path.decode(errors='ignore')} "
                    f"IP={client_ip}",
                    flush=True
                )

                # Return HTTP 403 for invalid paths
                writer.write(
                    b"HTTP/1.1 403 Forbidden\r\n"
                    b"Connection: close\r\n"
                    b"\r\n"
                )

                await writer.drain()
                writer.close()
                return

            # =================
            # RAW PATH ROUTING
            # =================
            target_port = resolve_backend(
                request_path,
                BACKEND_PORT
            )

        # ==================
        # BACKEND CONNECTION
        # ==================
        backend_reader, backend_writer = (
            # Connect to the selected backend
            await asyncio.wait_for(
                asyncio.open_connection(
                    BACKEND_HOST,
                    target_port
                ),
                timeout=BACKEND_TIMEOUT
            )
        )

        # Optimize client socket
        optimize_socket(
            writer.get_extra_info("socket")
        )

        # Optimize backend socket
        optimize_socket(
            backend_writer.get_extra_info("socket")
        )

        # Forward original client payload
        backend_writer.write(raw)
        await backend_writer.drain()

        # =================
        # FULL-DUPLEX RELAY
        # =================
        await asyncio.gather(
            # Client to backend
            relay_stream(reader, backend_writer),
            # Backend to client
            relay_stream(backend_reader, writer),
            return_exceptions=True
        )

    # Handle connection timeouts
    except asyncio.TimeoutError:
        print(
            f"[FORTRESS] Timeout IP={client_ip}",
            flush=True
        )

    # Handle other connection errors
    except Exception as e:
        print(
            f"[FORTRESS] Dropped "
            f"IP={client_ip} "
            f"ERR={type(e).__name__}: {e}",
            flush=True
        )

    finally:
        # Close backend connection
        if backend_writer:
            backend_writer.close()

        # Close client connection
        writer.close()


# ===============
# FORTRESS SERVER
# ===============
async def main():
    # Start server listener
    server = await asyncio.start_server(
        handle_client,
        "0.0.0.0",
        LISTEN_PORT,
        limit=BUF_SIZE,
        backlog=1024
    )

    print(
        f"[FORTRESS] Shield Active "
        f"{LISTEN_PORT} -> "
        f"{BACKEND_HOST}:{BACKEND_PORT}",
        flush=True
    )

    async with server:
        await server.serve_forever()

# ====================
# CONTAINER ENTRYPOINT
# ====================
if __name__ == "__main__":
    import gc

    # Set garbage collection thresholds
    gc.set_threshold(50_000, 500, 100)

    try:
        import uvloop

        asyncio.set_event_loop_policy(
            uvloop.EventLoopPolicy()
        )
        print("[FORTRESS] uvloop ACTIVE", flush=True)

    except ImportError:
        print(
            "[FORTRESS] uvloop MISSING (asyncio fallback)",
            flush=True
        )

    asyncio.run(main())
