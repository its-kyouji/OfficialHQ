#!/usr/bin/env python3

import asyncio
import json
import sys

from config import BUF_SIZE
from socket_utils import optimize_socket
from relay import relay
from handshake import (
    handle_handshake,
    read_request_headers,
    parse_request_headers,
)
from xh import handle_xh


# ==========================
# CLIENT CONNECTION HANDLER
# ==========================
async def handle_client(client_reader, client_writer, cfg):
    label = cfg.get("label", "UNKNOWN")
    raw_headers = None

    try:
        # Read HTTP headers once.
        # Any body bytes already buffered remain available to the handler.
        raw_headers = await read_request_headers(client_reader)

        method, path, _version, headers = parse_request_headers(
            raw_headers
        )

        clean_path = path.split("?", 1)[0]

        # Dispatch ONLY valid candidates for the custom POST-based XH
        # transport. All other requests retain the legacy handshake path.
        is_xh = (
            clean_path.endswith("-xh")
            and method.upper() == "POST"
            and "upgrade" not in headers
            and headers.get(
                "transfer-encoding",
                ""
            ).strip().lower() == "chunked"
        )

        if is_xh:
            await handle_xh(
                client_reader,
                client_writer,
                method,
                clean_path,
                headers,
                cfg,
            )
            return

    except asyncio.CancelledError:
        raise

    except Exception as exc:
        print(
            f"[BRIDGE:{label}] HTTP header error: "
            f"{type(exc).__name__}: {exc}",
            flush=True,
        )

        if not client_writer.is_closing():
            client_writer.close()

            try:
                await client_writer.wait_closed()
            except Exception:
                pass

        return

    # Existing WS/HU and fake-XH handshake behavior.
    # The raw header block was already consumed, so pass it to avoid
    # attempting to read the same request headers a second time.
    leftover = await handle_handshake(
        client_reader,
        client_writer,
        label,
        raw=raw_headers,
    )

    if leftover is None:
        return

    # ==========================
    # CONNECT TO SSH UPSTREAM
    # ==========================
    try:
        ssh_reader, ssh_writer = await asyncio.open_connection(
            cfg["target_host"],
            cfg["target_port"],
            limit=BUF_SIZE,
        )

    except Exception as exc:
        print(
            f"[BRIDGE:{label}] Upstream fail "
            f"{cfg['target_host']}:{cfg['target_port']}: {exc}",
            flush=True,
        )

        client_writer.close()

        try:
            await client_writer.wait_closed()
        except Exception:
            pass

        return

    # Keep socket optimization for the existing raw-relay transport.
    optimize_socket(
        client_writer.get_extra_info("socket")
    )
    optimize_socket(
        ssh_writer.get_extra_info("socket")
    )

    # Forward any payload bytes returned by the legacy handshake.
    if leftover:
        ssh_writer.write(leftover)
        await ssh_writer.drain()

    # ==========================
    # BIDIRECTIONAL RAW RELAY
    # ==========================
    try:
        await asyncio.gather(
            relay(
                client_reader,
                ssh_writer,
                label=f"client->{label}",
            ),
            relay(
                ssh_reader,
                client_writer,
                label=f"{label}->client",
            ),
            return_exceptions=True,
        )

    finally:
        for writer in (ssh_writer, client_writer):
            if not writer.is_closing():
                writer.close()

        await asyncio.gather(
            ssh_writer.wait_closed(),
            client_writer.wait_closed(),
            return_exceptions=True,
        )


# ==========================
# BRIDGE SERVER
# ==========================
async def main():
    if len(sys.argv) < 2:
        print(
            "[BRIDGE] FATAL: Missing JSON config path",
            flush=True,
        )
        sys.exit(1)

    json_path = sys.argv[1]

    try:
        with open(json_path, "r") as file:
            cfg = json.load(file)

    except Exception as exc:
        print(
            f"[BRIDGE] FATAL: bad config {json_path}: {exc}",
            flush=True,
        )
        sys.exit(1)

    if not cfg.get("target_host"):
        print(
            f"[BRIDGE:{cfg.get('label', '?')}] "
            "disabled (empty target_host)",
            flush=True,
        )
        sys.exit(0)

    server = await asyncio.start_server(
        lambda reader, writer: handle_client(
            reader,
            writer,
            cfg,
        ),
        "127.0.0.1",
        cfg["listen_port"],
        limit=BUF_SIZE,
    )

    print(
        f"[BRIDGE:{cfg['label']}] Listening on "
        f"127.0.0.1:{cfg['listen_port']} -> "
        f"{cfg['target_host']}:{cfg['target_port']}",
        flush=True,
    )

    async with server:
        await server.serve_forever()


# =====================
# CONTAINER ENTRYPOINT
# =====================
if __name__ == "__main__":
    try:
        import uvloop

        uvloop.install()

        print(
            f"[BRIDGE] uvloop ACTIVE ({sys.argv[1:]})",
            flush=True,
        )

    except ImportError:
        print(
            f"[BRIDGE] uvloop MISSING "
            f"(asyncio fallback) ({sys.argv[1:]})",
            flush=True,
        )

    asyncio.run(main())
