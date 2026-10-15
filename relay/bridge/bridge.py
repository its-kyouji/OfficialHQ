#!/usr/bin/env python3

import asyncio
import json
import sys

from config import BUF_SIZE
from socket_utils import optimize_socket
from relay import relay
from handshake import handle_handshake
# ==========================
# CLIENT CONNECTION HANDLER
# ==========================
async def handle_client(client_reader, client_writer, cfg):
    label = cfg.get("label", "UNKNOWN")

    # Process the client handshake
    leftover = await handle_handshake(
        client_reader,
        client_writer,
        label
    )

    if leftover is None:
        return

    # Connect to the configured upstream
    try:
        ssh_reader, ssh_writer = await asyncio.open_connection(
            cfg["target_host"],
            cfg["target_port"],
            limit=BUF_SIZE
        )

    except Exception as e:
        print(
            f"[BRIDGE:{label}] Upstream fail "
            f"{cfg['target_host']}:{cfg['target_port']}: {e}",
            flush=True
        )

        client_writer.close()
        return

    # Optimize both connection sockets
    optimize_socket(client_writer.get_extra_info("socket"))
    optimize_socket(ssh_writer.get_extra_info("socket"))

    # Forward any data left after the handshake
    if leftover:
        ssh_writer.write(leftover)
        await ssh_writer.drain()

    # Forward traffic in both directions
    await asyncio.gather(
        relay(
            client_reader,
            ssh_writer,
            label=f"client->{label}"
        ),
        relay(
            ssh_reader,
            client_writer,
            label=f"{label}->client"
        ),
        return_exceptions=True
    )


# ===============
# BRIDGE SERVER
# ===============
async def main():
    # Require the JSON configuration path
    if len(sys.argv) < 2:
        print("[BRIDGE] FATAL: Missing JSON config path", flush=True)
        sys.exit(1)

    json_path = sys.argv[1]

    # Load the bridge configuration
    try:
        with open(json_path, "r") as f:
            cfg = json.load(f)

    except Exception as e:
        print(
            f"[BRIDGE] FATAL: bad config {json_path}: {e}",
            flush=True
        )
        sys.exit(1)

    # Skip bridges without an upstream target
    if not cfg.get("target_host"):
        print(
            f"[BRIDGE:{cfg.get('label', '?')}] "
            "disabled (empty target_host)",
            flush=True
        )
        sys.exit(0)

    # Start the local bridge listener
    server = await asyncio.start_server(
        lambda r, w: handle_client(r, w, cfg),
        "127.0.0.1",
        cfg["listen_port"],
        limit=BUF_SIZE
    )

    print(
        f"[BRIDGE:{cfg['label']}] Listening on "
        f"127.0.0.1:{cfg['listen_port']} -> "
        f"{cfg['target_host']}:{cfg['target_port']}",
        flush=True
    )

    async with server:
        await server.serve_forever()


# =====================
# CONTAINER ENTRYPOINT
# =====================
if __name__ == "__main__":
    # Enable uvloop when available
    try:
        import uvloop

        uvloop.install()
        print(
            f"[BRIDGE] uvloop ACTIVE ({sys.argv[1:]})",
            flush=True
        )

    except ImportError:
        print(
            f"[BRIDGE] uvloop MISSING (asyncio fallback) ({sys.argv[1:]})",
            flush=True
        )

    asyncio.run(main())
