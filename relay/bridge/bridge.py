#!/usr/bin/env python3

import asyncio
import json
import re
import sys

from config import BUF_SIZE
from socket_utils import optimize_socket
from relay import relay
from handshake import handle_handshake, read_request_headers, parse_request_headers
from xh import handle_xh

CXH_PATH_RE = re.compile(
    r"/(?:open|drop)-cxh(?:/([A-Za-z0-9_-]+)(?:/([0-9]+))?)?$"
)


# ==========================
# CLIENT CONNECTION HANDLER
# ==========================
async def handle_client(client_reader, client_writer, cfg):
    label = cfg.get("label", "UNKNOWN")
    raw_headers = None

    try:
        raw_headers = await read_request_headers(client_reader)
        method, path, _version, headers = parse_request_headers(raw_headers)
        clean_path = path.split("?", 1)[0]

        cxh_match = CXH_PATH_RE.search(clean_path)
        has_upgrade = "upgrade" in headers
        method_upper = method.upper()

        # Route CXH packet-up requests:
        # - GET /.../(open|drop)-cxh/<sid> (without Upgrade)
        # - POST /.../(open|drop)-cxh[/<sid>[/<seq>]] (without Upgrade)
        if cxh_match is not None and not has_upgrade:
            sid_group = cxh_match.group(1)
            if method_upper == "POST" or (
                method_upper == "GET" and sid_group is not None
            ):
                await handle_xh(
                    client_reader,
                    client_writer,
                    method_upper,
                    clean_path,
                    headers,
                    cfg,
                )
                return

    except asyncio.CancelledError:
        raise
    except Exception as exc:
        print(
            f"[BRIDGE:{label}] HTTP header error: {type(exc).__name__}: {exc}",
            flush=True,
        )
        if not client_writer.is_closing():
            client_writer.close()
            try:
                await client_writer.wait_closed()
            except Exception:
                pass
        return

    # Existing WS/HU and legacy handshake behavior.
    leftover = await handle_handshake(
        client_reader, client_writer, label, raw=raw_headers
    )
    if leftover is None:
        return

    # ========================
    # CONNECT TO SSH UPSTREAM
    # ========================
    try:
        ssh_reader, ssh_writer = await asyncio.open_connection(
            cfg["target_host"], cfg["target_port"], limit=BUF_SIZE
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

    optimize_socket(client_writer.get_extra_info("socket"))
    optimize_socket(ssh_writer.get_extra_info("socket"))

    if leftover:
        ssh_writer.write(leftover)
        await ssh_writer.drain()

    # ========================
    # BIDIRECTIONAL RAW RELAY
    # ========================
    try:
        await asyncio.gather(
            relay(client_reader, ssh_writer, label=f"client->{label}"),
            relay(ssh_reader, client_writer, label=f"{label}->client"),
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


# ==============
# BRIDGE SERVER
# ==============
async def main():
    if len(sys.argv) < 2:
        print("[BRIDGE] FATAL: Missing JSON config path", flush=True)
        sys.exit(1)

    json_path = sys.argv[1]

    try:
        with open(json_path, "r") as file:
            cfg = json.load(file)
    except Exception as exc:
        print(f"[BRIDGE] FATAL: bad config {json_path}: {exc}", flush=True)
        sys.exit(1)

    if not cfg.get("target_host"):
        print(
            f"[BRIDGE:{cfg.get('label', '?')}] disabled (empty target_host)",
            flush=True,
        )
        sys.exit(0)

    server = await asyncio.start_server(
        lambda reader, writer: handle_client(reader, writer, cfg),
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
        print(f"[BRIDGE] uvloop ACTIVE ({sys.argv[1:]})", flush=True)
    except ImportError:
        print(
            f"[BRIDGE] uvloop MISSING (asyncio fallback) ({sys.argv[1:]})",
            flush=True,
        )

    asyncio.run(main())
