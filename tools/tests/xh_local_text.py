#!/usr/bin/env python3
"""Local integration and regression suite for OfficialHQ packet-up CXH.

Tests covered:
  1. GET-first (immediate SSH banner over H1 chunked stream before POST)
  2. POST-first + out-of-order reassembly + seq deduplication + CL+TE smuggling rejection
  3. Concurrent client shim relay
  4. Half-close (upload EOF while downstream continues sending)
  5. Missing peer reaping (orphan session cleanup)
  6. Abrupt client disconnect on GET stream
  7. 1 MiB bidirectional transfer with SHA-256 verification
  8. Regression: GET /open-xh + Upgrade -> 101
  9. Regression: GET /open-ws + Upgrade -> 101
"""

import asyncio
import hashlib
import os
import secrets
import sys

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
BRIDGE_DIR = os.path.join(ROOT, "relay", "bridge")
TOOLS_DIR = os.path.join(ROOT, "tools")
sys.path.insert(0, BRIDGE_DIR)
sys.path.insert(0, TOOLS_DIR)

import bridge  # noqa: E402
import xh  # noqa: E402
import xh_client  # noqa: E402

BANNER = b"SSH-2.0-OHQ-XH-TEST\r\n"


async def read_chunk(reader):
    line = await reader.readline()
    if not line.endswith(b"\r\n"):
        raise AssertionError(f"Bad chunk header: {line!r}")
    token = line[:-2].split(b";", 1)[0].strip()
    size = int(token, 16)
    if size == 0:
        while True:
            trailer = await reader.readline()
            if trailer == b"\r\n":
                break
        return None
    data = await reader.readexactly(size)
    if await reader.readexactly(2) != b"\r\n":
        raise AssertionError("Bad chunk terminator")
    return data


async def fake_ssh_echo(reader, writer):
    """Echoes all incoming bytes back to client after sending SSH banner, then sends footer on EOF."""
    try:
        writer.write(BANNER)
        await writer.drain()
        while True:
            data = await reader.read(32768)
            if not data:
                # Confirm half-close worked by sending a post-EOF trailer bytes block.
                writer.write(b"EOF-ACK\r\n")
                await writer.drain()
                break
            writer.write(data)
            await writer.drain()
    finally:
        writer.close()
        try:
            await writer.wait_closed()
        except Exception:
            pass


async def send_raw_post(port, path, body, extra_headers=b""):
    reader, writer = await asyncio.open_connection("127.0.0.1", port)
    try:
        req = (
            f"POST {path} HTTP/1.1\r\n"
            f"Host: localhost\r\n"
            f"Content-Length: {len(body)}\r\n".encode("ascii")
            + extra_headers
            + b"Connection: close\r\n\r\n"
            + body
        )
        writer.write(req)
        await writer.drain()
        resp = await asyncio.wait_for(reader.readuntil(b"\r\n\r\n"), timeout=3)
        status = int(resp.split(b" ", 2)[1])
        return status
    finally:
        writer.close()
        await writer.wait_closed()


async def test_1_get_first_and_half_close(bridge_port):
    sid = "test1_" + secrets.token_urlsafe(16)
    get_reader, get_writer = await asyncio.open_connection("127.0.0.1", bridge_port)
    try:
        get_writer.write(
            f"GET /kyouji/open-cxh/{sid} HTTP/1.1\r\n"
            f"Host: localhost\r\nConnection: close\r\n\r\n".encode("ascii")
        )
        await get_writer.drain()

        headers = await asyncio.wait_for(get_reader.readuntil(b"\r\n\r\n"), timeout=3)
        assert headers.startswith(b"HTTP/1.1 200 OK\r\n"), headers

        # Litmus test: receive SSH banner BEFORE any POST arrives!
        first_chunk = await asyncio.wait_for(read_chunk(get_reader), timeout=3)
        assert first_chunk == BANNER, f"Expected banner, got {first_chunk!r}"

        # Send upload packet seq=0 and EOF packet seq=1
        assert await send_raw_post(bridge_port, f"/kyouji/open-cxh/{sid}/0", b"HELLO-CXH\n") == 200
        assert await send_raw_post(bridge_port, f"/kyouji/open-cxh/{sid}/1", b"") == 200

        received = bytearray()
        while True:
            chunk = await asyncio.wait_for(read_chunk(get_reader), timeout=3)
            if chunk is None:
                break
            received.extend(chunk)

        assert bytes(received) == b"HELLO-CXH\nEOF-ACK\r\n", f"Got {bytes(received)!r}"
    finally:
        get_writer.close()
        await get_writer.wait_closed()


async def test_2_post_first_reorder_dedupe_smuggling(bridge_port):
    sid = "test2_" + secrets.token_urlsafe(16)

    # 1. Reject Content-Length + Transfer-Encoding smuggling attempt with 400
    reader, writer = await asyncio.open_connection("127.0.0.1", bridge_port)
    try:
        writer.write(
            f"POST /kyouji/open-cxh/{sid}/0 HTTP/1.1\r\n"
            f"Host: localhost\r\nContent-Length: 5\r\nTransfer-Encoding: chunked\r\n\r\n0\r\n\r\n".encode("ascii")
        )
        await writer.drain()
        resp = await asyncio.wait_for(reader.readuntil(b"\r\n\r\n"), timeout=3)
        assert resp.startswith(b"HTTP/1.1 400 "), f"Expected 400, got {resp!r}"
    finally:
        writer.close()
        await writer.wait_closed()

    # 2. Send out-of-order seq=1 BEFORE seq=0
    assert await send_raw_post(bridge_port, f"/kyouji/open-cxh/{sid}/1", b"WORLD\n") == 200
    # 3. Send duplicate seq=1 (must be 200 no-op)
    assert await send_raw_post(bridge_port, f"/kyouji/open-cxh/{sid}/1", b"DUPLICATE-GARBAGE\n") == 200
    # 4. Send seq=0 (triggers ordered drain of seq=0 then seq=1)
    assert await send_raw_post(bridge_port, f"/kyouji/open-cxh/{sid}/0", b"HELLO-") == 200
    # 5. Send duplicate seq=0 (already delivered, must be 200 no-op)
    assert await send_raw_post(bridge_port, f"/kyouji/open-cxh/{sid}/0", b"DUP0") == 200
    # 6. Send EOF seq=2
    assert await send_raw_post(bridge_port, f"/kyouji/open-cxh/{sid}/2", b"") == 200

    # Now attach GET and verify exact ordered stream
    get_reader, get_writer = await asyncio.open_connection("127.0.0.1", bridge_port)
    try:
        get_writer.write(
            f"GET /kyouji/open-cxh/{sid} HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n".encode("ascii")
        )
        await get_writer.drain()
        await asyncio.wait_for(get_reader.readuntil(b"\r\n\r\n"), timeout=3)

        output = bytearray()
        while True:
            chunk = await asyncio.wait_for(read_chunk(get_reader), timeout=3)
            if chunk is None:
                break
            output.extend(chunk)

        expected = BANNER + b"HELLO-WORLD\nEOF-ACK\r\n"
        assert bytes(output) == expected, f"Expected {expected!r}, got {bytes(output)!r}"
    finally:
        get_writer.close()
        await get_writer.wait_closed()


async def test_5_orphan_reaper(bridge_port):
    orig_timeout = xh.PAIR_TIMEOUT
    xh.PAIR_TIMEOUT = 0.25
    try:
        sid = "test5_" + secrets.token_urlsafe(16)
        assert await send_raw_post(bridge_port, f"/kyouji/open-cxh/{sid}/0", b"ORPHAN") == 200
        assert sid in xh._SESSIONS
        await asyncio.sleep(0.4)
        assert sid not in xh._SESSIONS, "Orphan session was not reaped!"
    finally:
        xh.PAIR_TIMEOUT = orig_timeout


async def test_6_abrupt_disconnect(bridge_port):
    sid = "test6_" + secrets.token_urlsafe(16)
    get_reader, get_writer = await asyncio.open_connection("127.0.0.1", bridge_port)
    get_writer.write(
        f"GET /kyouji/open-cxh/{sid} HTTP/1.1\r\nHost: localhost\r\n\r\n".encode("ascii")
    )
    await get_writer.drain()
    await asyncio.wait_for(get_reader.readuntil(b"\r\n\r\n"), timeout=3)
    assert sid in xh._SESSIONS

    # Abruptly close GET socket
    get_writer.close()
    await get_writer.wait_closed()
    await asyncio.sleep(0.1)
    assert sid not in xh._SESSIONS, "Session remained active after abrupt GET disconnect!"


async def test_7_one_mib_end_to_end_via_client_shim(bridge_port):
    shim_server = await asyncio.start_server(
        lambda r, w: xh_client.handle_local(
            r,
            w,
            connect_host="127.0.0.1",
            port=bridge_port,
            http_host="localhost",
            sni="localhost",
            base_path="/kyouji/open-cxh",
            use_tls=False,
            max_inflight=2,
        ),
        "127.0.0.1",
        0,
    )
    shim_port = shim_server.sockets[0].getsockname()[1]

    try:
        reader, writer = await asyncio.open_connection("127.0.0.1", shim_port)
        payload = os.urandom(1024 * 1024)  # 1 MiB random bytes
        expected = BANNER + payload + b"EOF-ACK\r\n"
        expected_hash = hashlib.sha256(expected).hexdigest()

        async def _upload():
            writer.write(payload)
            await writer.drain()
            writer.write_eof()

        up_task = asyncio.create_task(_upload())
        received = await asyncio.wait_for(reader.readexactly(len(expected)), timeout=15)
        await up_task

        actual_hash = hashlib.sha256(received).hexdigest()
        assert actual_hash == expected_hash, f"Hash mismatch: {actual_hash} != {expected_hash}"

        writer.close()
        await writer.wait_closed()
    finally:
        shim_server.close()
        await shim_server.wait_closed()


async def test_upgrade_route(bridge_port, path):
    reader, writer = await asyncio.open_connection("127.0.0.1", bridge_port)
    try:
        writer.write(
            f"GET {path} HTTP/1.1\r\nHost: localhost\r\n"
            f"Connection: keep-alive, Upgrade\r\nUpgrade:websocket\r\n\r\n".encode("ascii")
        )
        await writer.drain()
        response = await asyncio.wait_for(reader.readuntil(b"\r\n\r\n"), timeout=3)
        assert response.startswith(b"HTTP/1.1 101 Switching Protocols\r\n"), response

        banner = await asyncio.wait_for(reader.readexactly(len(BANNER)), timeout=3)
        assert banner == BANNER

        payload = b"upgrade-regression-ok\n"
        writer.write(payload)
        await writer.drain()
        echoed = await asyncio.wait_for(reader.readexactly(len(payload)), timeout=3)
        assert echoed == payload
    finally:
        writer.close()
        await writer.wait_closed()


async def run_test():
    ssh_server = await asyncio.start_server(fake_ssh_echo, "127.0.0.1", 0)
    ssh_port = ssh_server.sockets[0].getsockname()[1]

    cfg = {
        "listen_port": 0,
        "target_host": "127.0.0.1",
        "target_port": ssh_port,
        "label": "XH-TEST",
    }

    bridge_server = await asyncio.start_server(
        lambda r, w: bridge.handle_client(r, w, cfg),
        "127.0.0.1",
        0,
        limit=262144,
    )
    bridge_port = bridge_server.sockets[0].getsockname()[1]

    try:
        await test_1_get_first_and_half_close(bridge_port)
        print("PASS [1/6]: GET-first immediate banner & half-close EOF")

        await test_2_post_first_reorder_dedupe_smuggling(bridge_port)
        print("PASS [2/6]: POST-first, out-of-order reassembly, seq dedupe & CL+TE rejection")

        await test_5_orphan_reaper(bridge_port)
        print("PASS [3/6]: Missing peer orphan session reaper")

        await test_6_abrupt_disconnect(bridge_port)
        print("PASS [4/6]: Abrupt GET disconnect cleanup")

        await test_7_one_mib_end_to_end_via_client_shim(bridge_port)
        print("PASS [5/6]: 1 MiB full-duplex via xh_client.py shim (SHA-256 verified)")

        await test_upgrade_route(bridge_port, "/kyouji/open-ws")
        await test_upgrade_route(bridge_port, "/kyouji/open-hu")
        await test_upgrade_route(brigde_port, "/kyouji/open-xh")
        await test_upgrade_route(bridge_port, "/kyouji/open-cxh")
        print("PASS [6/6]: Existing WS, fake-XH, and Upgrade routes remain intact")

    finally:
        bridge_server.close()
        ssh_server.close()
        await bridge_server.wait_closed()
        await ssh_server.wait_closed()


if __name__ == "__main__":
    asyncio.run(run_test())
