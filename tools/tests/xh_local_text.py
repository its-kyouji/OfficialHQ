#!/usr/bin/env python3
"""Local integration and regression tests for OfficialHQ custom XH.

Run from the repository root:
    python3 tools/tests/xh_local_test.py

Uses a fake SSH upstream. It does not test Cloud Run ingress behavior.
"""

import asyncio
import os
import sys

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
BRIDGE_DIR = os.path.join(ROOT, "relay", "bridge")
sys.path.insert(0, BRIDGE_DIR)

import bridge  # noqa: E402


async def read_chunk(reader):
    line = await reader.readline()

    if not line.endswith(b"\r\n"):
        raise AssertionError(f"Bad chunk header: {line!r}")

    token = line[:-2].split(b";", 1)[0].strip()

    try:
        size = int(token, 16)
    except ValueError as exc:
        raise AssertionError(
            f"Invalid chunk size: {line!r}"
        ) from exc

    if size < 0:
        raise AssertionError(f"Negative chunk size: {size}")

    if size == 0:
        while True:
            trailer = await reader.readline()

            if not trailer:
                raise AssertionError(
                    "EOF while reading chunk trailers"
                )

            if trailer == b"\r\n":
                break

        return None

    data = await reader.readexactly(size)

    if await reader.readexactly(2) != b"\r\n":
        raise AssertionError("Bad chunk terminator")

    return data


async def fake_ssh(reader, writer):
    """Fake SSH server used to test the bridge without a real SSH daemon."""
    try:
        writer.write(b"SSH-2.0-OHQ-XH-TEST\r\n")
        await writer.drain()

        line = await reader.readline()

        if line:
            writer.write(b"ECHO:" + line)
            await writer.drain()

    finally:
        writer.close()

        try:
            await writer.wait_closed()
        except Exception:
            pass


async def test_xh_post(bridge_port):
    """Test chunked POST framing and bidirectional application data."""
    reader, writer = await asyncio.open_connection(
        "127.0.0.1",
        bridge_port,
    )

    try:
        writer.write(
            b"POST /kyouji/open-xh HTTP/1.1\r\n"
            b"Host: localhost\r\n"
            b"Transfer-Encoding: chunked\r\n"
            b"Content-Type: application/octet-stream\r\n"
            b"\r\n"
        )
        await writer.drain()

        response_headers = await asyncio.wait_for(
            reader.readuntil(b"\r\n\r\n"),
            timeout=3,
        )

        if not response_headers.startswith(
            b"HTTP/1.1 200 OK\r\n"
        ):
            raise AssertionError(
                f"Expected HTTP 200, got {response_headers!r}"
            )

        if b"Transfer-Encoding: chunked\r\n" not in response_headers:
            raise AssertionError(
                f"Expected chunked response, got {response_headers!r}"
            )

        # The server should send the SSH banner while the request
        # body is still open.
        first = await asyncio.wait_for(
            read_chunk(reader),
            timeout=3,
        )

        if first != b"SSH-2.0-OHQ-XH-TEST\r\n":
            raise AssertionError(
                f"Expected upstream banner, got {first!r}"
            )

        payload = b"SSH-2.0-OHQ-CLIENT\r\n"

        writer.write(
            f"{len(payload):X}\r\n".encode("ascii")
            + payload
            + b"\r\n0\r\n\r\n"
        )
        await writer.drain()

        output = bytearray(first)

        while True:
            data = await asyncio.wait_for(
                read_chunk(reader),
                timeout=3,
            )

            if data is None:
                break

            output.extend(data)

        expected = (
            b"SSH-2.0-OHQ-XH-TEST\r\n"
            b"ECHO:SSH-2.0-OHQ-CLIENT\r\n"
        )

        if bytes(output) != expected:
            raise AssertionError(
                f"Unexpected response payload: {bytes(output)!r}"
            )

    finally:
        writer.close()

        try:
            await writer.wait_closed()
        except Exception:
            pass


async def test_upgrade_route(bridge_port, path):
    """Verify that Upgrade requests still receive 101 and relay raw bytes."""
    reader, writer = await asyncio.open_connection(
        "127.0.0.1",
        bridge_port,
    )

    try:
        # Intentionally use Upgrade:websocket without a space after
        # the colon to test robust HTTP header parsing.
        writer.write(
            f"GET {path} HTTP/1.1\r\n".encode("ascii")
            + b"Host: localhost\r\n"
            + b"Connection: keep-alive, Upgrade\r\n"
            + b"Upgrade:websocket\r\n"
            + b"\r\n"
        )
        await writer.drain()

        response = await asyncio.wait_for(
            reader.readuntil(b"\r\n\r\n"),
            timeout=3,
        )

        if not response.startswith(
            b"HTTP/1.1 101 Switching Protocols\r\n"
        ):
            raise AssertionError(
                f"Upgrade failed for {path}: got {response!r}"
            )

        banner = await asyncio.wait_for(
            reader.readexactly(
                len(b"SSH-2.0-OHQ-XH-TEST\r\n")
            ),
            timeout=3,
        )

        if banner != b"SSH-2.0-OHQ-XH-TEST\r\n":
            raise AssertionError(
                f"Unexpected relayed banner: {banner!r}"
            )

        payload = b"existing-upgrade-relay-test\n"

        writer.write(payload)
        await writer.drain()

        echoed = await asyncio.wait_for(
            reader.readexactly(len(b"ECHO:" + payload)),
            timeout=3,
        )

        if echoed != b"ECHO:" + payload:
            raise AssertionError(
                f"Raw relay failed: {echoed!r}"
            )

    finally:
        writer.close()

        try:
            await writer.wait_closed()
        except Exception:
            pass


async def run_test():
    # Start a fake SSH upstream.
    ssh_server = await asyncio.start_server(
        fake_ssh,
        "127.0.0.1",
        0,
    )

    ssh_port = ssh_server.sockets[0].getsockname()[1]

    cfg = {
        "listen_port": 0,
        "target_host": "127.0.0.1",
        "target_port": ssh_port,
        "label": "XH-TEST",
    }

    # Start the bridge handler on an ephemeral local port.
    bridge_server = await asyncio.start_server(
        lambda reader, writer: bridge.handle_client(
            reader,
            writer,
            cfg,
        ),
        "127.0.0.1",
        0,
        limit=262144,
    )

    bridge_port = bridge_server.sockets[0].getsockname()[1]

    try:
        await test_xh_post(bridge_port)
        print("PASS: custom chunked POST XH framing and relay")

        await test_upgrade_route(
            bridge_port,
            "/kyouji/open-xh",
        )
        print(
            "PASS: GET /open-xh + Upgrade returns 101 "
            "and relays raw bytes"
        )

        await test_upgrade_route(
            bridge_port,
            "/kyouji/open-ws",
        )
        print(
            "PASS: existing WS Upgrade and raw relay remain functional"
        )

    finally:
        bridge_server.close()
        ssh_server.close()

        await bridge_server.wait_closed()
        await ssh_server.wait_closed()


if __name__ == "__main__":
    asyncio.run(run_test())
