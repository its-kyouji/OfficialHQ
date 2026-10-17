"""Custom HTTP/1.1 packet-up SSH tunnel (CXH) for Cloud Run."""

import asyncio
import re
import time

from config import BUF_SIZE, HANDSHAKE_TIMEOUT, HEADER_LIMIT, IDLE_TIMEOUT
from handshake import parse_request_headers, read_request_headers
from socket_utils import optimize_socket

CHUNK_LINE_LIMIT = 8192
MAX_PACKET_SIZE = 1 * 1024 * 1024
MAX_REORDER_WINDOW = 64
MAX_PENDING_BYTES = 4 * 1024 * 1024
MAX_SESSIONS = 256
MAX_SESSIONS_PER_IP = 32
PAIR_TIMEOUT = 30.0

_CXH_ROUTE_RE = re.compile(
    r"/(?:open|drop)-cxh/([A-Za-z0-9_-]{16,128})(?:/([0-9]{1,12}))?$"
)
_HEADER_NAME = re.compile(rb"[!#$%&'*+\-.^_`|~0-9A-Za-z]+\Z")
_FORBIDDEN_TRAILERS = {
    b"connection",
    b"content-length",
    b"host",
    b"trailer",
    b"transfer-encoding",
    b"upgrade",
}

_SESSIONS = {}
_IP_COUNTS = {}
_SESSIONS_LOCK = asyncio.Lock()


class CxhSession:
    """State container for a paired GET + packet-up POST SSH session."""

    def __init__(self, sid, client_ip, cfg):
        self.sid = sid
        self.client_ip = client_ip
        self.cfg = cfg
        self.label = cfg.get("label", "XH")
        self.created_at = time.monotonic()

        self.init_lock = asyncio.Lock()
        self.write_lock = asyncio.Lock()

        self.ssh_reader = None
        self.ssh_writer = None
        self.connected = False
        self.closed = False
        self.close_reason = "active"

        self.get_attached = False
        self.post_seen = False
        self.paired_event = asyncio.Event()
        self.closed_event = asyncio.Event()

        self.next_seq = 0
        self.upload_eof = False
        self.pending = {}
        self.pending_bytes = 0

        self.bytes_up = 0
        self.bytes_down = 0
        self.reaper_task = None


# ======================
# HTTP RESPONSE HELPERS
# ======================
def _response(status, reason, keep_alive=False, extra_headers=b""):
    conn_hdr = b"Connection: keep-alive\r\n" if keep_alive else b"Connection: close\r\n"
    return (
        f"HTTP/1.1 {status} {reason}\r\n".encode("ascii")
        + b"Cache-Control: no-store\r\n"
        + extra_headers
        + conn_hdr
        + b"Content-Length: 0\r\n\r\n"
    )


async def _send_response(
    client_writer, status, reason, keep_alive=False, extra_headers=b""
):
    if client_writer.is_closing():
        return
    try:
        client_writer.write(
            _response(status, reason, keep_alive=keep_alive, extra_headers=extra_headers)
        )
        await asyncio.wait_for(client_writer.drain(), timeout=HANDSHAKE_TIMEOUT)
    except Exception:
        pass


def _extract_client_ip(headers, client_writer):
    xff = headers.get("x-forwarded-for", "")
    if xff:
        first = xff.split(",", 1)[0].strip()
        if first:
            return first
    peer = client_writer.get_extra_info("peername")
    return peer[0] if peer else "127.0.0.1"


def _wants_keep_alive(headers):
    tokens = {
        t.strip().lower() for t in headers.get("connection", "").split(",") if t.strip()
    }
    return "close" not in tokens


# =========================
# SESSION LIFECYCLE HELPERS
# =========================
async def _get_or_create_session(sid, client_ip, cfg):
    async with _SESSIONS_LOCK:
        session = _SESSIONS.get(sid)
        if session is not None:
            if session.closed:
                return None, "closed"
            return session, "existing"

        if len(_SESSIONS) >= MAX_SESSIONS:
            return None, "max_sessions"

        ip_count = _IP_COUNTS.get(client_ip, 0)
        if ip_count >= MAX_SESSIONS_PER_IP:
            return None, "max_per_ip"

        session = CxhSession(sid, client_ip, cfg)
        _SESSIONS[sid] = session
        _IP_COUNTS[client_ip] = ip_count + 1
        session.reaper_task = asyncio.create_task(
            _pair_watchdog(session), name=f"cxh-reaper-{sid[:8]}"
        )
        return session, "created"


async def _close_session(session, reason):
    async with _SESSIONS_LOCK:
        if session.closed:
            return
        session.closed = True
        session.close_reason = reason
        session.closed_event.set()
        session.pending.clear()
        session.pending_bytes = 0

        popped = _SESSIONS.pop(session.sid, None)
        if popped is not None:
            ip_count = _IP_COUNTS.get(session.client_ip, 1) - 1
            if ip_count <= 0:
                _IP_COUNTS.pop(session.client_ip, None)
            else:
                _IP_COUNTS[session.client_ip] = ip_count

    if (
        session.reaper_task is not None
        and session.reaper_task is not asyncio.current_task()
        and not session.reaper_task.done()
    ):
        session.reaper_task.cancel()

    if session.ssh_writer is not None and not session.ssh_writer.is_closing():
        session.ssh_writer.close()
        try:
            await session.ssh_writer.wait_closed()
        except Exception:
            pass

    print(
        f"[BRIDGE:{session.label}] CXH CLOSE sid={session.sid} "
        f"reason={reason} next_seq={session.next_seq} "
        f"up={session.bytes_up}B down={session.bytes_down}B",
        flush=True,
    )


async def _pair_watchdog(session):
    try:
        await asyncio.wait_for(session.paired_event.wait(), timeout=PAIR_TIMEOUT)
    except asyncio.TimeoutError:
        print(
            f"[BRIDGE:{session.label}] CXH REAP sid={session.sid} "
            f"reason=pair_timeout ({PAIR_TIMEOUT}s)",
            flush=True,
        )
        await _close_session(session, "pair_timeout")
    except asyncio.CancelledError:
        pass


async def _ensure_ssh_connected(session):
    async with session.init_lock:
        if session.closed:
            raise ConnectionError(f"Session {session.sid} is closed")
        if session.connected:
            return

        cfg = session.cfg
        if not cfg.get("target_host"):
            raise ConnectionError("Missing target_host in bridge config")

        ssh_reader, ssh_writer = await asyncio.wait_for(
            asyncio.open_connection(
                cfg["target_host"], cfg["target_port"], limit=BUF_SIZE
            ),
            timeout=HANDSHAKE_TIMEOUT,
        )
        optimize_socket(ssh_writer.get_extra_info("socket"))
        session.ssh_reader = ssh_reader
        session.ssh_writer = ssh_writer
        session.connected = True

        print(
            f"[BRIDGE:{session.label}] CXH SSH CONNECTED sid={session.sid} -> "
            f"{cfg['target_host']}:{cfg['target_port']}",
            flush=True,
        )


# ========================
# REQUEST BODY DECODERS
# ========================
async def _read_chunked_packet_body(client_reader):
    """Decode a chunk-framed POST packet body into raw bytes."""
    buf = bytearray()
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
        if size < 0 or len(buf) + size > MAX_PACKET_SIZE:
            raise OverflowError("HTTP chunked packet exceeds MAX_PACKET_SIZE")

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
                    return bytes(buf)

                name, sep, _value = trailer[:-2].partition(b":")
                if not sep or not _HEADER_NAME.fullmatch(name):
                    raise ValueError("Malformed HTTP trailer")
                if name.lower() in _FORBIDDEN_TRAILERS:
                    raise ValueError("Forbidden HTTP trailer field")

        chunk = await asyncio.wait_for(
            client_reader.readexactly(size), timeout=IDLE_TIMEOUT
        )
        buf.extend(chunk)

        ending = await asyncio.wait_for(
            client_reader.readexactly(2), timeout=IDLE_TIMEOUT
        )
        if ending != b"\r\n":
            raise ValueError("Invalid HTTP chunk terminator")


async def _read_post_body(client_reader, headers):
    """Validate framing headers and return (payload_bytes, framing_type)."""
    has_cl = "content-length" in headers
    has_te = "transfer-encoding" in headers

    # Reject simultaneous Content-Length and Transfer-Encoding (HTTP smuggling).
    if has_cl and has_te:
        raise ValueError("Both Content-Length and Transfer-Encoding present")

    if has_cl:
        raw_cl = headers["content-length"].strip()
        if not raw_cl.isdigit():
            raise ValueError("Malformed Content-Length")
        length = int(raw_cl)
        if length < 0 or length > MAX_PACKET_SIZE:
            raise OverflowError("Content-Length exceeds MAX_PACKET_SIZE")
        if length == 0:
            return b"", "CL"
        data = await asyncio.wait_for(
            client_reader.readexactly(length), timeout=IDLE_TIMEOUT
        )
        return data, "CL"

    if has_te:
        if headers["transfer-encoding"].strip().lower() != "chunked":
            raise ValueError("Unsupported Transfer-Encoding")
        data = await _read_chunked_packet_body(client_reader)
        return data, "TE"

    raise LookupError("Missing Content-Length or Transfer-Encoding")


# ========================
# GET DOWNLOAD STREAM
# ========================
async def _watch_client_disconnect(client_reader):
    """Detect abrupt client disconnect on the GET stream."""
    try:
        while True:
            data = await client_reader.read(256)
            if not data:
                return
    except Exception:
        return


async def _stream_ssh_response(session, client_writer):
    """Stream SSH upstream bytes as HTTP/1.1 chunked response."""
    while not session.closed:
        data = await asyncio.wait_for(
            session.ssh_reader.read(BUF_SIZE), timeout=IDLE_TIMEOUT
        )
        if not data:
            break
        if client_writer.is_closing():
            raise ConnectionError("Client GET writer closed during download")

        client_writer.write(
            f"{len(data):X}\r\n".encode("ascii") + data + b"\r\n"
        )
        await asyncio.wait_for(client_writer.drain(), timeout=IDLE_TIMEOUT)
        session.bytes_down += len(data)

    if not client_writer.is_closing():
        client_writer.write(b"0\r\n\r\n")
        await asyncio.wait_for(client_writer.drain(), timeout=IDLE_TIMEOUT)


async def _handle_cxh_get(
    client_reader, client_writer, sid, headers, cfg
):
    label = cfg.get("label", "XH")
    client_ip = _extract_client_ip(headers, client_writer)

    session, state = await _get_or_create_session(sid, client_ip, cfg)
    if session is None:
        status, reason = (
            (429, "Too Many Requests")
            if state == "max_per_ip"
            else (503, "Service Unavailable")
        )
        print(
            f"[BRIDGE:{label}] CXH GET REJECT {status} sid={sid} state={state}",
            flush=True,
        )
        await _send_response(client_writer, status, reason)
        return

    async with session.write_lock:
        if session.get_attached:
            print(
                f"[BRIDGE:{label}] CXH GET REJECT 409 sid={sid} (duplicate GET)",
                flush=True,
            )
            await _send_response(client_writer, 409, "Conflict")
            return
        session.get_attached = True
        if session.post_seen:
            session.paired_event.set()

    close_reason = "download_eof"
    stream_task = None
    disconnect_task = None
    closed_wait_task = None

    try:
        try:
            await _ensure_ssh_connected(session)
        except Exception as exc:
            close_reason = f"upstream_connect_error:{type(exc).__name__}"
            print(
                f"[BRIDGE:{label}] CXH GET 502 sid={sid} err={exc}",
                flush=True,
            )
            await _send_response(client_writer, 502, "Bad Gateway")
            return

        optimize_socket(client_writer.get_extra_info("socket"))

        client_writer.write(
            b"HTTP/1.1 200 OK\r\n"
            b"Content-Type: application/octet-stream\r\n"
            b"Cache-Control: no-store\r\n"
            b"Transfer-Encoding: chunked\r\n"
            b"Connection: close\r\n\r\n"
        )
        await asyncio.wait_for(client_writer.drain(), timeout=IDLE_TIMEOUT)

        print(
            f"[BRIDGE:{label}] CXH GET STREAM START sid={sid} state={state}",
            flush=True,
        )

        stream_task = asyncio.create_task(
            _stream_ssh_response(session, client_writer),
            name=f"cxh-down-{sid[:8]}",
        )
        disconnect_task = asyncio.create_task(
            _watch_client_disconnect(client_reader),
            name=f"cxh-disc-{sid[:8]}",
        )
        closed_wait_task = asyncio.create_task(
            session.closed_event.wait(),
            name=f"cxh-closewait-{sid[:8]}",
        )

        done, _ = await asyncio.wait(
            {stream_task, disconnect_task, closed_wait_task},
            return_when=asyncio.FIRST_COMPLETED,
        )

        if stream_task in done:
            err = stream_task.exception()
            if err is not None:
                close_reason = f"download_err:{type(err).__name__}"
                raise err
            close_reason = "ssh_eof"
        elif disconnect_task in done:
            close_reason = "client_abrupt_disconnect"
        elif closed_wait_task in done:
            close_reason = session.close_reason

    except asyncio.CancelledError:
        close_reason = "cancelled"
        raise
    except Exception as exc:
        print(
            f"[BRIDGE:{label}] CXH GET ERROR sid={sid} "
            f"{type(exc).__name__}: {exc}",
            flush=True,
        )
    finally:
        for task in (stream_task, disconnect_task, closed_wait_task):
            if task is not None and not task.done():
                task.cancel()
        tasks = [
            t
            for t in (stream_task, disconnect_task, closed_wait_task)
            if t is not None
        ]
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)

        await _close_session(session, close_reason)


# ========================
# POST PACKET-UP HANDLER
# ========================
async def _handle_cxh_post_one(
    client_reader, client_writer, sid, seq, headers, cfg
):
    """Process a single packet-up POST request. Returns True if keep-alive continues."""
    label = cfg.get("label", "XH")
    keep_alive = _wants_keep_alive(headers)
    client_ip = _extract_client_ip(headers, client_writer)

    try:
        payload, framing = await _read_post_body(client_reader, headers)
    except LookupError:
        print(
            f"[BRIDGE:{label}] CXH POST REJECT 411 sid={sid} seq={seq}",
            flush=True,
        )
        await _send_response(client_writer, 411, "Length Required", keep_alive=False)
        return False
    except OverflowError:
        print(
            f"[BRIDGE:{label}] CXH POST REJECT 413 sid={sid} seq={seq}",
            flush=True,
        )
        await _send_response(
            client_writer, 413, "Content Too Large", keep_alive=False
        )
        return False
    except Exception as exc:
        print(
            f"[BRIDGE:{label}] CXH POST REJECT 400 sid={sid} seq={seq} "
            f"err={type(exc).__name__}: {exc}",
            flush=True,
        )
        await _send_response(client_writer, 400, "Bad Request", keep_alive=False)
        return False

    fin_hdr = headers.get("x-cxh-fin", "").strip() == "1"
    is_eof = fin_hdr or (len(payload) == 0)

    session, state = await _get_or_create_session(sid, client_ip, cfg)
    if session is None:
        if state == "closed":
            await _send_response(client_writer, 410, "Gone", keep_alive=False)
        elif state == "max_per_ip":
            await _send_response(
                client_writer, 429, "Too Many Requests", keep_alive=False
            )
        else:
            await _send_response(
                client_writer, 503, "Service Unavailable", keep_alive=False
            )
        return False

    try:
        await _ensure_ssh_connected(session)
    except Exception as exc:
        print(
            f"[BRIDGE:{label}] CXH POST 502 sid={sid} seq={seq} err={exc}",
            flush=True,
        )
        await _close_session(session, f"upstream_connect_error:{type(exc).__name__}")
        await _send_response(client_writer, 502, "Bad Gateway", keep_alive=False)
        return False

    try:
        async with session.write_lock:
            if session.closed:
                await _send_response(client_writer, 410, "Gone", keep_alive=False)
                return False

            session.post_seen = True
            if session.get_attached:
                session.paired_event.set()

            # 1. Deduplication check: already processed or already buffered.
            if seq < session.next_seq or seq in session.pending:
                print(
                    f"[BRIDGE:{label}] CXH POST DEDUPE sid={sid} seq={seq} "
                    f"next_seq={session.next_seq} framing={framing}",
                    flush=True,
                )
                await _send_response(
                    client_writer, 200, "OK", keep_alive=keep_alive
                )
                return keep_alive

            # 2. Bounded reorder window enforcement.
            if seq >= session.next_seq + MAX_REORDER_WINDOW:
                print(
                    f"[BRIDGE:{label}] CXH POST WINDOW EXCEEDED sid={sid} "
                    f"seq={seq} next_seq={session.next_seq}",
                    flush=True,
                )
                await _close_session(session, "seq_window_exceeded")
                await _send_response(
                    client_writer, 400, "Bad Request", keep_alive=False
                )
                return False

            if session.pending_bytes + len(payload) > MAX_PENDING_BYTES:
                print(
                    f"[BRIDGE:{label}] CXH POST BUFFER OVERFLOW sid={sid} seq={seq}",
                    flush=True,
                )
                await _close_session(session, "reorder_buffer_overflow")
                await _send_response(
                    client_writer, 413, "Content Too Large", keep_alive=False
                )
                return False

            # 3. Queue packet and drain consecutive in-order sequence numbers.
            session.pending[seq] = (payload, is_eof)
            session.pending_bytes += len(payload)

            while session.next_seq in session.pending:
                pkt_data, pkt_eof = session.pending.pop(session.next_seq)
                session.pending_bytes -= len(pkt_data)
                cur_seq = session.next_seq
                session.next_seq += 1

                if pkt_data:
                    if (
                        session.ssh_writer is None
                        or session.ssh_writer.is_closing()
                    ):
                        raise ConnectionError("SSH upstream closed during upload")
                    session.ssh_writer.write(pkt_data)
                    await asyncio.wait_for(
                        session.ssh_writer.drain(), timeout=IDLE_TIMEOUT
                    )
                    session.bytes_up += len(pkt_data)

                if pkt_eof and not session.upload_eof:
                    session.upload_eof = True
                    if (
                        session.ssh_writer is not None
                        and not session.ssh_writer.is_closing()
                        and session.ssh_writer.can_write_eof()
                    ):
                        session.ssh_writer.write_eof()
                        await asyncio.wait_for(
                            session.ssh_writer.drain(), timeout=IDLE_TIMEOUT
                        )
                    print(
                        f"[BRIDGE:{label}] CXH UPLOAD EOF sid={sid} at seq={cur_seq}",
                        flush=True,
                    )

        print(
            f"[BRIDGE:{label}] CXH POST OK sid={sid} seq={seq} "
            f"framing={framing} state={state} len={len(payload)}B "
            f"eof={is_eof} next_seq={session.next_seq}",
            flush=True,
        )
        await _send_response(client_writer, 200, "OK", keep_alive=keep_alive)
        return keep_alive

    except Exception as exc:
        print(
            f"[BRIDGE:{label}] CXH POST ERROR sid={sid} seq={seq} "
            f"{type(exc).__name__}: {exc}",
            flush=True,
        )
        await _close_session(session, f"upload_err:{type(exc).__name__}")
        await _send_response(
            client_writer, 502, "Bad Gateway", keep_alive=False
        )
        return False


# ========================
# MAIN CXH DISPATCH ENTRY
# ========================
async def handle_xh(client_reader, client_writer, method, path, headers, cfg):
    """Entrypoint for CXH GET download stream and POST packet-up requests."""
    label = cfg.get("label", "XH")
    optimize_socket(client_writer.get_extra_info("socket"))

    try:
        cur_method = method.upper()
        cur_path = path
        cur_headers = headers

        while True:
            match = _CXH_ROUTE_RE.search(cur_path)
            if match is None:
                print(
                    f"[BRIDGE:{label}] CXH REJECT 400 invalid path={cur_path}",
                    flush=True,
                )
                await _send_response(
                    client_writer, 400, "Bad Request", keep_alive=False
                )
                return

            sid, seq_str = match.group(1), match.group(2)

            if cur_method == "GET":
                if seq_str is not None:
                    await _send_response(
                        client_writer, 400, "Bad Request", keep_alive=False
                    )
                    return
                await _handle_cxh_get(
                    client_reader, client_writer, sid, cur_headers, cfg
                )
                return

            if cur_method == "POST":
                if seq_str is None:
                    print(
                        f"[BRIDGE:{label}] CXH POST REJECT 400 missing seq path={cur_path}",
                        flush=True,
                    )
                    await _send_response(
                        client_writer, 400, "Bad Request", keep_alive=False
                    )
                    return

                seq = int(seq_str)
                keep_open = await _handle_cxh_post_one(
                    client_reader,
                    client_writer,
                    sid,
                    seq,
                    cur_headers,
                    cfg,
                )
                if not keep_open:
                    return

                # Wait for next keep-alive POST request on this connection.
                try:
                    raw_headers = await read_request_headers(client_reader)
                except (asyncio.IncompleteReadError, asyncio.TimeoutError, ConnectionError):
                    return

                cur_method, next_path, _ver, cur_headers = parse_request_headers(
                    raw_headers
                )
                cur_method = cur_method.upper()
                cur_path = next_path.split("?", 1)[0]
                continue

            await _send_response(
                client_writer,
                405,
                "Method Not Allowed",
                keep_alive=False,
                extra_headers=b"Allow: GET, POST\r\n",
            )
            return

    except asyncio.CancelledError:
        raise
    except Exception as exc:
        print(
            f"[BRIDGE:{label}] CXH FATAL {type(exc).__name__}: {exc}",
            flush=True,
        )
    finally:
        if not client_writer.is_closing():
            client_writer.close()
        try:
            await client_writer.wait_closed()
        except Exception:
            pass
