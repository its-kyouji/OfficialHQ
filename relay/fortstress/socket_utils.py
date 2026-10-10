import socket

from config import BUF_SIZE

# === SOCKET OPTIMIZATION ===

def optimize_socket(sock: socket.socket):
    if not sock:
        return

    try:
        sock.setsockopt(
            socket.IPPROTO_TCP,
            socket.TCP_NODELAY,
            1
        )

        sock.setsockopt(
            socket.SOL_SOCKET,
            socket.SO_RCVBUF,
            BUF_SIZE
        )

        sock.setsockopt(
            socket.SOL_SOCKET,
            socket.SO_SNDBUF,
            BUF_SIZE
        )

        sock.setsockopt(
            socket.SOL_SOCKET,
            socket.SO_KEEPALIVE,
            1
        )

    except OSError:
        pass
