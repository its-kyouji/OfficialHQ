import re

from config import BASE_PREFIX, ALLOWED_PATHS

# === RAW PATH ROUTING ===

RAW_ROUTES = [
    (re.compile(re.escape(BASE_PREFIX) + rb"ssh-dropbear-(ws|hu)$"), 2222),
    (re.compile(re.escape(BASE_PREFIX) + rb"ssh-openssh-(ws|hu)$"), 2223),
    (re.compile(re.escape(BASE_PREFIX) + rb"ovpn-(ws|hu)$"), 2224),
    (re.compile(re.escape(BASE_PREFIX) + rb"[xs]-[a-z]+-hu$"), 8083),
]


def validate_path(request_path):
    return (
        request_path in ALLOWED_PATHS
        or request_path.startswith(BASE_PREFIX)
    )


def resolve_backend(request_path, default_port):
    for pattern, port in RAW_ROUTES:
        if pattern.match(request_path):
            return port

    return default_port
