import json
import os

# === SERVER CONFIG ===
LISTEN_PORT = int(os.environ.get("PORT", "8080"))
BACKEND_HOST = "127.0.0.1"
BACKEND_PORT = 8081

# === SOCKET CONFIG ===
BUF_SIZE = 131072
HEADER_LIMIT = 16384
HEADER_TIMEOUT = 3.0
READ_TIMEOUT = 2.0
BACKEND_TIMEOUT = 10.0

# === PROTOCOL CONSTANTS ===
H2C_PREFACE = b"PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n"
BASE_PREFIX = b"/kyouji/kyouji-"

# === FORTRESS POLICY ===
DEFAULT_CONFIG = {
    "ban_threshold": 5000,
    "ban_time_seconds": 600,
    "whitelist_paths": [
        "/",
        "/health",
        "/runtime.json"
    ]
}

CONFIG_PATH = "/run/cfg/relay/fortress/fortress.json"

try:
    with open(CONFIG_PATH, "r") as f:
        config = json.load(f)
except Exception:
    config = DEFAULT_CONFIG

BAN_THRESHOLD = int(config.get("ban_threshold", 5000))
BAN_TIME = int(config.get("ban_time_seconds", 600))

ALLOWED_PATHS = {
    p.encode()
    for p in config.get(
        "whitelist_paths",
        DEFAULT_CONFIG["whitelist_paths"]
    )
}
