import time
from collections import defaultdict

# === IP RATE LIMITING ===

IP_HITS = defaultdict(list)
BANNED_IPS = {}


def get_client_ip(lines, fallback):
    for line in lines:
        if line.lower().startswith(b"x-forwarded-for:"):
            value = line.split(b":", 1)[1].strip()

            if value:
                return value.split(
                    b",", 1
                )[0].strip().decode("utf-8", "ignore")

    return fallback


def is_banned(ip, ban_threshold, ban_time):
    now = time.time()

    expiry = BANNED_IPS.get(ip)

    if expiry is not None:
        if now >= expiry:
            del BANNED_IPS[ip]
        else:
            return True

    hits = IP_HITS[ip]

    IP_HITS[ip] = [
        timestamp
        for timestamp in hits
        if now - timestamp < 60
    ]

    IP_HITS[ip].append(now)

    if len(IP_HITS[ip]) > ban_threshold:
        BANNED_IPS[ip] = now + ban_time

        print(
            f"[FORTRESS] BANNED IP: {ip}",
            flush=True
        )

        return True

    return False
