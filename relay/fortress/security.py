import time
from collections import defaultdict
# =================
# IP RATE LIMITING
# =================
IP_HITS = defaultdict(list)
BANNED_IPS = {}

_LAST_CLEANUP = 0


def cleanup_stale_state(now):
    global _LAST_CLEANUP

    # Run cleanup at most once every 60 seconds
    if now - _LAST_CLEANUP < 60:
        return

    _LAST_CLEANUP = now

    # Remove stale IP hit records
    stale_ips = [
        ip
        for ip, hits in IP_HITS.items()
        if not hits or now - hits[-1] >= 60
    ]

    for ip in stale_ips:
        del IP_HITS[ip]

    # Remove expired IP bans
    expired_ips = [
        ip
        for ip, expiry in BANNED_IPS.items()
        if now >= expiry
    ]

    for ip in expired_ips:
        del BANNED_IPS[ip]


def get_client_ip(lines, fallback):
    # Resolve client IP from forwarded headers
    for line in lines:
        if line.lower().startswith(b"x-forwarded-for:"):
            value = line.split(b":", 1)[1].strip()

            if value:
                return value.rsplit(
                    b",", 1
                )[-1].strip().decode("utf-8", "ignore")

    return fallback


def is_banned(ip, ban_threshold, ban_time):
    now = time.time()

    # Clean stale records and expired bans
    cleanup_stale_state(now)

    # Check whether the IP is still banned
    expiry = BANNED_IPS.get(ip)

    if expiry is not None:
        if now >= expiry:
            del BANNED_IPS[ip]
        else:
            return True

    # Update the 60-second request window
    hits = IP_HITS[ip]

    IP_HITS[ip] = [
        timestamp
        for timestamp in hits
        if now - timestamp < 60
    ]

    IP_HITS[ip].append(now)

    # Apply a temporary ban when the threshold is exceeded
    if len(IP_HITS[ip]) > ban_threshold:
        BANNED_IPS[ip] = now + ban_time

        print(
            f"[FORTRESS] BANNED IP: {ip}",
            flush=True
        )

        return True

    return False
