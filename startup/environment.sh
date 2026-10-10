#!/bin/bash
set -e
# =====================================
# ENVIRONMENT INITIALIZATION
# =====================================
export PATH_PREFIX="/kyouji"
export UUID="dba7e194-825f-4740-bb98-1689bc7b7ebc"
export SUFFIX="kyouji"

echo "[INIT] Fetching external IP..."

set +e

export SERVER_IP=$(curl -s --max-time 3 \
  -H "Metadata-Flavor: Google" \
  "http://metadata.google.internal/computeMetadata/v1/instance/network-interfaces/0/access-configs/0/external-ip" \
  2>/dev/null)

if [ -z "$SERVER_IP" ]; then
  export SERVER_IP=$(curl -s --max-time 5 ifconfig.me \
    || curl -s --max-time 5 api.ipify.org \
    || echo "127.0.0.1")
fi

set -e
# =====================================
# ROUTING & SECRETS ALLOCATION
# =====================================
export BASE_PATH="${PATH_PREFIX}/${SUFFIX}"
export IN_PASS="kyouji"
export IN_SS="chacha20-ietf-poly1305"

export EXIT_IP="${IP:-}"
export EXIT_PORT="${XPORT:-443}"
export EXIT_PASS="${PASS:-kyouji}"
export EXIT_PATH="${XPATH:-/}"
export EXIT_PROTO="${PROTO:-vless}"
export EXIT_SS="${SS:-chacha20-ietf-poly1305}"
export EXIT_SEC="${SEC:-tls}"
