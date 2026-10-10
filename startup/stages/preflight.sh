#!/bin/bash
set -e

# === PRE-FLIGHT VALIDATION ===
echo "[INIT] Pre-flight config tests..."

FAIL=0

# ---Xray---
/usr/local/bin/xray run \
  -test \
  -confdir /etc/xray/conf || FAIL=1

# ---Sing-box---
/usr/local/bin/sing-box check \
  -C /etc/singbox/conf || FAIL=1

# ---HAProxy---
/usr/sbin/haproxy \
  -c \
  -f /run/cfg/connector/haproxy/haproxy.cfg || FAIL=1

# ---Envoy---
envoy \
  --mode validate \
  -c /run/cfg/connector/envoy/envoy.yaml || FAIL=1

# ---Nginx---
nginx \
  -t \
  -c /run/cfg/connector/nginx/nginx.conf || FAIL=1

# ---Caddy---
caddy validate \
  --config /run/cfg/connector/caddy/Caddyfile \
  --adapter caddyfile || FAIL=1

# ---Python custom relay--- 
python3 -m py_compile \
  /run/cfg/relay/bridge/bridge.py \
  /run/cfg/relay/fortress/fortress.py || FAIL=1

# === VALIDATION RESULT ===
if [ "$FAIL" -ne 0 ]; then
  echo "[INIT][FATAL] Pre-flight tests failed!"
  exit 1
fi

echo "[INIT] All pre-flight tests passed."
