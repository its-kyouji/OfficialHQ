#!/bin/bash

CONFIG_FAIL="${1:-1}"
PORT_FAIL="${2:-1}"
HTTP1="${3:-0}"
HTTP2="${4:-0}"

echo
echo "========== SELFTEST REPORT =========="

if [ "$CONFIG_FAIL" -eq 0 ]; then
    echo "[PASS] Pre-flight Configs"
else
    echo "[FAIL] Pre-flight Configs"
fi

if [ "$PORT_FAIL" -eq 0 ]; then
    echo "[PASS] Listening Ports"
else
    echo "[FAIL] Listening Ports"
fi

if [ "$HTTP1" = "200" ]; then
    echo "[PASS] HTTP Smoke Test"
else
    echo "[FAIL] HTTP Smoke Test (HTTP ${HTTP1})"
fi

if [ "$HTTP2" = "200" ]; then
    echo "[PASS] HTTPS/Upstream Smoke Test"
else
    echo "[FAIL] HTTPS/Upstream Smoke Test (HTTP ${HTTP2})"
fi

echo "========== FIX ME! =========="
