#!/bin/bash
set -e

# === RUNTIME METADATA ===

echo "[INIT] Writing runtime.json..."

mkdir -p /etc/xray

cat > /etc/xray/runtime.json <<EOF
{
  "uuid": "${UUID}",
  "suffix": "${SUFFIX}",
  "path_prefix": "${PATH_PREFIX}",
  "pointed": $([ -n "${TARGET_IP}" ] && echo "true" || echo "false"),
  "server_ip": "${SERVER_IP}"
}
EOF

echo "[INIT] Runtime metadata written."
