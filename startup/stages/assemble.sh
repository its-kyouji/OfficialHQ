#!/bin/bash
set -e

# === CONFIGURATION ASSEMBLY ===
echo "[INIT] Assembling runtime configurations..."

# === XRAY CONFIGURATION ===
rm -rf /etc/xray/conf
mkdir -p /etc/xray/conf

cp "${APP_DIR}/core/xray/00-base/"*.json \
  /etc/xray/conf/ 2>/dev/null || true

for d in "${APP_DIR}"/core/xray/*-*/; do
  [ -d "$d" ] || continue
  [ -f "$d/inbounds.json" ] || continue

  n="$(basename "$d")"

  cp "$d/inbounds.json" \
    "/etc/xray/conf/20_${n}_inbounds.json"

  if [ -n "$EXIT_IP" ] && \
     [ "${n%%-*}" = "$EXIT_PROTO" ] && \
     [ -f "$d/chain.json" ]; then
    cp "$d/chain.json" \
      "/etc/xray/conf/30_${n}_chain.json"
  fi
done

# === SING-BOX CONFIGURATION ===
rm -rf /etc/singbox/conf
mkdir -p /etc/singbox/conf

cp "${APP_DIR}/core/singbox/00-base/"*.json \
  /etc/singbox/conf/ 2>/dev/null || true

for d in "${APP_DIR}"/core/singbox/*-*/; do
  [ -d "$d" ] || continue
  [ -f "$d/inbounds.json" ] || continue

  n="$(basename "$d")"

  cp "$d/inbounds.json" \
    "/etc/singbox/conf/20_${n}_inbounds.json"

  if [ -n "$EXIT_IP" ] && \
     [ "${n%%-*}" = "$EXIT_PROTO" ] && \
     [ -f "$d/chain.json" ]; then
    cp "$d/chain.json" \
      "/etc/singbox/conf/30_${n}_chain.json"
  fi
done

# === CONNECTOR & RELAY RUNTIME FILES ===
mkdir -p /run/cfg/connector /run/cfg/relay

cp -r "${APP_DIR}/connector/." \
  /run/cfg/connector/ 2>/dev/null || true

cp -r "${APP_DIR}/relay/." \
  /run/cfg/relay/ 2>/dev/null || true

echo "[INIT] Configuration assembly complete."
