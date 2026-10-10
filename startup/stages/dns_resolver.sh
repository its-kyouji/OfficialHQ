#!/bin/bash
set -e

# === BADVPN NAMESERVER ===

echo "[INIT] DNS resolver setup for BadVPN"

if ! grep -q "8.8.8.8" /etc/resolv.conf; then
  echo "options rotate timeout:1" >> /etc/resolv.conf
  echo "nameserver 8.8.8.8" >> /etc/resolv.conf
  echo "nameserver 1.1.1.1" >> /etc/resolv.conf
fi

echo "[INIT] DNS resolver setup complete."
