#!/bin/bash
set -e

# === SSH ACCOUNT CONFIGURATION ===
echo "[INIT] Configuring SSH accounts..."

ACCOUNTS="${ACCOUNTS:-kyouji:kyouji}"

echo "$ACCOUNTS" | tr ',' '\n' |
while IFS=: read -r u p; do
  [ -z "$u" ] || [ -z "$p" ] && continue

  id -u "$u" >/dev/null 2>&1 ||
    useradd -M -s /bin/false "$u"

  echo "$u:$p" | chpasswd
done

# === SSH SHELL AUTHORIZATION ===
if ! grep -q "/bin/false" /etc/shells; then
  echo "/bin/false" >> /etc/shells
fi

# === SSH HOST KEY PROVISIONING ===
if [ ! -f /etc/dropbear/dropbear_rsa_host_key ]; then
  mkdir -p /etc/dropbear

  dropbearkey \
    -t rsa \
    -f /etc/dropbear/dropbear_rsa_host_key \
    -s 2048 >/dev/null 2>&1
fi

ssh-keygen -A >/dev/null 2>&1
