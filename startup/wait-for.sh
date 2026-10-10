#!/bin/sh

# === WAIT CONFIGURATION ===
T="${WAIT_TIMEOUT:-60}"

# === WAIT FOR HOSTS ===
while [ $# -gt 0 ] && [ "$1" != "--" ]; do
  h="${1%:*}"
  p="${1##*:}"
  i=0

  # === CHECK HOST AVAILABILITY ===
  until nc -z "$h" "$p" 2>/dev/null; do
    i=$((i+1))

    # === HANDLE TIMEOUT ===
    [ "$i" -ge "$((T*2))" ] && {
      echo "[WAIT] timeout $h:$p, continuing"
      break
    }

    sleep 0.5
  done

  shift
done

# === EXECUTE COMMAND ===
shift
exec "$@"
