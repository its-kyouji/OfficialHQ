#!/bin/bash
set -e
# =====================
# STARTUP ORCHESTRATOR
# =====================
SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
APP_DIR="$(dirname "$SCRIPT_DIR")"
STAGES_DIR="${SCRIPT_DIR}/stages"

export APP_DIR
export SCRIPT_DIR
export STAGES_DIR


# === ENVIRONMENT INITIALIZATION ===
source "${STAGES_DIR}/environment.sh"

# === CONFIGURATION ASSEMBLY ===
source "${STAGES_DIR}/assemble.sh"

# === TEMPLATE RENDERING ===
python3 "${SCRIPT_DIR}/render.py"

# === RUNTIME METADATA ===
source "${STAGES_DIR}/metadata.sh"

# === BADVPN DNS RESOLVER ===
source "${STAGES_DIR}/dns_resolver.sh"

# === SSH INITIALIZATION ===
source "${STAGES_DIR}/ssh.sh"

# === PRE-FLIGHT VALIDATION ===
source "${STAGES_DIR}/preflight.sh"

# === PROCESS MANAGER ===
echo "[INIT] Initialization complete. Starting Supervisord..."

exec /usr/bin/supervisord \
  -c "${APP_DIR}/process/supervisord.conf"
