#!/bin/bash
set -e

SCRIPT_DIR="/app/startup"
STAGES_DIR="${SCRIPT_DIR}/stages"

source "${STAGES_DIR}/environment.sh"
source "${STAGES_DIR}/assemble.sh"
source "${SCRIPT_DIR}/render.py"
source "${STAGES_DIR}/metadata.sh"
source "${STAGES_DIR}/network.sh"
source "${STAGES_DIR}/ssh.sh"
source "${STAGES_DIR}/preflight.sh"

echo "[INIT] Initialization complete. Starting Supervisord..."
exec /usr/bin/supervisord -c /app/process/supervisord.conf
