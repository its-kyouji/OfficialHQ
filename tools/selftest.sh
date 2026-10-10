#!/bin/bash

set +e

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
TEST_DIR="${SCRIPT_DIR}/tests"

echo "[SELFTEST] Running pre-flight tests..."

bash "${TEST_DIR}/configs.sh"
CONFIG_FAIL=$?

bash "${TEST_DIR}/ports.sh"
PORT_FAIL=$?

HTTP1=0
HTTP2=0

source "${TEST_DIR}/smoke.sh"

bash "${TEST_DIR}/report.sh" \
    "$CONFIG_FAIL" \
    "$PORT_FAIL" \
    "$HTTP1" \
    "$HTTP2"

if [ "$CONFIG_FAIL" -eq 0 ] &&
   [ "$PORT_FAIL" -eq 0 ] &&
   [ "$HTTP1" = "200" ] &&
   [ "$HTTP2" = "200" ]; then
    echo "[SELFTEST] RESULT: PASS"
    exit 0
else
    echo "[SELFTEST] RESULT: FAIL"
    exit 1
fi
