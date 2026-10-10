#!/bin/bash

set +e

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
TEST_DIR="${SCRIPT_DIR}/tests"

echo "[SELFTEST] Running Pre-flight tests..."

FAIL=0

bash "${TEST_DIR}/configs.sh" || FAIL=1
bash "${TEST_DIR}/ports.sh" || FAIL=1

HTTP1=0
HTTP2=0

source "${TEST_DIR}/smoke.sh"

bash "${TEST_DIR}/report.sh" "$FAIL" "$HTTP1" "$HTTP2"

[ "$FAIL" -eq 0 ] && [ "$HTTP1" = "200" ] && [ "$HTTP2" = "200" ]
