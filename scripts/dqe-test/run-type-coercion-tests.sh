#!/usr/bin/env bash
# Copyright OpenSearch Contributors
# SPDX-License-Identifier: Apache-2.0

# Runs type coercion test cases (30 cases).
# Usage: ./run-type-coercion-tests.sh [OPENSEARCH_URL]

set -euo pipefail

OPENSEARCH_URL="${1:-http://localhost:9200}"
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"

echo "=== DQE Type Coercion Tests ==="

TOTAL_PASS=0
TOTAL_FAIL=0
TOTAL=0

for case_file in $(find "${SCRIPT_DIR}/cases/type_coercion" -name '*.json' | sort); do
    TOTAL=$((TOTAL + 1))
    python3 "${SCRIPT_DIR}/validate.py" --url "${OPENSEARCH_URL}" --case "${case_file}" --verbose && TOTAL_PASS=$((TOTAL_PASS + 1)) || TOTAL_FAIL=$((TOTAL_FAIL + 1))
done

echo ""
echo "============================================================"
echo "Type Coercion Total: ${TOTAL_PASS}/${TOTAL} passed, ${TOTAL_FAIL} failed"
echo "============================================================"

[ "${TOTAL_FAIL}" -eq 0 ] || exit 1
