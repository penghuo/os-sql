#!/usr/bin/env bash
# Copyright OpenSearch Contributors
# SPDX-License-Identifier: Apache-2.0

# Runs Phase 1 DQE test cases (basic SELECT, WHERE, ORDER BY, LIMIT).
# Scans all subdirectories under cases/phase1/ for .json test files.
# Usage: ./run-phase1-tests.sh [OPENSEARCH_URL]

set -euo pipefail

OPENSEARCH_URL="${1:-http://localhost:9200}"
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
CASES_DIR="${SCRIPT_DIR}/cases/phase1"

echo "=== DQE Phase 1 Tests ==="

TOTAL_PASS=0
TOTAL_FAIL=0
TOTAL=0

for subdir in basic_select where_predicates type_specific order_by_limit multi_shard expressions error_cases; do
    dir="${CASES_DIR}/${subdir}"
    if [ -d "${dir}" ]; then
        echo ""
        echo "--- ${subdir} ---"
        for case_file in $(find "${dir}" -name '*.json' | sort); do
            TOTAL=$((TOTAL + 1))
            python3 "${SCRIPT_DIR}/validate.py" --url "${OPENSEARCH_URL}" --case "${case_file}" --verbose && TOTAL_PASS=$((TOTAL_PASS + 1)) || TOTAL_FAIL=$((TOTAL_FAIL + 1))
        done
    fi
done

echo ""
echo "============================================================"
echo "Phase 1 Total: ${TOTAL_PASS}/${TOTAL} passed, ${TOTAL_FAIL} failed"
echo "============================================================"

[ "${TOTAL_FAIL}" -eq 0 ] || exit 1
