#!/usr/bin/env bash
# Copyright OpenSearch Contributors
# SPDX-License-Identifier: Apache-2.0

# Runs all DQE test suites: phase1, nulls, type-coercion, timezone.
# Usage: ./run-all.sh [OPENSEARCH_URL]

set -euo pipefail

OPENSEARCH_URL="${1:-http://localhost:9200}"
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"

echo "============================================================"
echo "DQE Full Test Suite"
echo "OpenSearch URL: ${OPENSEARCH_URL}"
echo "============================================================"
echo ""

# Set up test data first
"${SCRIPT_DIR}/setup-data.sh" "${OPENSEARCH_URL}"
echo ""

FAILURES=0

run_suite() {
    local name="$1"
    local script="$2"
    echo "--- Running: ${name} ---"
    if "${script}" "${OPENSEARCH_URL}"; then
        echo "--- ${name}: PASSED ---"
    else
        echo "--- ${name}: FAILED ---"
        FAILURES=$((FAILURES + 1))
    fi
    echo ""
}

run_suite "Phase 1 Tests" "${SCRIPT_DIR}/run-phase1-tests.sh"
run_suite "NULL Handling Tests" "${SCRIPT_DIR}/run-null-tests.sh"
run_suite "Type Coercion Tests" "${SCRIPT_DIR}/run-type-coercion-tests.sh"
run_suite "Timezone Tests" "${SCRIPT_DIR}/run-timezone-tests.sh"

echo "============================================================"
if [ "${FAILURES}" -eq 0 ]; then
    echo "All test suites PASSED."
else
    echo "${FAILURES} test suite(s) FAILED."
    exit 1
fi
