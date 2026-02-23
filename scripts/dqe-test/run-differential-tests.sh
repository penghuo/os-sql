#!/usr/bin/env bash
# Copyright OpenSearch Contributors
# SPDX-License-Identifier: Apache-2.0

# Runs differential tests: executes each query against both DQE (via OpenSearch
# REST API) and standalone Trino, then compares results using diff-results.py.
#
# Usage: ./run-differential-tests.sh [os_host:port] [trino_host:port]
#
# Prerequisites:
#   - OpenSearch running with DQE plugin: ./gradlew :dqe-plugin:run
#   - Standalone Trino running: docker run -d --name trino-diff -p 8080:8080 \
#       -v $(pwd)/scripts/dqe-test/trino:/etc/trino/catalog trinodb/trino:439
#   - Test data loaded: scripts/dqe-test/setup-data.sh
#   - Trino ready: scripts/dqe-test/wait-for-trino.sh

set -euo pipefail

OS_HOST="${1:-localhost:9200}"
TRINO_HOST="${2:-localhost:8080}"
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
CASES_DIR="${SCRIPT_DIR}/cases/differential"

echo "============================================================"
echo "DQE Differential Tests"
echo "OpenSearch: ${OS_HOST}   Trino: ${TRINO_HOST}"
echo "============================================================"

if [ ! -d "${CASES_DIR}" ]; then
    echo "No differential test cases directory: ${CASES_DIR}"
    echo "0 tests run."
    exit 0
fi

CASE_FILES=$(find "${CASES_DIR}" -name '*.json' 2>/dev/null | sort)
if [ -z "${CASE_FILES}" ]; then
    echo "No differential test case files found in ${CASES_DIR}."
    echo "0 tests run."
    exit 0
fi

PASSED=0
FAILED=0
ERRORS=0
TOTAL=0

for case_file in ${CASE_FILES}; do
    TOTAL=$((TOTAL + 1))

    CASE_ID=$(python3 -c "import json; print(json.load(open('${case_file}')).get('id','?'))")
    CASE_NAME=$(python3 -c "import json; print(json.load(open('${case_file}')).get('name','unnamed'))")
    QUERY=$(python3 -c "import json; print(json.load(open('${case_file}')).get('query',''))")
    IS_DIFF=$(python3 -c "import json; print(json.load(open('${case_file}')).get('differential', False))")

    if [ "${IS_DIFF}" != "True" ]; then
        echo "  SKIP: ${CASE_ID} ${CASE_NAME} (not differential)"
        TOTAL=$((TOTAL - 1))
        continue
    fi

    # 1. Build DQE request payload safely via Python (handles SQL quoting)
    DQE_PAYLOAD=$(python3 -c "import json,sys; print(json.dumps({'query': json.load(open(sys.argv[1])).get('query',''), 'engine': 'dqe'}))" "${case_file}")

    DQE_RESPONSE=$(curl -sf -X POST "http://${OS_HOST}/_plugins/_sql" \
        -H 'Content-Type: application/json' \
        -d "${DQE_PAYLOAD}" 2>/dev/null) || {
        echo "  ERROR: ${CASE_ID} ${CASE_NAME} — DQE request failed"
        ERRORS=$((ERRORS + 1))
        continue
    }

    # 2. Execute via standalone Trino (POST /v1/statement, then poll)
    TRINO_INITIAL=$(curl -sf -X POST "http://${TRINO_HOST}/v1/statement" \
        -H 'X-Trino-User: test' \
        -H 'X-Trino-Catalog: opensearch' \
        -H 'X-Trino-Schema: default' \
        -d "${QUERY}" 2>/dev/null) || {
        echo "  ERROR: ${CASE_ID} ${CASE_NAME} — Trino request failed"
        ERRORS=$((ERRORS + 1))
        continue
    }

    TRINO_RESULT=$(echo "${TRINO_INITIAL}" | "${SCRIPT_DIR}/trino-poll.sh" - 2>/dev/null) || {
        echo "  ERROR: ${CASE_ID} ${CASE_NAME} — Trino polling failed"
        ERRORS=$((ERRORS + 1))
        continue
    }

    # 3. Compare results
    DIFF_OUTPUT=$(python3 "${SCRIPT_DIR}/diff-results.py" "${case_file}" "${DQE_RESPONSE}" "${TRINO_RESULT}" 2>/dev/null) || true
    MATCH=$(echo "${DIFF_OUTPUT}" | python3 -c "import sys,json; print(json.load(sys.stdin).get('match', False))" 2>/dev/null || echo "False")

    if [ "${MATCH}" = "True" ]; then
        PASSED=$((PASSED + 1))
        echo "  PASS: ${CASE_ID} ${CASE_NAME}"
    else
        FAILED=$((FAILED + 1))
        echo "  FAIL: ${CASE_ID} ${CASE_NAME}"
        # Print first few diffs for context
        echo "${DIFF_OUTPUT}" | python3 -c "
import sys, json
data = json.load(sys.stdin)
for err in data.get('errors', [])[:3]:
    print(f'        {err}')
for diff in data.get('row_diffs', [])[:5]:
    row = diff.get('row', '?')
    col = diff.get('column', '?')
    dv = diff.get('dqe_value', '?')
    tv = diff.get('trino_value', '?')
    print(f'        Row {row}, Col {col}: DQE={dv!r} vs Trino={tv!r}')
for diff in data.get('column_diffs', [])[:3]:
    ci = diff.get('column', '?')
    field = diff.get('field', '?')
    print(f'        Column {ci} {field}: DQE={diff.get(\"dqe\")} vs Trino={diff.get(\"trino\")}')
" 2>/dev/null || true
    fi
done

echo "============================================================"
echo "Results: ${PASSED}/${TOTAL} passed, ${FAILED} failed, ${ERRORS} errors"

if [ "${FAILED}" -gt 0 ] || [ "${ERRORS}" -gt 0 ]; then
    exit 1
fi
exit 0
