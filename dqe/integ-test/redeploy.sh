#!/usr/bin/env bash
# Copyright OpenSearch Contributors
# SPDX-License-Identifier: Apache-2.0

# TDD inner loop: rebuild the SQL plugin, restart OpenSearch, reload test data.
#
# Usage: ./redeploy.sh                          # full cycle
#        ./redeploy.sh --skip-data              # skip data reload
#        ./redeploy.sh --module dqe             # only recompile one module first

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"
OPENSEARCH_URL="${OPENSEARCH_URL:-http://localhost:9200}"
SKIP_DATA=false
MODULE=""

while [[ $# -gt 0 ]]; do
    case "$1" in
        --skip-data) SKIP_DATA=true; shift ;;
        --module) MODULE="$2"; shift 2 ;;
        *) shift ;;
    esac
done

echo "=== Redeploy Cycle ==="

# Step 1: Build
if [ -n "${MODULE}" ]; then
    echo "[1/4] Compiling :${MODULE}..."
    "${REPO_ROOT}/gradlew" -p "${REPO_ROOT}" ":${MODULE}:compileJava" -q
fi
echo "[1/4] Assembling plugin..."
"${REPO_ROOT}/gradlew" -p "${REPO_ROOT}" assemble -x test -x javadoc -q

# Step 2: Stop
echo "[2/4] Stopping OpenSearch..."
"${SCRIPT_DIR}/stop-opensearch.sh" 2>/dev/null || true
sleep 2

# Step 3: Start
echo "[3/4] Starting OpenSearch..."
"${SCRIPT_DIR}/start-opensearch.sh" --daemon

# Step 4: Reload data
if [ "${SKIP_DATA}" = false ]; then
    echo "[4/4] Loading test data..."
    "${SCRIPT_DIR}/setup-data.sh" "${OPENSEARCH_URL}"
else
    echo "[4/4] Skipping data reload (--skip-data)"
fi

echo ""
echo "=== Redeploy complete. Ready to run tests. ==="
