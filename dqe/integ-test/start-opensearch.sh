#!/usr/bin/env bash
# Copyright OpenSearch Contributors
# SPDX-License-Identifier: Apache-2.0

# Starts OpenSearch with the SQL plugin via Gradle's testClusters.
# The cluster runs in the foreground (Ctrl-C to stop).
#
# Usage: ./start-opensearch.sh          # foreground
#        ./start-opensearch.sh --daemon  # background (PID saved)

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"
PID_FILE="${SCRIPT_DIR}/.opensearch.pid"

if [ "${1:-}" = "--daemon" ]; then
    echo "Starting OpenSearch in background..."
    "${REPO_ROOT}/gradlew" -p "${REPO_ROOT}" :opensearch-sql-plugin:run \
        > "${SCRIPT_DIR}/.opensearch-daemon.log" 2>&1 &
    GRADLE_PID=$!
    echo "${GRADLE_PID}" > "${PID_FILE}"
    echo "Gradle PID: ${GRADLE_PID} (saved to ${PID_FILE})"
    echo "Waiting for OpenSearch to be ready..."
    for i in $(seq 1 60); do
        if curl -sf http://localhost:9200/_cluster/health > /dev/null 2>&1; then
            echo "OpenSearch ready after ${i} attempts."
            curl -sf http://localhost:9200/ 2>/dev/null \
                | python3 -c "import sys,json; d=json.load(sys.stdin); print(f\"  Cluster: {d['cluster_name']}, Version: {d['version']['number']}\")"
            exit 0
        fi
        sleep 3
    done
    echo "ERROR: OpenSearch did not start within 180s. Check .opensearch-daemon.log"
    exit 1
else
    echo "Starting OpenSearch in foreground (Ctrl-C to stop)..."
    exec "${REPO_ROOT}/gradlew" -p "${REPO_ROOT}" :opensearch-sql-plugin:run
fi
