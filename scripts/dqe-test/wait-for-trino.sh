#!/usr/bin/env bash
# Copyright OpenSearch Contributors
# SPDX-License-Identifier: Apache-2.0

# Waits for a standalone Trino instance to become ready.
# Usage: ./wait-for-trino.sh [TRINO_HOST:PORT] [TIMEOUT_SECONDS]

set -euo pipefail

TRINO_HOST="${1:-localhost:8080}"
TIMEOUT="${2:-120}"

echo "Waiting for Trino at ${TRINO_HOST} (timeout: ${TIMEOUT}s)..."

elapsed=0
interval=3

while [ "${elapsed}" -lt "${TIMEOUT}" ]; do
    # Trino exposes /v1/info which returns {"starting":false} when ready
    if response=$(curl -sf "http://${TRINO_HOST}/v1/info" 2>/dev/null); then
        starting=$(echo "${response}" | python3 -c "import sys,json; print(json.load(sys.stdin).get('starting', True))" 2>/dev/null || echo "True")
        if [ "${starting}" = "False" ]; then
            echo "Trino is ready at ${TRINO_HOST}"
            exit 0
        fi
        echo "  Trino is starting... (${elapsed}s elapsed)"
    else
        echo "  Trino not reachable yet... (${elapsed}s elapsed)"
    fi
    sleep "${interval}"
    elapsed=$((elapsed + interval))
done

echo "ERROR: Trino did not become ready within ${TIMEOUT}s at ${TRINO_HOST}" >&2
exit 1
