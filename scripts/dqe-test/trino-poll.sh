#!/usr/bin/env bash
# Copyright OpenSearch Contributors
# SPDX-License-Identifier: Apache-2.0

# Polls the Trino async statement protocol until the query completes.
# Trino's POST /v1/statement returns a JSON response with a "nextUri" field.
# This script follows nextUri links until the final result (no nextUri) is reached.
#
# Usage: trino-poll.sh <initial_response_json>
#   or:  echo '{"nextUri":"..."}' | trino-poll.sh -
#
# Outputs the final complete response JSON to stdout.

set -euo pipefail

POLL_INTERVAL="${TRINO_POLL_INTERVAL:-0.5}"
MAX_POLLS="${TRINO_MAX_POLLS:-600}"

if [ "${1:-}" = "-" ]; then
    RESPONSE=$(cat)
else
    RESPONSE="${1:?Usage: trino-poll.sh <initial_response_json>}"
fi

poll_count=0

while true; do
    # Extract nextUri from the response
    NEXT_URI=$(echo "${RESPONSE}" | python3 -c "
import sys, json
data = json.load(sys.stdin)
print(data.get('nextUri', ''))" 2>/dev/null || echo "")

    # If no nextUri, the query is complete
    if [ -z "${NEXT_URI}" ]; then
        echo "${RESPONSE}"
        exit 0
    fi

    # Check for error state
    ERROR_STATE=$(echo "${RESPONSE}" | python3 -c "
import sys, json
data = json.load(sys.stdin)
err = data.get('error')
if err:
    print(err.get('message', 'Unknown error'))
else:
    print('')" 2>/dev/null || echo "")

    if [ -n "${ERROR_STATE}" ]; then
        echo "${RESPONSE}"
        echo "ERROR: Trino query failed: ${ERROR_STATE}" >&2
        exit 1
    fi

    poll_count=$((poll_count + 1))
    if [ "${poll_count}" -ge "${MAX_POLLS}" ]; then
        echo "ERROR: Trino polling exceeded ${MAX_POLLS} attempts" >&2
        echo "${RESPONSE}"
        exit 1
    fi

    sleep "${POLL_INTERVAL}"

    # Follow the nextUri
    RESPONSE=$(curl -sf "${NEXT_URI}" \
        -H 'X-Trino-User: test' 2>/dev/null || echo '{"error":{"message":"Failed to poll nextUri"}}')
done
