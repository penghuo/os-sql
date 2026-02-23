#!/usr/bin/env bash
# Copyright OpenSearch Contributors
# SPDX-License-Identifier: Apache-2.0

# Sets up test datasets for DQE testing by creating indices with mappings
# and bulk-indexing test data via the OpenSearch REST API.
#
# Usage: ./setup-data.sh [OPENSEARCH_URL]
#   Default: http://localhost:9200

set -euo pipefail

OPENSEARCH_URL="${1:-http://localhost:9200}"
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
DATA_DIR="${SCRIPT_DIR}/data"
MAPPINGS_DIR="${DATA_DIR}/mappings"
BULK_DIR="${DATA_DIR}/bulk"

echo "=== DQE Test Data Setup ==="
echo "OpenSearch URL: ${OPENSEARCH_URL}"
echo ""

# Wait for OpenSearch to be ready
echo "Waiting for OpenSearch..."
for i in $(seq 1 30); do
    if curl -sf "${OPENSEARCH_URL}/_cluster/health" > /dev/null 2>&1; then
        echo "OpenSearch is ready."
        break
    fi
    if [ "$i" -eq 30 ]; then
        echo "ERROR: OpenSearch not reachable at ${OPENSEARCH_URL} after 30 attempts."
        exit 1
    fi
    sleep 2
done

# Function to create an index with mapping and load bulk data
setup_index() {
    local index_name="$1"
    local mapping_file="$2"
    local bulk_file="$3"

    echo "--- Setting up index: ${index_name} ---"

    # Delete index if it exists
    curl -sf -X DELETE "${OPENSEARCH_URL}/${index_name}" > /dev/null 2>&1 || true

    # Create index with mapping
    if [ -f "${mapping_file}" ]; then
        echo "  Creating index with mapping from $(basename "${mapping_file}")"
        response=$(curl -sf -X PUT "${OPENSEARCH_URL}/${index_name}" \
            -H 'Content-Type: application/json' \
            -d @"${mapping_file}" 2>&1) || {
            echo "  ERROR: Failed to create index ${index_name}: ${response}"
            return 1
        }
        echo "  Index created."
    fi

    # Bulk-load data
    if [ -f "${bulk_file}" ]; then
        echo "  Loading bulk data from $(basename "${bulk_file}")"
        response=$(curl -sf -X POST "${OPENSEARCH_URL}/${index_name}/_bulk" \
            -H 'Content-Type: application/x-ndjson' \
            --data-binary @"${bulk_file}" 2>&1) || {
            echo "  ERROR: Failed to bulk load ${index_name}: ${response}"
            return 1
        }
        # Check for errors in bulk response
        has_errors=$(echo "${response}" | python3 -c "import sys,json; print(json.load(sys.stdin).get('errors', False))" 2>/dev/null || echo "unknown")
        if [ "${has_errors}" = "True" ]; then
            echo "  WARNING: Bulk load had errors. Check response."
        else
            echo "  Bulk load complete."
        fi
    fi

    # Refresh index
    curl -sf -X POST "${OPENSEARCH_URL}/${index_name}/_refresh" > /dev/null 2>&1
    echo ""
}

# Set up each test index
setup_index "dqe_test_employees" \
    "${MAPPINGS_DIR}/dqe_test_employees.json" \
    "${BULK_DIR}/dqe_test_employees.ndjson"

setup_index "dqe_test_nulls" \
    "${MAPPINGS_DIR}/dqe_test_nulls.json" \
    "${BULK_DIR}/dqe_test_nulls.ndjson"

setup_index "dqe_test_datatypes" \
    "${MAPPINGS_DIR}/dqe_test_datatypes.json" \
    "${BULK_DIR}/dqe_test_datatypes.ndjson"

echo "=== DQE Test Data Setup Complete ==="
echo ""
echo "Indices created:"
curl -sf "${OPENSEARCH_URL}/_cat/indices/dqe_test_*?v&h=index,docs.count,store.size" 2>/dev/null || true
