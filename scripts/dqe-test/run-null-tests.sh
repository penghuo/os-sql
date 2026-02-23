#!/usr/bin/env bash
# Copyright OpenSearch Contributors
# SPDX-License-Identifier: Apache-2.0

# Runs NULL handling test cases (IS NULL, IS NOT NULL, COALESCE, NULL-heavy columns).
# Usage: ./run-null-tests.sh [OPENSEARCH_URL]

set -euo pipefail

OPENSEARCH_URL="${1:-http://localhost:9200}"
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"

echo "=== DQE NULL Handling Tests ==="
python3 "${SCRIPT_DIR}/validate.py" --url "${OPENSEARCH_URL}" --cases "${SCRIPT_DIR}/cases/nulls/" --verbose
