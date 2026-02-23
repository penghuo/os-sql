#!/usr/bin/env bash
# Copyright OpenSearch Contributors
# SPDX-License-Identifier: Apache-2.0

# Runs timezone handling test cases (date/timestamp with timezone, date_nanos, custom formats).
# Usage: ./run-timezone-tests.sh [OPENSEARCH_URL]

set -euo pipefail

OPENSEARCH_URL="${1:-http://localhost:9200}"
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"

echo "=== DQE Timezone Tests ==="
python3 "${SCRIPT_DIR}/validate.py" --url "${OPENSEARCH_URL}" --cases "${SCRIPT_DIR}/cases/timezone/" --verbose
