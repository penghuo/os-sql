#!/usr/bin/env bash
# Copyright OpenSearch Contributors
# SPDX-License-Identifier: Apache-2.0

# Runs Phase 1 DQE test cases (basic SELECT, WHERE, ORDER BY, LIMIT).
# Usage: ./run-phase1-tests.sh [OPENSEARCH_URL]

set -euo pipefail

OPENSEARCH_URL="${1:-http://localhost:9200}"
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"

echo "=== DQE Phase 1 Tests ==="
python3 "${SCRIPT_DIR}/validate.py" --url "${OPENSEARCH_URL}" --cases "${SCRIPT_DIR}/cases/phase1/" --verbose
