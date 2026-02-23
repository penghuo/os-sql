#!/usr/bin/env bash
# Copyright OpenSearch Contributors
# SPDX-License-Identifier: Apache-2.0

# Runs type coercion test cases (CAST, implicit widening, cross-type comparisons).
# Usage: ./run-type-coercion-tests.sh [OPENSEARCH_URL]

set -euo pipefail

OPENSEARCH_URL="${1:-http://localhost:9200}"
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"

echo "=== DQE Type Coercion Tests ==="
python3 "${SCRIPT_DIR}/validate.py" --url "${OPENSEARCH_URL}" --cases "${SCRIPT_DIR}/cases/type-coercion/" --verbose
