#!/usr/bin/env bash
#
# DQE Phase 1 Integration Test
#
# Runs against a live OpenSearch cluster with the SQL plugin installed.
# Tests the /_plugins/_trino_sql REST endpoint end-to-end.
#
# Usage:
#   ./run-tests.sh                          # defaults to http://localhost:9200
#   ./run-tests.sh http://my-cluster:9200   # custom endpoint
#
# Prerequisites:
#   - OpenSearch running with opensearch-sql plugin (including dqe module)
#   - curl and jq installed
#
set -euo pipefail

OPENSEARCH_URL="${1:-http://localhost:9200}"
DQE_ENDPOINT="${OPENSEARCH_URL}/_plugins/_trino_sql"
EXPLAIN_ENDPOINT="${OPENSEARCH_URL}/_plugins/_trino_sql/_explain"
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
QUERIES_FILE="${SCRIPT_DIR}/queries.json"

PASS=0
FAIL=0
ERRORS=""

# Colors (disabled if not a terminal)
if [ -t 1 ]; then
  GREEN='\033[0;32m'
  RED='\033[0;31m'
  YELLOW='\033[0;33m'
  NC='\033[0m'
else
  GREEN='' RED='' YELLOW='' NC=''
fi

log_pass() { echo -e "${GREEN}PASS${NC}: $1"; PASS=$((PASS + 1)); }
log_fail() { echo -e "${RED}FAIL${NC}: $1 — $2"; FAIL=$((FAIL + 1)); ERRORS="${ERRORS}\n  - $1: $2"; }
log_info() { echo -e "${YELLOW}INFO${NC}: $1"; }

# --- Preflight ---

log_info "Testing against: ${OPENSEARCH_URL}"

# Check cluster is reachable
if ! curl -sf "${OPENSEARCH_URL}/_cluster/health" > /dev/null 2>&1; then
  echo "ERROR: Cannot reach OpenSearch at ${OPENSEARCH_URL}"
  echo "Start OpenSearch with the SQL plugin before running this test."
  exit 1
fi

log_info "Cluster is reachable"

# --- Setup: Create index and load test data ---

INDEX_NAME="dqe_test_logs"

# Delete index if it exists (clean slate)
curl -sf -X DELETE "${OPENSEARCH_URL}/${INDEX_NAME}" > /dev/null 2>&1 || true

# Create index with mapping
log_info "Creating index ${INDEX_NAME} (2 shards)..."
MAPPING=$(jq -c '{
  settings: .setup.settings,
  mappings: { properties: .setup.mapping.properties }
}' "${QUERIES_FILE}")

CREATE_RESPONSE=$(curl -sf -X PUT "${OPENSEARCH_URL}/${INDEX_NAME}" \
  -H "Content-Type: application/json" \
  -d "${MAPPING}" 2>&1) || {
  echo "ERROR: Failed to create index: ${CREATE_RESPONSE}"
  exit 1
}

# Bulk-index test data
log_info "Loading test data..."
BULK_BODY=""
DATA_COUNT=$(jq '.setup.data | length' "${QUERIES_FILE}")
for i in $(seq 0 $((DATA_COUNT - 1))); do
  DOC=$(jq -c ".setup.data[$i]" "${QUERIES_FILE}")
  BULK_BODY="${BULK_BODY}{\"index\":{\"_id\":\"${i}\"}}\n${DOC}\n"
done

echo -e "${BULK_BODY}" | curl -sf -X POST "${OPENSEARCH_URL}/${INDEX_NAME}/_bulk?refresh=wait_for" \
  -H "Content-Type: application/x-ndjson" \
  --data-binary @- > /dev/null 2>&1 || {
  echo "ERROR: Failed to bulk-index test data"
  exit 1
}

# Verify data loaded
DOC_COUNT=$(curl -sf "${OPENSEARCH_URL}/${INDEX_NAME}/_count" | jq '.count')
log_info "Indexed ${DOC_COUNT} documents into ${INDEX_NAME}"

if [ "${DOC_COUNT}" != "${DATA_COUNT}" ]; then
  echo "ERROR: Expected ${DATA_COUNT} documents, got ${DOC_COUNT}"
  exit 1
fi

# --- Run Tests ---

echo ""
log_info "Running Phase 1 DQE tests..."
echo "---"

TEST_COUNT=$(jq '.tests | length' "${QUERIES_FILE}")
for i in $(seq 0 $((TEST_COUNT - 1))); do
  TEST_NAME=$(jq -r ".tests[$i].name" "${QUERIES_FILE}")
  QUERY=$(jq -r ".tests[$i].query" "${QUERIES_FILE}")
  ENDPOINT=$(jq -r ".tests[$i].endpoint // \"/_plugins/_trino_sql\"" "${QUERIES_FILE}")
  FULL_URL="${OPENSEARCH_URL}${ENDPOINT}"

  # Execute query
  RESPONSE=$(curl -sf -X POST "${FULL_URL}" \
    -H "Content-Type: application/json" \
    -d "{\"query\": \"${QUERY}\"}" 2>&1) || {
    log_fail "${TEST_NAME}" "HTTP request failed: ${RESPONSE}"
    continue
  }

  # Parse assertions
  ASSERTS=$(jq -c ".tests[$i].assert" "${QUERIES_FILE}")

  # Assert: status
  EXPECTED_STATUS=$(echo "${ASSERTS}" | jq -r '.status // empty')
  if [ -n "${EXPECTED_STATUS}" ]; then
    ACTUAL_STATUS=$(echo "${RESPONSE}" | jq -r '.status // empty')
    if [ "${ACTUAL_STATUS}" != "${EXPECTED_STATUS}" ]; then
      log_fail "${TEST_NAME}" "status: expected ${EXPECTED_STATUS}, got ${ACTUAL_STATUS}"
      continue
    fi
  fi

  # Assert: total row count
  EXPECTED_TOTAL=$(echo "${ASSERTS}" | jq -r '.total // empty')
  if [ -n "${EXPECTED_TOTAL}" ]; then
    ACTUAL_TOTAL=$(echo "${RESPONSE}" | jq -r '.total // empty')
    if [ "${ACTUAL_TOTAL}" != "${EXPECTED_TOTAL}" ]; then
      log_fail "${TEST_NAME}" "total: expected ${EXPECTED_TOTAL}, got ${ACTUAL_TOTAL}"
      continue
    fi
  fi

  # Assert: schema length
  EXPECTED_SCHEMA_LEN=$(echo "${ASSERTS}" | jq -r '.schema_length // empty')
  if [ -n "${EXPECTED_SCHEMA_LEN}" ]; then
    ACTUAL_SCHEMA_LEN=$(echo "${RESPONSE}" | jq '.schema | length')
    if [ "${ACTUAL_SCHEMA_LEN}" != "${EXPECTED_SCHEMA_LEN}" ]; then
      log_fail "${TEST_NAME}" "schema length: expected ${EXPECTED_SCHEMA_LEN}, got ${ACTUAL_SCHEMA_LEN}"
      continue
    fi
  fi

  # Assert: schema column names
  EXPECTED_NAMES=$(echo "${ASSERTS}" | jq -c '.schema_names // empty')
  if [ "${EXPECTED_NAMES}" != "" ] && [ "${EXPECTED_NAMES}" != "null" ]; then
    ACTUAL_NAMES=$(echo "${RESPONSE}" | jq -c '[.schema[].name]')
    if [ "${ACTUAL_NAMES}" != "${EXPECTED_NAMES}" ]; then
      log_fail "${TEST_NAME}" "schema names: expected ${EXPECTED_NAMES}, got ${ACTUAL_NAMES}"
      continue
    fi
  fi

  # Assert: all rows in a column match a value
  ALL_MATCH_COL=$(echo "${ASSERTS}" | jq -r '.all_rows_match.column_index // empty')
  if [ -n "${ALL_MATCH_COL}" ]; then
    EXPECTED_VAL=$(echo "${ASSERTS}" | jq -r '.all_rows_match.value')
    BAD_ROWS=$(echo "${RESPONSE}" | jq "[.datarows[] | select(.[${ALL_MATCH_COL}] != ${EXPECTED_VAL})] | length")
    if [ "${BAD_ROWS}" != "0" ]; then
      log_fail "${TEST_NAME}" "${BAD_ROWS} rows have column[${ALL_MATCH_COL}] != ${EXPECTED_VAL}"
      continue
    fi
  fi

  # Assert: sum of COUNT column equals expected
  EXPECTED_COUNT_SUM=$(echo "${ASSERTS}" | jq -r '.total_count_sum // empty')
  if [ -n "${EXPECTED_COUNT_SUM}" ]; then
    ACTUAL_SUM=$(echo "${RESPONSE}" | jq '[.datarows[][1]] | add')
    if [ "${ACTUAL_SUM}" != "${EXPECTED_COUNT_SUM}" ]; then
      log_fail "${TEST_NAME}" "count sum: expected ${EXPECTED_COUNT_SUM}, got ${ACTUAL_SUM}"
      continue
    fi
  fi

  # Assert: minimum number of rows
  EXPECTED_MIN_ROWS=$(echo "${ASSERTS}" | jq -r '.min_rows // empty')
  if [ -n "${EXPECTED_MIN_ROWS}" ]; then
    ACTUAL_ROWS=$(echo "${RESPONSE}" | jq '.datarows | length')
    if [ "${ACTUAL_ROWS}" -lt "${EXPECTED_MIN_ROWS}" ]; then
      log_fail "${TEST_NAME}" "min rows: expected >= ${EXPECTED_MIN_ROWS}, got ${ACTUAL_ROWS}"
      continue
    fi
  fi

  # Assert: descending order on a column
  DESC_COL=$(echo "${ASSERTS}" | jq -r '.descending_column_index // empty')
  if [ -n "${DESC_COL}" ]; then
    # Check each consecutive pair is non-increasing
    ROW_COUNT=$(echo "${RESPONSE}" | jq '.datarows | length')
    SORTED=true
    for j in $(seq 0 $((ROW_COUNT - 2))); do
      CURR=$(echo "${RESPONSE}" | jq ".datarows[$j][${DESC_COL}]")
      NEXT=$(echo "${RESPONSE}" | jq ".datarows[$((j + 1))][${DESC_COL}]")
      if [ "$(echo "${CURR} < ${NEXT}" | bc -l 2>/dev/null || echo 0)" = "1" ]; then
        SORTED=false
        break
      fi
    done
    if [ "${SORTED}" != "true" ]; then
      log_fail "${TEST_NAME}" "column[${DESC_COL}] is not in descending order"
      continue
    fi
  fi

  # Assert: response contains substrings (for EXPLAIN)
  CONTAINS=$(echo "${ASSERTS}" | jq -c '.contains // empty')
  if [ "${CONTAINS}" != "" ] && [ "${CONTAINS}" != "null" ]; then
    ALL_FOUND=true
    MISSING=""
    for k in $(echo "${CONTAINS}" | jq -r '.[]'); do
      if ! echo "${RESPONSE}" | grep -q "${k}"; then
        ALL_FOUND=false
        MISSING="${k}"
        break
      fi
    done
    if [ "${ALL_FOUND}" != "true" ]; then
      log_fail "${TEST_NAME}" "response missing expected string: ${MISSING}"
      continue
    fi
  fi

  log_pass "${TEST_NAME}"
done

# --- Cleanup ---

log_info "Cleaning up..."
curl -sf -X DELETE "${OPENSEARCH_URL}/${INDEX_NAME}" > /dev/null 2>&1 || true

# --- Summary ---

echo ""
echo "=== DQE Phase 1 Integration Test Results ==="
echo "  Passed: ${PASS}"
echo "  Failed: ${FAIL}"
echo "  Total:  $((PASS + FAIL))"

if [ "${FAIL}" -gt 0 ]; then
  echo ""
  echo "Failures:"
  echo -e "${ERRORS}"
  exit 1
fi

echo ""
echo "All tests passed."
exit 0
