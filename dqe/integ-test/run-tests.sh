#!/usr/bin/env bash
#
# DQE Phase 1 Integration Test Runner
#
# Tests /_plugins/_trino_sql against a live OpenSearch cluster.
# Reads test data from data/, queries from queries/, writes report to reports/.
#
# Usage:
#   ./run-tests.sh                          # defaults to http://localhost:9200
#   ./run-tests.sh http://my-cluster:9200
#
# Prerequisites: curl, jq, bc
#
set -euo pipefail

OPENSEARCH_URL="${1:-http://localhost:9200}"
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
DATA_DIR="${SCRIPT_DIR}/data"
QUERIES_DIR="${SCRIPT_DIR}/queries"
REPORTS_DIR="${SCRIPT_DIR}/reports"
REPORT_FILE="${REPORTS_DIR}/report-$(date +%Y%m%d-%H%M%S).txt"

PASS=0
FAIL=0
TOTAL=0
RESULTS=()

# Colors (disabled if not a terminal)
if [ -t 1 ]; then
  GREEN='\033[0;32m'; RED='\033[0;31m'; YELLOW='\033[0;33m'; BOLD='\033[1m'; NC='\033[0m'
else
  GREEN='' RED='' YELLOW='' BOLD='' NC=''
fi

log_pass() { echo -e "  ${GREEN}PASS${NC}  $1"; }
log_fail() { echo -e "  ${RED}FAIL${NC}  $1"; echo "        $2"; }
log_info() { echo -e "${YELLOW}>>>${NC} $1"; }

record() {
  local id="$1" name="$2" status="$3" detail="${4:-}"
  RESULTS+=("${id}|${status}|${name}|${detail}")
  TOTAL=$((TOTAL + 1))
  if [ "${status}" = "PASS" ]; then PASS=$((PASS + 1)); else FAIL=$((FAIL + 1)); fi
}

# =====================================================================
# Preflight
# =====================================================================

log_info "DQE Phase 1 Integration Tests"
log_info "Cluster: ${OPENSEARCH_URL}"
echo ""

if ! curl -sf "${OPENSEARCH_URL}/_cluster/health" > /dev/null 2>&1; then
  echo "ERROR: Cannot reach OpenSearch at ${OPENSEARCH_URL}"
  exit 1
fi
log_info "Cluster reachable"

# =====================================================================
# Setup: Load test data from data/
# =====================================================================

for DATA_FILE in "${DATA_DIR}"/*.json; do
  INDEX_NAME=$(jq -r '.index' "${DATA_FILE}")
  log_info "Setting up index: ${INDEX_NAME}"

  # Delete if exists
  curl -sf -X DELETE "${OPENSEARCH_URL}/${INDEX_NAME}" > /dev/null 2>&1 || true

  # Create with mapping
  BODY=$(jq -c '{settings: .settings, mappings: {properties: .mapping.properties}}' "${DATA_FILE}")
  curl -sf -X PUT "${OPENSEARCH_URL}/${INDEX_NAME}" \
    -H "Content-Type: application/json" -d "${BODY}" > /dev/null

  # Bulk index documents
  BULK=""
  DOC_COUNT=$(jq '.documents | length' "${DATA_FILE}")
  for i in $(seq 0 $((DOC_COUNT - 1))); do
    DOC_ID=$(jq -r ".documents[$i]._id" "${DATA_FILE}")
    DOC=$(jq -c ".documents[$i] | del(._id)" "${DATA_FILE}")
    BULK="${BULK}{\"index\":{\"_id\":\"${DOC_ID}\"}}\n${DOC}\n"
  done
  echo -e "${BULK}" | curl -sf -X POST "${OPENSEARCH_URL}/${INDEX_NAME}/_bulk?refresh=wait_for" \
    -H "Content-Type: application/x-ndjson" --data-binary @- > /dev/null

  ACTUAL_COUNT=$(curl -sf "${OPENSEARCH_URL}/${INDEX_NAME}/_count" | jq '.count')
  log_info "Indexed ${ACTUAL_COUNT}/${DOC_COUNT} documents"
done

# =====================================================================
# Run queries
# =====================================================================

echo ""
log_info "Running queries..."
echo ""

for QUERY_FILE in "${QUERIES_DIR}"/Q*.json; do
  QID=$(jq -r '.id' "${QUERY_FILE}")
  QNAME=$(jq -r '.name' "${QUERY_FILE}")
  QUERY=$(jq -r '.query' "${QUERY_FILE}")
  ENDPOINT=$(jq -r '.endpoint' "${QUERY_FILE}")
  FULL_URL="${OPENSEARCH_URL}${ENDPOINT}"

  # Execute
  RESPONSE=$(curl -sf -X POST "${FULL_URL}" \
    -H "Content-Type: application/json" \
    -d "{\"query\": \"${QUERY}\"}" 2>&1) || {
    log_fail "${QID}: ${QNAME}" "HTTP request failed"
    record "${QID}" "${QNAME}" "FAIL" "HTTP request failed"
    continue
  }

  EXPECTED=$(jq -c '.expected' "${QUERY_FILE}")
  FAILED=""

  # --- Assert: contains (for EXPLAIN) ---
  CONTAINS=$(echo "${EXPECTED}" | jq -c '.contains // empty')
  if [ -n "${CONTAINS}" ] && [ "${CONTAINS}" != "null" ]; then
    for STR in $(echo "${CONTAINS}" | jq -r '.[]'); do
      if ! echo "${RESPONSE}" | grep -q "${STR}"; then
        FAILED="Response missing '${STR}'"
        break
      fi
    done
    if [ -z "${FAILED}" ]; then
      log_pass "${QID}: ${QNAME}"
      record "${QID}" "${QNAME}" "PASS"
    else
      log_fail "${QID}: ${QNAME}" "${FAILED}"
      record "${QID}" "${QNAME}" "FAIL" "${FAILED}"
    fi
    continue
  fi

  # --- Assert: status ---
  EXP_STATUS=$(echo "${EXPECTED}" | jq -r '.status')
  ACT_STATUS=$(echo "${RESPONSE}" | jq -r '.status')
  if [ "${ACT_STATUS}" != "${EXP_STATUS}" ]; then
    FAILED="status: expected ${EXP_STATUS}, got ${ACT_STATUS}"
  fi

  # --- Assert: total ---
  EXP_TOTAL=$(echo "${EXPECTED}" | jq -r '.total')
  ACT_TOTAL=$(echo "${RESPONSE}" | jq -r '.total')
  if [ -z "${FAILED}" ] && [ "${ACT_TOTAL}" != "${EXP_TOTAL}" ]; then
    FAILED="total: expected ${EXP_TOTAL}, got ${ACT_TOTAL}"
  fi

  # --- Assert: schema ---
  EXP_SCHEMA=$(echo "${EXPECTED}" | jq -c '.schema // empty')
  if [ -z "${FAILED}" ] && [ -n "${EXP_SCHEMA}" ] && [ "${EXP_SCHEMA}" != "null" ]; then
    ACT_SCHEMA=$(echo "${RESPONSE}" | jq -c '.schema')
    if [ "${ACT_SCHEMA}" != "${EXP_SCHEMA}" ]; then
      FAILED="schema mismatch\n        expected: ${EXP_SCHEMA}\n        actual:   ${ACT_SCHEMA}"
    fi
  fi

  # --- Assert: datarows (with optional sort) ---
  EXP_ROWS=$(echo "${EXPECTED}" | jq -c '.datarows // empty')
  if [ -z "${FAILED}" ] && [ -n "${EXP_ROWS}" ] && [ "${EXP_ROWS}" != "null" ]; then
    SORT_COLS=$(jq -c '.sort_before_compare // empty' "${QUERY_FILE}")

    if [ -n "${SORT_COLS}" ] && [ "${SORT_COLS}" != "null" ]; then
      # Build a jq sort expression from column indices
      # e.g., [0, 1] -> sort_by(.[0], .[1])
      SORT_KEYS=$(echo "${SORT_COLS}" | jq -r '[.[] | ".[" + tostring + "]"] | join(", ")')
      SORT_EXPR="sort_by(${SORT_KEYS})"
      ACT_SORTED=$(echo "${RESPONSE}" | jq -c ".datarows | ${SORT_EXPR}")
      EXP_SORTED=$(echo "${EXP_ROWS}" | jq -c "${SORT_EXPR}")
    else
      ACT_SORTED=$(echo "${RESPONSE}" | jq -c '.datarows')
      EXP_SORTED="${EXP_ROWS}"
    fi

    if [ "${ACT_SORTED}" != "${EXP_SORTED}" ]; then
      # Show first diff
      ACT_LEN=$(echo "${ACT_SORTED}" | jq 'length')
      EXP_LEN=$(echo "${EXP_SORTED}" | jq 'length')
      if [ "${ACT_LEN}" != "${EXP_LEN}" ]; then
        FAILED="datarows: expected ${EXP_LEN} rows, got ${ACT_LEN}"
      else
        # Find first differing row
        for r in $(seq 0 $((ACT_LEN - 1))); do
          ACT_ROW=$(echo "${ACT_SORTED}" | jq -c ".[$r]")
          EXP_ROW=$(echo "${EXP_SORTED}" | jq -c ".[$r]")
          if [ "${ACT_ROW}" != "${EXP_ROW}" ]; then
            FAILED="datarows[${r}] mismatch\n        expected: ${EXP_ROW}\n        actual:   ${ACT_ROW}"
            break
          fi
        done
      fi
    fi
  fi

  if [ -z "${FAILED}" ]; then
    log_pass "${QID}: ${QNAME}"
    record "${QID}" "${QNAME}" "PASS"
  else
    log_fail "${QID}: ${QNAME}" "${FAILED}"
    record "${QID}" "${QNAME}" "FAIL" "${FAILED}"
  fi
done

# =====================================================================
# Cleanup
# =====================================================================

echo ""
log_info "Cleaning up test indices..."
for DATA_FILE in "${DATA_DIR}"/*.json; do
  INDEX_NAME=$(jq -r '.index' "${DATA_FILE}")
  curl -sf -X DELETE "${OPENSEARCH_URL}/${INDEX_NAME}" > /dev/null 2>&1 || true
done

# =====================================================================
# Report
# =====================================================================

mkdir -p "${REPORTS_DIR}"

{
  echo "============================================"
  echo "  DQE Phase 1 Integration Test Report"
  echo "  $(date '+%Y-%m-%d %H:%M:%S')"
  echo "  Cluster: ${OPENSEARCH_URL}"
  echo "============================================"
  echo ""
  printf "  %-6s %-6s %s\n" "ID" "Result" "Name"
  printf "  %-6s %-6s %s\n" "------" "------" "----"
  for ENTRY in "${RESULTS[@]}"; do
    IFS='|' read -r ID STATUS NAME DETAIL <<< "${ENTRY}"
    printf "  %-6s %-6s %s\n" "${ID}" "${STATUS}" "${NAME}"
    if [ -n "${DETAIL}" ]; then
      echo "               ${DETAIL}"
    fi
  done
  echo ""
  echo "  Passed: ${PASS}/${TOTAL}"
  echo "  Failed: ${FAIL}/${TOTAL}"
  echo ""
  if [ "${FAIL}" -eq 0 ]; then
    echo "  ALL TESTS PASSED"
  else
    echo "  SOME TESTS FAILED"
  fi
  echo "============================================"
} | tee "${REPORT_FILE}"

echo ""
log_info "Report saved to: ${REPORT_FILE}"

if [ "${FAIL}" -gt 0 ]; then
  exit 1
fi
exit 0
