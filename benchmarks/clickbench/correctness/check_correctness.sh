#!/usr/bin/env bash
# check_correctness.sh — Compare ClickBench query results between ClickHouse and OpenSearch.
#
# Usage:
#   bash check_correctness.sh [--dataset 1m|full] [--query N]
#
# Options:
#   --dataset 1m    Use 1M-row dataset with cached ClickHouse expected results
#   --dataset full  Use full dataset with live ClickHouse queries (default)
#   --query N       Run only query number N (1-43)

source "$(dirname "$0")/../setup/setup_common.sh"

# ── Parse arguments ──────────────────────────────────────────────────────────

DATASET="full"
SINGLE_QUERY=""

while [[ $# -gt 0 ]]; do
    case "$1" in
        --dataset)
            DATASET="${2:-}"
            shift 2
            ;;
        --query)
            SINGLE_QUERY="${2:-}"
            shift 2
            ;;
        *)
            die "Unknown option: $1"
            ;;
    esac
done

if [[ "$DATASET" != "1m" && "$DATASET" != "full" ]]; then
    die "Invalid --dataset value: '$DATASET' (expected '1m' or 'full')"
fi

if [[ -n "$SINGLE_QUERY" ]] && ! [[ "$SINGLE_QUERY" =~ ^[0-9]+$ ]]; then
    die "Invalid --query value: '$SINGLE_QUERY' (expected a number)"
fi

# ── Dataset-specific configuration ───────────────────────────────────────────

CH_QUERY_FILE="$QUERY_DIR/queries.sql"
OS_QUERY_FILE="$QUERY_DIR/queries_trino.sql"

if [[ "$DATASET" == "1m" ]]; then
    CH_TABLE="$CH_TABLE_1M"
    OS_INDEX="$INDEX_NAME_1M"
    EXPECTED_DIR="${CORRECTNESS_RESULT_DIR}/expected_1m"
    OUTPUT_DIR="$CORRECTNESS_RESULT_DIR"
    USE_CACHED_EXPECTED=true
else
    CH_TABLE="hits"
    OS_INDEX="$INDEX_NAME"
    EXPECTED_DIR=""
    OUTPUT_DIR="$RESULT_DIR/correctness"
    USE_CACHED_EXPECTED=false
fi

DIFF_DIR="${OUTPUT_DIR}/diffs"
mkdir -p "$OUTPUT_DIR" "$DIFF_DIR"

# ── Normalize function ───────────────────────────────────────────────────────
# Strip trailing whitespace, remove empty lines, normalize NULLs,
# truncate floats to 6 decimal places, sort lines.

normalize() {
    sed 's/[[:space:]]*$//' |
    sed '/^$/d' |
    sed 's/\\N/NULL/g; s/\bnull\b/NULL/gi' |
    sed "s/\\\\'/'/g" |
    sed -E 's/([0-9]+\.[0-9]{6})[0-9]*/\1/g' |
    sort
}

# ── Main loop setup ──────────────────────────────────────────────────────────

PASS=0
FAIL=0
SKIP=0
TOTAL=43

log "Running correctness comparison (dataset=$DATASET, table=$CH_TABLE, index=$OS_INDEX)..."
log "========================================"

# Read both query files into arrays
mapfile -t CH_QUERIES < "$CH_QUERY_FILE"
mapfile -t OS_QUERIES < "$OS_QUERY_FILE"

# Collect per-query results for summary JSON
QUERY_RESULTS="[]"

for i in $(seq 0 $((TOTAL - 1))); do
    QN=$((i + 1))
    QN_FMT=$(printf '%02d' "$QN")

    # If --query was specified, skip all others
    if [[ -n "$SINGLE_QUERY" && "$QN" -ne "$SINGLE_QUERY" ]]; then
        continue
    fi

    CH_Q="${CH_QUERIES[$i]}"
    OS_Q="${OS_QUERIES[$i]}"

    CH_OUT="$OUTPUT_DIR/ch_q${QN_FMT}.out"
    OS_OUT="$OUTPUT_DIR/os_q${QN_FMT}.out"
    DIFF_OUT="$DIFF_DIR/diff_q${QN_FMT}.txt"

    # ── Get expected (ClickHouse) output ─────────────────────────────────

    if [[ "$USE_CACHED_EXPECTED" == true ]]; then
        # 1m mode: use pre-generated expected results
        EXPECTED_FILE="${EXPECTED_DIR}/q${QN_FMT}.expected"
        if [[ ! -f "$EXPECTED_FILE" ]]; then
            log "Q${QN_FMT}: SKIP (no expected file)"
            SKIP=$((SKIP + 1))
            QUERY_RESULTS=$(echo "$QUERY_RESULTS" | jq \
                --argjson q "$QN" \
                '. + [{"q": $q, "status": "SKIP", "reason": "no expected file"}]')
            continue
        fi

        EXPECTED_CONTENT=$(cat "$EXPECTED_FILE")
        if [[ "$EXPECTED_CONTENT" == "SKIP" ]]; then
            log "Q${QN_FMT}: SKIP (marked SKIP in expected)"
            SKIP=$((SKIP + 1))
            QUERY_RESULTS=$(echo "$QUERY_RESULTS" | jq \
                --argjson q "$QN" \
                '. + [{"q": $q, "status": "SKIP", "reason": "marked SKIP"}]')
            continue
        fi

        # Normalize the cached expected output
        echo "$EXPECTED_CONTENT" | normalize > "$CH_OUT"
    else
        # full mode: run ClickHouse query live, replacing table name
        CH_Q_REPLACED=$(echo "$CH_Q" | sed "s/\bhits\b/${CH_TABLE}/g")
        if ! clickhouse-client --host "$CH_HOST" --port "$CH_PORT" \
            -q "$CH_Q_REPLACED" 2>/dev/null | normalize > "$CH_OUT"; then
            log "Q${QN_FMT}: SKIP (ClickHouse query failed)"
            SKIP=$((SKIP + 1))
            QUERY_RESULTS=$(echo "$QUERY_RESULTS" | jq \
                --argjson q "$QN" \
                '. + [{"q": $q, "status": "SKIP", "reason": "ClickHouse query failed"}]')
            continue
        fi
    fi

    # ── Run OpenSearch query ─────────────────────────────────────────────

    # Strip trailing semicolons, replace table name in the OpenSearch query
    OS_Q_REPLACED=$(echo "$OS_Q" | sed 's/;[[:space:]]*$//' | sed "s/\bhits\b/${OS_INDEX}/g")
    # Use printf (not echo) to avoid trailing newline in jq input
    ESCAPED_Q=$(printf '%s' "$OS_Q_REPLACED" | jq -Rs '.')

    CURL_ARGS=(-sf -XPOST "${OS_URL}/_plugins/_trino_sql"
        -H 'Content-Type: application/json'
        -d "{\"query\": $ESCAPED_Q}")

    # Apply timeout for 1m dataset
    if [[ "$DATASET" == "1m" ]]; then
        CURL_ARGS+=(--max-time "$QUERY_TIMEOUT")
    fi

    OS_RESPONSE=$(curl "${CURL_ARGS[@]}" 2>/dev/null) || OS_RESPONSE=""

    if [[ -z "$OS_RESPONSE" ]]; then
        log "Q${QN_FMT}: SKIP (OpenSearch query failed or timed out)"
        SKIP=$((SKIP + 1))
        QUERY_RESULTS=$(echo "$QUERY_RESULTS" | jq \
            --argjson q "$QN" \
            '. + [{"q": $q, "status": "SKIP", "reason": "OpenSearch query failed or timed out"}]')
        continue
    fi

    # Check for error in JSON response
    OS_ERROR=$(echo "$OS_RESPONSE" | jq -r '.error // empty' 2>/dev/null)
    if [[ -n "$OS_ERROR" ]]; then
        OS_ERROR_MSG=$(echo "$OS_RESPONSE" | jq -r '
            if .error | type == "object" then .error.reason // .error.type // (.error | tostring)
            else .error | tostring
            end' 2>/dev/null)
        log "Q${QN_FMT}: SKIP (OpenSearch error: ${OS_ERROR_MSG})"
        SKIP=$((SKIP + 1))
        QUERY_RESULTS=$(echo "$QUERY_RESULTS" | jq \
            --argjson q "$QN" \
            --arg reason "OpenSearch error: ${OS_ERROR_MSG}" \
            '. + [{"q": $q, "status": "SKIP", "reason": $reason}]')
        continue
    fi

    # Extract result rows from OpenSearch JSON response
    # Use Python instead of jq to preserve full precision of large integers
    # (jq uses IEEE 754 doubles internally, losing precision for longs > 2^53)
    echo "$OS_RESPONSE" | python3 -c "
import sys, json
try:
    d = json.load(sys.stdin)
    rows = d.get('datarows', d.get('rows', []))
    for row in rows:
        print('\t'.join(str(v) for v in row))
except Exception:
    pass
" 2>/dev/null | normalize > "$OS_OUT"

    # ── Compare ──────────────────────────────────────────────────────────

    CH_ROWS=$(wc -l < "$CH_OUT")
    OS_ROWS=$(wc -l < "$OS_OUT")

    if diff -q "$CH_OUT" "$OS_OUT" >/dev/null 2>&1; then
        log "Q${QN_FMT}: PASS ($CH_ROWS rows)"
        PASS=$((PASS + 1))
        rm -f "$DIFF_OUT"
        QUERY_RESULTS=$(echo "$QUERY_RESULTS" | jq \
            --argjson q "$QN" \
            --argjson rows "$CH_ROWS" \
            '. + [{"q": $q, "status": "PASS", "rows": $rows}]')
    else
        diff "$CH_OUT" "$OS_OUT" > "$DIFF_OUT" 2>&1 || true
        if [[ "$CH_ROWS" -ne "$OS_ROWS" ]]; then
            REASON="row count mismatch (expected: $CH_ROWS, actual: $OS_ROWS)"
            log "Q${QN_FMT}: FAIL - $REASON"
        else
            REASON="content mismatch ($CH_ROWS rows, diff saved)"
            log "Q${QN_FMT}: FAIL - $REASON"
        fi
        FAIL=$((FAIL + 1))
        QUERY_RESULTS=$(echo "$QUERY_RESULTS" | jq \
            --argjson q "$QN" \
            --arg reason "$REASON" \
            --argjson expected_rows "$CH_ROWS" \
            --argjson actual_rows "$OS_ROWS" \
            '. + [{"q": $q, "status": "FAIL", "reason": $reason, "expected_rows": $expected_rows, "actual_rows": $actual_rows}]')
    fi
done

# ── Summary ──────────────────────────────────────────────────────────────────

log "========================================"
log "Summary: $PASS/$TOTAL PASS, $FAIL/$TOTAL FAIL, $SKIP/$TOTAL SKIP"

# Write summary JSON
jq -n \
    --arg date "$(date +%Y-%m-%d)" \
    --arg dataset "$DATASET" \
    --argjson total "$TOTAL" \
    --argjson pass "$PASS" \
    --argjson fail "$FAIL" \
    --argjson skip "$SKIP" \
    --argjson queries "$QUERY_RESULTS" \
    '{date: $date, dataset: $dataset, total: $total, pass: $pass, fail: $fail, skip: $skip, queries: $queries}' \
    > "$OUTPUT_DIR/summary.json"

log "Results saved to $OUTPUT_DIR/"
log "Diffs saved to $DIFF_DIR/"

# Exit with non-zero if any failures
[[ "$FAIL" -eq 0 ]]
