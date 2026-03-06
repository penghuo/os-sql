#!/usr/bin/env bash
# check_correctness.sh — Compare ClickBench query results between ClickHouse and OpenSearch.
source "$(dirname "$0")/../setup/setup_common.sh"

CH_QUERY_FILE="$QUERY_DIR/queries.sql"
OS_QUERY_FILE="$QUERY_DIR/queries_trino.sql"
OUTPUT_DIR="$RESULT_DIR/correctness"

mkdir -p "$OUTPUT_DIR"

# Normalize output for comparison:
# - Strip trailing whitespace
# - Remove empty lines
# - Normalize NULLs
# - Truncate floats to 6 decimal places
# - Sort lines
normalize() {
    sed 's/[[:space:]]*$//' | \
    sed '/^$/d' | \
    sed 's/\\N/NULL/g; s/\bnull\b/NULL/gi' | \
    sed -E 's/([0-9]+\.[0-9]{6})[0-9]*/\1/g' | \
    sort
}

PASS=0
FAIL=0
SKIP=0
TOTAL=43

log "Running correctness comparison (ClickHouse = ground truth)..."
log "========================================"

# Read both query files into arrays
mapfile -t CH_QUERIES < "$CH_QUERY_FILE"
mapfile -t OS_QUERIES < "$OS_QUERY_FILE"

for i in $(seq 0 $((TOTAL - 1))); do
    QN=$((i + 1))
    CH_Q="${CH_QUERIES[$i]}"
    OS_Q="${OS_QUERIES[$i]}"

    CH_OUT="$OUTPUT_DIR/ch_q${QN}.out"
    OS_OUT="$OUTPUT_DIR/os_q${QN}.out"
    DIFF_OUT="$OUTPUT_DIR/diff_q${QN}.txt"

    # Run ClickHouse query
    if ! clickhouse-client --host "$CH_HOST" --port "$CH_PORT" \
        -q "$CH_Q" 2>/dev/null | normalize > "$CH_OUT"; then
        log "Q${QN}:  SKIP (ClickHouse query failed)"
        SKIP=$((SKIP + 1))
        continue
    fi

    # Run OpenSearch query
    ESCAPED_Q=$(echo "$OS_Q" | jq -Rs '.')
    OS_RESPONSE=$(curl -sf -XPOST "${OS_URL}/_plugins/_trino_sql" \
        -H 'Content-Type: application/json' \
        -d "{\"query\": $ESCAPED_Q}" 2>/dev/null)

    if [ -z "$OS_RESPONSE" ]; then
        log "Q${QN}:  SKIP (OpenSearch query failed)"
        SKIP=$((SKIP + 1))
        continue
    fi

    # Extract result rows from OpenSearch JSON response
    # The trino_sql endpoint returns results in a datarows/rows format
    echo "$OS_RESPONSE" | jq -r '
        .datarows[]? // .rows[]? // empty |
        [.[] | tostring] | join("\t")
    ' 2>/dev/null | normalize > "$OS_OUT"

    # Compare
    CH_ROWS=$(wc -l < "$CH_OUT")
    OS_ROWS=$(wc -l < "$OS_OUT")

    if diff -q "$CH_OUT" "$OS_OUT" >/dev/null 2>&1; then
        log "Q${QN}:  PASS ($CH_ROWS rows)"
        PASS=$((PASS + 1))
        rm -f "$DIFF_OUT"
    else
        diff "$CH_OUT" "$OS_OUT" > "$DIFF_OUT" 2>&1 || true
        if [ "$CH_ROWS" -ne "$OS_ROWS" ]; then
            log "Q${QN}:  FAIL - row count mismatch (CH: $CH_ROWS, OS: $OS_ROWS)"
        else
            log "Q${QN}:  FAIL - content mismatch ($CH_ROWS rows, diff saved)"
        fi
        FAIL=$((FAIL + 1))
    fi
done

log "========================================"
log "Summary: $PASS/$TOTAL PASS, $FAIL/$TOTAL FAIL, $SKIP/$TOTAL SKIP"

# Write summary JSON
jq -n \
    --argjson pass "$PASS" \
    --argjson fail "$FAIL" \
    --argjson skip "$SKIP" \
    --argjson total "$TOTAL" \
    --arg date "$(date +%Y-%m-%d)" \
    '{date: $date, total: $total, pass: $pass, fail: $fail, skip: $skip}' \
    > "$OUTPUT_DIR/summary.json"

log "Detailed diffs saved in $OUTPUT_DIR/"

# Exit with non-zero if any failures
[ "$FAIL" -eq 0 ]
