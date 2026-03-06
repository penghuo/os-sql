#!/usr/bin/env bash
# run_opensearch.sh — Run 43 ClickBench queries on OpenSearch trino_sql, output results JSON.
source "$(dirname "$0")/../setup/setup_common.sh"

QUERY_FILE="$QUERY_DIR/queries_trino.sql"
RESULT_FILE="$RESULT_DIR/opensearch/$(get_instance_type).json"

if [ ! -f "$QUERY_FILE" ]; then
    die "Query file not found: $QUERY_FILE"
fi

# Load metadata from load phase
LOAD_META="$RESULT_DIR/opensearch/load_meta.txt"
LOAD_TIME=0
DATA_SIZE=0
if [ -f "$LOAD_META" ]; then
    source "$LOAD_META"
    LOAD_TIME="${LOAD_TIME_OS:-0}"
    DATA_SIZE="${DATA_SIZE_OS:-0}"
fi

INSTANCE_TYPE=$(get_instance_type)
TODAY=$(date +%Y-%m-%d)

log "Running ClickBench on OpenSearch trino_sql ($NUM_TRIES runs per query)..."
log "Instance: $INSTANCE_TYPE"

RESULTS="[]"
QUERY_NUM=0

while IFS= read -r query; do
    QUERY_NUM=$((QUERY_NUM + 1))
    [ -z "$query" ] && continue

    # Escape the query for JSON payload
    ESCAPED_QUERY=$(echo "$query" | jq -Rs '.')

    TIMES="[]"
    for i in $(seq 1 "$NUM_TRIES"); do
        # Clear caches before first run (cold run)
        if [ "$i" -eq 1 ]; then
            curl -sf -XPOST "${OS_URL}/_cache/clear" >/dev/null 2>&1 || true
            curl -sf -XPOST "${OS_URL}/${INDEX_NAME}/_cache/clear" >/dev/null 2>&1 || true
            drop_caches
        fi

        # Run query with wall-clock timing
        START_NS=$(date +%s%N)
        HTTP_CODE=$(curl -sf -o /tmp/os_result_q${QUERY_NUM}.json -w '%{http_code}' \
            -XPOST "${OS_URL}/_plugins/_trino_sql" \
            -H 'Content-Type: application/json' \
            -d "{\"query\": $ESCAPED_QUERY}")
        END_NS=$(date +%s%N)

        if [ "$HTTP_CODE" -ge 200 ] && [ "$HTTP_CODE" -lt 300 ]; then
            # Calculate elapsed time in seconds (with 3 decimal places)
            ELAPSED_MS=$(( (END_NS - START_NS) / 1000000 ))
            TIME=$(echo "scale=3; $ELAPSED_MS / 1000" | bc)
            log "  Q${QUERY_NUM} run $i: ${TIME}s"
        else
            log "  Q${QUERY_NUM} run $i: FAILED (HTTP $HTTP_CODE)"
            TIME="null"
        fi

        TIMES=$(echo "$TIMES" | jq ". + [$TIME]")
    done

    RESULTS=$(echo "$RESULTS" | jq ". + [$TIMES]")
done < "$QUERY_FILE"

# Write results JSON
jq -n \
    --arg system "OpenSearch (trino_sql)" \
    --arg date "$TODAY" \
    --arg machine "$INSTANCE_TYPE" \
    --argjson cluster_size 1 \
    --arg proprietary "no" \
    --arg hardware "cpu" \
    --arg tuned "no" \
    --argjson load_time "$LOAD_TIME" \
    --argjson data_size "$DATA_SIZE" \
    --argjson result "$RESULTS" \
    '{
        system: $system,
        date: $date,
        machine: $machine,
        cluster_size: $cluster_size,
        proprietary: $proprietary,
        hardware: $hardware,
        tuned: $tuned,
        tags: ["Java", "OpenSearch"],
        load_time: $load_time,
        data_size: $data_size,
        result: $result
    }' > "$RESULT_FILE"

log "Results written to $RESULT_FILE"
