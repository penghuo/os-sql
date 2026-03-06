#!/usr/bin/env bash
# run_clickhouse.sh — Run 43 ClickBench queries on ClickHouse, output results JSON.
source "$(dirname "$0")/../setup/setup_common.sh"

QUERY_FILE="$QUERY_DIR/queries.sql"
RESULT_FILE="$RESULT_DIR/clickhouse/$(get_instance_type).json"

if [ ! -f "$QUERY_FILE" ]; then
    die "Query file not found: $QUERY_FILE"
fi

# Load metadata from load phase
LOAD_META="$RESULT_DIR/clickhouse/load_meta.txt"
LOAD_TIME=0
DATA_SIZE=0
if [ -f "$LOAD_META" ]; then
    source "$LOAD_META"
    LOAD_TIME="${LOAD_TIME_CH:-0}"
    DATA_SIZE="${DATA_SIZE_CH:-0}"
fi

INSTANCE_TYPE=$(get_instance_type)
TODAY=$(date +%Y-%m-%d)

log "Running ClickBench on ClickHouse ($NUM_TRIES runs per query)..."
log "Instance: $INSTANCE_TYPE"

RESULTS="[]"
QUERY_NUM=0

while IFS= read -r query; do
    QUERY_NUM=$((QUERY_NUM + 1))
    [ -z "$query" ] && continue

    TIMES="[]"
    for i in $(seq 1 "$NUM_TRIES"); do
        # Clear caches before first run (cold run)
        if [ "$i" -eq 1 ]; then
            drop_caches
        fi

        # Run query with timing
        TIME=$(clickhouse-client --host "$CH_HOST" --port "$CH_PORT" \
            --time --format Null \
            -q "$query" 2>&1 | tail -1)

        # If query failed, record null
        if ! echo "$TIME" | grep -qE '^[0-9]+\.?[0-9]*$'; then
            log "  Q${QUERY_NUM} run $i: FAILED"
            TIME="null"
        else
            log "  Q${QUERY_NUM} run $i: ${TIME}s"
        fi

        TIMES=$(echo "$TIMES" | jq ". + [$TIME]")
    done

    RESULTS=$(echo "$RESULTS" | jq ". + [$TIMES]")
done < "$QUERY_FILE"

# Write results JSON
jq -n \
    --arg system "ClickHouse" \
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
        tags: ["C++", "column-oriented"],
        load_time: $load_time,
        data_size: $data_size,
        result: $result
    }' > "$RESULT_FILE"

log "Results written to $RESULT_FILE"
