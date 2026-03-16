#!/usr/bin/env bash
# run_clickhouse.sh — Run 43 ClickBench queries on ClickHouse, output results JSON.
#
# Usage:
#   ./run_clickhouse.sh [OPTIONS]
#
# Options:
#   --dataset 1m|full     Dataset to query (default: full).
#                         "1m" rewrites queries to use $CH_TABLE_1M.
#   --timeout SECONDS     Per-query timeout; 0 = no timeout (default: 0).
#   --output-dir DIR      Directory for result JSON (default: $RESULT_DIR/clickhouse).
#   --num-tries N         Number of runs per query (default: $NUM_TRIES from setup_common.sh).
#   --query N             Run only query N; others get [null] in results.
#   --no-cache-clear      Skip drop_caches before the cold run.
#
# The script sources setup/setup_common.sh for shared variables:
#   $CH_TABLE_1M, $QUERY_DIR, $RESULT_DIR, $NUM_TRIES, $CH_HOST, $CH_PORT

source "$(dirname "$0")/../setup/setup_common.sh"

# ── Defaults ──────────────────────────────────────────────────────────────────
DATASET="full"
TIMEOUT=0
OUTPUT_DIR=""
TRIES=""
ONLY_QUERY=""
CACHE_CLEAR=true
USE_HTTP=false

# ── Parse flags ───────────────────────────────────────────────────────────────
while [ $# -gt 0 ]; do
    case "$1" in
        --dataset)
            DATASET="$2"; shift 2 ;;
        --timeout)
            TIMEOUT="$2"; shift 2 ;;
        --output-dir)
            OUTPUT_DIR="$2"; shift 2 ;;
        --num-tries)
            TRIES="$2"; shift 2 ;;
        --query)
            ONLY_QUERY="$2"; shift 2 ;;
        --no-cache-clear)
            CACHE_CLEAR=false; shift ;;
        --use-http)
            USE_HTTP=true; shift ;;
        *)
            die "Unknown flag: $1" ;;
    esac
done

# ── Resolve parameters ───────────────────────────────────────────────────────
TRIES="${TRIES:-$NUM_TRIES}"

# Table name: use the 1M table when --dataset 1m is specified
TABLE_NAME="hits"
if [ "$DATASET" = "1m" ]; then
    TABLE_NAME="$CH_TABLE_1M"
fi

# Output directory and file
OUTPUT_DIR="${OUTPUT_DIR:-$RESULT_DIR/clickhouse}"
mkdir -p "$OUTPUT_DIR"
RESULT_FILE="${OUTPUT_DIR}/$(get_instance_type).json"

# ── Query file ────────────────────────────────────────────────────────────────
QUERY_FILE="$QUERY_DIR/queries.sql"
if [ ! -f "$QUERY_FILE" ]; then
    die "Query file not found: $QUERY_FILE"
fi

# ── Load metadata from load phase ────────────────────────────────────────────
LOAD_META="$RESULT_DIR/clickhouse/load_meta.txt"
LOAD_TIME=0
DATA_SIZE=0
if [ -f "$LOAD_META" ]; then
    # shellcheck source=/dev/null
    source "$LOAD_META"
    LOAD_TIME="${LOAD_TIME_CH:-0}"
    DATA_SIZE="${DATA_SIZE_CH:-0}"
fi

INSTANCE_TYPE=$(get_instance_type)
TODAY=$(date +%Y-%m-%d)

log "Running ClickBench on ClickHouse ($TRIES runs per query)..."
log "Instance: $INSTANCE_TYPE | Dataset: $DATASET | Table: $TABLE_NAME"
if [ "$TIMEOUT" -gt 0 ]; then
    log "Per-query timeout: ${TIMEOUT}s"
fi
if [ -n "$ONLY_QUERY" ]; then
    log "Running only query #$ONLY_QUERY"
fi

# ── Execute queries ───────────────────────────────────────────────────────────
RESULTS="[]"
QUERY_NUM=0

while IFS= read -r query; do
    QUERY_NUM=$((QUERY_NUM + 1))
    [ -z "$query" ] && continue

    # If --query is set, skip non-target queries (emit [null] placeholder)
    if [ -n "$ONLY_QUERY" ] && [ "$QUERY_NUM" -ne "$ONLY_QUERY" ]; then
        RESULTS=$(echo "$RESULTS" | jq ". + [[null]]")
        continue
    fi

    # Rewrite table name when using the 1M dataset
    if [ "$DATASET" = "1m" ]; then
        query=$(echo "$query" | sed "s/\bhits\b/${TABLE_NAME}/g")
    fi

    TIMES="[]"
    for i in $(seq 1 "$TRIES"); do
        # Clear caches before first run (cold run), unless --no-cache-clear
        if [ "$i" -eq 1 ] && [ "$CACHE_CLEAR" = true ]; then
            drop_caches
        fi

        if [ "$USE_HTTP" = true ]; then
            # HTTP mode: measure via curl (same transport as OpenSearch benchmark)
            CH_HTTP_PORT="${CH_HTTP_PORT:-8123}"
            CURL_ARGS=(-sf -o /dev/null)
            if [ "$TIMEOUT" -gt 0 ]; then
                CURL_ARGS+=(--max-time "$TIMEOUT")
            fi

            START_NS=$(date +%s%N)
            EXIT_CODE=0
            curl "${CURL_ARGS[@]}" "http://${CH_HOST}:${CH_HTTP_PORT}/" \
                --data-binary "$query" 2>/dev/null || EXIT_CODE=$?
            END_NS=$(date +%s%N)

            if [ "$EXIT_CODE" -eq 28 ]; then
                log "  Q${QUERY_NUM} run $i: TIMEOUT (${TIMEOUT}s)"
                TIME="null"
            elif [ "$EXIT_CODE" -ne 0 ]; then
                log "  Q${QUERY_NUM} run $i: FAILED (curl exit $EXIT_CODE)"
                TIME="null"
            else
                ELAPSED_MS=$(( (END_NS - START_NS) / 1000000 ))
                TIME=$(echo "scale=3; $ELAPSED_MS / 1000" | bc)
                log "  Q${QUERY_NUM} run $i: ${TIME}s"
            fi
        else
            # Native protocol mode: use clickhouse-client --time
            ch_cmd=(clickhouse-client --host "$CH_HOST" --port "$CH_PORT"
                    --time --format Null -q "$query")

            EXIT_CODE=0
            if [ "$TIMEOUT" -gt 0 ]; then
                RAW_OUTPUT=$(timeout "$TIMEOUT" "${ch_cmd[@]}" 2>&1) || EXIT_CODE=$?
            else
                RAW_OUTPUT=$("${ch_cmd[@]}" 2>&1) || EXIT_CODE=$?
            fi
            TIME=$(echo "$RAW_OUTPUT" | tail -1)

            if [ "$EXIT_CODE" -eq 124 ]; then
                log "  Q${QUERY_NUM} run $i: TIMEOUT (${TIMEOUT}s)"
                TIME="null"
            elif ! echo "$TIME" | grep -qE '^[0-9]+\.?[0-9]*$'; then
                log "  Q${QUERY_NUM} run $i: FAILED"
                TIME="null"
            else
                log "  Q${QUERY_NUM} run $i: ${TIME}s"
            fi
        fi

        TIMES=$(echo "$TIMES" | jq ". + [$TIME]")
    done

    RESULTS=$(echo "$RESULTS" | jq ". + [$TIMES]")
done < "$QUERY_FILE"

# ── Write results JSON ───────────────────────────────────────────────────────
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
