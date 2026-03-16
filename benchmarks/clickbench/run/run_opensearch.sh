#!/usr/bin/env bash
# run_opensearch.sh — Run 43 ClickBench queries on OpenSearch trino_sql, output results JSON.
#
# Usage:
#   ./run_opensearch.sh [OPTIONS]
#
# Options:
#   --dataset 1m|full     Dataset to query (default: full).
#                         "1m" rewrites queries to use $INDEX_NAME_1M.
#   --timeout SECONDS     Per-query curl timeout; 0 = no timeout (default: 0).
#                         Curl exit code 28 = timeout.
#   --output-dir DIR      Directory for result JSON (default: $RESULT_DIR/opensearch).
#   --num-tries N         Number of runs per query (default: $NUM_TRIES from setup_common.sh).
#   --query N             Run only query N; others get [null] in results.
#   --no-cache-clear      Skip cache clearing and drop_caches before cold run.
#   --warmup N            Run N warmup passes (all queries) before timing to trigger JIT.
#
# The script sources setup/setup_common.sh for shared variables:
#   $INDEX_NAME_1M, $INDEX_NAME, $QUERY_DIR, $RESULT_DIR, $NUM_TRIES, $OS_URL, $QUERY_TIMEOUT

source "$(dirname "$0")/../setup/setup_common.sh"

# -- Defaults ------------------------------------------------------------------
DATASET="full"
TIMEOUT=0
OUTPUT_DIR=""
TRIES=""
ONLY_QUERY=""
CACHE_CLEAR=true
WARMUP_PASSES=0

# -- Parse flags ---------------------------------------------------------------
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
        --warmup)
            WARMUP_PASSES="$2"; shift 2 ;;
        *)
            die "Unknown flag: $1" ;;
    esac
done

# -- Resolve parameters --------------------------------------------------------
TRIES="${TRIES:-$NUM_TRIES}"

# Index name: use the 1M index when --dataset 1m is specified
TARGET_INDEX="$INDEX_NAME"
if [ "$DATASET" = "1m" ]; then
    TARGET_INDEX="$INDEX_NAME_1M"
fi

# Output directory and file
OUTPUT_DIR="${OUTPUT_DIR:-$RESULT_DIR/opensearch}"
mkdir -p "$OUTPUT_DIR"
RESULT_FILE="${OUTPUT_DIR}/$(get_instance_type).json"

# -- Query file ----------------------------------------------------------------
QUERY_FILE="$QUERY_DIR/queries_trino.sql"
if [ ! -f "$QUERY_FILE" ]; then
    die "Query file not found: $QUERY_FILE"
fi

# -- Load metadata from load phase ---------------------------------------------
LOAD_META="$RESULT_DIR/opensearch/load_meta.txt"
LOAD_TIME=0
DATA_SIZE=0
if [ -f "$LOAD_META" ]; then
    # shellcheck source=/dev/null
    source "$LOAD_META"
    LOAD_TIME="${LOAD_TIME_OS:-0}"
    DATA_SIZE="${DATA_SIZE_OS:-0}"
fi

INSTANCE_TYPE=$(get_instance_type)
TODAY=$(date +%Y-%m-%d)

log "Running ClickBench on OpenSearch trino_sql ($TRIES runs per query)..."
log "Instance: $INSTANCE_TYPE | Dataset: $DATASET | Index: $TARGET_INDEX"
if [ "$TIMEOUT" -gt 0 ]; then
    log "Per-query timeout: ${TIMEOUT}s"
fi
if [ -n "$ONLY_QUERY" ]; then
    log "Running only query #$ONLY_QUERY"
fi

# -- Warmup passes (JIT compilation) ------------------------------------------
if [ "$WARMUP_PASSES" -gt 0 ]; then
    log "Running $WARMUP_PASSES warmup pass(es) to trigger JIT compilation..."
    for wp in $(seq 1 "$WARMUP_PASSES"); do
        WP_NUM=0
        while IFS= read -r wq; do
            WP_NUM=$((WP_NUM + 1))
            [ -z "$wq" ] && continue
            # Skip Q35 during warmup (can timeout)
            [ "$WP_NUM" -eq 35 ] && continue
            wq=$(echo "$wq" | sed 's/;[[:space:]]*$//')
            if [ "$DATASET" = "1m" ]; then
                wq=$(echo "$wq" | sed "s/\bhits\b/${TARGET_INDEX}/g")
            fi
            WQ_ESC=$(printf '%s' "$wq" | jq -Rs '.')
            curl -sf -o /dev/null \
                -XPOST "${OS_URL}/_plugins/_trino_sql" \
                -H 'Content-Type: application/json' \
                -d "{\"query\": $WQ_ESC}" \
                --max-time 30 2>/dev/null || true
        done < "$QUERY_FILE"
        log "  Warmup pass $wp/$WARMUP_PASSES complete"
    done
fi

# -- Execute queries -----------------------------------------------------------
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

    # Strip trailing semicolons
    query=$(echo "$query" | sed 's/;[[:space:]]*$//')

    # Rewrite index name when using the 1M dataset
    if [ "$DATASET" = "1m" ]; then
        query=$(echo "$query" | sed "s/\bhits\b/${TARGET_INDEX}/g")
    fi

    # Escape the query for JSON payload (printf avoids trailing newline)
    ESCAPED_QUERY=$(printf '%s' "$query" | jq -Rs '.')

    TIMES="[]"
    for i in $(seq 1 "$TRIES"); do
        # Clear caches before first run (cold run), unless --no-cache-clear
        if [ "$i" -eq 1 ] && [ "$CACHE_CLEAR" = true ]; then
            curl -sf -XPOST "${OS_URL}/_cache/clear" >/dev/null 2>&1 || true
            curl -sf -XPOST "${OS_URL}/${TARGET_INDEX}/_cache/clear" >/dev/null 2>&1 || true
            drop_caches
        fi

        # Build curl arguments
        CURL_ARGS=(-sf -o "/tmp/os_result_q${QUERY_NUM}.json" -w '%{http_code}')
        if [ "$TIMEOUT" -gt 0 ]; then
            CURL_ARGS+=(--max-time "$TIMEOUT")
        fi
        CURL_ARGS+=(
            -XPOST "${OS_URL}/_plugins/_trino_sql"
            -H 'Content-Type: application/json'
            -d "{\"query\": $ESCAPED_QUERY}"
        )

        # Run query with wall-clock timing
        START_NS=$(date +%s%N)
        EXIT_CODE=0
        HTTP_CODE=$(curl "${CURL_ARGS[@]}") || EXIT_CODE=$?
        END_NS=$(date +%s%N)

        if [ "$EXIT_CODE" -eq 28 ]; then
            # curl exit code 28 = timeout
            log "  Q${QUERY_NUM} run $i: TIMEOUT (${TIMEOUT}s)"
            TIME="null"
        elif [ "$EXIT_CODE" -ne 0 ]; then
            log "  Q${QUERY_NUM} run $i: FAILED (curl exit $EXIT_CODE)"
            TIME="null"
        elif [ "$HTTP_CODE" -ge 200 ] && [ "$HTTP_CODE" -lt 300 ]; then
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

# -- Write results JSON --------------------------------------------------------
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
