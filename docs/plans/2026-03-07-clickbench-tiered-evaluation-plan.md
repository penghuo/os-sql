# ClickBench Tiered Evaluation — Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Restructure ClickBench evaluation into 3 tiers (correctness/perf-lite/performance) with shared 1M dataset, plugin reload workflow, and 60s query timeouts.

**Architecture:** Parameterize existing `run_clickhouse.sh` and `run_opensearch.sh` to accept dataset/timeout/output-dir flags. Extract 1M data loading and snapshot logic from `quick_test.sh` into the main framework. Extend `run_all.sh` as the single orchestrator with new commands.

**Tech Stack:** Bash, jq, curl, clickhouse-client, OpenSearch REST API

**Design doc:** `docs/plans/2026-03-07-clickbench-tiered-evaluation-design.md`

---

### Task 1: Add tier variables and reload helper to setup_common.sh

**Files:**
- Modify: `benchmarks/clickbench/setup/setup_common.sh`

**Step 1: Add tier-related variables after the existing benchmark config section (after line 38)**

Add these variables after `INDEX_NAME="hits"` (line 38):

```bash
# --- Tier config ---
QUERY_TIMEOUT="${QUERY_TIMEOUT:-60}"   # seconds, applies to all tiers
INDEX_NAME_1M="hits_1m"
CH_TABLE_1M="hits_1m"
SNAPSHOT_REPO="clickbench_repo"
SNAPSHOT_NAME_1M="hits_1m_snap"
SNAPSHOT_DIR="${BENCH_DIR}/snapshots"

# --- Result directories per tier ---
CORRECTNESS_RESULT_DIR="${RESULT_DIR}/correctness"
PERF_LITE_RESULT_DIR="${RESULT_DIR}/perf-lite"
PERFORMANCE_RESULT_DIR="${RESULT_DIR}/performance"
```

**Step 2: Add `reload_sql_plugin` helper function at the end of the file (after `drop_caches`)**

```bash
# Rebuild and reinstall the SQL plugin (preserves data)
reload_sql_plugin() {
    log "Building SQL plugin..."
    cd "$REPO_DIR"
    ./gradlew :opensearch-sql-plugin:assemble -x test -x integTest

    local plugin_zip
    plugin_zip=$(find "$REPO_DIR/plugin/build/distributions" -name "opensearch-sql-*.zip" | head -1)
    if [ -z "$plugin_zip" ]; then
        die "Plugin ZIP not found after build."
    fi

    log "Stopping OpenSearch..."
    local pid_file="$OS_INSTALL_DIR/opensearch.pid"
    if [ -f "$pid_file" ]; then
        local pid
        pid=$(cat "$pid_file")
        if kill -0 "$pid" 2>/dev/null; then
            kill "$pid"
            # Wait for shutdown
            local waited=0
            while kill -0 "$pid" 2>/dev/null && [ "$waited" -lt 30 ]; do
                sleep 1
                waited=$((waited + 1))
            done
            if kill -0 "$pid" 2>/dev/null; then
                kill -9 "$pid"
                sleep 2
            fi
        fi
    fi

    log "Removing old SQL plugin..."
    sudo "$OS_INSTALL_DIR/bin/opensearch-plugin" remove opensearch-sql || true

    log "Installing new SQL plugin from $plugin_zip..."
    sudo "$OS_INSTALL_DIR/bin/opensearch-plugin" install --batch "file://$plugin_zip"
    sudo chown -R opensearch:opensearch "$OS_INSTALL_DIR"

    log "Starting OpenSearch..."
    sudo -u opensearch "$OS_INSTALL_DIR/bin/opensearch" -d -p "$OS_INSTALL_DIR/opensearch.pid"
    wait_for_opensearch 180

    cd "$BENCH_DIR"
    log "Plugin reload complete."
}
```

**Step 3: Add `load_1m_data` helper function (loads 1M into both engines + snapshot)**

```bash
# Load 1M dataset (1 parquet file) into both engines
load_1m_data() {
    local mapping_file="$REPO_DIR/integ-test/src/test/resources/clickbench/mappings/clickbench_index_mapping.json"
    local parquet_file="${DATA_DIR}/hits_0.parquet"

    if [ ! -f "$parquet_file" ]; then
        die "Parquet file not found: $parquet_file. Run download_data.sh first."
    fi

    # --- ClickHouse 1M ---
    local ch_count
    ch_count=$(clickhouse-client --host "$CH_HOST" --port "$CH_PORT" \
        -q "SELECT count() FROM ${CH_TABLE_1M}" 2>/dev/null || echo "0")

    if [ "$ch_count" -gt 0 ]; then
        log "ClickHouse '${CH_TABLE_1M}' already has $ch_count rows."
    else
        log "Creating ClickHouse '${CH_TABLE_1M}' from first parquet file..."
        clickhouse-client --host "$CH_HOST" --port "$CH_PORT" \
            -q "DROP TABLE IF EXISTS ${CH_TABLE_1M}"
        clickhouse-client --host "$CH_HOST" --port "$CH_PORT" \
            -q "CREATE TABLE ${CH_TABLE_1M} AS hits ENGINE = MergeTree() ORDER BY tuple()"
        clickhouse-client --host "$CH_HOST" --port "$CH_PORT" \
            -q "INSERT INTO ${CH_TABLE_1M} SELECT * FROM file('hits_0.parquet', Parquet)"
        ch_count=$(clickhouse-client --host "$CH_HOST" --port "$CH_PORT" \
            -q "SELECT count() FROM ${CH_TABLE_1M}")
        log "ClickHouse '${CH_TABLE_1M}': $ch_count rows loaded."
    fi

    # --- Generate expected results ---
    local expected_dir="${CORRECTNESS_RESULT_DIR}/expected_1m"
    if [ -f "${expected_dir}/q01.expected" ]; then
        log "Expected results for 1M already exist."
    else
        log "Generating expected results from ClickHouse ${CH_TABLE_1M}..."
        mkdir -p "$expected_dir"
        local query_num=0
        while IFS= read -r query; do
            query_num=$((query_num + 1))
            printf -v padded "%02d" "$query_num"
            local q_1m
            q_1m=$(echo "$query" | sed "s/\bhits\b/${CH_TABLE_1M}/g")
            if clickhouse-client --host "$CH_HOST" --port "$CH_PORT" \
                -q "$q_1m" > "${expected_dir}/q${padded}.expected" 2>/dev/null; then
                log "  Q${padded}: $(wc -l < "${expected_dir}/q${padded}.expected") rows"
            else
                echo "SKIP" > "${expected_dir}/q${padded}.expected"
                log "  Q${padded}: SKIP (query failed)"
            fi
        done < "$QUERY_DIR/queries.sql"
    fi

    # --- OpenSearch 1M ---
    local os_count
    os_count=$(curl -sf "${OS_URL}/${INDEX_NAME_1M}/_count" 2>/dev/null | jq -r '.count // 0')

    if [ "$os_count" -gt 0 ]; then
        log "OpenSearch '${INDEX_NAME_1M}' already has $os_count docs."
    else
        # Try snapshot restore first
        if _try_restore_1m_snapshot; then
            return 0
        fi

        log "Loading 1M docs into OpenSearch '${INDEX_NAME_1M}'..."
        curl -sf -XPUT "${OS_URL}/${INDEX_NAME_1M}" \
            -H 'Content-Type: application/json' \
            -d @"$mapping_file" >/dev/null

        curl -sf -XPUT "${OS_URL}/${INDEX_NAME_1M}/_settings" \
            -H 'Content-Type: application/json' \
            -d '{"index": {"refresh_interval": "-1", "number_of_replicas": 0}}' >/dev/null

        clickhouse-local -q "SELECT * FROM file('$parquet_file', Parquet) FORMAT JSONEachRow" | \
        awk -v idx="$INDEX_NAME_1M" '{print "{\"index\":{\"_index\":\"" idx "\"}}"; print}' | \
        split -l 10000 -a 4 - /tmp/os_1m_chunk_

        for chunk in /tmp/os_1m_chunk_*; do
            curl -sf -XPOST "${OS_URL}/_bulk" \
                -H 'Content-Type: application/x-ndjson' \
                --data-binary @"$chunk" >/dev/null
            rm -f "$chunk"
        done

        curl -sf -XPUT "${OS_URL}/${INDEX_NAME_1M}/_settings" \
            -H 'Content-Type: application/json' \
            -d '{"index": {"refresh_interval": "1s"}}' >/dev/null
        curl -sf -XPOST "${OS_URL}/${INDEX_NAME_1M}/_refresh" >/dev/null

        os_count=$(curl -sf "${OS_URL}/${INDEX_NAME_1M}/_count" | jq -r '.count')
        log "Loaded $os_count docs into '${INDEX_NAME_1M}'."

        # Create snapshot for fast restore
        _create_1m_snapshot
    fi
}

_try_restore_1m_snapshot() {
    _register_1m_snapshot_repo
    if ! curl -sf "${OS_URL}/_snapshot/${SNAPSHOT_REPO}/${SNAPSHOT_NAME_1M}" >/dev/null 2>&1; then
        return 1
    fi
    log "Restoring '${INDEX_NAME_1M}' from snapshot..."
    local resp
    resp=$(curl -sf -XPOST "${OS_URL}/_snapshot/${SNAPSHOT_REPO}/${SNAPSHOT_NAME_1M}/_restore" \
        -H 'Content-Type: application/json' \
        -d "{\"indices\": \"${INDEX_NAME_1M}\", \"include_global_state\": false}" 2>/dev/null)
    if echo "$resp" | jq -e '.accepted' >/dev/null 2>&1; then
        sleep 2
        local count
        count=$(curl -sf "${OS_URL}/${INDEX_NAME_1M}/_count" 2>/dev/null | jq -r '.count // 0')
        log "Restored '${INDEX_NAME_1M}': $count docs."
        return 0
    fi
    return 1
}

_register_1m_snapshot_repo() {
    mkdir -p "$SNAPSHOT_DIR"
    sudo chown -R opensearch:opensearch "$SNAPSHOT_DIR" 2>/dev/null || true
    curl -sf -XPUT "${OS_URL}/_snapshot/${SNAPSHOT_REPO}" \
        -H 'Content-Type: application/json' \
        -d "{\"type\": \"fs\", \"settings\": {\"location\": \"${SNAPSHOT_DIR}\"}}" >/dev/null
}

_create_1m_snapshot() {
    _register_1m_snapshot_repo
    log "Creating snapshot '${SNAPSHOT_NAME_1M}'..."
    curl -sf -XPUT "${OS_URL}/_snapshot/${SNAPSHOT_REPO}/${SNAPSHOT_NAME_1M}?wait_for_completion=true" \
        -H 'Content-Type: application/json' \
        -d "{\"indices\": \"${INDEX_NAME_1M}\", \"include_global_state\": false}" >/dev/null
    log "Snapshot created."
}

# Restore 1M index from snapshot (for run_all.sh restore-1m)
restore_1m_snapshot() {
    # Delete index if it exists
    curl -sf -XDELETE "${OS_URL}/${INDEX_NAME_1M}" >/dev/null 2>&1 || true
    if _try_restore_1m_snapshot; then
        log "1M snapshot restored successfully."
    else
        die "No 1M snapshot found. Run 'load-1m' first."
    fi
}
```

**Step 4: Run shellcheck**

Run: `shellcheck benchmarks/clickbench/setup/setup_common.sh`
Expected: No errors (warnings about unused vars are OK)

**Step 5: Commit**

```bash
git add benchmarks/clickbench/setup/setup_common.sh
git commit -m "feat(clickbench): add tier config, reload_sql_plugin, and load_1m_data helpers"
```

---

### Task 2: Parameterize run_clickhouse.sh

**Files:**
- Modify: `benchmarks/clickbench/run/run_clickhouse.sh`

**Step 1: Replace the entire script with the parameterized version**

The script accepts `--dataset 1m|full`, `--timeout SECONDS`, `--output-dir DIR`, and `--num-tries N` flags. Defaults match the existing behavior (full dataset, no timeout, 3 tries).

```bash
#!/usr/bin/env bash
# run_clickhouse.sh — Run 43 ClickBench queries on ClickHouse, output results JSON.
#
# Usage:
#   bash run_clickhouse.sh                                  # Full dataset, default settings
#   bash run_clickhouse.sh --dataset 1m --output-dir DIR    # 1M dataset
#   bash run_clickhouse.sh --timeout 60 --num-tries 1       # With timeout, single run
#   bash run_clickhouse.sh --query 5                        # Single query only
source "$(dirname "$0")/../setup/setup_common.sh"

# --- Parse flags ---
DATASET="full"
TIMEOUT=0
TRIES="$NUM_TRIES"
OUTPUT_DIR=""
QUERY_NUM=""
CLEAR_CACHES=true

while [[ $# -gt 0 ]]; do
    case "$1" in
        --dataset)      DATASET="$2"; shift 2 ;;
        --timeout)      TIMEOUT="$2"; shift 2 ;;
        --num-tries)    TRIES="$2"; shift 2 ;;
        --output-dir)   OUTPUT_DIR="$2"; shift 2 ;;
        --query)        QUERY_NUM="$2"; shift 2 ;;
        --no-cache-clear) CLEAR_CACHES=false; shift ;;
        *) die "Unknown option: $1" ;;
    esac
done

# --- Resolve table and output path ---
if [ "$DATASET" = "1m" ]; then
    TABLE="$CH_TABLE_1M"
else
    TABLE="hits"
fi

if [ -z "$OUTPUT_DIR" ]; then
    OUTPUT_DIR="$RESULT_DIR/clickhouse"
fi
mkdir -p "$OUTPUT_DIR"

QUERY_FILE="$QUERY_DIR/queries.sql"
RESULT_FILE="$OUTPUT_DIR/$(get_instance_type).json"

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

log "Running ClickBench on ClickHouse ($TRIES runs per query, dataset=$DATASET, timeout=${TIMEOUT}s)..."
log "Instance: $INSTANCE_TYPE"

RESULTS="[]"
CURRENT_Q=0

while IFS= read -r query; do
    CURRENT_Q=$((CURRENT_Q + 1))
    [ -z "$query" ] && continue

    # Skip if --query specified and this isn't the target
    if [ -n "$QUERY_NUM" ] && [ "$CURRENT_Q" -ne "$QUERY_NUM" ]; then
        RESULTS=$(echo "$RESULTS" | jq '. + [[null]]')
        continue
    fi

    # Replace table name for 1M dataset
    if [ "$DATASET" = "1m" ]; then
        query=$(echo "$query" | sed "s/\bhits\b/${TABLE}/g")
    fi

    TIMES="[]"
    for i in $(seq 1 "$TRIES"); do
        # Clear caches before first run (cold run)
        if [ "$i" -eq 1 ] && [ "$CLEAR_CACHES" = true ]; then
            drop_caches
        fi

        # Run query with timing
        if [ "$TIMEOUT" -gt 0 ]; then
            TIME=$(timeout "$TIMEOUT" clickhouse-client --host "$CH_HOST" --port "$CH_PORT" \
                --time --format Null \
                -q "$query" 2>&1 | tail -1)
            EXIT_CODE=$?
        else
            TIME=$(clickhouse-client --host "$CH_HOST" --port "$CH_PORT" \
                --time --format Null \
                -q "$query" 2>&1 | tail -1)
            EXIT_CODE=$?
        fi

        if [ "$EXIT_CODE" -eq 124 ]; then
            log "  Q${CURRENT_Q} run $i: TIMEOUT (${TIMEOUT}s)"
            TIME="null"
        elif ! echo "$TIME" | grep -qE '^[0-9]+\.?[0-9]*$'; then
            log "  Q${CURRENT_Q} run $i: FAILED"
            TIME="null"
        else
            log "  Q${CURRENT_Q} run $i: ${TIME}s"
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
```

**Step 2: Run shellcheck**

Run: `shellcheck benchmarks/clickbench/run/run_clickhouse.sh`
Expected: No errors

**Step 3: Commit**

```bash
git add benchmarks/clickbench/run/run_clickhouse.sh
git commit -m "feat(clickbench): parameterize run_clickhouse.sh for dataset/timeout/output-dir"
```

---

### Task 3: Parameterize run_opensearch.sh

**Files:**
- Modify: `benchmarks/clickbench/run/run_opensearch.sh`

**Step 1: Replace the entire script with the parameterized version**

Same flag interface as `run_clickhouse.sh`. The key addition is `--max-time` on the curl call for timeout enforcement.

```bash
#!/usr/bin/env bash
# run_opensearch.sh — Run 43 ClickBench queries on OpenSearch trino_sql, output results JSON.
#
# Usage:
#   bash run_opensearch.sh                                  # Full dataset, default settings
#   bash run_opensearch.sh --dataset 1m --output-dir DIR    # 1M dataset
#   bash run_opensearch.sh --timeout 60 --num-tries 1       # With timeout, single run
#   bash run_opensearch.sh --query 5                        # Single query only
source "$(dirname "$0")/../setup/setup_common.sh"

# --- Parse flags ---
DATASET="full"
TIMEOUT=0
TRIES="$NUM_TRIES"
OUTPUT_DIR=""
QUERY_NUM=""
CLEAR_CACHES=true

while [[ $# -gt 0 ]]; do
    case "$1" in
        --dataset)      DATASET="$2"; shift 2 ;;
        --timeout)      TIMEOUT="$2"; shift 2 ;;
        --num-tries)    TRIES="$2"; shift 2 ;;
        --output-dir)   OUTPUT_DIR="$2"; shift 2 ;;
        --query)        QUERY_NUM="$2"; shift 2 ;;
        --no-cache-clear) CLEAR_CACHES=false; shift ;;
        *) die "Unknown option: $1" ;;
    esac
done

# --- Resolve index and output path ---
if [ "$DATASET" = "1m" ]; then
    TARGET_INDEX="$INDEX_NAME_1M"
else
    TARGET_INDEX="$INDEX_NAME"
fi

if [ -z "$OUTPUT_DIR" ]; then
    OUTPUT_DIR="$RESULT_DIR/opensearch"
fi
mkdir -p "$OUTPUT_DIR"

QUERY_FILE="$QUERY_DIR/queries_trino.sql"
RESULT_FILE="$OUTPUT_DIR/$(get_instance_type).json"

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

log "Running ClickBench on OpenSearch trino_sql ($TRIES runs per query, dataset=$DATASET, timeout=${TIMEOUT}s)..."
log "Instance: $INSTANCE_TYPE"

# Build curl timeout flag
CURL_TIMEOUT_FLAG=""
if [ "$TIMEOUT" -gt 0 ]; then
    CURL_TIMEOUT_FLAG="--max-time $TIMEOUT"
fi

RESULTS="[]"
CURRENT_Q=0

while IFS= read -r query; do
    CURRENT_Q=$((CURRENT_Q + 1))
    [ -z "$query" ] && continue

    # Skip if --query specified and this isn't the target
    if [ -n "$QUERY_NUM" ] && [ "$CURRENT_Q" -ne "$QUERY_NUM" ]; then
        RESULTS=$(echo "$RESULTS" | jq '. + [[null]]')
        continue
    fi

    # Replace table name for 1M dataset
    if [ "$DATASET" = "1m" ]; then
        query=$(echo "$query" | sed 's/;$//' | sed "s/\bhits\b/${TARGET_INDEX}/g")
    else
        query=$(echo "$query" | sed 's/;$//')
    fi

    ESCAPED_QUERY=$(echo "$query" | jq -Rs '.')

    TIMES="[]"
    for i in $(seq 1 "$TRIES"); do
        # Clear caches before first run (cold run)
        if [ "$i" -eq 1 ] && [ "$CLEAR_CACHES" = true ]; then
            curl -sf -XPOST "${OS_URL}/_cache/clear" >/dev/null 2>&1 || true
            curl -sf -XPOST "${OS_URL}/${TARGET_INDEX}/_cache/clear" >/dev/null 2>&1 || true
            drop_caches
        fi

        # Run query with wall-clock timing
        START_NS=$(date +%s%N)
        HTTP_CODE=$(curl -sf -o /tmp/os_result_q${CURRENT_Q}.json -w '%{http_code}' \
            $CURL_TIMEOUT_FLAG \
            -XPOST "${OS_URL}/_plugins/_trino_sql" \
            -H 'Content-Type: application/json' \
            -d "{\"query\": $ESCAPED_QUERY}")
        CURL_EXIT=$?
        END_NS=$(date +%s%N)

        if [ "$CURL_EXIT" -eq 28 ]; then
            log "  Q${CURRENT_Q} run $i: TIMEOUT (${TIMEOUT}s)"
            TIME="null"
        elif [ "$HTTP_CODE" -ge 200 ] && [ "$HTTP_CODE" -lt 300 ]; then
            ELAPSED_MS=$(( (END_NS - START_NS) / 1000000 ))
            TIME=$(echo "scale=3; $ELAPSED_MS / 1000" | bc)
            log "  Q${CURRENT_Q} run $i: ${TIME}s"
        else
            log "  Q${CURRENT_Q} run $i: FAILED (HTTP $HTTP_CODE)"
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
```

**Step 2: Run shellcheck**

Run: `shellcheck benchmarks/clickbench/run/run_opensearch.sh`
Expected: No errors

**Step 3: Commit**

```bash
git add benchmarks/clickbench/run/run_opensearch.sh
git commit -m "feat(clickbench): parameterize run_opensearch.sh for dataset/timeout/output-dir"
```

---

### Task 4: Parameterize check_correctness.sh for 1M dataset

**Files:**
- Modify: `benchmarks/clickbench/correctness/check_correctness.sh`

**Step 1: Replace the entire script with the parameterized version**

Accepts `--dataset 1m|full` and `--query N`. When using 1M, reads from cached expected results instead of running ClickHouse queries live.

```bash
#!/usr/bin/env bash
# check_correctness.sh — Compare ClickBench query results between ClickHouse and OpenSearch.
#
# Usage:
#   bash check_correctness.sh                        # Full dataset
#   bash check_correctness.sh --dataset 1m           # 1M dataset (uses cached expected)
#   bash check_correctness.sh --dataset 1m --query 5 # Single query on 1M
source "$(dirname "$0")/../setup/setup_common.sh"

# --- Parse flags ---
DATASET="full"
QUERY_NUM=""

while [[ $# -gt 0 ]]; do
    case "$1" in
        --dataset)  DATASET="$2"; shift 2 ;;
        --query)    QUERY_NUM="$2"; shift 2 ;;
        *) die "Unknown option: $1" ;;
    esac
done

# --- Resolve dataset-specific settings ---
if [ "$DATASET" = "1m" ]; then
    CH_TABLE="$CH_TABLE_1M"
    OS_INDEX="$INDEX_NAME_1M"
    EXPECTED_DIR="${CORRECTNESS_RESULT_DIR}/expected_1m"
    OUTPUT_DIR="$CORRECTNESS_RESULT_DIR"
else
    CH_TABLE="hits"
    OS_INDEX="$INDEX_NAME"
    EXPECTED_DIR=""
    OUTPUT_DIR="$RESULT_DIR/correctness"
fi

CH_QUERY_FILE="$QUERY_DIR/queries.sql"
OS_QUERY_FILE="$QUERY_DIR/queries_trino.sql"

mkdir -p "$OUTPUT_DIR/diffs"

# Normalize output for comparison
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
QUERY_RESULTS="[]"

log "Running correctness comparison (dataset=$DATASET, CH_TABLE=$CH_TABLE, OS_INDEX=$OS_INDEX)..."
log "========================================"

mapfile -t CH_QUERIES < "$CH_QUERY_FILE"
mapfile -t OS_QUERIES < "$OS_QUERY_FILE"

START_Q=0
END_Q=$((TOTAL - 1))
if [ -n "$QUERY_NUM" ]; then
    START_Q=$((QUERY_NUM - 1))
    END_Q=$START_Q
fi

for i in $(seq $START_Q $END_Q); do
    QN=$((i + 1))
    printf -v PADDED "%02d" "$QN"
    CH_Q="${CH_QUERIES[$i]}"
    OS_Q="${OS_QUERIES[$i]}"

    CH_OUT="/tmp/ch_q${QN}.out"
    CH_NORM="/tmp/ch_q${QN}.norm"
    OS_NORM="/tmp/os_q${QN}.norm"
    DIFF_OUT="$OUTPUT_DIR/diffs/diff_q${PADDED}.txt"

    # --- Get ClickHouse expected results ---
    if [ -n "$EXPECTED_DIR" ] && [ -f "${EXPECTED_DIR}/q${PADDED}.expected" ]; then
        local_expected="${EXPECTED_DIR}/q${PADDED}.expected"
        if grep -qx "SKIP" "$local_expected" 2>/dev/null; then
            log "Q${QN}:  SKIP (ClickHouse expected=SKIP)"
            SKIP=$((SKIP + 1))
            QUERY_RESULTS=$(echo "$QUERY_RESULTS" | jq ". + [{\"q\": $QN, \"status\": \"skip\"}]")
            continue
        fi
        normalize < "$local_expected" > "$CH_NORM"
    else
        # Run ClickHouse query live
        local ch_q_resolved
        ch_q_resolved=$(echo "$CH_Q" | sed "s/\bhits\b/${CH_TABLE}/g")
        if ! clickhouse-client --host "$CH_HOST" --port "$CH_PORT" \
            -q "$ch_q_resolved" 2>/dev/null | normalize > "$CH_NORM"; then
            log "Q${QN}:  SKIP (ClickHouse query failed)"
            SKIP=$((SKIP + 1))
            QUERY_RESULTS=$(echo "$QUERY_RESULTS" | jq ". + [{\"q\": $QN, \"status\": \"skip\"}]")
            continue
        fi
    fi

    # --- Run OpenSearch query ---
    local os_q_resolved
    os_q_resolved=$(echo "$OS_Q" | sed 's/;$//' | sed "s/\bhits\b/${OS_INDEX}/g")
    ESCAPED_Q=$(echo "$os_q_resolved" | jq -Rs '.')
    OS_RESPONSE=$(curl -sf --max-time "$QUERY_TIMEOUT" -XPOST "${OS_URL}/_plugins/_trino_sql" \
        -H 'Content-Type: application/json' \
        -d "{\"query\": $ESCAPED_Q}" 2>/dev/null)

    if [ -z "$OS_RESPONSE" ]; then
        log "Q${QN}:  SKIP (OpenSearch timeout/no response)"
        SKIP=$((SKIP + 1))
        QUERY_RESULTS=$(echo "$QUERY_RESULTS" | jq ". + [{\"q\": $QN, \"status\": \"skip\"}]")
        rm -f "$CH_OUT" "$CH_NORM"
        continue
    fi

    # Check for error in response
    OS_ERROR=$(echo "$OS_RESPONSE" | jq -r '.error // empty' 2>/dev/null)
    if [ -n "$OS_ERROR" ]; then
        log "Q${QN}:  FAIL (error: ${OS_ERROR:0:80})"
        FAIL=$((FAIL + 1))
        echo "$OS_ERROR" > "$DIFF_OUT"
        QUERY_RESULTS=$(echo "$QUERY_RESULTS" | jq ". + [{\"q\": $QN, \"status\": \"fail\", \"error\": \"${OS_ERROR:0:200}\"}]")
        rm -f "$CH_OUT" "$CH_NORM"
        continue
    fi

    # Extract and normalize OpenSearch results
    echo "$OS_RESPONSE" | jq -r '
        .datarows[]? // .rows[]? // empty |
        [.[] | tostring] | join("\t")
    ' 2>/dev/null | normalize > "$OS_NORM"

    # Compare
    CH_ROWS=$(wc -l < "$CH_NORM")
    OS_ROWS=$(wc -l < "$OS_NORM")

    if diff -q "$CH_NORM" "$OS_NORM" >/dev/null 2>&1; then
        log "Q${QN}:  PASS ($CH_ROWS rows)"
        PASS=$((PASS + 1))
        rm -f "$DIFF_OUT"
        QUERY_RESULTS=$(echo "$QUERY_RESULTS" | jq ". + [{\"q\": $QN, \"status\": \"pass\", \"rows\": $CH_ROWS}]")
    else
        diff "$CH_NORM" "$OS_NORM" > "$DIFF_OUT" 2>&1 || true
        if [ "$CH_ROWS" -ne "$OS_ROWS" ]; then
            log "Q${QN}:  FAIL - row count mismatch (CH: $CH_ROWS, OS: $OS_ROWS)"
        else
            log "Q${QN}:  FAIL - content mismatch ($CH_ROWS rows, diff saved)"
        fi
        FAIL=$((FAIL + 1))
        QUERY_RESULTS=$(echo "$QUERY_RESULTS" | jq ". + [{\"q\": $QN, \"status\": \"fail\", \"ch_rows\": $CH_ROWS, \"os_rows\": $OS_ROWS}]")
    fi

    rm -f "$CH_OUT" "$CH_NORM" "$OS_NORM"
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
    --arg dataset "$DATASET" \
    --argjson queries "$QUERY_RESULTS" \
    '{date: $date, dataset: $dataset, total: $total, pass: $pass, fail: $fail, skip: $skip, queries: $queries}' \
    > "$OUTPUT_DIR/summary.json"

log "Summary written to $OUTPUT_DIR/summary.json"
log "Diffs saved in $OUTPUT_DIR/diffs/"
```

**Step 2: Run shellcheck**

Run: `shellcheck benchmarks/clickbench/correctness/check_correctness.sh`
Expected: No errors

**Step 3: Commit**

```bash
git add benchmarks/clickbench/correctness/check_correctness.sh
git commit -m "feat(clickbench): parameterize check_correctness.sh for 1m/full dataset"
```

---

### Task 5: Rewrite run_all.sh as the unified orchestrator

**Files:**
- Modify: `benchmarks/clickbench/run/run_all.sh`

**Step 1: Replace the entire script**

```bash
#!/usr/bin/env bash
# run_all.sh — Orchestrate ClickBench benchmark: setup, loading, and tiered evaluation.
#
# Usage: ./run_all.sh <command> [options]
#
# Commands:
#   setup           Install OpenSearch + ClickHouse (idempotent)
#   load-1m         Load 1M subset into both engines + create snapshot
#   load-full       Load 100M full dataset into both engines
#   reload-plugin   Rebuild SQL plugin, restart OpenSearch (preserves data)
#   correctness     Run 43 queries on 1M, compare results vs ClickHouse
#   perf-lite       Run 43 queries x 3 on 1M, record timing + correctness
#   performance     Run 43 queries x 3 on 100M, standard ClickBench benchmark
#   restore-1m      Restore 1M index from snapshot
#   snapshot        Create EBS snapshots of loaded data
#   restore         Restore data from EBS snapshots
#
# Aliases:
#   load            -> load-full
#   benchmark       -> performance
#   all             -> setup -> load-full -> performance -> correctness (legacy)
#
# Options:
#   --query N       Run only query N (works with correctness, perf-lite, performance)
#   --dry-run       Print what would be executed without running

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
BENCH_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"

source "$BENCH_DIR/setup/setup_common.sh"

DRY_RUN=false
QUERY_FLAG=""

# Parse global options
ARGS=()
while [[ $# -gt 0 ]]; do
    case "$1" in
        --dry-run)  DRY_RUN=true; shift ;;
        --query)    QUERY_FLAG="--query $2"; shift 2 ;;
        *)          ARGS+=("$1"); shift ;;
    esac
done
set -- "${ARGS[@]}"

COMMAND="${1:-}"
[ -z "$COMMAND" ] && { usage; exit 1; }

usage() {
    cat <<EOF
Usage: $(basename "$0") <command> [options]

Commands:
  setup           Install OpenSearch + ClickHouse (idempotent)
  load-1m         Load 1M subset into both engines + create snapshot
  load-full       Load 100M full dataset into both engines
  reload-plugin   Rebuild SQL plugin, restart OpenSearch (preserves data)
  correctness     Run 43 queries on 1M, compare results vs ClickHouse
  perf-lite       Run 43 queries x 3 on 1M, record timing + correctness
  performance     Run 43 queries x 3 on 100M, standard ClickBench benchmark
  restore-1m      Restore 1M index from snapshot
  snapshot        Create EBS snapshots of loaded data
  restore         Restore data from EBS snapshots

Aliases:
  load            -> load-full
  benchmark       -> performance

Options:
  --query N       Run only query N
  --dry-run       Print what would be executed without running

Examples:
  ./run_all.sh load-1m                    # One-time: load 1M subset
  ./run_all.sh reload-plugin              # After code change: rebuild + restart
  ./run_all.sh correctness                # Fast correctness check on 1M
  ./run_all.sh correctness --query 5      # Test single query
  ./run_all.sh perf-lite                  # Timing + correctness on 1M
  ./run_all.sh performance                # Full 100M benchmark
EOF
}

run_cmd() {
    if [ "$DRY_RUN" = true ]; then
        log "[DRY RUN] $*"
    else
        "$@"
    fi
}

do_setup() {
    log "=== SETUP: ClickHouse ==="
    run_cmd bash "$BENCH_DIR/setup/setup_clickhouse.sh"
    echo ""
    log "=== SETUP: OpenSearch ==="
    run_cmd bash "$BENCH_DIR/setup/setup_opensearch.sh"
}

do_load_1m() {
    log "=== LOAD: 1M Dataset ==="
    run_cmd load_1m_data
}

do_load_full() {
    log "=== DOWNLOAD: ClickBench Dataset ==="
    run_cmd bash "$BENCH_DIR/data/download_data.sh"
    echo ""
    log "=== LOAD: ClickHouse ==="
    run_cmd bash "$BENCH_DIR/data/load_clickhouse.sh"
    echo ""
    log "=== LOAD: OpenSearch ==="
    run_cmd bash "$BENCH_DIR/data/load_opensearch.sh"
}

do_reload_plugin() {
    log "=== RELOAD: SQL Plugin ==="
    run_cmd reload_sql_plugin
}

do_correctness() {
    log "=== CORRECTNESS: 1M Dataset ==="
    mkdir -p "$CORRECTNESS_RESULT_DIR"
    run_cmd bash "$BENCH_DIR/correctness/check_correctness.sh" --dataset 1m $QUERY_FLAG
}

do_perf_lite() {
    log "=== PERF-LITE: 1M Dataset ==="
    mkdir -p "$PERF_LITE_RESULT_DIR"

    # Run timed benchmarks on 1M
    log "--- ClickHouse timing ---"
    run_cmd bash "$BENCH_DIR/run/run_clickhouse.sh" \
        --dataset 1m --timeout "$QUERY_TIMEOUT" --num-tries "$NUM_TRIES" \
        --output-dir "$PERF_LITE_RESULT_DIR" $QUERY_FLAG

    # Rename to clickhouse.json
    local ch_result="$PERF_LITE_RESULT_DIR/$(get_instance_type).json"
    if [ -f "$ch_result" ]; then
        mv "$ch_result" "$PERF_LITE_RESULT_DIR/clickhouse.json"
    fi

    echo ""
    log "--- OpenSearch timing ---"
    run_cmd bash "$BENCH_DIR/run/run_opensearch.sh" \
        --dataset 1m --timeout "$QUERY_TIMEOUT" --num-tries "$NUM_TRIES" \
        --output-dir "$PERF_LITE_RESULT_DIR" $QUERY_FLAG

    # Rename to opensearch.json
    local os_result="$PERF_LITE_RESULT_DIR/$(get_instance_type).json"
    if [ -f "$os_result" ]; then
        mv "$os_result" "$PERF_LITE_RESULT_DIR/opensearch.json"
    fi

    # Run correctness check and merge into perf-lite summary
    echo ""
    log "--- Correctness cross-reference ---"
    run_cmd bash "$BENCH_DIR/correctness/check_correctness.sh" --dataset 1m $QUERY_FLAG

    # Build perf-lite summary combining timing + correctness
    if [ "$DRY_RUN" = false ]; then
        _build_perf_lite_summary
    fi
}

_build_perf_lite_summary() {
    local correctness_file="$CORRECTNESS_RESULT_DIR/summary.json"
    local ch_file="$PERF_LITE_RESULT_DIR/clickhouse.json"
    local os_file="$PERF_LITE_RESULT_DIR/opensearch.json"
    local summary_file="$PERF_LITE_RESULT_DIR/summary.json"

    if [ ! -f "$correctness_file" ] || [ ! -f "$ch_file" ] || [ ! -f "$os_file" ]; then
        log "WARNING: Missing files for perf-lite summary."
        return
    fi

    # Merge correctness status with timing data
    jq -n \
        --arg date "$(date +%Y-%m-%d)" \
        --slurpfile correctness "$correctness_file" \
        --slurpfile ch "$ch_file" \
        --slurpfile os "$os_file" \
        '{
            date: $date,
            dataset: "1m",
            correctness: $correctness[0],
            queries: [
                range(43) as $i |
                {
                    q: ($i + 1),
                    correct: (($correctness[0].queries // [])[$i].status // "unknown"),
                    ch_times: ($ch[0].result[$i] // []),
                    os_times: ($os[0].result[$i] // [])
                }
            ]
        }' > "$summary_file"

    log "Perf-lite summary written to $summary_file"
}

do_performance() {
    log "=== PERFORMANCE: Full Dataset ==="
    mkdir -p "$PERFORMANCE_RESULT_DIR/clickhouse" "$PERFORMANCE_RESULT_DIR/opensearch"

    log "--- ClickHouse ---"
    run_cmd bash "$BENCH_DIR/run/run_clickhouse.sh" \
        --timeout "$QUERY_TIMEOUT" --output-dir "$PERFORMANCE_RESULT_DIR/clickhouse" $QUERY_FLAG

    echo ""
    log "--- OpenSearch ---"
    run_cmd bash "$BENCH_DIR/run/run_opensearch.sh" \
        --timeout "$QUERY_TIMEOUT" --output-dir "$PERFORMANCE_RESULT_DIR/opensearch" $QUERY_FLAG

    echo ""
    log "=== RESULTS ==="
    log "ClickHouse: $PERFORMANCE_RESULT_DIR/clickhouse/"
    ls -la "$PERFORMANCE_RESULT_DIR/clickhouse/"*.json 2>/dev/null || echo "  (no results)"
    log "OpenSearch:  $PERFORMANCE_RESULT_DIR/opensearch/"
    ls -la "$PERFORMANCE_RESULT_DIR/opensearch/"*.json 2>/dev/null || echo "  (no results)"
}

do_restore_1m() {
    log "=== RESTORE: 1M Snapshot ==="
    run_cmd restore_1m_snapshot
}

do_snapshot() {
    run_cmd bash "$BENCH_DIR/data/snapshot.sh" create
}

do_restore() {
    run_cmd bash "$BENCH_DIR/data/snapshot.sh" restore
}

case "$COMMAND" in
    setup)          do_setup ;;
    load-1m)        do_load_1m ;;
    load-full|load) do_load_full ;;
    reload-plugin)  do_reload_plugin ;;
    correctness)    do_correctness ;;
    perf-lite)      do_perf_lite ;;
    performance|benchmark) do_performance ;;
    restore-1m)     do_restore_1m ;;
    snapshot)       do_snapshot ;;
    restore)        do_restore ;;
    all)
        do_setup
        echo ""
        do_load_full
        echo ""
        do_performance
        ;;
    *)              usage; exit 1 ;;
esac
```

**Step 2: Run shellcheck**

Run: `shellcheck benchmarks/clickbench/run/run_all.sh`
Expected: No errors

**Step 3: Commit**

```bash
git add benchmarks/clickbench/run/run_all.sh
git commit -m "feat(clickbench): rewrite run_all.sh with correctness/perf-lite/performance tiers"
```

---

### Task 6: Delete quick_test.sh

**Files:**
- Delete: `benchmarks/clickbench/quick_test.sh`

**Step 1: Delete the file**

```bash
git rm benchmarks/clickbench/quick_test.sh
```

**Step 2: Commit**

```bash
git commit -m "refactor(clickbench): remove quick_test.sh (absorbed into run_all.sh)"
```

---

### Task 7: Verify — shellcheck all modified scripts

**Step 1: Run shellcheck on all scripts**

Run:
```bash
shellcheck benchmarks/clickbench/setup/setup_common.sh \
    benchmarks/clickbench/run/run_all.sh \
    benchmarks/clickbench/run/run_clickhouse.sh \
    benchmarks/clickbench/run/run_opensearch.sh \
    benchmarks/clickbench/correctness/check_correctness.sh
```

Expected: No errors. Fix any warnings.

**Step 2: Verify dry-run mode works**

Run:
```bash
cd benchmarks/clickbench
bash run/run_all.sh --dry-run correctness
bash run/run_all.sh --dry-run perf-lite
bash run/run_all.sh --dry-run performance
bash run/run_all.sh --dry-run reload-plugin
bash run/run_all.sh --dry-run load-1m
```

Expected: Each prints `[DRY RUN] ...` messages without executing anything.

---

### Task 8: Verify — golden comparison on live cluster

This task runs on the live cluster with OpenSearch + ClickHouse already running.

**Step 1: Load 1M data**

Run:
```bash
cd benchmarks/clickbench
bash run/run_all.sh load-1m
```

Expected: `hits_1m` created in both engines with ~1M rows. Expected results generated in `results/correctness/expected_1m/`.

**Step 2: Run correctness tier**

Run:
```bash
bash run/run_all.sh correctness
```

Expected: Summary printed with PASS/FAIL/SKIP counts. `results/correctness/summary.json` created.

**Step 3: Test single-query mode**

Run:
```bash
bash run/run_all.sh correctness --query 1
```

Expected: Only Q1 results printed.

**Step 4: Commit any fixes needed**

If the golden comparison reveals issues, fix and commit.

---

### Task 9: Verify — perf-lite tier

**Step 1: Run perf-lite**

Run:
```bash
cd benchmarks/clickbench
bash run/run_all.sh perf-lite
```

Expected:
- `results/perf-lite/clickhouse.json` — valid ClickBench JSON with timing
- `results/perf-lite/opensearch.json` — valid ClickBench JSON with timing
- `results/perf-lite/summary.json` — merged timing + correctness
- Queries that timeout at 60s recorded as `null`

**Step 2: Validate JSON format**

Run:
```bash
jq '.result | length' results/perf-lite/clickhouse.json
jq '.result | length' results/perf-lite/opensearch.json
jq '.queries | length' results/perf-lite/summary.json
```

Expected: All return `43`.

---

### Task 10: Update README.md

**Files:**
- Modify: `benchmarks/clickbench/README.md`

**Step 1: Update the README to document the new tier commands**

Read the current README first, then update the usage section to document:
- The 3 tiers (correctness, perf-lite, performance)
- The dev iteration loop (load-1m → reload-plugin → correctness)
- The --query flag
- The --dry-run flag
- The results directory structure

**Step 2: Commit**

```bash
git add benchmarks/clickbench/README.md
git commit -m "docs(clickbench): update README with tiered evaluation commands"
```
