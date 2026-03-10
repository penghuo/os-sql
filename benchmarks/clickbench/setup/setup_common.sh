#!/usr/bin/env bash
# setup_common.sh — shared environment variables and helpers for ClickBench benchmark.
# Source this file from other scripts: source "$(dirname "$0")/../setup/setup_common.sh"

set -euo pipefail

# --- Paths ---
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BENCH_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
REPO_DIR="$(cd "$BENCH_DIR/../.." && pwd)"

DATA_DIR="${DATA_DIR:-$BENCH_DIR/data/parquet}"
RESULT_DIR="${RESULT_DIR:-$BENCH_DIR/results}"
QUERY_DIR="${QUERY_DIR:-$BENCH_DIR/queries}"
CORRECTNESS_DIR="${CORRECTNESS_DIR:-$BENCH_DIR/correctness}"

# --- Hosts ---
OS_HOST="${OS_HOST:-localhost}"
OS_PORT="${OS_PORT:-9200}"
OS_URL="http://${OS_HOST}:${OS_PORT}"
CH_HOST="${CH_HOST:-localhost}"
CH_PORT="${CH_PORT:-9000}"

# --- OpenSearch install paths ---
OS_INSTALL_DIR="${OS_INSTALL_DIR:-/opt/opensearch}"
OS_DATA_DIR="${OS_DATA_DIR:-${OS_INSTALL_DIR}/data}"

# --- ClickHouse data dir ---
CH_DATA_DIR="${CH_DATA_DIR:-/var/lib/clickhouse}"

# --- Dataset ---
DATASET_BASE_URL="https://datasets.clickhouse.com/hits_compatible/athena_partitioned"
NUM_PARQUET_FILES=100

# --- Benchmark config ---
NUM_TRIES="${NUM_TRIES:-3}"
INDEX_NAME="hits"

# --- Tier config ---
QUERY_TIMEOUT="${QUERY_TIMEOUT:-120}"  # seconds, applies to all tiers
INDEX_NAME_1M="hits_1m"
CH_TABLE_1M="hits_1m"
SNAPSHOT_REPO="clickbench_repo"
SNAPSHOT_NAME_1M="hits_1m_snap"
SNAPSHOT_DIR="${BENCH_DIR}/snapshots"

# --- Result directories per tier ---
CORRECTNESS_RESULT_DIR="${RESULT_DIR}/correctness"
PERF_LITE_RESULT_DIR="${RESULT_DIR}/perf-lite"
PERFORMANCE_RESULT_DIR="${RESULT_DIR}/performance"

# --- OpenSearch snapshot build ---
OS_VERSION="${OS_VERSION:-3.6.0.0-SNAPSHOT}"
OS_SNAPSHOT_URL="${OS_SNAPSHOT_URL:-https://artifacts.opensearch.org/snapshots/core/opensearch/3.6.0-SNAPSHOT/opensearch-min-3.6.0-SNAPSHOT-linux-x64-latest.tar.gz}"

# --- EBS snapshot config ---
SNAPSHOT_CONFIG="${BENCH_DIR}/.snapshot_config"

# --- Helpers ---
log() {
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] $*"
}

die() {
    log "ERROR: $*" >&2
    exit 1
}

# Wait for OpenSearch to become ready (green or yellow status)
wait_for_opensearch() {
    local max_wait="${1:-120}"
    local elapsed=0
    log "Waiting for OpenSearch at ${OS_URL}..."
    while [ "$elapsed" -lt "$max_wait" ]; do
        if curl -sf "${OS_URL}/_cluster/health" >/dev/null 2>&1; then
            log "OpenSearch is ready."
            return 0
        fi
        sleep 2
        elapsed=$((elapsed + 2))
    done
    die "OpenSearch did not become ready within ${max_wait}s"
}

# Wait for ClickHouse to become ready
wait_for_clickhouse() {
    local max_wait="${1:-60}"
    local elapsed=0
    log "Waiting for ClickHouse at ${CH_HOST}:${CH_PORT}..."
    while [ "$elapsed" -lt "$max_wait" ]; do
        if clickhouse-client --host "$CH_HOST" --port "$CH_PORT" -q "SELECT 1" >/dev/null 2>&1; then
            log "ClickHouse is ready."
            return 0
        fi
        sleep 2
        elapsed=$((elapsed + 2))
    done
    die "ClickHouse did not become ready within ${max_wait}s"
}

# Collect EC2 instance metadata for results JSON
get_instance_type() {
    local token
    token=$(curl -sf -X PUT "http://169.254.169.254/latest/api/token" \
        -H "X-aws-ec2-metadata-token-ttl-seconds: 21600" 2>/dev/null || true)
    if [ -n "$token" ]; then
        curl -sf -H "X-aws-ec2-metadata-token: $token" \
            "http://169.254.169.254/latest/meta-data/instance-type" 2>/dev/null || echo "unknown"
    else
        echo "unknown"
    fi
}

# Drop OS-level caches
drop_caches() {
    sync
    echo 3 | sudo tee /proc/sys/vm/drop_caches >/dev/null
}

# ---------------------------------------------------------------------------
# Tier helpers — 1M dataset loading, plugin reload, snapshot management
# ---------------------------------------------------------------------------

# Rebuild the SQL plugin from $REPO_DIR, reinstall into $OS_INSTALL_DIR, and
# restart OpenSearch.  Useful after code changes between benchmark runs.
reload_sql_plugin() {
    log "Rebuilding SQL plugin from $REPO_DIR ..."
    cd "$REPO_DIR"
    ./gradlew :opensearch-sql-plugin:assemble -x test -x integTest

    local plugin_zip
    plugin_zip=$(find "$REPO_DIR/plugin/build/distributions" -name "opensearch-sql-*.zip" | head -1)
    if [ -z "$plugin_zip" ]; then
        die "Plugin ZIP not found after build."
    fi

    log "Stopping OpenSearch ..."
    local pid_file="$OS_INSTALL_DIR/opensearch.pid"
    if [ -f "$pid_file" ]; then
        sudo -u opensearch kill "$(cat "$pid_file")" 2>/dev/null || true
        sleep 5
    fi
    # Wait until the process is actually gone
    local waited=0
    while curl -sf "${OS_URL}" >/dev/null 2>&1 && [ "$waited" -lt 30 ]; do
        sleep 2
        waited=$((waited + 2))
    done

    log "Removing old SQL plugin ..."
    sudo "$OS_INSTALL_DIR/bin/opensearch-plugin" remove opensearch-sql 2>/dev/null || true

    log "Installing new SQL plugin from $plugin_zip ..."
    sudo "$OS_INSTALL_DIR/bin/opensearch-plugin" install --batch "file://$plugin_zip"

    log "Starting OpenSearch ..."
    sudo chown -R opensearch:opensearch "$OS_INSTALL_DIR"
    sudo -u opensearch "$OS_INSTALL_DIR/bin/opensearch" -d -p "$pid_file"
    wait_for_opensearch 180

    log "SQL plugin reloaded successfully."
    cd "$BENCH_DIR"
}

# Load 1M-row dataset into both ClickHouse and OpenSearch.
# All steps are idempotent — they skip work that has already been done.
load_1m_data() {
    local ch_schema_file="$QUERY_DIR/create_clickhouse.sql"
    local mapping_file="$REPO_DIR/integ-test/src/test/resources/clickbench/mappings/clickbench_index_mapping.json"
    local first_parquet
    first_parquet=$(find "$DATA_DIR" -maxdepth 1 -name 'hits_*.parquet' 2>/dev/null | sort | head -1)

    if [ -z "$first_parquet" ]; then
        die "No parquet files found in $DATA_DIR"
    fi

    # ---------------------------------------------------------------
    # 1.  ClickHouse — load hits_1m from the first parquet file
    # ---------------------------------------------------------------
    local ch_rows
    ch_rows=$(clickhouse-client --host "$CH_HOST" --port "$CH_PORT" \
        -q "SELECT count() FROM ${CH_TABLE_1M}" 2>/dev/null || echo "0")

    if [ "$ch_rows" -gt 0 ]; then
        log "ClickHouse '${CH_TABLE_1M}' already has $ch_rows rows. Skipping."
    else
        log "Creating ClickHouse '${CH_TABLE_1M}' table ..."
        # Derive the 1M CREATE TABLE from the full-dataset schema
        local create_sql
        create_sql=$(sed "s/\bhits\b/${CH_TABLE_1M}/g" "$ch_schema_file")
        clickhouse-client --host "$CH_HOST" --port "$CH_PORT" \
            -q "DROP TABLE IF EXISTS ${CH_TABLE_1M}"
        echo "$create_sql" | clickhouse-client --host "$CH_HOST" --port "$CH_PORT"

        log "Loading first parquet file into ${CH_TABLE_1M} (LIMIT 1000000) ..."
        local ch_user_files="${CH_DATA_DIR}/user_files"
        sudo mkdir -p "$ch_user_files"
        sudo cp "$first_parquet" "$ch_user_files/"
        sudo chown clickhouse:clickhouse "${ch_user_files}/$(basename "$first_parquet")"
        clickhouse-client --host "$CH_HOST" --port "$CH_PORT" \
            -q "INSERT INTO ${CH_TABLE_1M}
                SELECT * FROM file('$(basename "$first_parquet")', Parquet)
                LIMIT 1000000"
        ch_rows=$(clickhouse-client --host "$CH_HOST" --port "$CH_PORT" \
            -q "SELECT count() FROM ${CH_TABLE_1M}")
        log "ClickHouse ${CH_TABLE_1M}: $ch_rows rows loaded."
    fi

    # ---------------------------------------------------------------
    # 2.  Generate ClickHouse expected results for the 1M table
    # ---------------------------------------------------------------
    local expected_dir="${CORRECTNESS_RESULT_DIR}/expected_1m"
    if [ -d "$expected_dir" ] && [ "$(find "$expected_dir" -maxdepth 1 -name '*.expected' 2>/dev/null | wc -l)" -eq 43 ]; then
        log "ClickHouse expected results for 1M already exist. Skipping."
    else
        log "Generating ClickHouse expected results for ${CH_TABLE_1M} ..."
        mkdir -p "$expected_dir"
        local qnum=0
        while IFS= read -r query; do
            qnum=$((qnum + 1))
            [ -z "$query" ] && continue
            local q_1m
            q_1m=$(echo "$query" | sed "s/\bhits\b/${CH_TABLE_1M}/g")
            local outfile
            outfile=$(printf "%s/q%02d.expected" "$expected_dir" "$qnum")
            if clickhouse-client --host "$CH_HOST" --port "$CH_PORT" \
                    -q "$q_1m" > "$outfile" 2>/dev/null; then
                log "  q$(printf '%02d' "$qnum"): OK"
            else
                log "  q$(printf '%02d' "$qnum"): FAILED (empty expected)"
                : > "$outfile"   # empty file so count stays at 43
            fi
        done < "$QUERY_DIR/queries.sql"
        log "Expected results written to $expected_dir"
    fi

    # ---------------------------------------------------------------
    # 3.  OpenSearch — load hits_1m via bulk API (or restore snapshot)
    # ---------------------------------------------------------------
    local os_rows
    os_rows=$(curl -sf "${OS_URL}/${INDEX_NAME_1M}/_count" 2>/dev/null \
        | jq -r '.count // 0' 2>/dev/null || echo "0")

    if [ "$os_rows" -gt 0 ]; then
        log "OpenSearch '${INDEX_NAME_1M}' already has $os_rows docs. Skipping."
    else
        # Try snapshot restore first (fast path)
        if _try_restore_1m_snapshot; then
            log "OpenSearch '${INDEX_NAME_1M}' restored from snapshot."
        else
            log "Loading 1M rows into OpenSearch via bulk API ..."
            # Create index
            if ! curl -sf "${OS_URL}/${INDEX_NAME_1M}" >/dev/null 2>&1; then
                curl -sf -XPUT "${OS_URL}/${INDEX_NAME_1M}" \
                    -H 'Content-Type: application/json' \
                    -d @"$mapping_file" | jq .
            fi

            # Disable refresh during load
            curl -sf -XPUT "${OS_URL}/${INDEX_NAME_1M}/_settings" \
                -H 'Content-Type: application/json' \
                -d '{"index": {"refresh_interval": "-1", "number_of_replicas": 0}}'

            # Convert first 1M rows of first parquet to NDJSON and bulk load.
            # Date/DateTime columns in the parquet are stored as epoch days (UInt16)
            # and epoch seconds (UInt32) respectively, but OpenSearch expects epoch_millis
            # or formatted date strings.  REPLACE converts them to ISO format strings.
            local bulk_size=10000
            clickhouse-local \
                -q "SELECT * REPLACE(
                      toString(toDate(EventDate)) AS EventDate,
                      formatDateTime(toDateTime(EventTime), '%Y-%m-%d %H:%i:%S') AS EventTime,
                      formatDateTime(toDateTime(ClientEventTime), '%Y-%m-%d %H:%i:%S') AS ClientEventTime,
                      formatDateTime(toDateTime(LocalEventTime), '%Y-%m-%d %H:%i:%S') AS LocalEventTime
                    ) FROM file('$first_parquet', Parquet) LIMIT 1000000 FORMAT JSONEachRow" | \
            awk -v idx="$INDEX_NAME_1M" '{print "{\"index\":{\"_index\":\"" idx "\"}}"; print}' | \
            split -l $((bulk_size * 2)) -a 4 - /tmp/os_1m_bulk_

            local total=0
            for chunk in /tmp/os_1m_bulk_*; do
                local http_code
                http_code=$(curl -s -o /tmp/os_1m_resp.json -w '%{http_code}' \
                    -XPOST "${OS_URL}/_bulk" \
                    -H 'Content-Type: application/x-ndjson' \
                    --data-binary @"$chunk")
                if [ "$http_code" != "200" ]; then
                    log "  WARNING: bulk returned HTTP $http_code"
                fi
                local chunk_docs
                chunk_docs=$(wc -l < "$chunk")
                total=$((total + chunk_docs / 2))
                rm -f "$chunk"
            done
            rm -f /tmp/os_1m_resp.json

            # Re-enable refresh and force merge
            curl -sf -XPUT "${OS_URL}/${INDEX_NAME_1M}/_settings" \
                -H 'Content-Type: application/json' \
                -d '{"index": {"refresh_interval": "1s"}}'
            curl -sf -XPOST "${OS_URL}/${INDEX_NAME_1M}/_refresh"
            curl -sf -XPOST "${OS_URL}/${INDEX_NAME_1M}/_forcemerge?max_num_segments=1" >/dev/null 2>&1 || true

            os_rows=$(curl -sf "${OS_URL}/${INDEX_NAME_1M}/_count" | jq -r '.count // 0')
            log "OpenSearch ${INDEX_NAME_1M}: $os_rows docs loaded."

            # Save snapshot for fast future restores
            _create_1m_snapshot
        fi
    fi

    log "1M dataset ready in both ClickHouse and OpenSearch."
}

# ---------------------------------------------------------------------------
# Internal: OpenSearch snapshot helpers for hits_1m
# ---------------------------------------------------------------------------

# Try to restore the 1M index from an existing snapshot.
# Returns 0 on success, 1 if no snapshot is available.
_try_restore_1m_snapshot() {
    _register_1m_snapshot_repo || return 1

    # Check if the snapshot exists
    local snap_status
    snap_status=$(curl -sf "${OS_URL}/_snapshot/${SNAPSHOT_REPO}/${SNAPSHOT_NAME_1M}" 2>/dev/null)
    if [ -z "$snap_status" ]; then
        return 1
    fi
    local state
    state=$(echo "$snap_status" | jq -r '.snapshots[0].state // empty' 2>/dev/null)
    if [ "$state" != "SUCCESS" ]; then
        return 1
    fi

    log "Restoring ${INDEX_NAME_1M} from snapshot ${SNAPSHOT_NAME_1M} ..."
    # Delete index if it exists (even with 0 docs, mapping may conflict)
    curl -sf -XDELETE "${OS_URL}/${INDEX_NAME_1M}" >/dev/null 2>&1 || true

    local resp
    resp=$(curl -sf -XPOST \
        "${OS_URL}/_snapshot/${SNAPSHOT_REPO}/${SNAPSHOT_NAME_1M}/_restore" \
        -H 'Content-Type: application/json' \
        -d "{\"indices\": \"${INDEX_NAME_1M}\", \"include_global_state\": false}" 2>/dev/null)
    if echo "$resp" | jq -e '.accepted // false' >/dev/null 2>&1; then
        # Wait for recovery
        local waited=0
        while [ "$waited" -lt 120 ]; do
            local health
            health=$(curl -sf "${OS_URL}/_cluster/health/${INDEX_NAME_1M}" 2>/dev/null \
                | jq -r '.status // "red"' 2>/dev/null)
            if [ "$health" = "green" ] || [ "$health" = "yellow" ]; then
                return 0
            fi
            sleep 2
            waited=$((waited + 2))
        done
        log "WARNING: snapshot restore timed out waiting for health."
        return 1
    fi
    return 1
}

# Register the shared-filesystem snapshot repo (idempotent).
_register_1m_snapshot_repo() {
    mkdir -p "$SNAPSHOT_DIR"
    # Ensure opensearch user can write to the directory
    sudo chown -R opensearch:opensearch "$SNAPSHOT_DIR" 2>/dev/null || true

    local resp
    resp=$(curl -sf -XPUT "${OS_URL}/_snapshot/${SNAPSHOT_REPO}" \
        -H 'Content-Type: application/json' \
        -d "{
            \"type\": \"fs\",
            \"settings\": {
                \"location\": \"${SNAPSHOT_DIR}\"
            }
        }" 2>/dev/null)
    if echo "$resp" | jq -e '.acknowledged // false' >/dev/null 2>&1; then
        return 0
    fi
    # Repo may already exist — check
    if curl -sf "${OS_URL}/_snapshot/${SNAPSHOT_REPO}" >/dev/null 2>&1; then
        return 0
    fi
    log "WARNING: could not register snapshot repo ${SNAPSHOT_REPO}"
    return 1
}

# Create a snapshot of hits_1m for fast future restores.
_create_1m_snapshot() {
    _register_1m_snapshot_repo || return 0   # best-effort

    log "Creating snapshot ${SNAPSHOT_NAME_1M} ..."
    curl -sf -XPUT \
        "${OS_URL}/_snapshot/${SNAPSHOT_REPO}/${SNAPSHOT_NAME_1M}?wait_for_completion=true" \
        -H 'Content-Type: application/json' \
        -d "{
            \"indices\": \"${INDEX_NAME_1M}\",
            \"include_global_state\": false
        }" >/dev/null 2>&1 || true
    log "Snapshot ${SNAPSHOT_NAME_1M} created."
}

# Public: restore the 1M index from snapshot (deletes existing index first).
restore_1m_snapshot() {
    log "Restoring ${INDEX_NAME_1M} from snapshot ..."
    curl -sf -XDELETE "${OS_URL}/${INDEX_NAME_1M}" >/dev/null 2>&1 || true
    if _try_restore_1m_snapshot; then
        local count
        count=$(curl -sf "${OS_URL}/${INDEX_NAME_1M}/_count" | jq -r '.count // 0')
        log "Restored ${INDEX_NAME_1M}: $count docs."
    else
        die "Failed to restore ${INDEX_NAME_1M} from snapshot. Run load_1m_data first."
    fi
}
