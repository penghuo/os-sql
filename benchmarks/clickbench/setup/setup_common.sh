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
