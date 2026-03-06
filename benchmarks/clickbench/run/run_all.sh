#!/usr/bin/env bash
# run_all.sh — Orchestrate ClickBench benchmark setup, loading, and execution.
# Usage: ./run_all.sh [setup|load|benchmark|correctness|all]

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
BENCH_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"

usage() {
    cat <<EOF
Usage: $(basename "$0") <command>

Commands:
  setup        Install OpenSearch + ClickHouse (idempotent)
  load         Download dataset + load into both engines
  snapshot     Create EBS snapshots of loaded data
  restore      Restore data from EBS snapshots (skip load)
  benchmark    Run 43 queries × 3 runs on both engines
  correctness  Compare results between engines
  all          Run setup → load → benchmark → correctness

Environment variables (override defaults in setup/setup_common.sh):
  OS_HOST, OS_PORT, CH_HOST, CH_PORT, NUM_TRIES, BULK_SIZE

Examples:
  ./run_all.sh all                          # Full benchmark from scratch
  ./run_all.sh restore && ./run_all.sh benchmark  # Re-run with cached data
  NUM_TRIES=5 ./run_all.sh benchmark        # 5 runs per query
EOF
    exit 1
}

COMMAND="${1:-}"
[ -z "$COMMAND" ] && usage

do_setup() {
    echo "=== SETUP: ClickHouse ==="
    bash "$BENCH_DIR/setup/setup_clickhouse.sh"

    echo ""
    echo "=== SETUP: OpenSearch ==="
    bash "$BENCH_DIR/setup/setup_opensearch.sh"
}

do_load() {
    echo "=== DOWNLOAD: ClickBench Dataset ==="
    bash "$BENCH_DIR/data/download_data.sh"

    echo ""
    echo "=== LOAD: ClickHouse ==="
    bash "$BENCH_DIR/data/load_clickhouse.sh"

    echo ""
    echo "=== LOAD: OpenSearch ==="
    bash "$BENCH_DIR/data/load_opensearch.sh"
}

do_snapshot() {
    bash "$BENCH_DIR/data/snapshot.sh" create
}

do_restore() {
    bash "$BENCH_DIR/data/snapshot.sh" restore
}

do_benchmark() {
    echo "=== BENCHMARK: ClickHouse ==="
    bash "$BENCH_DIR/run/run_clickhouse.sh"

    echo ""
    echo "=== BENCHMARK: OpenSearch ==="
    bash "$BENCH_DIR/run/run_opensearch.sh"

    echo ""
    echo "=== RESULTS ==="
    echo "ClickHouse: $BENCH_DIR/results/clickhouse/"
    ls -la "$BENCH_DIR/results/clickhouse/"*.json 2>/dev/null || echo "  (no results)"
    echo "OpenSearch:  $BENCH_DIR/results/opensearch/"
    ls -la "$BENCH_DIR/results/opensearch/"*.json 2>/dev/null || echo "  (no results)"
}

do_correctness() {
    echo "=== CORRECTNESS CHECK ==="
    bash "$BENCH_DIR/correctness/check_correctness.sh"
}

case "$COMMAND" in
    setup)       do_setup ;;
    load)        do_load ;;
    snapshot)    do_snapshot ;;
    restore)     do_restore ;;
    benchmark)   do_benchmark ;;
    correctness) do_correctness ;;
    all)
        do_setup
        echo ""
        do_load
        echo ""
        do_benchmark
        echo ""
        do_correctness
        ;;
    *)           usage ;;
esac
