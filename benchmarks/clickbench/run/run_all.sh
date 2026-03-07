#!/usr/bin/env bash
# run_all.sh — Unified orchestrator for ClickBench benchmark tiers.
#
# Supports setup, data loading, correctness checking, and performance
# benchmarking across 1M-row and full (100M) datasets.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
BENCH_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"

# Source shared variables and helper functions
# shellcheck source=../setup/setup_common.sh
source "$BENCH_DIR/setup/setup_common.sh"

# ---------------------------------------------------------------------------
# Global option defaults
# ---------------------------------------------------------------------------
DRY_RUN=false
QUERY_FLAG=""          # left empty unless --query N is provided

# ---------------------------------------------------------------------------
# Usage
# ---------------------------------------------------------------------------
usage() {
    cat <<'EOF'
Usage: run_all.sh [OPTIONS] <command>

Commands:
  setup           Install OpenSearch + ClickHouse (idempotent)
  load-1m         Load 1M subset into both engines + create snapshot
  load-full       Download full dataset + load into both engines
  reload-plugin   Rebuild SQL plugin, restart OpenSearch
  correctness     Run 43 queries on 1M, compare results vs ClickHouse
  perf-lite       Run 43 queries x 3 on 1M, record timing + correctness
  performance     Run 43 queries x 3 on 100M, standard ClickBench benchmark
  restore-1m      Restore 1M index from OpenSearch snapshot
  snapshot        Create EBS snapshots of loaded data
  restore         Restore data from EBS snapshots

Aliases (backwards compatibility):
  load            Same as load-full
  benchmark       Same as performance
  all             Run setup -> load-full -> performance (legacy)

Global options:
  --query N       Run only query N (passed to correctness/perf-lite/performance)
  --dry-run       Print what would be executed without running
  -h, --help      Show this help message

Environment variables (override defaults in setup/setup_common.sh):
  OS_HOST, OS_PORT, CH_HOST, CH_PORT, NUM_TRIES, QUERY_TIMEOUT

Examples — typical dev iteration loop:
  ./run_all.sh load-1m                # One-time: load 1M subset
  ./run_all.sh reload-plugin          # After code change: rebuild + restart
  ./run_all.sh correctness            # Fast correctness check on 1M
  ./run_all.sh correctness --query 5  # Test single query
  ./run_all.sh perf-lite              # Timing + correctness on 1M
  ./run_all.sh performance            # Full 100M benchmark

Examples — full benchmark from scratch:
  ./run_all.sh setup                  # Install engines
  ./run_all.sh load-full              # Download + load full dataset
  ./run_all.sh performance            # Run benchmark
  ./run_all.sh snapshot               # Save EBS snapshots

Examples — restore and re-run:
  ./run_all.sh restore                # Restore from EBS snapshots
  ./run_all.sh restore-1m             # Restore 1M index from OS snapshot
  ./run_all.sh benchmark              # Alias for performance
EOF
    exit 1
}

# ---------------------------------------------------------------------------
# Dry-run helper
# ---------------------------------------------------------------------------
# For external script calls, wraps execution so --dry-run prints instead of
# running.  Functions from setup_common.sh (load_1m_data, reload_sql_plugin,
# restore_1m_snapshot) are called directly — they are in-process and cannot be
# meaningfully dry-run.
run_cmd() {
    if [ "$DRY_RUN" = true ]; then
        echo "[DRY RUN] $*"
    else
        "$@"
    fi
}

# ---------------------------------------------------------------------------
# Parse all arguments (options and command can be in any order)
# ---------------------------------------------------------------------------
COMMAND=""

while [ $# -gt 0 ]; do
    case "$1" in
        --dry-run)
            DRY_RUN=true; shift ;;
        --query)
            QUERY_FLAG="--query $2"; shift 2 ;;
        -h|--help)
            usage ;;
        -*)
            die "Unknown option: $1" ;;
        *)
            if [ -z "$COMMAND" ]; then
                COMMAND="$1"
            else
                die "Unexpected argument: $1 (command already set to '$COMMAND')"
            fi
            shift ;;
    esac
done

[ -z "$COMMAND" ] && usage

# ---------------------------------------------------------------------------
# Command implementations
# ---------------------------------------------------------------------------

do_setup() {
    echo "=== SETUP: ClickHouse ==="
    run_cmd bash "$BENCH_DIR/setup/setup_clickhouse.sh"

    echo ""
    echo "=== SETUP: OpenSearch ==="
    run_cmd bash "$BENCH_DIR/setup/setup_opensearch.sh"
}

do_load_1m() {
    echo "=== LOAD: 1M subset into ClickHouse + OpenSearch ==="
    if [ "$DRY_RUN" = true ]; then
        echo "[DRY RUN] load_1m_data"
    else
        load_1m_data
    fi
}

do_load_full() {
    echo "=== DOWNLOAD: ClickBench Dataset ==="
    run_cmd bash "$BENCH_DIR/data/download_data.sh"

    echo ""
    echo "=== LOAD: ClickHouse ==="
    run_cmd bash "$BENCH_DIR/data/load_clickhouse.sh"

    echo ""
    echo "=== LOAD: OpenSearch ==="
    run_cmd bash "$BENCH_DIR/data/load_opensearch.sh"
}

do_reload_plugin() {
    echo "=== RELOAD: SQL Plugin ==="
    if [ "$DRY_RUN" = true ]; then
        echo "[DRY RUN] reload_sql_plugin"
    else
        reload_sql_plugin
    fi
}

do_correctness() {
    echo "=== CORRECTNESS: 1M dataset ==="
    # shellcheck disable=SC2086
    run_cmd bash "$BENCH_DIR/correctness/check_correctness.sh" --dataset 1m $QUERY_FLAG
}

do_perf_lite() {
    local instance_type
    instance_type=$(get_instance_type)

    echo "=== PERF-LITE: ClickHouse (1M, ${NUM_TRIES} tries) ==="
    mkdir -p "$PERF_LITE_RESULT_DIR"
    # shellcheck disable=SC2086
    run_cmd bash "$BENCH_DIR/run/run_clickhouse.sh" \
        --dataset 1m \
        --timeout "$QUERY_TIMEOUT" \
        --num-tries "$NUM_TRIES" \
        --output-dir "$PERF_LITE_RESULT_DIR" \
        $QUERY_FLAG

    # Rename instance-type result to clickhouse.json
    if [ "$DRY_RUN" != true ] && [ -f "$PERF_LITE_RESULT_DIR/${instance_type}.json" ]; then
        mv "$PERF_LITE_RESULT_DIR/${instance_type}.json" "$PERF_LITE_RESULT_DIR/clickhouse.json"
    fi

    echo ""
    echo "=== PERF-LITE: OpenSearch (1M, ${NUM_TRIES} tries) ==="
    # shellcheck disable=SC2086
    run_cmd bash "$BENCH_DIR/run/run_opensearch.sh" \
        --dataset 1m \
        --timeout "$QUERY_TIMEOUT" \
        --num-tries "$NUM_TRIES" \
        --output-dir "$PERF_LITE_RESULT_DIR" \
        $QUERY_FLAG

    # Rename instance-type result to opensearch.json
    if [ "$DRY_RUN" != true ] && [ -f "$PERF_LITE_RESULT_DIR/${instance_type}.json" ]; then
        mv "$PERF_LITE_RESULT_DIR/${instance_type}.json" "$PERF_LITE_RESULT_DIR/opensearch.json"
    fi

    echo ""
    echo "=== PERF-LITE: Correctness cross-reference ==="
    # shellcheck disable=SC2086
    run_cmd bash "$BENCH_DIR/correctness/check_correctness.sh" --dataset 1m $QUERY_FLAG || true

    echo ""
    echo "=== PERF-LITE: Building summary ==="
    if [ "$DRY_RUN" = true ]; then
        echo "[DRY RUN] _build_perf_lite_summary"
    else
        _build_perf_lite_summary
    fi
}

do_performance() {
    echo "=== PERFORMANCE: ClickHouse (full dataset, ${NUM_TRIES} tries) ==="
    mkdir -p "$PERFORMANCE_RESULT_DIR/clickhouse" "$PERFORMANCE_RESULT_DIR/opensearch"
    # shellcheck disable=SC2086
    run_cmd bash "$BENCH_DIR/run/run_clickhouse.sh" \
        --timeout "$QUERY_TIMEOUT" \
        --output-dir "$PERFORMANCE_RESULT_DIR/clickhouse" \
        $QUERY_FLAG

    echo ""
    echo "=== PERFORMANCE: OpenSearch (full dataset, ${NUM_TRIES} tries) ==="
    # shellcheck disable=SC2086
    run_cmd bash "$BENCH_DIR/run/run_opensearch.sh" \
        --timeout "$QUERY_TIMEOUT" \
        --output-dir "$PERFORMANCE_RESULT_DIR/opensearch" \
        $QUERY_FLAG
}

do_restore_1m() {
    echo "=== RESTORE: 1M index from snapshot ==="
    if [ "$DRY_RUN" = true ]; then
        echo "[DRY RUN] restore_1m_snapshot"
    else
        restore_1m_snapshot
    fi
}

do_snapshot() {
    echo "=== SNAPSHOT: Create EBS snapshots ==="
    run_cmd bash "$BENCH_DIR/data/snapshot.sh" create
}

do_restore() {
    echo "=== RESTORE: EBS snapshots ==="
    run_cmd bash "$BENCH_DIR/data/snapshot.sh" restore
}

# ---------------------------------------------------------------------------
# Perf-lite summary builder
# ---------------------------------------------------------------------------
# Merges correctness summary.json with clickhouse.json and opensearch.json
# timing data into a single perf-lite summary.
_build_perf_lite_summary() {
    local ch_file="$PERF_LITE_RESULT_DIR/clickhouse.json"
    local os_file="$PERF_LITE_RESULT_DIR/opensearch.json"
    local correctness_file="$CORRECTNESS_RESULT_DIR/summary.json"
    local summary_file="$PERF_LITE_RESULT_DIR/summary.json"

    if [ ! -f "$ch_file" ]; then
        log "WARNING: $ch_file not found, skipping summary."
        return
    fi
    if [ ! -f "$os_file" ]; then
        log "WARNING: $os_file not found, skipping summary."
        return
    fi

    # Build the summary with jq, merging timing + correctness
    jq -n \
        --arg date "$(date +%Y-%m-%d)" \
        --arg dataset "1m" \
        --slurpfile ch "$ch_file" \
        --slurpfile os "$os_file" \
        --slurpfile corr "$correctness_file" \
        '
        {
            date: $date,
            dataset: $dataset,
            correctness: ($corr[0] // {}),
            queries: [
                range($ch[0].result | length) |
                . as $i |
                {
                    q: ($i + 1),
                    correct: (
                        ($corr[0].queries // [])[]? |
                        select(.q == ($i + 1)) |
                        (.status | ascii_downcase)
                    ) // "skip",
                    ch_times: $ch[0].result[$i],
                    os_times: $os[0].result[$i]
                }
            ]
        }
        ' > "$summary_file" 2>/dev/null || {
        # Fallback: build without correctness data
        log "WARNING: could not merge correctness data, building summary without it."
        jq -n \
            --arg date "$(date +%Y-%m-%d)" \
            --arg dataset "1m" \
            --slurpfile ch "$ch_file" \
            --slurpfile os "$os_file" \
            '
            {
                date: $date,
                dataset: $dataset,
                correctness: {},
                queries: [
                    range($ch[0].result | length) |
                    . as $i |
                    {
                        q: ($i + 1),
                        correct: "skip",
                        ch_times: $ch[0].result[$i],
                        os_times: $os[0].result[$i]
                    }
                ]
            }
            ' > "$summary_file"
    }

    log "Perf-lite summary written to $summary_file"
}

# ---------------------------------------------------------------------------
# Command dispatch
# ---------------------------------------------------------------------------
case "$COMMAND" in
    setup)          do_setup ;;
    load-1m)        do_load_1m ;;
    load-full|load) do_load_full ;;
    reload-plugin)  do_reload_plugin ;;
    correctness)    do_correctness ;;
    perf-lite)      do_perf_lite ;;
    performance|benchmark)
                    do_performance ;;
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
    *)              usage ;;
esac
