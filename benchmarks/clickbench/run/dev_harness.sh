#!/usr/bin/env bash
# dev_harness.sh — Dev iteration loop: compile → reload-plugin → correctness-gate → benchmark.
# Fail-fast on any step. Emits structured markers for sisyphus monitoring.
#
# Usage:
#   bash dev_harness.sh [OPTIONS]
#
# Options:
#   --query Q1,Q2,...           Comma-separated query numbers to benchmark (1-based)
#   --skip-correctness          Skip the correctness gate
#   --warmup N                  Warmup passes for benchmark (default: 1)
#   --correctness-threshold N   Minimum PASS count to proceed (default: 38)

set -euo pipefail

source "$(dirname "$0")/../setup/setup_common.sh"

# -- Defaults ------------------------------------------------------------------
QUERY_LIST=""
SKIP_CORRECTNESS=false
WARMUP_PASSES=1
CORRECTNESS_THRESHOLD=38

# -- Parse flags ---------------------------------------------------------------
while [ $# -gt 0 ]; do
    case "$1" in
        --query)
            QUERY_LIST="$2"; shift 2 ;;
        --skip-correctness)
            SKIP_CORRECTNESS=true; shift ;;
        --warmup)
            WARMUP_PASSES="$2"; shift 2 ;;
        --correctness-threshold)
            CORRECTNESS_THRESHOLD="$2"; shift 2 ;;
        *)
            die "Unknown flag: $1" ;;
    esac
done

# -- Derive PROJECT_ROOT from BENCH_DIR (benchmarks/clickbench -> repo root) ---
PROJECT_ROOT="${REPO_DIR}"

# =============================================================================
# Step 1: COMPILE — fast compile check before expensive reload
# =============================================================================
log "Step 1/4: Compiling DQE..."
if ! (cd "$PROJECT_ROOT" && ./gradlew :dqe:compileJava); then
    log "###FAILED: compile###"
    exit 1
fi
log "Compile OK."

# =============================================================================
# Step 2: RELOAD — rebuild plugin zip, reinstall, restart OpenSearch
# =============================================================================
log "Step 2/4: Reloading SQL plugin..."
if ! reload_sql_plugin; then
    log "###FAILED: reload-plugin###"
    exit 1
fi
log "Reload OK."

# =============================================================================
# Step 3: CORRECTNESS GATE — ensure minimum pass count before benchmarking
# =============================================================================
CORRECTNESS_PASS="skipped"
if [ "$SKIP_CORRECTNESS" = true ]; then
    log "Step 3/4: Correctness gate SKIPPED (--skip-correctness)."
else
    log "Step 3/4: Running correctness check..."
    # Stream output in real-time via tee, capture to temp file for parsing
    CORRECTNESS_TMP=$(mktemp)
    bash "${BENCH_DIR}/correctness/check_correctness.sh" 2>&1 | tee "$CORRECTNESS_TMP" || true

    # Parse "Summary: N/43 PASS, ..." from captured output
    PASS_COUNT=$(grep -oP 'Summary:\s+\K[0-9]+(?=/43 PASS)' "$CORRECTNESS_TMP" || echo "0")
    rm -f "$CORRECTNESS_TMP"
    CORRECTNESS_PASS="${PASS_COUNT}/43"

    if [ "$PASS_COUNT" -lt "$CORRECTNESS_THRESHOLD" ]; then
        log "###FAILED: correctness=${PASS_COUNT}/43 (below threshold ${CORRECTNESS_THRESHOLD})###"
        exit 1
    fi
    log "Correctness gate passed: ${PASS_COUNT}/43 (threshold: ${CORRECTNESS_THRESHOLD})."
fi

# =============================================================================
# Step 4: BENCHMARK — run queries with timing
# =============================================================================
log "Step 4/4: Running benchmark..."
BENCH_ARGS=(--warmup "$WARMUP_PASSES" --num-tries 3)

# Pass each query number individually (run_opensearch.sh takes --query N)
if [ -n "$QUERY_LIST" ]; then
    IFS=',' read -ra QUERIES <<< "$QUERY_LIST"
    for q in "${QUERIES[@]}"; do
        log "  Benchmarking query $q..."
        if ! bash "${BENCH_DIR}/run/run_opensearch.sh" "${BENCH_ARGS[@]}" --query "$q"; then
            log "###FAILED: benchmark (query $q)###"
            exit 1
        fi
    done
else
    if ! bash "${BENCH_DIR}/run/run_opensearch.sh" "${BENCH_ARGS[@]}"; then
        log "###FAILED: benchmark###"
        exit 1
    fi
fi

log "###COMPLETE: compile=OK reload=OK correctness=${CORRECTNESS_PASS} benchmark=done###"
