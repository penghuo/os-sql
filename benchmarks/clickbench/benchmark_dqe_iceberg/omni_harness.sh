#!/usr/bin/env bash
# omni_harness.sh — Run DQE Iceberg ClickBench: correctness + performance + Trino comparison.
#
# Usage:
#   bash omni_harness.sh [OPTIONS]
#
# Options:
#   --query N           Run only query N (1-based). Default: all 43.
#   --tries N           Runs per query for timing (default: 3).
#   --timeout N         Per-query timeout in seconds (default: 120).
#   --skip-correctness  Skip correctness check, run perf only.
#   --correctness-only  Run correctness only, skip perf.
#
# Outputs:
#   results/correctness_report.txt   — per-query PASS/FAIL/ERROR
#   results/<instance>.json          — performance results (ClickBench format)
#   results/comparison.txt           — side-by-side DQE vs Trino

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
GOLDEN_DIR="$SCRIPT_DIR/golden"
BASELINE="$SCRIPT_DIR/baseline/trino_r5.4xlarge.json"
QUERY_FILE="$SCRIPT_DIR/../queries/queries_iceberg.sql"
RESULT_DIR="$SCRIPT_DIR/results"
OS_URL="${OS_URL:-http://localhost:9200}"

TRIES=3
TIMEOUT=120
ONLY_QUERY=""
SKIP_CORRECTNESS=false
CORRECTNESS_ONLY=false

while [ $# -gt 0 ]; do
    case "$1" in
        --query)         ONLY_QUERY="$2"; shift 2 ;;
        --tries)         TRIES="$2"; shift 2 ;;
        --timeout)       TIMEOUT="$2"; shift 2 ;;
        --skip-correctness) SKIP_CORRECTNESS=true; shift ;;
        --correctness-only) CORRECTNESS_ONLY=true; shift ;;
        *) echo "Unknown flag: $1" >&2; exit 1 ;;
    esac
done

mkdir -p "$RESULT_DIR"

log() { echo "[$(date +%H:%M:%S)] $*"; }

# Detect instance type
INSTANCE=$(curl -sf http://169.254.169.254/latest/meta-data/instance-type 2>/dev/null || hostname)

run_query() {
    local query="$1"
    local escaped
    escaped=$(printf '%s' "$query" | jq -Rs '.')
    curl -sf -XPOST "${OS_URL}/_plugins/_trino_sql" \
        -H 'Content-Type: application/json' \
        -d "{\"query\": $escaped}" \
        --max-time "$TIMEOUT" 2>/dev/null
}

# Convert JSON response to TSV (matching golden format)
json_to_tsv() {
    python3 -c "
import sys, json
d = json.loads(sys.stdin.read())
if 'error' in d:
    print('ERROR: ' + d['error'][:200], file=sys.stderr)
    sys.exit(1)
for row in d.get('datarows', []):
    print('\t'.join(str(v) for v in row))
"
}

# ── Correctness ──────────────────────────────────────────────────────────────
run_correctness() {
    log "=== Correctness Check ==="
    local pass=0 fail=0 error=0 skip=0
    local report="$RESULT_DIR/correctness_report.txt"
    : > "$report"

    local qnum=0
    while IFS= read -r query; do
        qnum=$((qnum + 1))
        [ -z "$query" ] && continue
        if [ -n "$ONLY_QUERY" ] && [ "$qnum" -ne "$ONLY_QUERY" ]; then
            skip=$((skip + 1))
            continue
        fi

        # No non-deterministic queries — all have tiebreakers now

        query=$(echo "$query" | sed 's/;[[:space:]]*$//')
        local golden="$GOLDEN_DIR/q$(printf '%02d' "$qnum").expected"
        if [ ! -f "$golden" ]; then
            echo "Q${qnum}: SKIP (no golden)" >> "$report"
            skip=$((skip + 1))
            continue
        fi

        local actual
        actual=$(run_query "$query" | json_to_tsv 2>/tmp/harness_q${qnum}_err.txt) || true

        if [ -z "$actual" ]; then
            local err
            err=$(cat /tmp/harness_q${qnum}_err.txt 2>/dev/null | head -1)
            echo "Q${qnum}: ERROR ${err}" >> "$report"
            error=$((error + 1))
            log "  Q${qnum}: ERROR ${err}"
            continue
        fi

        local expected
        expected=$(cat "$golden")
        if [ "$actual" = "$expected" ]; then
            echo "Q${qnum}: PASS" >> "$report"
            pass=$((pass + 1))
            log "  Q${qnum}: PASS"
        else
            echo "Q${qnum}: FAIL" >> "$report"
            diff <(echo "$expected") <(echo "$actual") > "$RESULT_DIR/diff_q${qnum}.txt" 2>/dev/null || true
            fail=$((fail + 1))
            log "  Q${qnum}: FAIL (diff saved)"
        fi
    done < "$QUERY_FILE"

    log "Correctness: ${pass} pass, ${fail} fail, ${error} error, ${skip} skip (out of 43)"
    echo "---" >> "$report"
    echo "PASS=${pass} FAIL=${fail} ERROR=${error} SKIP=${skip}" >> "$report"
    return 0
}

# ── Performance ──────────────────────────────────────────────────────────────
run_performance() {
    log "=== Performance Benchmark ($TRIES runs per query) ==="
    local results="[]"
    local qnum=0

    while IFS= read -r query; do
        qnum=$((qnum + 1))
        [ -z "$query" ] && continue
        if [ -n "$ONLY_QUERY" ] && [ "$qnum" -ne "$ONLY_QUERY" ]; then
            results=$(echo "$results" | jq ". + [[null]]")
            continue
        fi

        query=$(echo "$query" | sed 's/;[[:space:]]*$//')
        local escaped
        escaped=$(printf '%s' "$query" | jq -Rs '.')
        local times="[]"

        for i in $(seq 1 "$TRIES"); do
            local start_ns end_ns exit_code http_code
            start_ns=$(date +%s%N)
            exit_code=0
            http_code=$(curl -sf -o /dev/null -w '%{http_code}' \
                -XPOST "${OS_URL}/_plugins/_trino_sql" \
                -H 'Content-Type: application/json' \
                -d "{\"query\": $escaped}" \
                --max-time "$TIMEOUT" 2>/dev/null) || exit_code=$?
            end_ns=$(date +%s%N)

            if [ "$exit_code" -eq 0 ] && [ "$http_code" -ge 200 ] && [ "$http_code" -lt 300 ]; then
                local ms=$(( (end_ns - start_ns) / 1000000 ))
                local t
                t=$(awk "BEGIN {printf \"%.3f\", $ms / 1000}")
                times=$(echo "$times" | jq ". + [$t]")
                log "  Q${qnum} run $i: ${t}s"
            else
                times=$(echo "$times" | jq ". + [null]")
                log "  Q${qnum} run $i: FAILED (exit=$exit_code http=$http_code)"
            fi
        done
        results=$(echo "$results" | jq ". + [$times]")
    done < "$QUERY_FILE"

    local result_file="$RESULT_DIR/${INSTANCE}.json"
    jq -n \
        --arg system "DQE Iceberg (trino_sql)" \
        --arg date "$(date +%Y-%m-%d)" \
        --arg machine "$INSTANCE" \
        --argjson result "$results" \
        '{system:$system, date:$date, machine:$machine, tags:["Java","OpenSearch","Iceberg"], result:$result}' \
        > "$result_file"
    log "Results written to $result_file"
}

# ── Comparison ───────────────────────────────────────────────────────────────
run_comparison() {
    local dqe_file="$RESULT_DIR/${INSTANCE}.json"
    [ ! -f "$dqe_file" ] && return
    [ ! -f "$BASELINE" ] && { log "No Trino baseline found, skipping comparison"; return; }

    log "=== DQE vs Trino Comparison ==="
    python3 -c "
import json, sys

with open('$dqe_file') as f: dqe = json.load(f)
with open('$BASELINE') as f: trino = json.load(f)

print(f'{\"Q\":>3} {\"DQE(s)\":>9} {\"Trino(s)\":>9} {\"Ratio\":>7} {\"Status\":>8}')
print('-' * 42)
dqe_total = trino_total = 0
wins = losses = 0
for i in range(43):
    q = i + 1
    dv = [x for x in dqe['result'][i] if x is not None]
    tv = [x for x in trino['result'][i] if x is not None]
    d = min(dv) if dv else None
    t = min(tv) if tv else None
    if d and t:
        ratio = d / t
        status = '✓' if ratio <= 2.0 else '✗'
        if ratio <= 1.0: wins += 1
        else: losses += 1
        dqe_total += d; trino_total += t
        print(f'Q{q:02d} {d:9.3f} {t:9.3f} {ratio:7.2f}x {status:>8}')
    elif d:
        dqe_total += d
        print(f'Q{q:02d} {d:9.3f} {\"N/A\":>9} {\"\":>7} {\"\":>8}')
    else:
        print(f'Q{q:02d} {\"FAIL\":>9} {t or \"N/A\":>9} {\"\":>7} {\"\":>8}')

within2x = sum(1 for i in range(43)
    if (dv := [x for x in dqe['result'][i] if x is not None])
    and (tv := [x for x in trino['result'][i] if x is not None])
    and min(dv) / min(tv) <= 2.0)
total_q = sum(1 for i in range(43) if any(x is not None for x in dqe['result'][i]))
print(f'\nDQE total: {dqe_total:.1f}s | Trino total: {trino_total:.1f}s')
print(f'Within 2x of Trino: {within2x}/{total_q}')
print(f'Faster than Trino: {wins} | Slower: {losses}')
" | tee "$RESULT_DIR/comparison.txt"
}

# ── Main ─────────────────────────────────────────────────────────────────────
log "DQE Iceberg ClickBench Harness"
log "Instance: $INSTANCE | Endpoint: $OS_URL"

if [ "$CORRECTNESS_ONLY" = true ]; then
    run_correctness
elif [ "$SKIP_CORRECTNESS" = true ]; then
    run_performance
    run_comparison
else
    run_correctness
    run_performance
    run_comparison
fi

log "Done."
