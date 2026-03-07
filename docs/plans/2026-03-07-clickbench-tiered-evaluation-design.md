# ClickBench Tiered Evaluation Design

**Date:** 2026-03-07
**Status:** Approved

## Problem

The current ClickBench evaluation runs all 43 queries against the full 100M-row dataset. With 0/43 queries passing and even `SELECT COUNT(*) FROM hits` taking a long time, this is too slow for iterative development. We need a faster feedback loop for correctness work, plus a lightweight performance regression tier.

## Solution: 3-Tier Evaluation

### Tier Definitions

| Property | Correctness | Performance-lite | Performance |
|---|---|---|---|
| Dataset | 1M (1 parquet file) | 1M (1 parquet file) | 100M (100 parquet files) |
| Purpose | Validate query results match ClickHouse | Timing baseline for regression detection | Standard ClickBench comparison |
| Query runs | 1 (result only) | 3 (timed) | 3 (timed) |
| Timeout | 60s | 60s | 60s |
| Output | Diffs + summary.json | ClickBench JSON + correctness flags | ClickBench JSON |
| ClickHouse table | `hits_1m` | `hits_1m` (reuse) | `hits` |
| OpenSearch index | `hits_1m` | `hits_1m` (reuse) | `hits` |
| Cache clearing | No | Yes (before cold run) | Yes (before cold run) |

Correctness and perf-lite share the same 1M dataset. Data is loaded once, both tiers reuse it.

### Commands

```
run_all.sh <command>

  setup           Install OpenSearch + ClickHouse (existing, unchanged)
  load-1m         Load 1 parquet file into hits_1m (both engines) + snapshot
  load-full       Load 100 parquet files into hits (both engines)
  reload-plugin   Rebuild SQL plugin -> stop OS -> swap -> restart
  correctness     Run 43 queries on 1M, compare results (PASS/FAIL/SKIP)
  perf-lite       Run 43 queries x 3 on 1M, record timing + correctness flags
  performance     Run 43 queries x 3 on 100M, record timing (standard ClickBench)
  restore-1m      Restore hits_1m from snapshot (fast reset)

Aliases (backwards compatibility):
  load            -> load-full
  benchmark       -> performance
```

### Dev Iteration Loop

```bash
# First time (cluster already running with 100M data):
run_all.sh load-1m           # One-time: add 1M subset

# Iteration:
# 1. Edit SQL plugin code
run_all.sh reload-plugin     # Build + restart (~60s)
run_all.sh correctness       # Test 43 queries on 1M (~2min)
# 2. Check results, fix, repeat
```

### Plugin Reload Flow

`run_all.sh reload-plugin` does:
1. Build SQL plugin: `./gradlew :opensearch-sql-plugin:assemble -x test`
2. Stop OpenSearch
3. Remove old SQL plugin: `opensearch-plugin remove opensearch-sql`
4. Install new plugin from `plugin/build/distributions/opensearch-sql-*.zip`
5. Restart OpenSearch
6. Wait for cluster health green

The 1M and 100M data survive plugin reloads (persisted in index, not plugin).

### Results Structure

```
benchmarks/clickbench/results/
├── correctness/
│   ├── summary.json              # { pass, fail, skip, total, queries: [...] }
│   ├── expected_1m/              # ClickHouse ground truth for 1M
│   │   ├── q01.expected ... q43.expected
│   └── diffs/                    # Per-query diffs (failures only)
│       ├── diff_q08.txt ...
├── perf-lite/
│   ├── opensearch.json           # ClickBench JSON format, 1M dataset
│   ├── clickhouse.json           # ClickBench JSON format, 1M dataset
│   └── summary.json              # Timing + correctness flags per query
├── performance/
│   ├── opensearch/
│   │   └── {instance-type}.json  # ClickBench-compatible JSON
│   └── clickhouse/
│       └── {instance-type}.json  # ClickBench-compatible JSON
```

- Correctness: human-readable diffs + machine-readable summary.json
- Perf-lite: ClickBench JSON + summary with correctness cross-reference
- Performance: existing ClickBench-compatible format for viewer dashboard

### Performance-lite Behavior

- Times all queries regardless of correctness status
- Flags each query with its correctness result (pass/fail/skip)
- Summary format: `{ "queries": [{"q": 1, "correct": true, "os_times": [...], "ch_times": [...]}] }`

## Files to Modify

1. **`run/run_all.sh`** — Add new commands, keep backwards-compatible aliases
2. **`setup/setup_common.sh`** — Add tier variables, `reload_sql_plugin()` helper
3. **`run/run_opensearch.sh`** — Parameterize: `--dataset 1m|full`, `--timeout`, `--output-dir`
4. **`run/run_clickhouse.sh`** — Parameterize similarly
5. **`correctness/check_correctness.sh`** — Parameterize for 1M and full datasets

## Files to Delete

- **`quick_test.sh`** — Absorbed into `run_all.sh correctness` + `run_all.sh load-1m`

## Verification Strategy

1. **shellcheck** on all modified scripts
2. **Dry-run mode**: `run_all.sh --dry-run <command>` prints commands without executing
3. **Golden comparison**: correctness tier output must match existing `quick_test.sh` output on same 1M data
4. **Format validation**: perf-lite and performance JSON must be valid ClickBench format
5. **Single-query testing**: `--query N` flag carries over for debugging individual queries
