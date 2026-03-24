# Phase D Handover Document

> **Purpose:** Bootstrap a code-agent on a fresh machine to continue DQE optimization from 25/43 to 43/43 within 2x of ClickHouse-Parquet.

## 1. Goal

Optimize the DQE (Direct Query Engine) in OpenSearch so that all 43 ClickBench queries run within **2x of the official ClickHouse-Parquet benchmark** on c6a.4xlarge.

Current score: **25/43 within 2x**. Target: **43/43**.

## 2. Prerequisites

**Minimum hardware:** 16+ vCPU, 64+ GB RAM (recommended: 32 vCPU, 128 GB — e.g., m5.8xlarge or c6a.4xlarge).

**Required software:** JDK 21, Python 3, `jq`, `wget`, `curl`, `bc`, `sudo` access.

ClickHouse is needed for correctness testing (comparing DQE output vs CH ground truth). The setup scripts install it automatically.

## 3. Environment Setup From Scratch

All paths below are relative to where you clone the repo. The scripts use `$REPO_DIR` internally and do not hardcode home directories.

### Step 1: Clone and checkout

```bash
git clone git@github.com:penghuo/os-sql.git <your-workspace>/wukong
cd <your-workspace>/wukong
git checkout wukong
```

### Step 2: Install OpenSearch + ClickHouse

```bash
cd benchmarks/clickbench
bash run/run_all.sh setup
```

This runs `setup/setup_opensearch.sh` and `setup/setup_clickhouse.sh`, which:
- Install JDK 21 if missing (via yum or apt)
- Download OpenSearch 3.6.0-SNAPSHOT minimal distribution to `/opt/opensearch/`
- Build the SQL plugin from the repo (`./gradlew :opensearch-sql-plugin:assemble`)
- Install the plugin + job-scheduler dependency into OpenSearch
- Configure single-node cluster (no security, `discovery.type: single-node`)
- Set JVM heap to 50% of system RAM (capped at 16GB by default)
- Install ClickHouse via the official repo

All steps are idempotent — safe to re-run.

### Step 3: Increase heap to 32GB (required for 100M benchmark)

The default 16GB cap is too low for high-cardinality GROUP BY queries. Override it:

```bash
echo "-Xms32g" | sudo tee /opt/opensearch/config/jvm.options.d/heap.options
echo "-Xmx32g" | sudo tee -a /opt/opensearch/config/jvm.options.d/heap.options
```

Then restart OpenSearch:

```bash
bash run/run_all.sh reload-plugin
```

### Step 4: Download data and load into both engines

```bash
# Download 100 Parquet files (~15GB total, parallel download)
# Then load into ClickHouse (MergeTree) and OpenSearch (Lucene)
# OpenSearch bulk load takes ~45 min for 100M rows
bash run/run_all.sh load-full
```

What this does:
1. `data/download_data.sh` — downloads `hits_0.parquet` through `hits_99.parquet` from `https://datasets.clickhouse.com/hits_compatible/athena_partitioned/`
2. `data/load_clickhouse.sh` — creates CH `hits` table (MergeTree), loads all Parquet files
3. `data/load_opensearch.sh` — creates OS `hits` index (103 fields, sorted by CounterID+EventDate+UserID+EventTime+WatchID, 4 primary shards, 0 replicas), uses `clickhouse-local` to convert Parquet→JSON, bulk-loads via `/_bulk` API, then force-merges

Index mapping file: `integ-test/src/test/resources/clickbench/mappings/clickbench_index_mapping.json`

### Step 5: Load 1M subset (for correctness testing)

```bash
bash run/run_all.sh load-1m
```

Creates `hits_1m` in both CH and OS (first 1M rows), generates expected query results from ClickHouse, creates an OpenSearch filesystem snapshot for fast restores.

### Step 6: Verify

```bash
curl -sf http://localhost:9200/_cluster/health | jq .status      # → "yellow"
curl -sf http://localhost:9200/hits/_count | jq .count            # → 99997497
curl -sf http://localhost:9200/hits_1m/_count | jq .count         # → 1000000
clickhouse-client -q "SELECT count() FROM hits"                   # → 99997497

# Quick correctness test
bash run/run_all.sh correctness --query 1                         # → PASS

# Quick benchmark test
bash run/run_opensearch.sh --warmup 1 --num-tries 1 --query 1 --output-dir /tmp/test
```

### Restoring 1M Index (if lost)

```bash
bash run/run_all.sh restore-1m
```

### OpenSearch Snapshot Restore (100M)

Refer to: `docs/plans/2026-03-18-clickbench-full-dataset-benchmark.md` for S3-based snapshot restore of the 100M index.

## 4. Dev Iteration Loop

```bash
# 1. Make code changes in dqe/src/main/java/...

# 2. Compile (fast, ~5s)
./gradlew :dqe:compileJava

# 3. Deploy (rebuild plugin + restart OpenSearch, ~3 min)
cd benchmarks/clickbench && bash run/run_all.sh reload-plugin

# 4. Correctness (1M dataset, ~2 min)
bash run/run_all.sh correctness               # Full: 33/43 pass baseline
bash run/run_all.sh correctness --query 17    # Single query

# 5. Benchmark single query (100M, ~30s)
bash run/run_opensearch.sh --warmup 3 --num-tries 5 --query 17 --output-dir /tmp/q17

# 6. Full benchmark (43 queries × 5 runs + 3 warmup, ~25 min)
bash run/run_opensearch.sh --warmup 3 --num-tries 5 --output-dir /tmp/full

# CRITICAL: Never run reload-plugin while a benchmark is running!
```

### Comparing Results Against Baseline

The official CH-Parquet baseline is in the repo. Compare with:

```bash
cd benchmarks/clickbench
python3 -c "
import json, glob
with open('results/performance/clickhouse_parquet_official/c6a.4xlarge.json') as f:
    ch = json.load(f)
os_file = glob.glob('/tmp/full/*.json')[0]  # filename = instance-type.json
with open(os_file) as f:
    os_data = json.load(f)
within = 0
for i in range(43):
    ch_best = min(ch['result'][i])
    os_times = [x for x in os_data['result'][i] if x is not None]
    if not os_times: continue
    os_best = min(os_times)
    ratio = os_best / ch_best if ch_best > 0 else 999
    if ratio <= 2.0: within += 1
    else: print(f'Q{i:02d}: {ratio:.1f}x ({os_best:.3f}s vs {ch_best:.3f}s)')
print(f'\n{within}/43 within 2x')
"
```

Note: The output filename is `<instance-type>.json` (e.g., `m5.8xlarge.json`, `c6a.4xlarge.json`). Use `glob` to find it.

## 5. Baseline: ClickHouse-Parquet (NOT Native)

**CRITICAL:** Compare against official ClickHouse-Parquet results, not native MergeTree.

- Baseline file (in repo): `benchmarks/clickbench/results/performance/clickhouse_parquet_official/c6a.4xlarge.json`
- Source: https://github.com/ClickHouse/ClickBench/tree/main/clickhouse-parquet/results
- The local ClickHouse loads data into native MergeTree (much faster) — DO NOT use local CH times as baseline
- JSON has 43 entries (0-indexed), each with 3 runs. Use `min()` as the baseline.

## 6. Query Numbering — READ THIS

| Context | Indexing | Example: "Q17" |
|---------|----------|-----------------|
| `--query N` in run scripts | **1-based** | `--query 18` |
| `queries_trino.sql` line | **1-based** | line 18 |
| JSON `result[N]` array | **0-based** | `result[17]` |
| This document (Q00-Q42) | **0-based** | Q17 |

Previous agents confused indexing and measured wrong queries. Double-check before benchmarking.

## 7. Current Status (25/43 Within 2x)

```
WITHIN 2x (25 queries):
Q00(0.3x) Q01(0.1x) Q03(1.1x) Q06(0.1x) Q07(0.8x) Q10(1.5x) Q12(0.5x)
Q14(1.6x) Q17(0.0x) Q19(0.1x) Q20(0.9x) Q21(0.8x) Q22(0.6x) Q23(0.2x)
Q24(0.0x) Q25(1.2x) Q26(0.0x) Q27(1.4x) Q29(1.2x) Q33(0.3x) Q34(0.3x)
Q38(0.6x) Q40(0.2x) Q41(0.7x) Q42(0.6x)

ABOVE 2x (18 queries, sorted by DQE time):
Q28: 2.7x (25.5s vs 9.5s) — REGEXP_REPLACE GROUP BY HAVING
Q18: 5.9x (21.7s vs 3.7s) — GROUP BY UserID, minute, SearchPhrase ORDER BY COUNT(*) LIMIT 10
Q32: 3.2x (14.3s vs 4.5s) — GROUP BY WatchID, ClientIP full-table
Q16: 4.1x ( 7.3s vs 1.8s) — GROUP BY UserID, SearchPhrase ORDER BY COUNT(*) LIMIT 10
Q13: 6.2x ( 6.0s vs 1.0s) — GROUP BY SearchPhrase + COUNT(DISTINCT UserID)
Q09: 8.0x ( 4.9s vs 0.6s) — GROUP BY RegionID + mixed aggs + COUNT(DISTINCT)
Q05: 5.8x ( 4.0s vs 0.7s) — COUNT(DISTINCT SearchPhrase) global
Q08: 3.4x ( 1.9s vs 0.5s) — GROUP BY RegionID + COUNT(DISTINCT UserID)
Q11: 9.5x ( 2.6s vs 0.3s) — GROUP BY MobilePhone,Model + COUNT(DISTINCT)
Q15: 4.1x ( 2.1s vs 0.5s) — GROUP BY UserID ORDER BY COUNT(*) LIMIT 10
Q39: 16.9x (2.4s vs 0.1s) — CounterID=62 filtered, OFFSET, 5 keys + CASE WHEN
Q04: 5.4x ( 2.3s vs 0.4s) — COUNT(DISTINCT UserID) global
Q31: 2.0x ( 2.2s vs 1.1s) — GROUP BY WatchID, ClientIP (borderline — needs 3ms!)
Q35: 5.0x ( 1.9s vs 0.4s) — GROUP BY 1, URL (full-table, high-card)
Q30: 2.3x ( 1.8s vs 0.8s) — GROUP BY SearchEngineID, ClientIP
Q36: 4.0x ( 0.5s vs 0.1s) — GROUP BY ClientIP expressions (4 keys)
Q37: 2.7x ( 0.3s vs 0.1s) — CounterID=62 filtered URL GROUP BY
Q02: 2.2x ( 0.2s vs 0.1s) — SUM+COUNT+AVG, borderline
```

Correctness: 33/43 pass on 1M. Do NOT regress below this.

## 8. What Was Already Done

| Commit | What | Impact |
|--------|------|--------|
| `f071e0e35` | Early termination for LIMIT without ORDER BY | Q18: 62s → 0.17s |
| `4d2bb89e6` | Replace Collector with Weight+Scorer loop (8 paths) | Filtered queries skip empty segments |
| `d519329a9` | `IndexSearcher.count()` for filtered COUNT(*), hoist Weight, skip GC | Minor filtered gains |
| `80f906c13` | Parallelize `executeWithEval` segment iteration | Q29: 450ms → 116ms |
| `89e0ee80d` | Parallel segment scanning for COUNT(DISTINCT), raw HashSet merge | Q09 -46% |
| `9c6001423` | AVG decomposition fix (SUM+COUNT at shard, correct coordinator merge) | Q03: 1.7s → 0.12s |

### What Was Tried But Did NOT Work

- **Routing COUNT(DISTINCT) dedup through FusedGroupByAggregate** — 3x regression
- **Parallel scalar COUNT(DISTINCT) across shards×segments** — 7x regression (thread oversubscription)
- **nextDoc() fast path for executeSingleKeyNumericFlat** — no benefit on 100M (HashMap misses dominate)

## 9. Root Cause Analysis for Remaining 18 Queries

### Category A: COUNT(DISTINCT) Decomposition (6 queries — Q04, Q05, Q08, Q09, Q11, Q13)

Calcite decomposes `GROUP BY x, COUNT(DISTINCT y)` into two-level aggregation, doubling key space. Fix: intercept at `TransportShardExecuteAction` dispatch (line ~280-360, `AggDedupNode` detection), route to fused GROUP BY with per-group `LongOpenHashSet`.

### Category B: High-Cardinality GROUP BY (4 queries — Q15, Q16, Q18, Q32)

HashMap with 17M-50M entries doesn't fit in CPU cache. Fix: parallelize single-key path (Q15), hash-partitioned aggregation (Q16/Q18/Q32).

### Category C: Full-Table VARCHAR GROUP BY (3 queries — Q35, Q36, Q39)

Millions of unique groups from URL/IP keys. Fundamentally limited by per-doc hash overhead.

### Category D: Borderline (5 queries — Q02, Q28, Q30, Q31, Q37)

Q31 needs 3ms. Q28 needs regex Pattern caching. Q02/Q30/Q37 need small targeted fixes.

## 10. Key Code Map

### Execution Flow

```
POST /_plugins/_trino_sql {"query": "SELECT ..."}
  → TransportTrinoSqlAction (coordinator)
    → Parse → Calcite plan → PlanOptimizer → PlanFragmenter
    → Dispatch to shards via TransportShardExecuteAction
      → Route to fused path:
          FusedScanAggregate.execute()          — scalar aggs
          FusedScanAggregate.executeWithEval()   — SUM(col+k) shortcut
          FusedGroupByAggregate.execute()         — GROUP BY
          FusedGroupByAggregate.executeWithTopN() — GROUP BY + ORDER BY + LIMIT
          executeCountDistinctWithHashSets()      — COUNT(DISTINCT)
    → Merge at coordinator → JSON response
```

### Key Files (relative to repo root)

| File | ~Lines | Purpose |
|------|--------|---------|
| `dqe/.../shard/transport/TransportShardExecuteAction.java` | 2,200 | Shard dispatch, pattern detection |
| `dqe/.../shard/source/FusedGroupByAggregate.java` | 11,700 | All GROUP BY execution |
| `dqe/.../shard/source/FusedScanAggregate.java` | 1,600 | Scalar aggregation |
| `dqe/.../coordinator/transport/TransportTrinoSqlAction.java` | 2,000 | Coordinator, result merging |
| `dqe/.../planner/optimizer/PlanOptimizer.java` | 500 | PARTIAL vs SINGLE mode |
| `dqe/.../coordinator/fragment/PlanFragmenter.java` | 800 | Shard plan construction |

Full path prefix: `dqe/src/main/java/org/opensearch/sql/dqe/`

### FusedGroupByAggregate Key Methods

| Method | ~Line | Purpose |
|--------|-------|---------|
| `executeInternal` | 932 | Main dispatch |
| `executeSingleVarcharCountStar` | 1268 | VARCHAR + COUNT(*) with global ordinals |
| `executeSingleVarcharGeneric` | 1937 | VARCHAR + any aggregates |
| `executeNumericOnly` | 2786 | Numeric GROUP BY |
| `executeSingleKeyNumericFlat` | ~4269 | Single numeric key |
| `executeTwoKeyNumericFlat` | ~4626 | Two numeric keys |
| `executeNKeyVarcharPath` | 8657 | Multi-key VARCHAR dispatch |
| `executeNKeyVarcharParallelDocRange` | ~9946 | Parallel multi-key |

## 11. Known Pitfalls

1. **Plugin reload kills benchmarks.** Never run concurrently.
2. **JIT warmup.** Always `--warmup 3`. Without it, first runs are 2-5x slower.
3. **Query numbering.** See Section 6. `--query 17` = line 17 = `result[16]` = Q16.
4. **Q17 vs Q18.** Q17 HAS `ORDER BY` — cannot early-terminate. Q18 does NOT.
5. **1M vs 100M.** Always benchmark on 100M (`hits`). 1M (`hits_1m`) is correctness only.
6. **GC pressure.** Sequential queries cause heap pressure. Use warmup.
7. **OrdinalMap cost.** First VARCHAR query pays ~200ms-3s. Cached afterward.
8. **Result filename.** Output is `<instance-type>.json` — varies by machine.

## 12. Recommended Next Steps

1. **COUNT(DISTINCT) Fusion** — 6 queries, highest leverage
2. **Parallelize executeSingleKeyNumericFlat** — Q15 and similar
3. **Hash-Partitioned Aggregation** — Q16, Q18, Q32
4. **Borderline fixes** — Q02, Q30, Q31, Q37
5. **Q28 REGEXP_REPLACE** — cache Pattern, hoist regex

## 13. Reference Documents

- Benchmark setup: `docs/plans/2026-03-18-clickbench-full-dataset-benchmark.md`
- Design: `docs/plans/2026-03-20-phase-d-optimization-design.md`
- Impl plan: `docs/plans/2026-03-20-phase-d-impl-plan.md`
- Phase C report: `docs/reports/2026-03-19-phase-c-progress.md`
- Research: `docs/research/2026-03-19-groupby-filter-analysis.md`
- Queries: `benchmarks/clickbench/queries/queries_trino.sql`
- Baseline: `benchmarks/clickbench/results/performance/clickhouse_parquet_official/c6a.4xlarge.json`
