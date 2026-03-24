# Phase D Handover Document

> **Purpose:** Bootstrap another agent to continue DQE optimization from 25/43 to 43/43 within 2x of ClickHouse-Parquet.

## 1. Current Status

**Branch:** `wukong` (pushed to `origin/wukong`)
**Score:** 25/43 queries within 2x of official ClickHouse-Parquet (c6a.4xlarge)
**Correctness:** 33/43 pass on 1M dataset
**Total time:** DQE 113.3s vs CH-Parquet 47.7s (2.4x overall)
**Hardware:** OpenSearch on m5.8xlarge (32 vCPU, 128GB RAM), 4 shards, ~100M docs

### Full Query Status (0-indexed, 1-indexed in run scripts)

Queries are 1-indexed in `run_opensearch.sh --query N` and the query file, but 0-indexed in the JSON results array.

```
WITHIN 2x (25 queries):
Q00(0.3x) Q01(0.1x) Q03(1.1x) Q06(0.1x) Q07(0.8x) Q10(1.5x) Q12(0.5x)
Q14(1.6x) Q17(0.0x) Q19(0.1x) Q20(0.9x) Q21(0.8x) Q22(0.6x) Q23(0.2x)
Q24(0.0x) Q25(1.2x) Q26(0.0x) Q27(1.4x) Q29(1.2x) Q33(0.3x) Q34(0.3x)
Q38(0.6x) Q40(0.2x) Q41(0.7x) Q42(0.6x)

ABOVE 2x (18 queries):
Q02: 2.2x (0.232s vs 0.105s) — SUM+COUNT+AVG, borderline
Q04: 5.4x (2.334s vs 0.434s) — COUNT(DISTINCT UserID) global
Q05: 5.8x (3.979s vs 0.690s) — COUNT(DISTINCT SearchPhrase) global
Q08: 3.4x (1.858s vs 0.540s) — GROUP BY RegionID + COUNT(DISTINCT UserID)
Q09: 8.0x (4.908s vs 0.614s) — GROUP BY RegionID + mixed aggs + COUNT(DISTINCT)
Q11: 9.5x (2.551s vs 0.268s) — GROUP BY MobilePhone,Model + COUNT(DISTINCT)
Q13: 6.2x (5.965s vs 0.956s) — GROUP BY SearchPhrase + COUNT(DISTINCT UserID)
Q15: 4.1x (2.126s vs 0.520s) — GROUP BY UserID ORDER BY COUNT(*) LIMIT 10
Q16: 4.1x (7.277s vs 1.794s) — GROUP BY UserID, SearchPhrase ORDER BY COUNT(*) LIMIT 10
Q18: 5.9x (21.702s vs 3.668s) — GROUP BY UserID, minute, SearchPhrase ORDER BY COUNT(*) LIMIT 10
Q28: 2.7x (25.531s vs 9.526s) — REGEXP_REPLACE GROUP BY HAVING
Q30: 2.3x (1.827s vs 0.778s) — GROUP BY SearchEngineID, ClientIP
Q31: 2.0x (2.230s vs 1.095s) — GROUP BY WatchID, ClientIP (borderline!)
Q32: 3.2x (14.268s vs 4.493s) — GROUP BY WatchID, ClientIP full-table
Q35: 5.0x (1.862s vs 0.370s) — GROUP BY 1, URL (full-table, high-card)
Q36: 4.0x (0.487s vs 0.121s) — GROUP BY ClientIP expressions (4 keys)
Q37: 2.7x (0.280s vs 0.102s) — CounterID=62 filtered URL GROUP BY
Q39: 16.9x (2.422s vs 0.143s) — CounterID=62 filtered, OFFSET, 5 keys + CASE WHEN
```

## 2. Baseline: ClickHouse-Parquet (NOT Native)

**CRITICAL:** Compare against official ClickHouse-Parquet results, not native MergeTree.

- Official baseline file: `benchmarks/clickbench/results/performance/clickhouse_parquet_official/c6a.4xlarge.json`
- Source: https://github.com/ClickHouse/ClickBench/tree/main/clickhouse-parquet/results
- The local ClickHouse installation uses native MergeTree format (much faster on trivial queries)
- The local CH file (`results/performance/clickhouse_clickbench_c6a.4xlarge.json`) is MergeTree — DO NOT use as baseline

**Why Parquet?** OpenSearch stores data in Lucene segments (not columnar). Parquet is a fairer comparison than ClickHouse's optimized native MergeTree format. The user explicitly chose this baseline.

## 3. How to Test & Benchmark

### Build & Deploy

```bash
# Compile DQE module only (fast, ~5s)
cd ~/oss/wukong && ./gradlew :dqe:compileJava

# Full rebuild + restart OpenSearch + reinstall plugin (~3 min)
cd ~/oss/wukong/benchmarks/clickbench && bash run/run_all.sh reload-plugin

# Verify OpenSearch is running
curl -sf http://localhost:9200/_cluster/health | jq .status
# Expected: "yellow" (single-node, no replicas)

# Verify data is loaded
curl -sf http://localhost:9200/hits/_count | jq .count
# Expected: 99997497
```

### Correctness Testing

```bash
# Full correctness suite (1M dataset, ~2 min)
cd ~/oss/wukong/benchmarks/clickbench && bash run/run_all.sh correctness

# Single query correctness
bash run/run_all.sh correctness --query 17

# Current baseline: 33/43 pass. Do NOT regress below this.
```

### Benchmarking

```bash
# Single query (fast iteration, ~30s per query)
cd ~/oss/wukong/benchmarks/clickbench
bash run/run_opensearch.sh --warmup 3 --num-tries 5 --query 17 --output-dir /tmp/q17_test

# Extract results (note: query N in script = index N-1 in JSON)
cat /tmp/q17_test/*.json | python3 -c "
import json, sys; d = json.load(sys.stdin)
q = 16  # 0-indexed for Q17
times = [x for x in d['result'][q] if x is not None]
print(f'Times: {times}')
print(f'Best: {min(times):.3f}s')
"

# Full benchmark (43 queries x 5 runs + 3 warmup, ~25 min)
bash run/run_opensearch.sh --warmup 3 --num-tries 5 --output-dir /tmp/full_bench

# Compare with CH-Parquet baseline
python3 -c "
import json
with open('results/performance/clickhouse_parquet_official/c6a.4xlarge.json') as f:
    ch = json.load(f)
with open('/tmp/full_bench/m5.8xlarge.json') as f:
    os = json.load(f)
within = 0
for i in range(43):
    ch_best = min(ch['result'][i])
    os_times = [x for x in os['result'][i] if x is not None]
    if not os_times: continue
    os_best = min(os_times)
    ratio = os_best / ch_best if ch_best > 0 else 999
    if ratio <= 2.0: within += 1
    else: print(f'Q{i:02d}: {ratio:.1f}x ({os_best:.3f}s vs {ch_best:.3f}s)')
print(f'\n{within}/43 within 2x')
"
```

### IMPORTANT: Query Number Indexing

The run script uses **1-based** indexing (`--query 17` = Q17 in the table above). The JSON results array is **0-based** (`result[16]` = Q17). The query file `queries/queries_trino.sql` is 1-based (line 17 = Q17). Be very careful with this — previous agents got confused and measured wrong queries.

## 4. What Was Already Done (Phase D Commits)

| Commit | What | Impact |
|--------|------|--------|
| `f071e0e35` | Early termination for LIMIT without ORDER BY | Q18: 62s → 0.17s |
| `4d2bb89e6` | Replace Collector with Weight+Scorer loop for filtered queries (8 execution paths) | Q41 -19%, filtered queries skip empty segments |
| `d519329a9` | `IndexSearcher.count()` for filtered COUNT(*), hoist Weight before segment loop, skip `System.gc()` for small results | Minor gains on filtered queries |
| `80f906c13` | Parallelize `executeWithEval` segment iteration | Q29: 450ms → 116ms (SUM(col+k) algebraic shortcut) |
| `89e0ee80d` | Parallel segment scanning for `executeCountDistinctWithHashSets`, raw HashSet merge for scalar COUNT(DISTINCT) | Q09 -46%, Q06 -31% |
| `9c6001423` | AVG decomposition fix (SUM+COUNT at shard, correct merge at coordinator), nextDoc() fast path, dead code removal | Q03: 1.7s → 0.12s, correctness 32→33/43 |
| `726bf9bb8` | Design docs, benchmark results | Documentation only |

## 5. Root Cause Analysis for Remaining 18 Queries

### Category A: COUNT(DISTINCT) Decomposition (6 queries — Q04, Q05, Q08, Q09, Q11, Q13)

**Root cause:** Calcite decomposes `GROUP BY x, COUNT(DISTINCT y)` into two-level aggregation: `GROUP BY x, y → COUNT(*)` then `GROUP BY x → COUNT(*)`. This doubles the key space and prevents single-pass aggregation.

**What was tried:**
- Parallel segment scanning for per-group HashSets (Q09 -46%) — committed
- Raw HashSet coordinator merge for scalar COUNT(DISTINCT) — committed
- Routing dedup plans through FusedGroupByAggregate — 3x regression, reverted
- Parallel scalar COUNT(DISTINCT) — 7x regression from thread oversubscription, reverted

**What should be done:**
- Intercept the Calcite-decomposed plan at `TransportShardExecuteAction` dispatch level
- Detect pattern: outer Aggregate(GROUP BY x, COUNT(*)) + inner Aggregate(GROUP BY x, y, COUNT(*))
- Route directly to fused GROUP BY with per-group `LongOpenHashSet` accumulator
- The `AccumulatorGroup` class already has `CountDistinctAccum` infrastructure
- Key file: `TransportShardExecuteAction.java:280-360` (the dispatch logic that detects AggDedupNode)

### Category B: High-Cardinality GROUP BY (4 queries — Q15, Q16, Q18, Q32)

**Root cause:** HashMap doesn't fit in CPU cache when key space is millions (17M distinct UserIDs, 6M SearchPhrases). Per-doc hash probe + insert dominates execution time.

| Q | Keys | Distinct Groups | Bottleneck |
|---|------|----------------|------------|
| Q15 | UserID | 17M | FlatSingleKeyMap cache misses |
| Q16 | UserID + SearchPhrase | ~30M | NKeyVarchar global ordinals path |
| Q18 | UserID + minute + SearchPhrase | ~50M | HAS ORDER BY, cannot early-terminate |
| Q32 | WatchID + ClientIP | ~30M | FlatTwoKeyMap, full-table scan |

**What should be done:**
- Q15: Parallelize `executeSingleKeyNumericFlat` across segments (currently sequential)
- Q16: Hash-partitioned aggregation (proven pattern from Q33/Q34) + TopN-aware scanning
- Q18: Same as Q16 but 3-key variant; pre-compute `extract(minute)` values
- Q32: More hash-partitioned buckets in `executeNKeyVarcharParallelDocRange`

### Category C: Full-Table High-Cardinality VARCHAR (3 queries — Q35, Q36, Q39)

**Root cause:** GROUP BY on URL or multi-expression keys over full 100M rows. Millions of unique groups even without filter.

| Q | Keys | Issue |
|---|------|-------|
| Q35 | `1, URL` | 18M unique URLs, full-table scan |
| Q36 | `ClientIP, ClientIP-1, ClientIP-2, ClientIP-3` | 10M unique IPs, 4-key numeric HashMap |
| Q39 | 5 keys + CASE WHEN, CounterID=62 filter | 722K matching rows but complex keys |

**What should be done:** These are fundamentally limited by DQE's per-doc hash overhead vs CH's vectorized columnar processing. Consider:
- Hash-partitioned aggregation to control memory
- Parallel segment scanning where not already used

### Category D: Borderline (5 queries — Q02, Q28, Q30, Q31, Q37)

| Q | Gap to 2x | Notes |
|---|-----------|-------|
| Q31 | 3ms | Almost there — may cross with JIT variance |
| Q02 | 22ms | SUM+COUNT+AVG with expression; flat-array path doesn't trigger for `col + 1` |
| Q37 | 76ms | CounterID=62 + URL GROUP BY, 300K groups |
| Q30 | 271ms | 2-key numeric GROUP BY, moderate cardinality |
| Q28 | 6.5s | REGEXP_REPLACE on Referer, need regex caching |

## 6. Key Architecture & Code Map

### DQE Query Execution Flow

```
HTTP Request → TransportTrinoSqlAction (coordinator)
  → Parse SQL → Calcite plan → PlanOptimizer → PlanFragmenter
  → Dispatch to shards via TransportShardExecuteAction
    → Detect plan pattern (scan-only? GROUP BY? TopN? COUNT DISTINCT?)
    → Route to fused execution path:
        FusedScanAggregate.execute()        — scalar aggregations (Q01-Q07)
        FusedScanAggregate.executeWithEval() — SUM(col+k) algebraic shortcut (Q30)
        FusedGroupByAggregate.execute()      — GROUP BY (Q08-Q43)
        FusedGroupByAggregate.executeWithTopN() — GROUP BY + ORDER BY + LIMIT
        executeCountDistinctWithHashSets()   — COUNT(DISTINCT) special path
  → Merge shard results at coordinator
  → Return JSON response
```

### Key Source Files

| File | Lines | Purpose |
|------|-------|---------|
| `dqe/src/main/java/org/opensearch/sql/dqe/shard/transport/TransportShardExecuteAction.java` | ~2,200 | Shard-level dispatch. Detects plan patterns and routes to fused paths. |
| `dqe/src/main/java/org/opensearch/sql/dqe/shard/source/FusedGroupByAggregate.java` | ~11,700 | All GROUP BY execution. Inner scan loops, global ordinals, hash maps. |
| `dqe/src/main/java/org/opensearch/sql/dqe/shard/source/FusedScanAggregate.java` | ~1,600 | Scalar aggregation. Flat-array path, COUNT(*), MIN/MAX. |
| `dqe/src/main/java/org/opensearch/sql/dqe/coordinator/transport/TransportTrinoSqlAction.java` | ~2,000 | Coordinator. Dispatches to shards, merges results. |
| `dqe/src/main/java/org/opensearch/sql/dqe/planner/optimizer/PlanOptimizer.java` | ~500 | Plan optimization. PARTIAL vs SINGLE execution mode. |
| `dqe/src/main/java/org/opensearch/sql/dqe/coordinator/fragment/PlanFragmenter.java` | ~800 | Breaks plan into shard fragments. AVG decomposition lives here. |

### FusedGroupByAggregate Key Methods

| Method | Line | Purpose |
|--------|------|---------|
| `executeInternal` | 932-1116 | Main dispatch — classifies keys, routes to specialized path |
| `executeSingleVarcharCountStar` | 1268 | Single VARCHAR key + COUNT(*) with global ordinals |
| `executeSingleVarcharGeneric` | 1937 | Single VARCHAR key + any aggregates |
| `executeNumericOnly` | 2786 | Numeric-only GROUP BY |
| `executeSingleKeyNumericFlat` | ~4269 | Single numeric key, open-addressing hash map |
| `executeTwoKeyNumericFlat` | ~4626 | Two numeric keys, FlatTwoKeyMap |
| `executeNKeyVarcharPath` | 8657 | Multi-key with VARCHAR, dispatch |
| `executeNKeyVarcharParallelDocRange` | ~9946 | Parallel multi-key GROUP BY |
| `executeMultiSegGlobalOrdFlatTwoKey` | (new, ~10400) | 2-key with global ordinals + early termination |

### Inner Classes in FusedGroupByAggregate

| Class | Purpose |
|-------|---------|
| `FlatSingleKeyMap` | Open-addressing hash map for 1 numeric key |
| `FlatTwoKeyMap` | Open-addressing hash map for 2 numeric keys |
| `FlatThreeKeyMap` | Open-addressing hash map for 3 keys |
| `AccumulatorGroup` | Per-group accumulators (SUM, COUNT, MIN, MAX, AVG, COUNT DISTINCT) |
| `KeyInfo` | GROUP BY key metadata (name, type, isVarchar) |
| `AggSpec` | Aggregate function metadata |

## 7. Known Issues & Pitfalls

1. **Plugin reload kills running benchmarks.** Never run benchmarks and `reload-plugin` concurrently. The subagents in Phase D repeatedly collided, producing null results.

2. **JIT warmup matters.** Always use `--warmup 3` for DQE benchmarks. Without warmup, first 1-2 runs can be 2-5x slower (JIT not compiled). The CH-Parquet baseline also has cold/warm runs — use `min()` of all runs.

3. **Query numbering confusion.** The run script uses 1-based (`--query 17`), JSON results are 0-based (`result[16]`). Previous agents got this wrong and optimized the wrong queries.

4. **Q17 vs Q18 swap.** Q17 (`...ORDER BY COUNT(*) DESC LIMIT 10`) HAS ORDER BY — cannot early-terminate. Q18 (`...LIMIT 10` without ORDER BY) was the one that benefited from early termination.

5. **1M vs 100M measurement.** Some subagents benchmarked on 1M (hits_1m) and reported fast times that didn't translate to 100M. Always benchmark on the full `hits` index (100M rows).

6. **GC pressure between queries.** Running all 43 queries sequentially can cause GC pressure. Q16's timing degrades across runs (7.3s → 10.3s). Consider `System.gc()` between heavy queries.

7. **OrdinalMap build cost.** First query touching a VARCHAR field pays ~200ms (SearchPhrase) to ~3s (URL) for OrdinalMap construction. Subsequent queries reuse the cache (keyed by IndexReader + field name, max 64 entries).

## 8. Environment & Test Setup

### Current Environment (Pre-Configured)

The current m5.8xlarge instance has everything running. Verify with:

```bash
# Check OpenSearch is up
curl -sf http://localhost:9200/_cluster/health | jq .status      # → "yellow"

# Check data is loaded
curl -sf http://localhost:9200/hits/_count | jq .count            # → 99997497 (100M full)
curl -sf http://localhost:9200/hits_1m/_count | jq .count         # → 1000000 (1M subset)

# Check ClickHouse (used for correctness reference)
clickhouse-client -q "SELECT count() FROM hits"                  # → 99997497
clickhouse-client -q "SELECT count() FROM hits_1m"               # → 1000000
```

### Directory Layout

```
~/oss/wukong/                    # Repository root
├── dqe/src/main/java/.../dqe/                # DQE source code
├── benchmarks/clickbench/
│   ├── queries/queries_trino.sql             # 43 ClickBench queries (1-indexed, line N = Q(N-1))
│   ├── run/run_opensearch.sh                 # DQE benchmark runner
│   ├── run/run_clickhouse.sh                 # CH benchmark runner
│   ├── run/run_all.sh                        # Unified orchestrator
│   ├── setup/setup_opensearch.sh             # OS install + plugin build
│   ├── setup/setup_clickhouse.sh             # CH install
│   ├── setup/setup_common.sh                 # Shared vars (INDEX_NAME, ports, etc.)
│   ├── data/download_data.sh                 # Download 100 Parquet files (~15GB)
│   ├── data/load_opensearch.sh               # Bulk-load Parquet → OS via clickhouse-local
│   ├── data/load_clickhouse.sh               # Load Parquet → CH MergeTree
│   ├── data/snapshot.sh                      # EBS snapshot create/restore
│   ├── data/parquet/hits_*.parquet           # 100 Parquet files (source data)
│   ├── correctness/                          # Correctness check scripts + expected results
│   ├── results/performance/
│   │   ├── clickhouse_parquet_official/      # Official CH-Parquet baseline (DO NOT MODIFY)
│   │   ├── opensearch_phaseD/                # Latest Phase D benchmark results
│   │   └── opensearch_corrected/             # Earlier corrected results
│   └── snapshots/                            # OpenSearch filesystem snapshot repo
├── integ-test/src/test/resources/clickbench/
│   └── mappings/clickbench_index_mapping.json  # Index mapping (103 fields, sorted by CounterID+EventDate+UserID+EventTime+WatchID)
└── /opt/opensearch/                          # OpenSearch installation
    ├── config/jvm.options.d/heap.options      # 32GB heap (-Xms32g -Xmx32g)
    ├── config/opensearch.yml                  # single-node, path.repo for snapshots
    └── data/                                  # Index data on disk
```

### Instance Specs

| Component | Value |
|-----------|-------|
| Instance | r5.4xlarge |
| vCPU | 16 |
| RAM | 128 GB |
| OS heap | 32 GB (`-Xms32g -Xmx32g`) |
| Java | OpenJDK 21.0.8 (Corretto) |
| OpenSearch | 3.6.0-SNAPSHOT (min distribution, no security) |
| ClickHouse | Latest stable (used for correctness only) |
| Index `hits` | 99,997,497 docs, 4 primary shards, 0 replicas |
| Index `hits_1m` | 1,000,000 docs, 8 primary shards, 0 replicas |

### Setting Up From Scratch (New Instance)

If starting on a fresh instance, use the automated scripts:

```bash
# 1. Clone the repo
git clone <repo-url> ~/oss/wukong
cd ~/oss/wukong
git checkout wukong

# 2. Install OpenSearch + ClickHouse (idempotent)
cd benchmarks/clickbench && bash run/run_all.sh setup

# 3. Download Parquet data (~15GB, 100 files, parallel download)
bash run/run_all.sh load-full
# This does: download_data.sh → load_clickhouse.sh → load_opensearch.sh
# OpenSearch bulk load takes ~45 min for 100M rows
# After load, runs _forcemerge to 1 segment per shard

# 4. Load 1M subset (for correctness testing)
bash run/run_all.sh load-1m
# Creates hits_1m in both CH and OS, generates expected results

# 5. Override heap to 32GB (the setup script caps at 16GB)
echo "-Xms32g" | sudo tee /opt/opensearch/config/jvm.options.d/heap.options
echo "-Xmx32g" | sudo tee -a /opt/opensearch/config/jvm.options.d/heap.options
# Restart OpenSearch to apply
bash run/run_all.sh reload-plugin
```

### Restoring OpenSearch Snapshot

- Refer: /home/penghuo/oss/os-sql/docs/plans/2026-03-18-clickbench-full-dataset-benchmark.md

The sort order is critical for narrow-filter queries (Q37-Q42): CounterID=62 filter
can use the sorted index to skip segments efficiently.

### Data Source

Parquet files are downloaded from the official ClickBench dataset:
- URL pattern: `https://datasets.clickhouse.com/hits_compatible/athena_partitioned/hits_N.parquet`
- 100 files (hits_0.parquet through hits_99.parquet), ~150MB each, ~15GB total
- Each file contains ~1M rows, totaling ~100M rows
- The `load_opensearch.sh` script uses `clickhouse-local` to convert Parquet → JSONEachRow, then bulk-loads via `/_bulk` API

### Dev Iteration Loop (After Environment is Ready)

```bash
# 1. Make code changes in dqe/src/main/java/...

# 2. Compile (fast check, ~5s)
cd ~/oss/wukong && ./gradlew :dqe:compileJava

# 3. Deploy (rebuild plugin + restart OS, ~3 min)
cd benchmarks/clickbench && bash run/run_all.sh reload-plugin

# 4. Quick correctness check
bash run/run_all.sh correctness --query 17    # Single query
bash run/run_all.sh correctness               # Full suite (33/43 baseline)

# 5. Benchmark specific query
bash run/run_opensearch.sh --warmup 3 --num-tries 5 --query 17 --output-dir /tmp/q17

# 6. Full benchmark (when ready)
bash run/run_opensearch.sh --warmup 3 --num-tries 5 --output-dir /tmp/full

# CRITICAL: Never run reload-plugin while a benchmark is running!
```

## 9. Recommended Next Steps (Priority Order)

### Step 1: COUNT(DISTINCT) Fusion (~6 queries, highest leverage)
Intercept the two-level Calcite plan and route to single-pass fused GROUP BY with per-group HashSet. This is the single highest-leverage change remaining.

### Step 2: Parallelize executeSingleKeyNumericFlat (~2 queries)
Q15 (UserID GROUP BY) and similar queries scan 100M rows sequentially in one thread. Splitting across 8 parallel workers (like `executeWithEval` already does) should give ~4x speedup.

### Step 3: Hash-Partitioned Aggregation for Q16, Q18, Q32
The proven pattern from Q33/Q34: partition group-key space into buckets, process one bucket at a time, merge results. This bounds memory and enables TopN pruning per bucket.

### Step 4: Borderline Queries (Q02, Q30, Q31, Q37)
Small targeted optimizations to close the remaining gap. Q31 needs only 3ms.

### Step 5: Q28 REGEXP_REPLACE
Cache compiled `Pattern` objects. Hoist regex computation before the aggregation loop. This is the single heaviest remaining query (25.5s).

## 10. Reference Documents
- Benchmark setup: `docs/plans/2026-03-18-clickbench-full-dataset-benchmark.md`
- Design doc: `docs/plans/2026-03-20-phase-d-optimization-design.md`
- Implementation plan: `docs/plans/2026-03-20-phase-d-impl-plan.md`
- Phase C progress report: `docs/reports/2026-03-19-phase-c-progress.md`
- Research analysis: `docs/research/2026-03-19-groupby-filter-analysis.md`
- Phase C global ordinals design: `docs/plans/2026-03-19-phase-c-global-ordinals-design.md`
