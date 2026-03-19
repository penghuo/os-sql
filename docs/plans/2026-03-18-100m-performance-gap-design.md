# DQE 100M Performance Gap — Design Document

## Problem

DQE achieves 41/43 within 2x ClickHouse on 1M rows (8 shards) but only 4/41 within 2x on 100M rows (4 shards). Two queries OOM-crash the JVM (Q14, Q33), one trips the circuit breaker (Q19), and most queries are 5-50x slower than ClickHouse. The aggregate ratio is 10.5x (CH faster).

See: `docs/reports/2026-03-18-clickbench-full-dataset.md`

## Goal

Match 1M-level performance at 100M scale: ~35-40/43 queries within 2x ClickHouse, no OOM crashes.

## Strategy

Three phases, each independently shippable:

```
Phase A: Stop the Bleeding     →  All 43 queries complete, no crashes
Phase B: Close the Gap         →  ~20-25/43 within 2x CH
Phase C: Match ClickHouse      →  ~35-40/43 within 2x CH
```

---

## Phase A: Stop the Bleeding

**Goal:** All 43 queries complete on 100M without OOM or circuit breaker trips.

### Root Cause

Two unbounded memory allocations crash the JVM:

1. **COUNT(DISTINCT) HashSets** (Q14): `executeVarcharCountDistinctWithHashSets()` at `TransportShardExecuteAction.java:1353` allocates one `LongOpenHashSet` per unique group key. On 100M rows with ~6M unique SearchPhrases, total HashSet memory exceeds 32GB heap.

2. **FlatTwoKeyMap** (Q33): `FusedGroupByAggregate.java:8842` doubles capacity on resize. With ~30M unique (URL, Title) pairs, the flat arrays exceed heap.

3. **Circuit breaker** (Q19): `GROUP BY UserID, minute, SearchPhrase` fills 15.3GB of heap, tripping the parent circuit breaker and blocking all subsequent queries.

### Tasks

**A0. Spike: Explore COUNT(DISTINCT) memory-bounding approaches** (~half day)

Explore at least these approaches for bounding COUNT(DISTINCT) memory:
- HyperLogLog fallback when cardinality exceeds threshold
- Bounded HashSet with early-stop (stop inserting, return lower bound)
- Sampling-based approximation
- Partitioned aggregation (hash-partition groups, process one partition at a time)

Produce a short recommendation with memory/accuracy tradeoffs measured on Q14 at 100M.

Key files:
- `TransportShardExecuteAction.java:1245-1411` (executeVarcharCountDistinctWithHashSets)
- `TransportShardExecuteAction.java:789-1200` (executeCountDistinctWithHashSets)
- `LongOpenHashSet.java`

**A1. Spike: Explore GROUP BY memory-bounding approaches** (~half day)

Explore at least these approaches for bounding GROUP BY memory:
- Hard cap on FlatTwoKeyMap capacity + graceful error
- Streaming partial flush (emit partial results, reset map)
- Partitioned aggregation (same as COUNT(DISTINCT))
- Spill to temporary file when threshold exceeded

Produce a short recommendation. Determine whether partial/approximate results are acceptable or if graceful error is sufficient.

Key files:
- `FusedGroupByAggregate.java:8796-8915` (FlatTwoKeyMap)
- `FusedGroupByAggregate.java:8857-8879` (findOrInsertCapped — existing cap mechanism)

**A2. Implement chosen approaches** (1-2 days, based on spike findings)

**A3. Verify all 43 queries complete on 100M without OOM/crash**

Run full benchmark: `bash run/run_opensearch.sh --timeout 120 --warmup 0 --num-tries 3`

### Success Criteria

- 0 OOM crashes on 43 queries at 100M
- 0 circuit breaker trips
- Correctness: no regression on 1M suite (32/43 pass)
- Performance: no regression on 1M timing (still 41/43 within 2x)

---

## Phase B: Close the Gap

**Goal:** ~20-25/43 queries within 2x ClickHouse on 100M.

### Root Cause

DQE is 5-50x slower than ClickHouse on full-scan queries because:

1. **No intra-shard parallelism**: Each shard scans sequentially on one thread. With 4 shards on 32 vCPUs, only 4 threads are active. ClickHouse uses 16 threads on the comparison baseline (c6a.4xlarge).

2. **Super-linear GROUP BY scaling**: Queries like Q17 (`GROUP BY URL`) scale 1,889x for 100x data because hash map resizing + GC pressure dominate scan time. ClickHouse uses vectorized streaming aggregation.

3. **Unbounded intermediate results**: Queries with `GROUP BY + LIMIT` (Q17, Q18, Q34, Q35) build complete hash maps before applying top-N. Could prune during scan.

### Tasks

**B1. Intra-shard segment-level parallelism** (~1 week)

Each Lucene shard contains multiple segments. Currently scanned sequentially. Change to:
- Parallelize across segments within a shard using a thread pool
- Each segment produces a partial result (partial hash map, partial HashSet)
- Merge partial results within the shard before returning to coordinator
- Thread pool size: `availableProcessors / numLocalShards` (e.g., 32 / 4 = 8 threads per shard)
- Works on both single-node and multi-node (parallelizes local segments only)

Key files:
- `TransportShardExecuteAction.java:162-600` (doExecute — all code paths need segment-level split)
- `FusedGroupByAggregate.java` (currently iterates segments sequentially)

Expected impact: ~4-8x speedup on full-scan queries (Q3-Q6, Q17-Q19). Brings them from 30-50x to 5-10x range.

**B2. Spike: Brainstorm approaches to increase parallelism without re-indexing** (~half day)

The existing index has 4 shards. Re-indexing is not an option. Explore approaches to extract more parallelism from the existing index:
- Virtual shard splitting (split doc ID range within a shard across threads)
- Lucene's built-in concurrent search (`IndexSearcher.search()` with executor)
- Sub-segment parallelism (split large segments by doc ID range)
- Async shard dispatch with work-stealing across shard threads
- Query-level thread pool tuning

Must work on multi-node clusters. Produce recommendation.

**B3. TopN pushdown for full-scan GROUP BY** (~3-5 days)

For queries with `GROUP BY + ORDER BY aggregate DESC LIMIT N` (Q17, Q18, Q34, Q35):
- Maintain a bounded priority queue of top-N groups during scan
- Evict low-ranking groups as new ones arrive
- Reduces peak memory from O(unique_groups) to O(N)
- Only works when sort key is an aggregate (COUNT, SUM) — not arbitrary column

Key files:
- `FusedGroupByAggregate.java` (executeTwoKeyNumeric, executeNumericOnly)
- `TransportShardExecuteAction.java:406-450` (executeFusedGroupByAggregateWithTopN)

Expected impact: Q17 from 47s to ~5-10s, Q18 from 49s to ~5-10s. Prevents GC storms by capping memory.

### Success Criteria

- 20-25/43 queries within 2x ClickHouse on 100M
- Aggregate ratio < 5x
- No regression on 1M results
- No OOM crashes

---

## Phase C: Match ClickHouse

**Goal:** ~35-40/43 queries within 2x ClickHouse on 100M.

### Root Cause

Remaining gap after Phase B comes from:

1. **All intermediate state is heap-resident**: Large GROUP BY results that don't fit in bounded maps need spill-to-disk.
2. **Per-doc DocValues access**: DQE reads one value at a time via `advanceExact()`. ClickHouse processes 1024-value column batches with SIMD.

### Tasks

**C1. Streaming aggregation with spill-to-disk** (~2-3 weeks)

When hash map exceeds memory budget:
- Hash-partition groups into N buckets
- Process one bucket at a time in memory
- Spill completed buckets to temporary files
- Merge spilled partitions in a final pass
- This is the standard approach in ClickHouse, Presto, and Spark

**C2. Vectorized DocValues scanning** (~1-2 weeks)

Batch DocValues reads (256-1024 values at a time) instead of per-doc `advanceExact()`:
- Enables CPU prefetching and SIMD
- Reduces function call overhead
- Lucene's `DocValuesIterator` already supports `advanceExact()` in sequence — the optimization is batching the downstream computation (hash, accumulate)

### Success Criteria

- 35-40/43 queries within 2x ClickHouse on 100M
- Aggregate ratio < 2x
- No regression on 1M results

---

## Summary

| Phase | Effort | Outcome | Aggregate Ratio |
|-------|--------|---------|:---------------:|
| Current | — | 4/41 within 2x, 2 OOM | 10.5x |
| **A** | 2-3 days | 43/43 complete, no OOM | ~10x |
| **B** | 1-2 weeks | ~20-25/43 within 2x | ~3-5x |
| **C** | 3-5 weeks | ~35-40/43 within 2x | ~1-2x |

## Benchmark Reference

- ClickHouse baseline: Official ClickBench c6a.4xlarge (16 vCPU, 32GB RAM)
- OpenSearch: m5.8xlarge (32 vCPU, 32GB heap), 4-shard `hits` index, 99.9M docs
- Full results: `docs/reports/2026-03-18-clickbench-full-dataset.md`
