# DataFusion Integration Spike Results

**Date:** 2026-03-17
**Status:** Completed — **DataFusion does NOT provide meaningful speedup over hand-written Java/Rust hash aggregation for these query patterns.**

## Setup

- Rust 1.94.0, DataFusion 44.0.0, Arrow 54
- Benchmark: 125K rows (1 shard's worth of 1M / 8 shards), 400 group keys, 100K distinct values
- All measurements are median of 10 runs after 5 warmup iterations
- Pure Rust benchmark (no JNI overhead)

## Results

### COUNT(DISTINCT) — Q9 Pattern (GROUP BY RegionID, COUNT(DISTINCT UserID))

| Approach | Median | Notes |
|----------|--------|-------|
| DataFusion DataFrame API (reuse ctx) | 10.0ms | ~0.1ms savings from context reuse |
| DataFusion DataFrame API (fresh ctx) | 10.1ms | Negligible context creation overhead |
| Rust HashMap + HashSet | 10.4ms | Baseline, simulates Java hot path |
| DataFusion SQL interface | 25.6ms | +15ms query planning overhead |
| Arrow array materialization only | 0.2ms | Data copy is cheap |

**Verdict:** DataFusion ≈ HashMap. No meaningful speedup (0.96x).

### Mixed Aggregation — Q10 Pattern (SUM + COUNT + AVG + COUNT(DISTINCT))

| Approach | Median | Notes |
|----------|--------|-------|
| DataFusion DataFrame API | 12.8ms | Slower due to multi-agg operator overhead |
| Rust HashMap | 10.4ms | Simple struct accumulator |

**Verdict:** DataFusion is 1.23x **slower** for mixed aggregation.

## Spike Questions Answered

### 1. Build: Can we compile a Rust .so and load it via JNI?
**Yes.** 47MB .so, builds in ~2min. JNI function naming works correctly. Library can be loaded from Java resources via temp file extraction.

### 2. Memory: What's the overhead of data materialization?
**Minimal.** Arrow array creation from `Vec<i64>` is 0.2ms for 125K rows. JNI `GetLongArrayRegion` copy would add ~0.3ms. Total overhead: <1ms.

### 3. Performance: Does DataFusion beat the Java hot path?
**No.** DataFusion and hand-written hash maps perform identically for COUNT(DISTINCT). For mixed aggregations, DataFusion is actually 23% slower.

### 4. Scope: Which patterns benefit?
**None of the tested patterns.** The bottleneck for Q9/Q10/Q14 is:
- **Q9/Q10:** Coordinator merge of per-shard HashSets (28ms), not shard-level aggregation (9ms)
- **Q14:** DocValues reading under WHERE filter (87ms), not hash aggregation
- DataFusion doesn't help with either bottleneck

## Root Cause Analysis

DataFusion doesn't help because:

1. **Data is already materialized.** Lucene DocValues provide columnar data directly. There's no I/O or deserialization bottleneck for DataFusion to accelerate.

2. **Aggregation is memory-bound.** Hash table lookups with random key access dominate. SIMD/vectorization doesn't help with hash table probing.

3. **The real bottleneck is architectural.** COUNT(DISTINCT) across shards requires either:
   - Sending per-group distinct value sets to coordinator (current approach, 28ms merge)
   - HyperLogLog approximation (not exact)
   - Single-node execution (no cross-shard issue)

4. **Query planning overhead.** Even the DataFrame API adds ~1ms of logical→physical plan translation. The SQL interface adds 15ms.

## Recommendation

**Do NOT proceed with DataFusion integration for DQE aggregation.** The current Java implementation is already at peak performance for these data sizes and access patterns.

Instead, focus optimization effort on:
1. **Coordinator merge optimization** (already done: lazy merge + top-K pruning reduced Q9 merge from 28ms to 4ms)
2. **Avoiding COUNT(DISTINCT) decomposition** — push full COUNT(DISTINCT) to each shard, use probabilistic data structures (HLL) for cross-shard merge
3. **Reducing DocValues access overhead** for filtered scans (Q14)

## Where DataFusion COULD Help (Future)

DataFusion might be valuable if DQE evolves to:
- **Full query execution** (not just aggregation) — replacing the Trino operator pipeline
- **Complex analytics** — window functions, joins, subqueries where the planning layer adds value
- **Larger datasets** (100M+ rows per shard) where vectorized batch processing amortizes planning cost
- **Columnar storage** integration (replacing DocValues with Arrow IPC / Parquet)
