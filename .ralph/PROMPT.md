# DQE vs Elasticsearch — Q04 & Q05 HyperLogLog Optimization

## Problem

Q04: `SELECT COUNT(DISTINCT UserID) FROM hits`
- DQE: 1.448s (exact, LongOpenHashSet dedup across 4 shards)
- ES: 0.287s (approximate, HyperLogLog++ with ~2% error)
- Ratio: 5.05x — largest gap among actionable queries

Q05: `SELECT COUNT(DISTINCT SearchPhrase) FROM hits WHERE SearchPhrase <> ''`
- DQE: 0.694s (exact, ordinal-based FixedBitSet dedup)
- ES: 0.517s (approximate, HyperLogLog++)
- Ratio: 1.34x

## Root Cause

DQE computes exact COUNT(DISTINCT) by materializing all distinct values into hash sets (~64-96MB/shard for Q04). ES uses HyperLogLog++ (precision=14, ~16KB/shard sketch, O(1) memory, ~2% error). Shard merge is a register-wise max — O(16K) time, no raw value transfer.

## Approach

Add an HLL-based fast path for scalar COUNT(DISTINCT). OpenSearch's `HyperLogLogPlusPlus` is already on DQE's classpath via `compileOnly 'org.opensearch:opensearch'` — no new dependency needed. Keep the exact path as fallback.

## Success Criteria

- [ ] Q04 DQE/ES ratio drops below 1.0x (target: <0.3s)
- [ ] Q05 DQE/ES ratio drops below 1.0x (target: <0.5s)
- [ ] Correctness gate passes (>= 36/43 — Q04/Q05 results may differ by ~2% from exact, which is expected since ES also returns approximate)
- [ ] No regressions on other queries
- [ ] DQE faster than ES: >= 32/43 (gain Q04 + Q05 from current 31/43)

## Detailed Plan

See: docs/plans/20260402_q04_hll_optimization.md

## Key Implementation Notes

1. `HyperLogLogPlusPlus` is in `org.opensearch.search.aggregations.metrics` — already on classpath
2. It requires `BigArrays` — obtain from `SearchContext` or create via `BigArrays.NON_RECYCLING_INSTANCE`
3. HLL instances MUST be closed (they allocate off-heap via BigArrays). Use try-with-resources or explicit close in finally blocks.
4. For numeric: hash values with MurmurHash3 before feeding to HLL (check CardinalityAggregator.DirectCollector for the exact pattern)
5. For VARCHAR: hash term bytes with MurmurHash3.hash128, cache ordinal→hash mapping
6. The existing exact paths stay untouched as fallback.

## Execution Path (Q04)

```
PlanFragmenter strips AggregationNode (SINGLE mode)
→ shard receives bare TableScanNode(UserID)
→ dispatch: isBareSingleNumericColumnScan → executeDistinctValuesScanWithRawSet
→ FusedScanAggregate.collectDistinctValuesRaw (parallel 2-phase DocValues → LongOpenHashSet)
→ coordinator: mergeCountDistinctValuesViaRawSets (union raw sets)
```

Target: replace shard collection with HLL sketch, replace coordinator merge with HLL sketch merge.

## Execution Path (Q05)

VARCHAR scalar COUNT(DISTINCT) with WHERE filter. Currently uses ordinal-based FixedBitSet dedup.
Target: replace with HLL sketch collection + merge.
