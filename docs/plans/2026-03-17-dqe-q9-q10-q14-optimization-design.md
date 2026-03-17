# DQE Q9/Q10/Q14 Optimization Design

## Problem
Three ClickBench queries exceed the 2x ClickHouse target:
- Q9: 60ms vs 17ms CH (3.5x) — `GROUP BY RegionID, COUNT(DISTINCT UserID)`
- Q10: 69ms vs 18ms CH (3.8x) — `GROUP BY RegionID, SUM+COUNT*+AVG+COUNT(DISTINCT UserID)`
- Q14: 114ms vs 19ms CH (6.0x) — `WHERE SearchPhrase<>'' GROUP BY SearchPhrase, COUNT(DISTINCT UserID)`

## Profiling Results (warmed, ms)

| Query | Plan | Shards | Merge | HTTP | Total | Target |
|-------|------|--------|-------|------|-------|--------|
| Q9    | 0    | 20     | 28    | 11   | 60    | ≤34    |
| Q10   | 0    | 28     | 29    | 12   | 69    | ≤36    |
| Q14   | 0    | 87     | 14    | 12   | 114   | ≤38    |

## Root Causes

### Q9/Q10: Coordinator merge dominates (28-29ms)
- PlanFragmenter decomposes COUNT(DISTINCT UserID) into GROUP BY (RegionID, UserID) + COUNT(*)
- Shard builds per-group LongOpenHashSets and attaches to response
- Coordinator merge: `mergeDedupCountDistinctViaSets()` unions ALL ~400 groups × 8 shards
- ~200K LongOpenHashSet.add() operations = 28ms
- But only top-10 groups needed (ORDER BY DESC LIMIT 10)

### Q14: Shard execution dominates (87ms)
- WHERE filter forces Collector path with per-doc advanceExact() on UserID DocValues
- ~40K matching docs per shard with advanceExact = slower than sequential nextDoc()
- ~50K unique SearchPhrase groups × LongOpenHashSet each

## Optimizations

### Optimization A: Lazy Merge with Top-K Pruning (Q9/Q10)
For ORDER BY COUNT(DISTINCT) DESC LIMIT K:
1. Compute per-group bounds: `lower = max(shard_counts)`, `upper = sum(shard_counts)`
2. Build min-heap of top-K lower bounds as pruning threshold
3. Skip groups where upper_bound < threshold (prunes ~90% of groups)
4. Fully merge only candidate groups (~50 of 400)
5. Take ownership of first shard's set (zero-copy) for each merged group

**Expected:** Merge 28ms → ~4ms. Q9 total: ~35ms (≤34ms target).

### Optimization B: Collect-then-sequential-scan for Q14 shards
Instead of per-doc advanceExact() in the Collector:
1. Collect matching doc IDs into int[] array first
2. Sequential nextDoc() iteration on UserID DocValues, matching against doc ID array
3. Converts O(N × log) seeks to O(M) sequential reads

**Expected:** Shard 87ms → ~30ms. Q14 total: ~56ms. Combined with lazy merge on the coordinator side (14ms → ~5ms): ~47ms.
