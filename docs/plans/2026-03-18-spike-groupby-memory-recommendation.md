# Spike: GROUP BY Memory Limit for High-Cardinality Keys

## Problem

`FlatTwoKeyMap.resize()` OOMs on Q33 and `FlatThreeKeyMap` trips the circuit breaker on Q19 when running against 100M rows.

**Q33:** `GROUP BY WatchID, ClientIP` (no WHERE) -- ~100M unique pairs across 100M docs.
Per shard (25M docs): 64M-slot capacity x (2 longs + 1 bool + 5 longs accData) = ~3.6GB. Four shards = ~14.4GB. Resize doubles this transiently (old + new arrays co-exist) = ~28.8GB peak. OOM at 32GB heap.

**Q19:** `GROUP BY UserID, minute, SearchPhrase` -- similar cardinality.
Per shard: 64M-slot capacity x (3 longs + 1 bool + 1 long accData) = ~2.5GB. Plus 3 extra key arrays = ~3.5GB. Four shards = ~14GB. Circuit breaker trips.

## Approach Evaluation

### Approach A: Hard cap + error (throw DqeMemoryLimitException)

- **Prevents OOM:** Yes. Cap at N entries, throw before resize exceeds budget.
- **Accuracy:** Query fails entirely. No partial results.
- **Complexity:** Low (3 lines in resize, new exception class, catch in TransportShardExecuteAction).
- **Coordinator changes:** Minimal -- propagate error to HTTP 400.
- **Verdict:** Safe but unhelpful. Q33 and Q19 become permanently unsupported. Not acceptable for a benchmark.

### Approach B: Extend findOrInsertCapped to ORDER BY paths

- **Prevents OOM:** Yes. Cap at N entries, skip new groups after cap.
- **Accuracy:** **PROBLEMATIC for ORDER BY agg DESC.** See analysis below.
- **Complexity:** Low -- `findOrInsertCapped` already exists for FlatTwoKeyMap. Need to add it to FlatThreeKeyMap, then wire it into `collectFlatTwoKeyDoc`, `collectFlatTwoKeyDocDedup`, and the three-key collect loops.
- **Coordinator changes:** None -- coordinator merges whatever the shard returns.

**Accuracy analysis for capped GROUP BY with ORDER BY COUNT(*) DESC LIMIT 10:**

The critical question: if we cap at N groups and silently drop new groups, do the top-10 by count converge to the correct answer?

**No, they do not.** Consider Q33: `GROUP BY WatchID, ClientIP ORDER BY COUNT(*) DESC LIMIT 10`.

- Documents are scanned in Lucene doc-ID order (insertion order, roughly chronological).
- The first N unique (WatchID, ClientIP) pairs fill the map.
- Subsequent pairs are dropped, BUT their documents are also dropped -- meaning if a high-count group's documents are spread across the timeline, only those appearing before the cap fills get counted.
- Worse: if the top-10 groups by count happen to first appear AFTER the cap is full (unlikely for truly high-count groups, but possible), they are entirely missing.

**Probabilistic argument:** For Q33 with ~100M nearly-unique pairs, almost every group has count=1. The "top 10" likely have counts in the single digits. With a cap of 5M (5% of groups), there is a reasonable chance the correct top-10 groups happen to first appear in the first 5M docs -- but this is NOT guaranteed. The first-appearance position of any specific group is uniformly distributed across 100M docs, so there is a ~5% chance each top-10 group appears in the first 5M.

**Estimated accuracy for cap=5M, 100M docs, top-10:** Each top-10 group has P(captured) ~ 5M/100M = 5%. Expected top-10 groups captured: 0.5. This is unacceptably low.

For cap=25M (the full shard): P(captured) = 100% for groups that appear on the shard at all. But 25M entries is exactly the OOM problem we're trying to solve.

**Verdict:** Silently capping is only correct for LIMIT-without-ORDER-BY (where any N groups suffice) -- which is how it is already used. Extending it to ORDER BY queries produces wrong results.

### Approach C: Pre-check with cardinality estimate

- **Prevents OOM:** Yes, by falling back before starting.
- **Accuracy:** Full correctness on the fallback path (if we have one).
- **Complexity:** Medium. Need cardinality estimation from segment metadata, and a fallback execution strategy.
- **Problem:** What is the fallback? The generic operator pipeline (HashAggregationOperator) has the same memory problem -- it also builds a full HashMap of all groups. There is no streaming/spill-to-disk GROUP BY in DQE today. This approach only helps if the fallback is "return error" -- which is Approach A.
- **Verdict:** Useful as a guard but doesn't solve the fundamental problem without a spill-capable fallback.

### Approach D: Resize with max capacity guard

- **Prevents OOM:** Yes. Check `newCap > MAX_CAPACITY` in resize(), throw or switch to capped mode.
- **Accuracy:** Same as A (error) or B (capped), depending on the fallback behavior.
- **Complexity:** Trivial (3 lines in resize).
- **Coordinator changes:** Same as A or B.
- **Verdict:** Implementation detail of A or B, not a distinct approach.

## Recommendation: Approach D (resize guard) + Approach A (error) as Phase 1, with Phase 2 design for two-pass exact top-N

### Phase 1: Immediate OOM prevention (Approach D + A)

Add a `MAX_CAPACITY` guard to `resize()` in all three flat map classes. When exceeded, throw a clear exception that propagates as HTTP 400 with a message like: "GROUP BY cardinality exceeds DQE memory limit (N groups). Reduce cardinality with a WHERE clause or use a smaller GROUP BY."

**Cap value:** 8M entries per map. Math:
- FlatTwoKeyMap at 8M entries: capacity 16M (next power of 2 above 8M/0.7), memory = 16M x (2x8 + 1 + 5x8) = 16M x 57 = ~912MB per shard. Four shards = ~3.6GB. Resize peak = ~7.2GB. Safe at 32GB.
- FlatThreeKeyMap at 8M entries: capacity 16M, memory = 16M x (3x8 + 1 + 1x8) = 16M x 33 = ~528MB per shard. Safe.
- Configurable via system property `dqe.maxGroupByEntries` for tuning.

**Enforcement point:** Inside `resize()`, before allocating new arrays:

```
// pseudocode for FlatTwoKeyMap.resize()
private static final int MAX_CAPACITY = Integer.getInteger("dqe.maxGroupByEntries", 8_000_000);

private void resize() {
    int newCap = capacity * 2;
    if (size > MAX_CAPACITY) {
        throw new DqeGroupByMemoryException(
            "GROUP BY exceeded " + MAX_CAPACITY + " groups (" + size + " unique keys). "
            + "Add a WHERE clause to reduce cardinality.");
    }
    // ... existing resize logic
}
```

Same pattern for FlatThreeKeyMap and FlatSingleKeyMap.

**Changes needed:**
1. `FlatTwoKeyMap.resize()` (line 8891): add MAX_CAPACITY check before allocation
2. `FlatThreeKeyMap.resize()` (line 8989): same
3. `FlatSingleKeyMap.resize()` (line 9085): same
4. New `DqeGroupByMemoryException` (RuntimeException subclass)
5. `TransportShardExecuteAction`: catch `DqeGroupByMemoryException`, return error response
6. No coordinator changes needed

### Phase 2: Two-pass exact top-N for high-cardinality ORDER BY queries (future)

For queries like Q33 (`GROUP BY x, y ORDER BY COUNT(*) DESC LIMIT 10`), the correct approach for high-cardinality keys is a **two-pass algorithm**:

**Pass 1 -- Count only:** Scan all docs, build a FlatTwoKeyMap with only COUNT(*) (1 slot per group). This uses less memory per entry. If cardinality exceeds MAX_CAPACITY, use a probabilistic structure (Count-Min Sketch or HyperLogLog) or a sampling strategy.

**Pass 2 -- Targeted aggregation:** Take the top-N groups from pass 1, build a small hash set of their keys, and re-scan docs to compute SUM/AVG only for those groups.

This is analogous to the existing two-phase optimization visible at line 5860 (the `k0Set` quick-filter path for LIMIT-without-ORDER-BY).

However, for Q33 with ~100M unique groups, even the count-only pass requires ~100M entries x 25 bytes (2 longs + 1 bool + 1 long count) = ~2.5GB per shard. This may still be too large without external spill.

**Alternative Phase 2: Approximate top-N with Space-Saving / Lossy Counting.** These streaming algorithms find approximate heavy hitters in O(1/epsilon) space. For top-10 with epsilon=0.0001, this needs ~10K counters -- trivial memory. The downside is approximate counts, but for ORDER BY the ranking is what matters, and Space-Saving guarantees the true top-K are in the result if their frequency exceeds N*epsilon.

**Recommendation for Phase 2:** Implement Space-Saving for the `ORDER BY COUNT(*) DESC LIMIT K` pattern when estimated cardinality > MAX_CAPACITY. This gives correct top-K with bounded memory. SUM/AVG require a second pass on the identified top-K groups.

## Summary

| Aspect | Phase 1 | Phase 2 (future) |
|--------|---------|-------------------|
| Approach | Resize guard + exception | Space-Saving approximate top-N |
| OOM prevention | Yes | Yes |
| Accuracy | Query fails (clear error) | Approximate but correct ranking |
| Memory budget | ~7.2GB peak (4 shards) | O(K/epsilon) -- negligible |
| Complexity | ~50 lines | ~300 lines |
| Q33/Q19 behavior | Returns error with guidance | Returns correct top-10 |
| Coordinator changes | None | None |

Phase 1 should ship immediately to prevent cluster crashes. Phase 2 can follow as a performance feature.
