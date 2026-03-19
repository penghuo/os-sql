# Spike: Bounded-Memory Top-N GROUP BY for Q33/Q19

## Problem Recap

Two ClickBench queries fail on 100M rows due to high-cardinality GROUP BY:

**Q33:** `GROUP BY WatchID, ClientIP ORDER BY COUNT(*) DESC LIMIT 10`
- ~12.5M unique (WatchID, ClientIP) pairs per shard (8 shards, 100M docs)
- Exceeds 8M MAX_CAPACITY in FlatTwoKeyMap, throws RuntimeException
- Top-10 groups have counts of 1-2 (nearly all groups are unique)

**Q19:** `GROUP BY UserID, extract(minute FROM EventTime), SearchPhrase ORDER BY COUNT(*) DESC LIMIT 10`
- Fits under 8M cap but fills 15GB heap (FlatThreeKeyMap with ~8M entries)
- Residual heap pressure causes Q20 to trip the circuit breaker

Both queries have `ORDER BY COUNT(*) DESC LIMIT 10` -- we only need the top-10 heaviest groups.

### Data Distribution (Critical Insight)

The ClickBench `hits` table has ~100M rows with WatchID being nearly unique per row. The expected output for Q33 on 100M rows shows top groups with count=2 (most have count=1). This is an extreme "all groups are rare" distribution where there are no true heavy hitters.

## Approach Evaluation

### Approach A: Two-Pass (Count-Only Pass + Targeted Re-Scan)

**Mechanism:** Pass 1 scans all docs building a minimal map (key -> count only). After pass 1, extract the top-K groups. Pass 2 re-scans docs, computing SUM/AVG only for the top-K groups.

**Memory analysis for Q33 pass 1:**
- FlatTwoKeyMap with slotsPerGroup=1 (count only): per entry = 2 longs (keys) + 1 bool + 1 long (count) = 17 raw bytes
- At 0.7 load factor: capacity = 12.5M / 0.7 = 17.9M -> next power of 2 = 33.5M
- Memory: 33.5M * (8+8+1+8) = 33.5M * 25 = ~838MB per shard
- 8 shards run sequentially within the process: peak is 1 shard at a time = ~838MB
- Wait -- shards run in parallel. 8 shards * 838MB = **6.7GB**. Resize peak (old+new) = **13.4GB**. This exceeds safe limits.

**Problem:** Even with count-only storage, 12.5M groups per shard exceeds the 8M MAX_CAPACITY. We would need to raise it to ~13M, and the total memory across 8 parallel shards would be ~6.7GB (resize peak ~13.4GB). This is feasible on a 32GB heap but tight.

**Verdict:** Feasible but requires raising MAX_CAPACITY conditionally for count-only pass. Does not fundamentally solve the memory problem -- just pushes the threshold higher. A dataset with 2x more docs would break again.

### Approach B: Space-Saving Algorithm

**Mechanism:** Streaming heavy-hitter algorithm maintaining a fixed-size map. When full, evict the entry with the smallest count, replace with the new key, inherit+increment its count.

**Accuracy guarantee:** All items with true frequency > N * epsilon are guaranteed in the result.

**Critical flaw for Q33:** With epsilon=0.0001 and N=12.5M per shard, the threshold is N*epsilon = 1,250. Any group with count > 1,250 is guaranteed found. But Q33's top groups have count=2. They are 625x below the threshold. Space-Saving **cannot distinguish a count-2 group from a count-1 group** at any reasonable epsilon.

To guarantee finding count-2 groups, we need epsilon < 2/N = 1.6e-7, requiring 1/epsilon = 6.25M counters per shard. This is essentially the full hash map -- no memory savings.

**Verdict: DOES NOT WORK for Q33.** Space-Saving is designed for skewed distributions (Zipfian) where heavy hitters have frequency >> N*epsilon. Q33 has a nearly uniform distribution where the "heavy hitters" have count=2 out of 12.5M. This is the worst case for any approximate streaming algorithm.

### Approach C: Raise MAX_CAPACITY for Count-Only Queries

A variant of Approach A where we raise MAX_CAPACITY specifically when all aggregates are COUNT(*).

**Memory per entry (count-only FlatTwoKeyMap):** 25 bytes/slot at capacity
- 13M entries -> capacity 32M -> 32M * 25 = 800MB per shard
- 8 parallel shards -> 6.4GB total, resize peak -> 12.8GB

**Problem:** Same as Approach A. Doesn't scale. Also, Q33 has SUM and AVG, not just COUNT(*), so we'd need a two-pass variant anyway.

**Verdict:** Not a standalone solution.

### Approach D: Cap + Skip New Groups

**Mechanism:** When FlatTwoKeyMap hits MAX_CAPACITY, stop inserting new groups but keep accumulating for existing groups.

**Accuracy for Q33:** With 8M cap and 12.5M groups per shard, 4.5M groups are missed entirely. A true top-K group first appearing after the cap is full is lost.

- P(top-K group captured) = 8M / 12.5M = 64% per shard
- But the count for captured groups is also wrong: only docs scanned before the cap filled contribute, while docs scanned after the cap contribute only if their group was already captured
- Expected correct results: ~6 out of 10, with wrong counts on all of them

**Verdict: Incorrect results.** Unacceptable for a benchmark.

### Approach E: Hash-Partitioned Aggregation (RECOMMENDED)

**Mechanism:** Partition the group-key space into B buckets by hashing the composite key. Execute B sequential passes over the data, each pass only aggregating groups that hash to the current bucket. Each pass has 1/B the groups, so it fits within MAX_CAPACITY.

**Concrete plan for Q33:**
- 12.5M groups per shard. With B=2 buckets: ~6.25M groups per bucket per shard. Fits in 8M cap.
- Each pass: full scan of 12.5M docs, hash (WatchID, ClientIP), skip docs not in current bucket
- Pass 1: aggregate bucket 0 groups (6.25M groups, full accumulators: COUNT, SUM, AVG)
- Pass 2: aggregate bucket 1 groups (6.25M groups)
- After both passes: merge results, sort by COUNT DESC, take top-10

**Memory per pass:** Same as current -- FlatTwoKeyMap with ~6.25M entries * 57 bytes/slot at capacity:
- Capacity = 16M (next pow2 above 6.25M/0.7)
- Memory = 16M * 57 = ~912MB per shard. Resize peak = ~1.8GB per shard.
- 8 parallel shards: 7.3GB total, resize peak 14.6GB. This is high.

**Optimization:** Run shard aggregation with at most 2-4 concurrent shards to limit peak memory. Or: reset the flat map between passes (free arrays, reallocate for next bucket).

**With B=4:** ~3.1M groups per bucket per shard. Fits comfortably in 8M cap.
- Capacity = 8M (next pow2 above 3.1M/0.7)
- Memory = 8M * 57 = ~456MB per shard per pass
- With 8 shards parallel: 3.6GB total. Resize peak: 7.2GB. Very safe.

**Timing estimate:**
- On 1M rows (8 shards), Q33 warm time is ~45ms, meaning ~5.6ms per shard per pass
- Scaling to 100M rows (100x data): ~560ms per shard per pass
- With B=4 passes: ~2.2s per shard total. Since shards run in parallel: ~2.2s wall clock
- Add coordinator merge overhead: total ~3-5s. Well within 120s timeout.

**For Q19 (3 keys):**
- FlatThreeKeyMap with ~8M entries eats ~15GB today. With B=2 buckets: 4M entries per pass.
- Capacity = 8M, memory = 8M * 33 = ~264MB per shard per pass. 8 parallel shards: 2.1GB. Safe.
- 2 passes * ~5s per pass (estimated) = ~10s total. Acceptable.

**Accuracy:** EXACT. Every group is processed in exactly one bucket (determined by hash). No approximation, no data loss.

**Complexity:** Medium (~100-150 lines).
1. Add a `hashBucket(key0, key1, numBuckets)` function (trivial)
2. Wrap the `executeTwoKeyNumericFlat` scan loop in a bucket loop
3. After each bucket pass, extract top-K from the flat map into a result accumulator
4. Clear the flat map between passes
5. After all passes, merge all bucket results and re-sort for final top-K

**Key implementation detail:** The scan loop already reads keys for every doc. Adding `if (hash(k0, k1) % B != currentBucket) continue;` is a single branch per doc. The DV reads for keys still happen (unavoidable -- we need to know the keys to hash them), but aggregate DV reads can be skipped for non-matching docs.

### Approach F: Explicit Cleanup After Heavy Queries

**Mechanism:** After query execution, null out hash map references and hint GC.

**Effect:** Does not fix Q33 (still hits 8M cap). Partially fixes Q19->Q20 cascade by reducing residual heap pressure.

**Verdict:** Should be done regardless as hygiene, but not a solution to the core problem.

## Recommendation

**Primary: Approach E (Hash-Partitioned Aggregation)**

This is the only approach that provides:
1. **Exact results** -- no approximation, no missed groups
2. **Bounded memory** -- each pass fits within MAX_CAPACITY
3. **Reasonable performance** -- B passes over the data, where B is small (2-4)
4. **Generality** -- works for any aggregate combination (COUNT, SUM, AVG), not just COUNT(*)

**Supplementary: Approach F (Explicit Cleanup)**

Add cleanup after every fused GROUP BY execution to reduce heap pressure between queries.

### Why Not Space-Saving?

The previous spike (2026-03-18-spike-groupby-memory-recommendation.md) recommended Space-Saving for Phase 2. This is incorrect for Q33's data distribution. Space-Saving's guarantee (items with frequency > N*epsilon) requires the top-K items to have frequency significantly above N*epsilon. When the top-K items have frequency 2 out of N=12.5M, no epsilon is small enough to find them without essentially building the full hash map.

Space-Saving works for Zipfian distributions (web search queries, word frequencies) but fails catastrophically for near-uniform distributions (unique IDs with occasional collisions). Q33 is the latter.

### Why Not Two-Pass?

Two-pass (Approach A) is a special case of hash-partitioned aggregation with B=1 for pass 1 (count-only) and a tiny pass 2. It requires that even the count-only map fits in memory. For Q33 with 12.5M groups per shard, count-only FlatTwoKeyMap needs ~800MB per shard * 8 shards = 6.4GB, which is feasible but fragile. Hash-partitioned aggregation is more robust because it divides the problem into arbitrarily small chunks.

However, if we're confident that count-only always fits (MAX_CAPACITY raised to 13M for count-only pass), two-pass is simpler. The downside is it always does 2 passes even when 1 partitioned pass would suffice.

## Implementation Sketch

```
// Pseudocode for hash-partitioned executeTwoKeyNumericFlat

int estimatedGroups = estimateCardinality(shard, keyInfos); // from segment metadata or first-pass sampling
int numBuckets = Math.max(1, (estimatedGroups / MAX_CAPACITY) + 1);

long[] topKeys0 = new long[topN];
long[] topKeys1 = new long[topN];
long[][] topAccData = new long[topN][slotsPerGroup];
int topCount = 0;

for (int bucket = 0; bucket < numBuckets; bucket++) {
    FlatTwoKeyMap flatMap = new FlatTwoKeyMap(slotsPerGroup);

    // Scan all docs, only aggregate groups in this bucket
    for (int doc = 0; doc < maxDoc; doc++) {
        long k0 = readKey0(doc);
        long k1 = readKey1(doc);
        if (hashBucket(k0, k1, numBuckets) != bucket) continue;

        int slot = flatMap.findOrInsert(k0, k1);
        // accumulate COUNT/SUM/AVG into flatMap.accData[slot*slotsPerGroup..]
    }

    // Extract top-K from this bucket, merge into global top-K
    mergeTopK(flatMap, topKeys0, topKeys1, topAccData, topCount, sortAggIndex, topN);
    // flatMap goes out of scope -> GC
}

// Build output Page from topKeys0, topKeys1, topAccData
```

### Cardinality Estimation

To determine `numBuckets`, we need a rough estimate of unique groups per shard. Options:

1. **Segment metadata:** Lucene's `SortedSetDocValues.getValueCount()` gives the number of unique values for a single column. For two-key GROUP BY, the product of unique key0 and key1 values is an upper bound.

2. **First-pass sampling:** Read the first 100K docs, count unique pairs, extrapolate. If 100K docs yield 99K unique pairs, extrapolate 12.5M docs -> ~12.4M unique pairs.

3. **Conservative default:** Assume worst case. If MAX_CAPACITY=8M and the shard has 12.5M docs, use numBuckets=2. If 25M docs, use numBuckets=4. Formula: `numBuckets = ceil(numDocs / MAX_CAPACITY)` since the worst case is 1 unique group per doc.

Option 3 is simplest and safest. The cost is at most 1 extra pass for datasets with significant key reuse.

### Estimated Effort

| Component | Lines | Complexity |
|-----------|-------|------------|
| Hash-bucket dispatch in executeTwoKeyNumericFlat | ~40 | Low |
| Hash-bucket dispatch in executeThreeKeyFlat | ~40 | Low |
| Top-K merge across buckets | ~30 | Medium |
| Cardinality estimation (option 3) | ~5 | Trivial |
| Explicit cleanup (Approach F) | ~10 | Trivial |
| Tests | ~50 | Medium |
| **Total** | **~175** | **Medium** |

### Expected Results

| Query | Current (100M) | After Fix | Notes |
|-------|----------------|-----------|-------|
| Q33 | ERROR (8M cap) | ~3-5s | 2-4 partitioned passes |
| Q19 | OOM / circuit breaker | ~5-10s | 2 partitioned passes |
| Q20 | Circuit breaker (cascaded from Q19) | Normal | Cleanup + reduced Q19 heap |

### Risks

1. **DV read overhead per skipped doc:** Even for docs not in the current bucket, we must read key DVs to compute the hash. This is unavoidable but cheap (SortedNumericDocValues.advanceExact is O(1)).

2. **Aggregate DV reads for skipped docs:** We can skip these by checking the bucket before reading aggregate DVs, saving ~50% of DV reads per non-matching doc. This requires restructuring the collect loop to check bucket membership first.

3. **Over-partitioning:** If cardinality estimation is too high, we do more passes than needed. The conservative formula (numDocs/MAX_CAPACITY) may cause 2 passes when 1 would suffice. This is a 2x slowdown for queries that would have fit. Mitigation: check if the first pass filled more than 70% of MAX_CAPACITY; if not, skip remaining buckets.
