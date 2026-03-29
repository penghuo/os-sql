# TopN + FusedGroupByAggregate Analysis for Q15

## Q15 Query
```sql
SELECT UserID, COUNT(*) FROM hits GROUP BY UserID ORDER BY COUNT(*) DESC LIMIT 10
```
- ~17M distinct UserIDs across 100M rows
- 4 shards → ~25M docs/shard, ~4.25M distinct keys/shard (assuming uniform distribution)

---

## 1. Call Chain

```
TransportShardExecuteAction.executeFusedGroupByAggregateWithTopN() (line 2372)
  → FusedGroupByAggregate.executeWithTopN() (line 907)
    → executeInternal() (line 937)
      → executeNumericOnly() (line 2812)  [UserID is numeric, no VARCHAR]
        → executeSingleKeyNumeric() (line 3172)  [single key, no expressions]
          → executeSingleKeyNumericFlat() (line 4443)  [COUNT(*) = flat accumulator]
```

**TopN info (sortAggIndex, sortAscending, topN) is passed all the way through** — it's not lost.

## 2. Bucket Partitioning (Critical for Q15)

At line ~3255, bucket count is estimated:
```java
long estimatedGroups = Math.max(1, totalDocs / 4);  // 25M/4 = 6.25M
numBuckets = ceil(estimatedGroups / MAX_CAPACITY);   // ceil(6.25M / 32M) = 1
```

**For Q15: numBuckets = 1** (6.25M < 32M). So it takes the single-bucket path — no partitioning overhead.

## 3. Parallelism (Segment-Level)

Inside `executeSingleKeyNumericFlat` (line ~4490):
- **Parallel path**: segments are distributed across `THREADS_PER_SHARD` workers
- Each worker gets its own `FlatSingleKeyMap` and processes assigned segments
- After all workers finish, maps are **merged sequentially** via `flatMap.mergeFrom(workerMap)` (line 4546)

**Merge is O(capacity)** — iterates all slots in the source map (line 12619):
```java
void mergeFrom(FlatSingleKeyMap other) {
    for (int s = 0; s < other.capacity; s++) {
        if (!other.occupied[s]) continue;
        int slot = findOrInsert(other.keys[s]);
        // add accData element-wise
    }
}
```

With ~4.25M groups per worker map, merge involves ~4.25M hash lookups per worker. With N workers, that's N × 4.25M lookups.

## 4. TopN Extraction (Post-Aggregation)

TopN is applied **AFTER full aggregation** (line ~4577):
```java
if (sortAggIndex >= 0 && topN > 0 && topN < flatMap.size) {
    int n = (int) Math.min(topN, flatMap.size);  // n = 10
    int[] heap = new int[n];  // min-heap of size 10
    // Scan ALL occupied slots in flatMap, maintain heap of top-10
    for (int slot = 0; slot < flatMap.capacity; slot++) {
        if (!flatMap.occupied[slot]) continue;
        // heap insert/replace logic
    }
}
```

**The heap is only size 10** — this is O(flatMap.capacity) which is fast. The topN extraction itself is NOT the bottleneck.

## 5. Where Time Is Actually Spent

### 5a. Hash Map Operations (Dominant Cost)
The inner loop in `scanSegmentFlatSingleKey` (line ~4860) for COUNT(*) + MatchAll + single bucket:
```java
long[] keyValues = loadNumericColumn(leafCtx, keyInfos.get(0).name());
for (int doc = 0; doc < maxDoc; doc++) {
    long key0 = keyValues[doc];
    int slot = flatMap.findOrInsert(key0);
    flatMap.accData[slot * slotsPerGroup + accOffset[0]]++;
}
```

For 25M docs with ~4.25M distinct keys:
- `findOrInsert` uses **linear probing** with `SingleKeyHashMap.hash1(key)`
- At 70% load factor, the map capacity is ~6M slots → ~4.25M/6M ≈ 71% full
- Linear probing at 71% load → average ~3.5 probes per lookup
- **25M × 3.5 probes = ~87.5M memory accesses** into a ~48MB array (6M × 8 bytes)
- This is **cache-hostile**: 4.25M distinct keys means the working set far exceeds L2/L3 cache

### 5b. FlatSingleKeyMap Memory Layout
```
keys:     long[capacity]     → 6M × 8B = 48MB
occupied: boolean[capacity]  → 6M × 1B = 6MB  
accData:  long[capacity × 1] → 6M × 8B = 48MB
Total: ~102MB per worker map
```

With THREADS_PER_SHARD workers (e.g., 4), that's **~408MB** of hash maps, all with random access patterns.

### 5c. Merge Phase
After parallel aggregation, N worker maps are merged sequentially into one:
- Each merge: iterate ~6M capacity slots, ~4.25M occupied → ~4.25M findOrInsert calls
- The destination map grows to ~17M/4 = 4.25M entries
- With N=4 workers: 4 × 4.25M = 17M hash lookups in merge phase alone

## 6. Why 26x Slower Than ClickHouse

| Factor | DQE | ClickHouse |
|--------|-----|------------|
| Hash map | Open-addressing long[], linear probing, 70% load | Two-level hash table with cache-line-sized buckets |
| Memory layout | 3 separate arrays (keys, occupied, accData) | Single contiguous allocation per bucket |
| Parallelism | Segment-level parallel + sequential merge | Partition-level parallel, no merge needed (each thread owns a partition) |
| TopN during agg | No — full aggregation then heap extract | Likely partial aggregation with spill |
| Cache behavior | ~102MB working set per worker, random access | Designed for L2-resident inner loops |

### Key Bottlenecks (Ranked):
1. **Cache misses in hash map lookups**: 4.25M distinct keys × 3 arrays = ~102MB working set, far exceeding L3 cache. Each `findOrInsert` touches `keys[]`, `occupied[]`, and `accData[]` at random offsets.
2. **No topN pruning during aggregation**: All 100M rows are fully aggregated into 17M groups before the top-10 heap extract. For ORDER BY COUNT(*) DESC LIMIT 10, there's no way to prune early since any group could become top-10.
3. **Sequential merge of parallel maps**: The merge phase does N × 4.25M random hash lookups into a growing map.
4. **Linear probing degradation at high load**: At 71% occupancy, probe chains average ~3.5 steps, each potentially a cache miss.

## 7. Paths for Other Queries

| Query | Keys | Path |
|-------|------|------|
| Q15: UserID COUNT(*) | 1 numeric | `executeSingleKeyNumericFlat` (line 4443) |
| Q16: UserID, SearchPhrase | 2 numeric | `executeTwoKeyNumeric` (line ~3270) |
| Q36: URL GROUP BY + WHERE | 1 VARCHAR | `executeSingleVarcharCountStar` or `executeSingleVarcharGeneric` (line ~1050) |
| Q39: URL GROUP BY + WHERE | 1 VARCHAR | Same VARCHAR path |

## 8. canFuse() Summary (line 197)

Returns true when:
- Has GROUP BY keys (non-empty)
- Child is a TableScanNode (or EvalNode → TableScanNode)
- All keys are VARCHAR, numeric, or timestamp (or recognized expressions: DATE_TRUNC, EXTRACT, arithmetic)
- All aggregate functions match `COUNT|SUM|MIN|MAX|AVG` pattern
- Aggregate arguments are resolvable columns or `*`

Q15 passes all checks: single numeric key (UserID), COUNT(*) aggregate.
