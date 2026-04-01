# Q39 Hot Path Analysis: `executeSingleVarcharCountStar` (lines 1616–2282)

**File**: `dqe/src/main/java/org/opensearch/sql/dqe/shard/source/FusedGroupByAggregate.java`

## Method Signature (line 1616)
```java
private static List<Page> executeSingleVarcharCountStar(
    IndexShard shard, Query query, String columnName,
    Map<String, Type> columnTypeMap, List<String> groupByKeys,
    int sortAggIndex, boolean sortAscending, long topN) throws Exception
```

## Architecture: 4 Execution Paths

### Path 1: Single-Segment Fast Path (lines 1633–1808)
- **Condition**: `leaves.size() == 1` AND `ordCount <= 10_000_000`
- Allocates `long[ordCount]` array, counts per-ordinal
- Sub-paths for MatchAll (forward-only iteration) vs filtered (Collector-based)
- Includes inline TopN heap selection (lines 1710–1790)
- **Zero HashMap, zero String allocation** — pure ordinal array

### Path 2: Global Ordinals Multi-Segment (lines 1810–1975) — THE BOTTLENECK
- **Lines 1810–1822 — The critical threshold check:**
```java
OrdinalMap ordinalMap = null;
if (query instanceof MatchAllDocsQuery) {
    ordinalMap = buildGlobalOrdinalMap(leaves, columnName);
} else {
    // Check first segment's ordinal count as estimate
    SortedSetDocValues checkDv = leaves.get(0).reader().getSortedSetDocValues(columnName);
    if (checkDv != null && checkDv.getValueCount() <= 1_000_000) {
        ordinalMap = buildGlobalOrdinalMap(leaves, columnName);
    }
}
```
- **For Q39**: URL field has >1M ordinals per segment → `ordinalMap` stays `null` → falls through
- When ordinalMap IS built: uses `long[globalOrdCount]` array + `segToGlobal.get()` mapping
- Includes TopN heap on global ordinal indices (lines 1893–1960)

### Path 3: Parallel HashMap Path (lines 1977–2186) — WHERE Q39 LANDS
- **Condition**: `canParallelize` AND `THREADS_PER_SHARD > 1` AND `leaves.size() > 1`
- **Lines 1984–1990 — Parallelism guard for MatchAll only:**
```java
boolean canParallelize = !"off".equals(PARALLELISM_MODE) && THREADS_PER_SHARD > 1 && leaves.size() > 1;
if (canParallelize && query instanceof MatchAllDocsQuery) {
    SortedSetDocValues checkDv = leaves.get(0).reader().getSortedSetDocValues(columnName);
    if (checkDv != null && checkDv.getValueCount() > 500_000) {
        canParallelize = false;
    }
}
```
- **For Q39 (filtered, not MatchAll)**: parallelism guard is SKIPPED → enters parallel path
- Partitions segments largest-first across `numWorkers` threads
- Each worker: per-segment ordinal array if `ordCount <= 1_000_000`, else `HashMap<Long, long[]>`
- **Per-worker merge**: resolves ordinals → `BytesRefKey` copies → `HashMap<BytesRefKey, long[]>`
- **Global merge** (line 2094): iterates all worker maps, merges into single `globalCounts` HashMap
- TopN heap on `Map.Entry<BytesRefKey, long[]>` (lines 2115–2175)

### Path 4: Sequential HashMap Fallback (lines 2188–2282)
- Single-threaded Collector-based path
- Same ordinal→BytesRefKey resolution in `finish()` callback
- Used when parallelism is disabled

## Q39 Bottleneck Root Cause

**The 1M ordinal threshold at line 1817 causes Q39 to skip the O(1)-array global ordinals path and fall into the O(n) HashMap<BytesRefKey> path.**

Flow for Q39 (filtered query, URL field with >1M ordinals):
1. Line 1815: `query instanceof MatchAllDocsQuery` → false (Q39 has WHERE clause)
2. Line 1817: `checkDv.getValueCount() <= 1_000_000` → false (URL has >1M ordinals)
3. `ordinalMap` stays `null` → skips entire global ordinals path
4. Line 1984: `canParallelize` = true (filtered query bypasses the 500K MatchAll guard)
5. Falls into parallel HashMap<BytesRefKey> path
6. Each worker creates BytesRefKey copies for every matching URL → massive allocation + hashing

## Key Constants
```java
PARALLELISM_MODE = System.getProperty("dqe.parallelism", "docrange")  // line 118
THREADS_PER_SHARD = availableProcessors / numLocalShards               // line 119
PARALLEL_POOL = new ForkJoinPool(availableProcessors)                  // line 124
```

## BytesRefKey (line 13222)
```java
private static final class BytesRefKey {
    final byte[] bytes;
    private final int hash;
    BytesRefKey(BytesRef ref) {
        this.bytes = new byte[ref.length];  // COPIES bytes from Lucene buffer
        System.arraycopy(ref.bytes, ref.offset, this.bytes, 0, ref.length);
        this.hash = java.util.Arrays.hashCode(this.bytes);
    }
}
```
- Every unique URL creates a byte[] copy + Arrays.hashCode computation
- For >1M unique URLs across multiple workers, this is the dominant cost

## buildGlobalOrdinalMap (line 12906)
- Cached per `readerKey + fieldName` in `ORDINAL_MAP_CACHE`
- Evicted on reader close via `CacheHelper.addClosedListener`
- Build cost for high-cardinality fields (18M ordinals) is ~3s — hence the 1M threshold

## Optimization Opportunities

1. **Raise the ordinal threshold for filtered queries**: Q39 touches only a fraction of the >1M URLs. The OrdinalMap build cost (~200ms for 1M ordinals) is amortized since it's cached. Even at 5M or 10M ordinals, the cached OrdinalMap + array counting would be far faster than HashMap<BytesRefKey>.

2. **Use per-segment ordinal arrays in parallel path without resolving to BytesRefKey**: Workers could return `long[]` ordinal count arrays per segment, then merge using the OrdinalMap at the end — avoiding all BytesRefKey allocation.

3. **TopN-aware early termination**: For filtered queries with TopN, if the number of matching docs is small relative to ordinal count, a direct HashMap on ordinals (not BytesRefKey) with deferred string resolution would avoid most allocations.
