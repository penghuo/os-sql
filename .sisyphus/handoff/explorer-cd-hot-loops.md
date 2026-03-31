# COUNT(DISTINCT) Hot Loop Analysis

## 1. LongOpenHashSet Implementation
**File:** `dqe/src/main/java/org/opensearch/sql/dqe/operator/LongOpenHashSet.java`

- Open-addressing hash set for primitive `long` values (no boxing)
- Default initial capacity: **8** (INITIAL_CAPACITY), load factor: **0.65**
- Sentinel-based: `Long.MIN_VALUE` marks empty slots; special booleans for `0` and sentinel values
- Hash function: Murmur3 finalizer (good distribution)
- Resize: doubles capacity, rehashes all entries

### Key Optimization Opportunities:
- **OPT-1: No pre-sizing in `scanSegmentForCountDistinct`** — Lines 1171, 1216, 1263 all call `new LongOpenHashSet()` (capacity=8). For Q04/Q08 with ~200K distinct UserIDs, this means **15 resize operations** (8→16→32→...→524288). Each resize rehashes all entries.
- **OPT-2: Varchar paths use `LongOpenHashSet(16)`** — Lines 2527, 2537, etc. use capacity 16, still tiny. Same resize storm.
- **OPT-3: `addAll()` iterates the full backing array** — When merging, `addAll()` scans all slots including empty ones. For a set at 65% load with capacity 524K, it scans ~180K empty slots.

## 2. Inner Loop: `scanSegmentForCountDistinct` (line ~1130)

### MatchAll + No Deleted Docs (Fast Path):
```java
// Columnar cache: loads both columns into flat long[] arrays
long[] key0Values = FusedGroupByAggregate.loadNumericColumn(leafCtx, keyName0);
long[] key1Values = FusedGroupByAggregate.loadNumericColumn(leafCtx, keyName1);
for (int doc = 0; doc < maxDoc; doc++) {
    long k0 = key0Values[doc];
    long k1 = key1Values[doc];
    // Open-addressing group lookup (linear probing)
    int gs = Long.hashCode(k0) & gm;
    while (grpOcc[gs] && grpKeys[gs] != k0) gs = (gs + 1) & gm;
    if (!grpOcc[gs]) {
        grpSets[gs] = new LongOpenHashSet(); // <-- capacity 8!
        // ... inline resize of group map at 70% load
    }
    grpSets[gs].add(k1);
}
```

### Optimization Opportunities:
- **OPT-4: Group map uses `Long.hashCode()`** — The group map (grpKeys/grpOcc/grpSets) uses `Long.hashCode()` for probing, but `LongOpenHashSet.add()` uses Murmur3. Inconsistent hash quality. `Long.hashCode()` is `(int)(value ^ (value >>> 32))` — poor for sequential keys.
- **OPT-5: Group map resize is inlined and duplicated** — The resize logic for the group map (grpKeys/grpOcc/grpSets) is copy-pasted 3 times in the method (MatchAll, liveDocs, filtered paths). Should be extracted.
- **OPT-6: `loadNumericColumn` allocates 2 full `long[maxDoc]` arrays** — For a 5M-doc segment, that's 2×40MB = 80MB per segment scan. These are temporary and could be pooled or streamed.

## 3. Merge Strategy: `mergeHashSets` (line 1309)

```java
private static void mergeHashSets(LongOpenHashSet target, LongOpenHashSet source) {
    if (source.hasZeroValue()) target.add(0L);
    if (source.hasSentinelValue()) target.add(emptyMarker());
    long[] srcKeys = source.keys();
    for (int i = 0; i < srcKeys.length; i++) {
        if (srcKeys[i] != emptyMarker) target.add(srcKeys[i]);
    }
}
```

Caller merges smaller into larger (good):
```java
if (other.size() > existing.size()) {
    mergeHashSets(other, existing);  // merge small into big
    finalSets.put(entry.getKey(), other);
} else {
    mergeHashSets(existing, other);
}
```

### Optimization Opportunities:
- **OPT-7: `mergeHashSets` is identical to `addAll()`** — The standalone `mergeHashSets` duplicates `LongOpenHashSet.addAll()`. Some callers use `mergeHashSets`, others use `addAll()` (line 2571 in varchar path). Should consolidate.
- **OPT-8: No pre-sizing before merge** — When merging a 100K-element set into a 150K-element set, the target may resize multiple times during the merge. Could `ensureCapacity(target.size() + source.size())` before merging.

## 4. Parallel Execution Setup

### Pool:
```java
// FusedGroupByAggregate.java:124
private static final ForkJoinPool PARALLEL_POOL =
    new ForkJoinPool(
        Runtime.getRuntime().availableProcessors(),
        ForkJoinPool.defaultForkJoinWorkerThreadFactory,
        null, true); // asyncMode=true
```

### Segment Distribution (2-key path, line ~1040):
- Single segment: direct scan on calling thread (no parallelism)
- Multi-segment: dispatch `leaves.size()-1` segments to `PARALLEL_POOL`, run last segment on current thread
- Uses `CountDownLatch` for synchronization
- **No segment batching** — each segment is a separate task, even if there are 100+ tiny segments

### Varchar Path Worker Count (line ~2470):
```java
int numWorkers = Math.min(
    Math.max(1, Runtime.getRuntime().availableProcessors()
        / Integer.getInteger("dqe.numLocalShards", 4)),
    leaves.size());
```
- Workers = `availableProcessors / numLocalShards` (default 4 shards)
- Segments distributed to workers by doc count (largest-first, lightest-worker assignment)
- **OPT-9: 2-key path doesn't limit workers** — `executeCountDistinctWithHashSets` dispatches ALL segments to the pool without the `numWorkers` throttle. Could oversubscribe CPU if many shards run concurrently.

## 5. Pre-sizing Opportunities from Segment Metadata

- **OPT-10: Segment `maxDoc` is available** — `reader.maxDoc()` gives total docs in segment. For the dedup set, `maxDoc` is an upper bound on distinct values. Pre-sizing `LongOpenHashSet(maxDoc)` would eliminate all resizes but may over-allocate.
- **OPT-11: SortedSetDocValues has `getValueCount()`** — For varchar GROUP BY keys, the ordinal count gives exact cardinality. For numeric keys, no equivalent exists in Lucene's DocValues API.
- **OPT-12: Heuristic pre-sizing** — Could use `Math.min(maxDoc, 65536)` as initial capacity for the dedup set. This avoids the worst resize storms while capping memory.

## 6. Summary of Top Optimization Opportunities (Ranked by Impact)

| # | Optimization | Impact | Effort |
|---|---|---|---|
| OPT-1 | Pre-size LongOpenHashSet based on segment maxDoc or heuristic | HIGH — eliminates 15 resizes per group per segment | LOW |
| OPT-8 | Pre-size target before merge (`ensureCapacity`) | HIGH — eliminates resize during merge | LOW |
| OPT-4 | Use Murmur3 hash in group map (match LongOpenHashSet) | MEDIUM — reduces probe chains for sequential keys | LOW |
| OPT-6 | Pool/reuse `long[maxDoc]` arrays from `loadNumericColumn` | MEDIUM — reduces GC pressure for large segments | MEDIUM |
| OPT-9 | Add worker count throttle to 2-key path | MEDIUM — prevents CPU oversubscription | LOW |
| OPT-12 | Heuristic pre-sizing `min(maxDoc, 64K)` | MEDIUM — balanced memory/resize tradeoff | LOW |
| OPT-5 | Extract group map resize into helper method | LOW (correctness) — reduces code duplication | LOW |
| OPT-7 | Consolidate `mergeHashSets` with `addAll()` | LOW (correctness) — reduces code duplication | LOW |
