# FlatSingleKeyMap Analysis — Hash Map for Single-Key Numeric GROUP BY

## 1. Class Definition (Inner Class)

**Location**: `FusedGroupByAggregate.java:13640` — private static final inner class.

```java
private static final class FlatSingleKeyMap {
    private static final int INITIAL_CAPACITY = 4096;
    private static final float LOAD_FACTOR = 0.7f;
    private static final int MAX_CAPACITY = 32_000_000;
    static final long EMPTY_KEY = Long.MIN_VALUE;

    long[] keys;
    long[] accData; // contiguous: slot i's data at [i*slotsPerGroup .. (i+1)*slotsPerGroup)
    int size;
    int capacity;
    int threshold;
    final int slotsPerGroup;
}
```

**Key design**: Open-addressing hash map with linear probing. Power-of-2 capacity for bitmask modulo. Sentinel `Long.MIN_VALUE` marks empty slots. ALL accumulator data stored in a single contiguous `long[]` array — zero per-group object allocation.

## 2. findOrInsert — The Hot Path (line ~13676)

```java
int findOrInsert(long key) {
    int mask = capacity - 1;
    int h = SingleKeyHashMap.hash1(key) & mask;
    while (keys[h] != EMPTY_KEY) {
        if (keys[h] == key) return h;
        h = (h + 1) & mask;
    }
    keys[h] = key;
    size++;
    if (size > threshold) { resize(); return findExisting(key); }
    return h;
}
```

**Per-doc cost**: hash1(key) → bitmask → linear probe → return slot index. Caller then does `flatMap.accData[slot * slotsPerGroup + accOffset]++`. No object allocation on the hot path.

## 3. Sizing / Initial Capacity

- **Default**: `INITIAL_CAPACITY = 4096` (via no-arg constructor)
- **Pre-sized**: Constructor accepts `initialCapacity`, rounded up to next power-of-2
- **Typical pre-sizing**: `new FlatSingleKeyMap(slotsPerGroup, 4_000_000)` for main map
- **Worker maps**: `new FlatSingleKeyMap(slotsPerGroup, initCap)` where `initCap = min(docCount * 10/7 + 1, 4_000_000)`
- **Load factor**: 0.7 — resize triggers at `capacity * 0.7`
- **Max capacity**: 32M groups. Throws RuntimeException on overflow, triggering multi-bucket fallback.

## 4. Hash Function — Murmur3 Finalizer (line ~13344)

```java
private static int hash1(long key) {
    key ^= key >>> 33;
    key *= 0xff51afd7ed558ccdL;
    key ^= key >>> 33;
    key *= 0xc4ceb9fe1a85ec53L;
    key ^= key >>> 33;
    return (int) key;
}
```

This is the **Murmur3 64-bit finalizer** — excellent avalanche properties for sequential integer keys (UserID, ClientIP). Defined in `SingleKeyHashMap` (line 13344) and reused by `FlatSingleKeyMap`.

## 5. Top-N Selection (line ~4330)

Uses an **inline min-heap** (for DESC) or max-heap (for ASC) over occupied slots:

```java
if (sortAggIndex >= 0 && topN > 0 && topN < flatMap.size) {
    int sortAccOff = accOffset[sortAggIndex];
    int n = (int) Math.min(topN, flatMap.size);
    int[] heap = new int[n];     // slot indices
    // ... standard heap insert/sift-down on flatMap.accData[slot * slotsPerGroup + sortAccOff]
}
```

Reads sort values directly from `accData[]` — no object dereference. O(N log K) where N = total groups, K = topN.

## 6. The Scan Loop — executeSingleKeyNumericFlat (line ~5150)

**Dispatch hierarchy** (from fastest to slowest):

1. **COUNT(*)-only + MatchAll + no liveDocs**: Loads key column into `long[]` via `loadNumericColumn()`, then:
   - If totalDocs > 10M: sequential scan into single map (better cache locality)
   - Else: doc-range parallel — splits docs across `THREADS_PER_SHARD` workers, each with own `FlatSingleKeyMap`, merged via `mergeFrom()`

2. **Numeric aggs + MatchAll + dense columns**: nextDoc() lockstep iteration (avoids advanceExact binary search)

3. **Filtered queries**: Forward-only DV advance with `dv.advance(doc)` instead of `advanceExact(doc)`

**The tightest inner loop** (COUNT(*) path, line ~14390):
```java
for (int doc = startDoc; doc < endDoc; doc++) {
    int slot = flatMap.findOrInsert(keyValues[doc]);
    flatMap.accData[slot * slotsPerGroup + accOffset0]++;
}
```

## 7. Overflow Handling — Multi-Bucket Fallback

When `FlatSingleKeyMap` overflows MAX_CAPACITY (32M), `executeSingleKeyNumeric` catches the RuntimeException and falls back to `executeSingleKeyNumericFlatMultiBucket`:

```java
try {
    return executeSingleKeyNumericFlat(..., 0, 1);  // single bucket
} catch (RuntimeException overflowEx) {
    // Estimate buckets: ceil(totalDocs / MAX_CAPACITY)
    int numBuckets = Math.max(2, (int) Math.ceil((double) totalDocs / FlatSingleKeyMap.MAX_CAPACITY));
    return executeSingleKeyNumericFlatMultiBucket(..., numBuckets);
}
```

Multi-bucket does ONE pass routing each doc to `bucketMaps[hash(key) % numBuckets].findOrInsert(key)`.

## 8. Memory Layout

For Q15 (GROUP BY UserID, COUNT(*)):
- `slotsPerGroup = 1` (one long for COUNT)
- `keys[]`: one `long` per slot
- `accData[]`: one `long` per slot
- At 4M capacity: `keys` = 32MB, `accData` = 8MB (for slotsPerGroup=1) → ~40MB total
- At 32M capacity (max): ~256MB keys + 256MB accData

## 9. Bottleneck Analysis for Q15/Q35

**Q15** (17.6M distinct UserIDs): Will overflow 32M MAX_CAPACITY at 0.7 load factor (threshold = 22.4M), so it fits in a single bucket. But the hash map is ~256MB, causing L3 cache thrashing on every `findOrInsert`.

**Q35** (~4M distinct ClientIPs): Fits comfortably. Pre-sized to 4M capacity. Hash map ~64MB.

**The bottleneck**: `findOrInsert` does random access into `keys[]` and `accData[]`. With 17.6M unique keys, the working set far exceeds L3 cache (~30MB typical). Every probe is a cache miss.
