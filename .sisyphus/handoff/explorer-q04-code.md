# collectDistinctValuesRaw — Full Code Analysis

**File**: `dqe/src/main/java/org/opensearch/sql/dqe/shard/source/FusedScanAggregate.java`
**Method starts**: line ~870 (after `executeDistinctValues`)

## Method Signature

```java
public static LongOpenHashSet collectDistinctValuesRaw(
    String columnName, IndexShard shard, Query query) throws Exception
```

Returns raw `LongOpenHashSet` directly — no Page/BlockBuilder overhead.

---

## Pre-sizing (line ~878)

```java
int totalDocs = engineSearcher.getIndexReader().maxDoc();
distinctSet = totalDocs > 0
    ? new LongOpenHashSet(Math.min(totalDocs, 32_000_000))
    : new LongOpenHashSet();
```

**Key observation**: Pre-sizes to `min(maxDoc, 32M)`. For a 100M-row shard, this allocates for 32M elements upfront. `LongOpenHashSet` constructor rounds up to power-of-two at 0.65 load factor:
- `rawCapacity = (32_000_000 / 0.65) + 1 ≈ 49.2M`
- Rounded to next power of 2 = **67,108,864 (64M) slots × 8 bytes = 512 MB per shard**

If actual distinct count is ~25K (e.g., UserID), this is **~20,000× over-allocated**.

---

## Single-Segment Path (MatchAllDocsQuery branch)

### Phase 1: DocValues → flat long[] array

```java
// Single segment — load directly (leaves.size() == 1 path)
LeafReader reader = leaves.get(0).reader();
SortedNumericDocValues dv = reader.getSortedNumericDocValues(columnName);
if (dv != null) {
    int maxDoc = reader.maxDoc();
    long[] vals = new long[maxDoc];       // <-- allocates maxDoc-sized array
    Bits liveDocs = reader.getLiveDocs();
    int count = 0;
    if (liveDocs == null) {
        int doc = dv.nextDoc();
        while (doc != DocIdSetIterator.NO_MORE_DOCS) {
            vals[count++] = dv.nextValue();
            doc = dv.nextDoc();
        }
    } else {
        for (int doc = 0; doc < maxDoc; doc++) {
            if (liveDocs.get(doc) && dv.advanceExact(doc)) {
                vals[count++] = dv.nextValue();
            }
        }
    }
    segArrays[0] = vals;
    segCounts[0] = count;
}
```

**Key observations**:
- Allocates `long[maxDoc]` = **800 MB for 100M docs** (just for the temp array)
- Uses `nextDoc()` sequential iteration (good for dense columns)
- No liveDocs check needed for force-merged index (liveDocs == null)

### Phase 2: Run-length dedup + hash insert (single-segment path)

```java
// Single segment — insert directly
long[] vals = segArrays[0];
if (vals != null) {
    int count = segCounts[0];
    long prev = Long.MIN_VALUE;
    for (int i = 0; i < count; i++) {
        long v = vals[i];
        if (v != prev) {          // <-- run-length dedup
            distinctSet.add(v);
            prev = v;
        }
    }
}
```

**Key observations**:
- Run-length dedup: skips consecutive duplicates. Comment says "index is sorted by UserID"
- If data is sorted, this reduces hash insertions from 100M to ~25K (distinct count)
- `prev` initialized to `Long.MIN_VALUE` which is also `LongOpenHashSet.EMPTY` sentinel — potential edge case if MIN_VALUE is a real value
- **No sorting step** — relies on DocValues being pre-sorted (which they are for SortedNumericDocValues)

---

## Multi-Segment Path (leaves.size() > 1)

### Phase 1: Parallel DocValues load

```java
CompletableFuture<?>[] futures = new CompletableFuture[leaves.size()];
for (int s = 0; s < leaves.size(); s++) {
    final int segIdx = s;
    futures[s] = CompletableFuture.runAsync(() -> {
        // Same nextDoc() loop as single-segment, but per-segment
        // Results stored in segArrays[segIdx], segCounts[segIdx]
    }, FusedGroupByAggregate.getParallelPool());
}
CompletableFuture.allOf(futures).join();
```

### Phase 2: Parallel per-segment hash insertion + merge

```java
CompletableFuture<LongOpenHashSet>[] hashFutures = new CompletableFuture[leaves.size()];
for (int s = 0; s < leaves.size(); s++) {
    final int segIdx = s;
    hashFutures[s] = CompletableFuture.supplyAsync(() -> {
        LongOpenHashSet set = new LongOpenHashSet(segCounts[segIdx]);  // pre-sized per segment
        int count = segCounts[segIdx];
        long prev = Long.MIN_VALUE;
        for (int i = 0; i < count; i++) {
            long v = vals[i];
            if (v != prev) {
                set.add(v);
                prev = v;
            }
        }
        return set;
    }, FusedGroupByAggregate.getParallelPool());
}

// Merge: find largest set, addAll smaller sets into it
int largestIdx = 0;
for (int s = 1; s < segSets.length; s++) {
    if (segSets[s].size() > segSets[largestIdx].size()) largestIdx = s;
}
for (int s = 0; s < segSets.length; s++) {
    if (s != largestIdx) {
        segSets[largestIdx].addAll(segSets[s]);
    }
}
distinctSet = segSets[largestIdx];
```

**Merge strategy**: Largest-absorbs-smallest via `addAll()`.

---

## LongOpenHashSet Pre-sizing Details

**File**: `dqe/src/main/java/org/opensearch/sql/dqe/operator/LongOpenHashSet.java`

```java
private static final float LOAD_FACTOR = 0.65f;
private static final long EMPTY = Long.MIN_VALUE;  // sentinel

public LongOpenHashSet(int expectedElements) {
    int rawCapacity = Math.max(8, (int)(expectedElements / LOAD_FACTOR) + 1);
    this.capacity = Integer.highestOneBit(rawCapacity - 1) << 1;  // next power of 2
    this.keys = new long[capacity];
    Arrays.fill(this.keys, EMPTY);
    // ...
}
```

Hash function: Murmur3 finalizer. Collision resolution: linear probing.

---

## Optimization Opportunities

### 1. Right-size hash set
Current: `min(maxDoc, 32M)` → 512 MB for 100M docs with 25K distinct values.
Fix: Use PointValues or segment statistics to estimate cardinality, or start small (e.g., 64K) and let resize handle it. Even `new LongOpenHashSet(65536)` would be ~800KB vs 512MB.

### 2. Fuse DV read + hash insert (eliminate Phase 1 temp array)
Current: Phase 1 allocates `long[maxDoc]` (800 MB), Phase 2 reads it.
Fix: For single-segment, read DocValues directly into hash set with run-length dedup inline:
```java
int doc = dv.nextDoc();
long prev = Long.MIN_VALUE;
while (doc != DocIdSetIterator.NO_MORE_DOCS) {
    long v = dv.nextValue();
    if (v != prev) { distinctSet.add(v); prev = v; }
    doc = dv.nextDoc();
}
```
Saves 800 MB allocation + eliminates a full pass over the data.

### 3. Doc-range parallelism within single segment
Current: Single-segment path is fully sequential.
Fix: Split doc range [0, maxDoc) into chunks, each worker builds local hash set, then merge. Requires `advanceExact()` instead of `nextDoc()` for random access, but enables parallelism on force-merged single-segment indexes.

### 4. Sentinel collision
`prev = Long.MIN_VALUE` matches `LongOpenHashSet.EMPTY`. If `Long.MIN_VALUE` is a real data value, the first occurrence would be skipped by run-length dedup. The `hasSentinel` flag in LongOpenHashSet handles storage correctly, but the dedup `prev` init is a latent bug.
