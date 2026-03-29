# Q15 Parallelism Analysis: executeSingleKeyNumericFlat

## 1. Current Parallelism Pattern (lines 4487–4557)

```
canParallelize = !"off".equals(PARALLELISM_MODE) && THREADS_PER_SHARD > 1 && leaves.size() > 1;
numWorkers = Math.min(THREADS_PER_SHARD, leaves.size());  // capped by segment count
```

Segments are assigned to workers via largest-first greedy bin-packing. Each worker gets a `List<LeafReaderContext>` and creates its own `FlatSingleKeyMap`. After all futures complete, worker maps are merged into a single `flatMap` via `mergeFrom`.

**Bottleneck**: With 4 segments per shard and `THREADS_PER_SHARD=4`, parallelism is exactly 4. On a 16-vCPU machine with 4 shards, only 4 of 16 cores are utilized per shard.

## 2. scanSegmentFlatSingleKey (line 4804)

For Q15 (MatchAll + allCountStar + numBuckets<=1), the hot path is the "ultra-fast" columnar loop:

```java
long[] keyValues = loadNumericColumn(leafCtx, keyInfos.get(0).name());  // ~25M longs
for (int doc = 0; doc < maxDoc; doc++) {
    long key0 = keyValues[doc];
    int slot = flatMap.findOrInsert(key0);
    flatMap.accData[slot * slotsPerGroup + accOffset[0]]++;
}
```

This is a simple sequential scan over a pre-materialized `long[]`. The loop body is: hash-probe + increment. No DocValues iterator state is involved — the array is random-access.

## 3. FlatSingleKeyMap (line 12563)

Open-addressing hash map with linear probing:
- `long[] keys` — hash slots, `EMPTY_KEY = Long.MIN_VALUE`
- `long[] accData` — contiguous accumulator storage, `slot * slotsPerGroup` offset
- `INITIAL_CAPACITY = 4096`, `LOAD_FACTOR = 0.7`, `MAX_CAPACITY = 32M`
- `findOrInsert(long key)` — returns slot index, auto-resizes
- `mergeFrom(FlatSingleKeyMap other)` — iterates other's slots, `findOrInsert` + element-wise add of `accData`

Key property: **thread-unsafe** — each worker must have its own instance. Merge is additive (COUNT/SUM are commutative).

## 4. loadNumericColumn (line 13300)

```java
public static long[] loadNumericColumn(LeafReaderContext leafCtx, String fieldName) {
    int maxDoc = leafCtx.reader().maxDoc();
    long[] values = new long[maxDoc];
    SortedNumericDocValues dv = DocValues.getSortedNumeric(leafCtx.reader(), fieldName);
    int doc = dv.nextDoc();
    while (doc != DocIdSetIterator.NO_MORE_DOCS) {
        values[doc] = dv.nextValue();
        doc = dv.nextDoc();
    }
    return values;
}
```

Materializes the entire segment's DocValues into a flat `long[]` indexed by docID. This array is **read-only after creation** and can be safely shared across threads.

## 5. Concrete Plan for Intra-Segment Doc-Range Parallelism

### Why it works for Q15

The ultra-fast path iterates `keyValues[0..maxDoc)` with no iterator state — just array reads. The `long[]` from `loadNumericColumn` is immutable and shareable. Each sub-range `[startDoc, endDoc)` can be processed independently into its own `FlatSingleKeyMap`, then merged.

### Implementation plan

**In `scanSegmentFlatSingleKey`**, when the ultra-fast columnar path is reached (MatchAll + allCountStar + numBuckets<=1), instead of a single-threaded loop:

1. **Load the column once** (shared): `long[] keyValues = loadNumericColumn(leafCtx, ...)`
2. **Split the doc range** `[0, maxDoc)` into `N` chunks where `N = THREADS_PER_SHARD`
3. **Spawn N tasks** on `PARALLEL_POOL`, each with its own `FlatSingleKeyMap`:
   ```java
   // Per chunk: [startDoc, endDoc)
   FlatSingleKeyMap chunkMap = new FlatSingleKeyMap(slotsPerGroup);
   for (int doc = startDoc; doc < endDoc; doc++) {
       int slot = chunkMap.findOrInsert(keyValues[doc]);
       chunkMap.accData[slot * slotsPerGroup + accOffset[0]]++;
   }
   ```
4. **Merge chunk maps** into the caller's `flatMap` (sequentially, after join)

### Changes needed

The change is **entirely within `scanSegmentFlatSingleKey`**. The method signature stays the same. The caller (both parallel and sequential paths in `executeSingleKeyNumericFlat`) is unaffected.

When `executeSingleKeyNumericFlat` uses the parallel segment path, each segment-worker calls `scanSegmentFlatSingleKey` which itself spawns sub-tasks. This gives `segments × chunks_per_segment` total parallelism. With 4 segments and 4 chunks each = 16-way parallelism.

### Threshold

Add a minimum doc count threshold (e.g., `maxDoc >= 100_000`) to avoid overhead on small segments. For segments below the threshold, fall through to the existing single-threaded loop.

### Existing pattern to follow

`executeNKeyVarcharParallelDocRange` (line 10834) already implements doc-range splitting: it collects matched doc IDs into an array, splits into chunks, spawns workers with their own DocValues readers, and merges results. The flat numeric path is simpler because `loadNumericColumn` gives a shared `long[]` — no per-worker DocValues needed.

### Risk: ForkJoinPool nesting

The outer `executeSingleKeyNumericFlat` already submits tasks to `PARALLEL_POOL`. Inner tasks from `scanSegmentFlatSingleKey` would also go to `PARALLEL_POOL`. ForkJoinPool handles this via work-stealing (`asyncMode=true` is already set). The pool has `availableProcessors()` threads total, so nested submission won't over-subscribe — idle threads will pick up inner tasks while outer tasks block on `join()`.
