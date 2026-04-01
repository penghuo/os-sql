# NKey COUNT(DISTINCT) Source — TransportShardExecuteAction.java

## Key Locations

| Method | File | Lines |
|--------|------|-------|
| `mergeHashSets` | TransportShardExecuteAction.java | 1368-1383 |
| `executeNKeyCountDistinctWithHashSets` | TransportShardExecuteAction.java | 1390-1555 |
| `scanSegmentForNKeyCountDistinct` | TransportShardExecuteAction.java | 1560-1790 |
| `LongArrayKey` (inner class) | TransportShardExecuteAction.java | ~1795-1815 |
| `loadNumericColumn` | FusedGroupByAggregate.java | 14541-14553 |
| `LongOpenHashSet` | LongOpenHashSet.java | full file |

## Architecture Overview

### executeNKeyCountDistinctWithHashSets (L1390-1555)

**Signature:**
```java
private ShardExecuteResponse executeNKeyCountDistinctWithHashSets(
    AggregationNode aggNode, ShardExecuteRequest req,
    List<String> keyNames, Type[] keyTypes) throws Exception
```

**Flow:**
1. Extracts `numKeys` and `numGroupKeys = numKeys - 1` (last key is dedup key)
2. Gets IndexShard, compiles Lucene query
3. Acquires searcher, gets leaf contexts
4. **Parallel segment scanning:**
   - If ≤1 leaf: calls `scanSegmentForNKeyCountDistinct` directly
   - If >1 leaf: submits leaves[0..n-2] to `FusedGroupByAggregate.getParallelPool()`, runs last leaf on current thread
   - Waits on CountDownLatch, checks for errors
5. **Merge phase:** iterates per-segment `HashMap<LongArrayKey, LongOpenHashSet>` results, unions sets per composite group key (swaps larger→target for efficiency)
6. **Output phase:** builds Trino Page with `numKeys+1` columns (group keys + dedup key + COUNT(*)=1), one row per distinct N-key combination

### scanSegmentForNKeyCountDistinct (L1560-1790)

**Signature:**
```java
private static HashMap<LongArrayKey, LongOpenHashSet> scanSegmentForNKeyCountDistinct(
    LeafReaderContext leafCtx, Weight weight,
    List<String> keyNames, int numGroupKeys, boolean isMatchAll) throws Exception
```

**Internal data structure — open-addressing group map:**
```java
int grpCap = 256;
long[] grpKeyStore = new long[grpCap * numGroupKeys];  // flat contiguous storage
LongOpenHashSet[] grpSets = new LongOpenHashSet[grpCap];
boolean[] grpOcc = new boolean[grpCap];
int grpThreshold = (int)(grpCap * 0.7f);
```

**Two code paths:**

#### MatchAll path (L~1590-1670):
1. Calls `FusedGroupByAggregate.loadNumericColumn()` for ALL keys → `long[][] keyColumns`
2. Iterates `doc = 0..maxDoc`:
   - Computes composite hash: `h = col[0][doc]; for k=1..numGroupKeys: h = h*31 + col[k][doc]`
   - Linear probes `grpOcc[]` / `grpKeyStore[]` to find or create group slot
   - On new group: copies key values into `grpKeyStore`, creates `new LongOpenHashSet(1024)`
   - If `grpSize > grpThreshold`: doubles capacity, rehashes all entries, re-probes current doc
   - Adds `keyColumns[numGroupKeys][doc]` (dedup key) to `grpSets[gs]`

#### Filtered path (L~1675-1780):
1. Opens `SortedNumericDocValues[]` per key
2. Uses `weight.scorer(leafCtx).iterator()` → `disi.nextDoc()` loop
3. Per doc: `advanceExact(doc)` on each DV to get values into `tmpGroupKey[]` + `dedupVal`
4. Same open-addressing probe/insert/resize logic as MatchAll path
5. Adds `dedupVal` to `grpSets[gs]`

#### Return (L~1780-1790):
Converts open-addressing arrays back to `HashMap<LongArrayKey, LongOpenHashSet>`.

### mergeHashSets (L1368-1383)

```java
private static void mergeHashSets(LongOpenHashSet target, LongOpenHashSet source) {
    target.ensureCapacity(target.size() + source.size());
    if (source.hasZeroValue()) target.add(0L);
    if (source.hasSentinelValue()) target.add(LongOpenHashSet.emptyMarker());
    long[] srcKeys = source.keys();
    long emptyMarker = LongOpenHashSet.emptyMarker();
    for (int i = 0; i < srcKeys.length; i++) {
        if (srcKeys[i] != emptyMarker) target.add(srcKeys[i]);
    }
}
```

### loadNumericColumn (FusedGroupByAggregate.java:14541)

```java
public static long[] loadNumericColumn(LeafReaderContext leafCtx, String fieldName) throws IOException {
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

### LongArrayKey (inner class, ~L1795)

```java
private static final class LongArrayKey {
    final long[] keys;
    private final int hash;
    LongArrayKey(long[] keys) { this.keys = keys; this.hash = Arrays.hashCode(keys); }
    public int hashCode() { return hash; }
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof LongArrayKey other)) return false;
        return Arrays.equals(keys, other.keys);
    }
}
```

## Current Bottlenecks

1. **MatchAll path uses columnar load** ✓ — calls `loadNumericColumn` for all keys
2. **Filtered path does NOT use columnar load** — uses per-doc `advanceExact()` on SortedNumericDocValues
3. **Open-addressing group map with manual resize** — custom hash map with `grpKeyStore[]`/`grpOcc[]`/`grpSets[]` parallel arrays, 0.7 load factor, doubles on threshold
4. **Hash function is weak** — uses `h = h*31 + col[k]` for composite key hash, then `Long.hashCode(h)` for slot. No mixing/finalizer on composite hash
5. **LongOpenHashSet(1024) initial capacity** — every new group allocates a 1024-slot set upfront
6. **Merge phase iterates full `keys[]` array** of source set (including empty slots)
7. **Output phase creates LongArrayKey per group** during conversion from open-addressing → HashMap (L1780-1790)
