# Two-Key Flat Parallelization — Code Extraction

File: `dqe/src/main/java/org/opensearch/sql/dqe/shard/source/FusedGroupByAggregate.java`

---

## 1. executeTwoKeyNumericFlat (lines 5163–5545)

### Signature (line 5163)

```java
private static List<Page> executeTwoKeyNumericFlat(
    IndexShard shard,
    Query query,
    List<KeyInfo> keyInfos,
    List<AggSpec> specs,
    Map<String, Type> columnTypeMap,
    List<String> groupByKeys,
    int numAggs,
    boolean[] isCountStar,
    int[] accType,
    int sortAggIndex,
    boolean sortAscending,
    long topN,
    int bucket,
    int numBuckets)
    throws Exception {
```

### Accumulator setup (lines 5180–5197)

```java
final int[] accOffset = new int[numAggs];
int totalSlots = 0;
for (int i = 0; i < numAggs; i++) {
  accOffset[i] = totalSlots;
  switch (accType[i]) {
    case 0: totalSlots += 1; break; // COUNT
    case 1: totalSlots += 1; break; // SUM long
    case 2: totalSlots += 2; break; // AVG long
    default: throw new IllegalStateException("...");
  }
}
final int slotsPerGroup = totalSlots;
final FlatTwoKeyMap flatMap = new FlatTwoKeyMap(slotsPerGroup);
```

### DV deduplication setup (lines 5200–5240)

Builds `dvColumnIndex`, `uniqueDvColumns[]`, `aggToDvIdx[]`, `hasDvDuplicates` — identical pattern to single-key.

### Sequential scan loop (lines 5242–5430)

**MatchAll path** (line 5244): iterates all `LeafReaderContext` leaves sequentially:
```java
try (Engine.Searcher engineSearcher = shard.acquireSearcher("dqe-fused-groupby-numeric-2key-flat")) {
  if (query instanceof MatchAllDocsQuery) {
    for (LeafReaderContext leafCtx : engineSearcher.getIndexReader().leaves()) {
      LeafReader reader = leafCtx.reader();
      int maxDoc = reader.maxDoc();
      Bits liveDocs = reader.getLiveDocs();
      SortedNumericDocValues dv0 = reader.getSortedNumericDocValues(keyInfos.get(0).name());
      SortedNumericDocValues dv1 = reader.getSortedNumericDocValues(keyInfos.get(1).name());
      // ... branches for hasDvDuplicates vs not, liveDocs vs not
      // calls collectFlatTwoKeyDoc() or collectFlatTwoKeyDocDedup() per doc
    }
  }
```

**Filtered path** (line ~5360): uses Weight+Scorer:
```java
  } else {
    IndexSearcher luceneSearcher = new IndexSearcher(engineSearcher.getIndexReader());
    Weight weight = luceneSearcher.createWeight(luceneSearcher.rewrite(query), ScoreMode.COMPLETE_NO_SCORES, 1.0f);
    for (LeafReaderContext leafCtx : engineSearcher.getIndexReader().leaves()) {
      Scorer scorer = weight.scorer(leafCtx);
      if (scorer == null) continue;
      // ... same DV setup, calls collectFlatTwoKeyDoc/Dedup per doc
    }
  }
}
```

**KEY OBSERVATION**: This is a single-threaded sequential loop over all segments. No parallel worker dispatch. This is what needs to be parallelized.

### Output building (lines 5432–5545)

Top-N heap selection + BlockBuilder output — identical pattern to single-key flat. Reads from `flatMap.keys0[slot]`, `flatMap.keys1[slot]`, `flatMap.accData[slot * slotsPerGroup + accOffset[a]]`.

---

## 2. FlatTwoKeyMap class (lines 11637–11768)

```java
private static final class FlatTwoKeyMap {
  private static final int INITIAL_CAPACITY = 8192;
  private static final float LOAD_FACTOR = 0.7f;
  private static final int MAX_CAPACITY = 8_000_000;

  long[] keys0;
  long[] keys1;
  boolean[] occupied;
  long[] accData; // contiguous: slot i's data at [i*slotsPerGroup .. (i+1)*slotsPerGroup)
  int size;
  int capacity;
  int threshold;
  final int slotsPerGroup;

  FlatTwoKeyMap(int slotsPerGroup) { /* allocates arrays at INITIAL_CAPACITY */ }
  int findOrInsert(long key0, long key1) { /* open-addressing with TwoKeyHashMap.hash2 */ }
  int findOrInsertCapped(long key0, long key1, int cap) { /* capped version for LIMIT */ }
  private int findExisting(long key0, long key1) { /* post-resize lookup */ }
  private void resize() { /* doubles capacity, rehashes, copies accData via System.arraycopy */ }
}
```

**CRITICAL**: FlatTwoKeyMap does NOT have a `mergeFrom` method. One must be added, modeled on FlatSingleKeyMap.mergeFrom.

---

## 3. FlatSingleKeyMap.mergeFrom (lines 11978–11990)

```java
/** Merge all entries from another FlatSingleKeyMap into this one, adding accData element-wise. */
void mergeFrom(FlatSingleKeyMap other) {
  for (int s = 0; s < other.capacity; s++) {
    if (!other.occupied[s]) continue;
    int slot = findOrInsert(other.keys[s]);
    int dstBase = slot * slotsPerGroup;
    int srcBase = s * other.slotsPerGroup;
    for (int j = 0; j < slotsPerGroup; j++) {
      accData[dstBase + j] += other.accData[srcBase + j];
    }
  }
}
```

**Template for FlatTwoKeyMap.mergeFrom**: Same pattern but call `findOrInsert(other.keys0[s], other.keys1[s])`.

---

## 4. scanSegmentFlatSingleKey (lines 4618–4808)

```java
private static void scanSegmentFlatSingleKey(
    LeafReaderContext leafCtx,
    Weight weight,
    boolean isMatchAll,
    FlatSingleKeyMap flatMap,
    List<KeyInfo> keyInfos,
    List<AggSpec> specs,
    int numAggs,
    boolean[] isCountStar,
    int[] accType,
    int[] accOffset,
    int slotsPerGroup)
    throws Exception {
```

This method encapsulates scanning a single segment:
- Opens DV readers for key + agg columns (including applyLength/varchar handling)
- **MatchAll path**: iterates `maxDoc` with liveDocs check, multiple fast-path branches:
  - All COUNT(*) ultra-fast path (nextDoc() on key column only)
  - Dense numeric aggs fast path (lockstep nextDoc() iteration)
  - General path with `collectFlatSingleKeyDocWithLength()`
- **Filtered path**: uses `weight.scorer(leafCtx)` → `DocIdSetIterator`
- All paths accumulate into the provided `flatMap`

**Template for scanSegmentFlatTwoKey**: Same structure but:
- Opens two key DV readers (dv0, dv1)
- Calls `collectFlatTwoKeyDoc` / `collectFlatTwoKeyDocDedup` instead
- Passes `bucket`/`numBuckets` for hash-partition filtering

---

## 5. executeTwoKeyNumeric caller — bucket dispatch (lines 4799–4955)

```java
private static List<Page> executeTwoKeyNumeric(...) throws Exception {
  // ... accType computation, canUseFlatAccumulators check ...

  if (canUseFlatAccumulators) {
    // Estimate docs to decide bucket count
    numBuckets = Math.max(1, (int) Math.ceil((double) totalDocs / FlatTwoKeyMap.MAX_CAPACITY));

    if (numBuckets <= 1) {
      return executeTwoKeyNumericFlat(shard, query, keyInfos, specs, columnTypeMap,
          groupByKeys, numAggs, isCountStar, accType, sortAggIndex, sortAscending, topN, 0, 1);
    } else {
      // Multi-bucket: parallel or sequential passes
      int parallelBuckets = Math.min(numBuckets, THREADS_PER_SHARD);
      if (parallelBuckets > 1 && !"off".equals(PARALLELISM_MODE)) {
        // CompletableFuture per bucket → executeTwoKeyNumericFlat(... bkt, numBuckets)
        // join all → mergePartitionedPages()
      }
      // Sequential fallback: loop buckets
    }
  }
```

**KEY INSIGHT**: The caller already parallelizes across *buckets* (for high-cardinality overflow). The missing parallelism is *within* `executeTwoKeyNumericFlat` — parallelizing across *segments* within a single bucket, exactly as `executeSingleKeyNumericFlat` does at lines 4314–4374.

---

## 6. executeSingleKeyNumericFlat parallel path (lines 4314–4374) — THE TEMPLATE

```java
boolean canParallelize = !"off".equals(PARALLELISM_MODE) && THREADS_PER_SHARD > 1 && leaves.size() > 1;

if (canParallelize) {
  int numWorkers = Math.min(THREADS_PER_SHARD, leaves.size());
  List<LeafReaderContext>[] workerSegments = new List[numWorkers];
  long[] workerDocCounts = new long[numWorkers];
  for (int i = 0; i < numWorkers; i++) workerSegments[i] = new ArrayList<>();

  // Largest-first greedy assignment for balanced load
  List<LeafReaderContext> sortedLeaves = new ArrayList<>(leaves);
  sortedLeaves.sort((a, b) -> Integer.compare(b.reader().maxDoc(), a.reader().maxDoc()));
  for (LeafReaderContext leaf : sortedLeaves) {
    int lightest = 0;
    for (int i = 1; i < numWorkers; i++) {
      if (workerDocCounts[i] < workerDocCounts[lightest]) lightest = i;
    }
    workerSegments[lightest].add(leaf);
    workerDocCounts[lightest] += leaf.reader().maxDoc();
  }

  final Weight weight = !isMatchAll
      ? luceneSearcher.createWeight(luceneSearcher.rewrite(query), ScoreMode.COMPLETE_NO_SCORES, 1.0f)
      : null;

  CompletableFuture<FlatSingleKeyMap>[] futures = new CompletableFuture[numWorkers];
  for (int w = 0; w < numWorkers; w++) {
    final List<LeafReaderContext> mySegments = workerSegments[w];
    futures[w] = CompletableFuture.supplyAsync(() -> {
      FlatSingleKeyMap localMap = new FlatSingleKeyMap(slotsPerGroup);
      for (LeafReaderContext leafCtx : mySegments) {
        scanSegmentFlatSingleKey(leafCtx, weight, isMatchAll, localMap, ...);
      }
      return localMap;
    }, PARALLEL_POOL);
  }

  CompletableFuture.allOf(futures).join();
  for (var future : futures) {
    FlatSingleKeyMap workerMap = future.join();
    if (workerMap.size > 0) flatMap.mergeFrom(workerMap);
  }
} else {
  // Sequential fallback
  for (LeafReaderContext leafCtx : leaves) {
    scanSegmentFlatSingleKey(leafCtx, weight, isMatchAll, flatMap, ...);
  }
}
```

---

## Summary of Changes Needed

1. **Add `FlatTwoKeyMap.mergeFrom(FlatTwoKeyMap other)`** — same as FlatSingleKeyMap.mergeFrom but with two keys
2. **Create `scanSegmentFlatTwoKey()`** — extract the per-segment scan logic from executeTwoKeyNumericFlat's MatchAll/Filtered loops into a standalone method (mirrors scanSegmentFlatSingleKey)
3. **Add parallel dispatch in `executeTwoKeyNumericFlat`** — greedy segment assignment + CompletableFuture workers + merge, identical pattern to executeSingleKeyNumericFlat lines 4314–4374
