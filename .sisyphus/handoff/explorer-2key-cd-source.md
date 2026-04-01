# 2-Key executeCountDistinctWithHashSets — Full Source Reference

**File:** `dqe/src/main/java/org/opensearch/sql/dqe/shard/transport/TransportShardExecuteAction.java`
**Lines:** 1001–1380 (approx)

## Architecture Overview

The 2-key path has 4 key optimizations the NKey path is missing:

1. **Columnar loading** — bulk-loads both key columns into `long[]` arrays via `FusedGroupByAggregate.loadNumericColumn()`
2. **Parallel segment scanning** — dispatches N-1 segments to `FusedGroupByAggregate.getParallelPool()`, runs last segment on current thread, merges via `CountDownLatch`
3. **Open-addressing group map** — `grpKeys[]`/`grpOcc[]`/`grpSets[]` parallel arrays with linear probing, 0.7 load factor, power-of-2 resize
4. **Cache-friendly partitioned path** (grpSize < 10000) — 3-pass approach: discover groups → scatter k1 values into per-group arrays → process each group sequentially so its HashSet stays in L2 cache

---

## 1. Top-level method (line 1001)

```java
private ShardExecuteResponse executeCountDistinctWithHashSets(
    AggregationNode aggNode, ShardExecuteRequest req,
    String keyName0, String keyName1, Type type0, Type type1) throws Exception {

  TableScanNode scanNode = findTableScanNode(aggNode);
  String indexName = scanNode.getIndexName();
  CachedIndexMeta cachedMeta = getOrBuildIndexMeta(indexName);
  IndexMetadata indexMeta = clusterService.state().metadata().index(indexName);
  IndexShard shard = indicesService.indexService(indexMeta.getIndex()).getShard(req.getShardId());
  Query luceneQuery = compileOrCacheLuceneQuery(scanNode.getDslFilter(), cachedMeta.fieldTypeMap());

  java.util.Map<Long, LongOpenHashSet> finalSets;

  try (Engine.Searcher engineSearcher = shard.acquireSearcher("dqe-count-distinct-hashset")) {
    List<LeafReaderContext> leaves = engineSearcher.getIndexReader().leaves();
    boolean isMatchAll = luceneQuery instanceof MatchAllDocsQuery;
    Weight weight = isMatchAll ? null
        : engineSearcher.createWeight(engineSearcher.rewrite(luceneQuery),
            ScoreMode.COMPLETE_NO_SCORES, 1.0f);

    if (leaves.size() <= 1) {
      // Single segment: direct scan
      finalSets = scanSegmentForCountDistinct(
          leaves.isEmpty() ? null : leaves.get(0), weight, keyName0, keyName1, isMatchAll);
    } else {
      // PARALLEL SEGMENT SCANNING
      Map<Long, LongOpenHashSet>[] segResults = new Map[leaves.size()];
      CountDownLatch segLatch = new CountDownLatch(leaves.size() - 1);
      Exception[] segError = new Exception[1];

      for (int s = 0; s < leaves.size() - 1; s++) {
        final int segIdx = s;
        final LeafReaderContext leafCtx = leaves.get(s);
        FusedGroupByAggregate.getParallelPool().execute(() -> {
          try {
            segResults[segIdx] = scanSegmentForCountDistinct(
                leafCtx, weight, keyName0, keyName1, isMatchAll);
          } catch (Exception e) { segError[0] = e; }
          segLatch.countDown();
        });
      }
      // Run last segment on current thread
      segResults[leaves.size() - 1] = scanSegmentForCountDistinct(
          leaves.get(leaves.size() - 1), weight, keyName0, keyName1, isMatchAll);
      segLatch.await();
      if (segError[0] != null) throw segError[0];

      // MERGE: union LongOpenHashSets per key0, merge smaller into larger
      finalSets = segResults[0] != null ? segResults[0] : new HashMap<>();
      for (int s = 1; s < segResults.length; s++) {
        if (segResults[s] == null) continue;
        for (var entry : segResults[s].entrySet()) {
          LongOpenHashSet existing = finalSets.get(entry.getKey());
          if (existing == null) {
            finalSets.put(entry.getKey(), entry.getValue());
          } else {
            LongOpenHashSet other = entry.getValue();
            if (other.size() > existing.size()) {
              mergeHashSets(other, existing);
              finalSets.put(entry.getKey(), other);
            } else {
              mergeHashSets(existing, other);
            }
          }
        }
      }
    }
  }

  // Build output page: (key0, key1_placeholder=0, COUNT(*)=local_distinct_count)
  int grpSize = finalSets.size();
  List<Type> colTypes = List.of(
      type0 instanceof IntegerType ? IntegerType.INTEGER : BigintType.BIGINT,
      type1 instanceof IntegerType ? IntegerType.INTEGER : BigintType.BIGINT,
      BigintType.BIGINT);
  BlockBuilder b0 = colTypes.get(0).createBlockBuilder(null, grpSize);
  BlockBuilder b1 = colTypes.get(1).createBlockBuilder(null, grpSize);
  BlockBuilder b2 = colTypes.get(2).createBlockBuilder(null, grpSize);
  for (var entry : finalSets.entrySet()) {
    colTypes.get(0).writeLong(b0, entry.getKey());
    colTypes.get(1).writeLong(b1, 0L);
    BigintType.BIGINT.writeLong(b2, entry.getValue().size());
  }
  Page page = new Page(b0.build(), b1.build(), b2.build());
  ShardExecuteResponse resp = new ShardExecuteResponse(List.of(page), colTypes);
  resp.setDistinctSets(finalSets);
  return resp;
}
```

## 2. Per-segment scanner: scanSegmentForCountDistinct (line ~1130)

```java
private static Map<Long, LongOpenHashSet> scanSegmentForCountDistinct(
    LeafReaderContext leafCtx, Weight weight,
    String keyName0, String keyName1, boolean isMatchAll) throws Exception {

  if (leafCtx == null) return new HashMap<>();
  LeafReader reader = leafCtx.reader();
  SortedNumericDocValues dv0 = reader.getSortedNumericDocValues(keyName0);
  SortedNumericDocValues dv1 = reader.getSortedNumericDocValues(keyName1);

  // OPEN-ADDRESSING GROUP MAP
  int grpCap = 256;
  long[] grpKeys = new long[grpCap];
  LongOpenHashSet[] grpSets = new LongOpenHashSet[grpCap];
  boolean[] grpOcc = new boolean[grpCap];
  int grpSize = 0;
  int grpThreshold = (int) (grpCap * 0.7f);

  if (isMatchAll && dv0 != null && dv1 != null) {
    int maxDoc = reader.maxDoc();
    Bits liveDocs = reader.getLiveDocs();

    if (liveDocs == null) {
      // === COLUMNAR LOADING ===
      long[] key0Values = FusedGroupByAggregate.loadNumericColumn(leafCtx, keyName0);
      long[] key1Values = FusedGroupByAggregate.loadNumericColumn(leafCtx, keyName1);

      // Pass 1: discover unique groups, count docs per group
      int[] grpCounts = new int[grpCap];
      for (int doc = 0; doc < maxDoc; doc++) {
        long k0 = key0Values[doc];
        int gm = grpCap - 1;
        int gs = Long.hashCode(k0) & gm;
        while (grpOcc[gs] && grpKeys[gs] != k0) gs = (gs + 1) & gm;
        if (!grpOcc[gs]) {
          grpKeys[gs] = k0; grpOcc[gs] = true; grpSize++;
          if (grpSize > grpThreshold) {
            // --- RESIZE open-addressing arrays ---
            int newGC = grpCap * 2;
            long[] ngk = new long[newGC];
            boolean[] ngo = new boolean[newGC];
            int[] ngc = new int[newGC];
            int ngm = newGC - 1;
            for (int g = 0; g < grpCap; g++) {
              if (grpOcc[g]) {
                int ns = Long.hashCode(grpKeys[g]) & ngm;
                while (ngo[ns]) ns = (ns + 1) & ngm;
                ngk[ns] = grpKeys[g]; ngo[ns] = true; ngc[ns] = grpCounts[g];
              }
            }
            grpKeys = ngk; grpOcc = ngo; grpCounts = ngc;
            grpCap = newGC; grpThreshold = (int)(newGC * 0.7f);
            gm = grpCap - 1;
            gs = Long.hashCode(k0) & gm;
            while (grpOcc[gs] && grpKeys[gs] != k0) gs = (gs + 1) & gm;
          }
        }
        grpCounts[gs]++;
      }

      if (grpSize < 10000) {
        // === CACHE-FRIENDLY PARTITIONED PATH ===
        // Allocate per-group k1 arrays with dense indices
        int[] grpIdx = new int[grpCap];
        long[][] perGroupK1 = new long[grpSize][];
        int idx = 0;
        for (int g = 0; g < grpCap; g++) {
          if (grpOcc[g]) { grpIdx[g] = idx; perGroupK1[idx] = new long[grpCounts[g]]; idx++; }
        }

        // Pass 2: scatter k1 values into per-group arrays
        int[] fillPos = new int[grpSize];
        for (int doc = 0; doc < maxDoc; doc++) {
          long k0 = key0Values[doc];
          int gm = grpCap - 1;
          int gs = Long.hashCode(k0) & gm;
          while (grpKeys[gs] != k0) gs = (gs + 1) & gm;
          int gi = grpIdx[gs];
          perGroupK1[gi][fillPos[gi]++] = key1Values[doc];
        }

        // Pass 3: process each group sequentially — HashSet stays in L2 cache
        grpSets = new LongOpenHashSet[grpCap];
        for (int g = 0; g < grpCap; g++) {
          if (grpOcc[g]) {
            int gi = grpIdx[g];
            long[] k1Arr = perGroupK1[gi];
            LongOpenHashSet set = new LongOpenHashSet(Math.max(1024, k1Arr.length));
            for (int i = 0; i < k1Arr.length; i++) set.add(k1Arr[i]);
            grpSets[g] = set;
            perGroupK1[gi] = null; // release memory early
          }
        }
      } else {
        // HIGH-CARDINALITY FALLBACK: round-robin insertion
        grpSets = new LongOpenHashSet[grpCap];
        for (int doc = 0; doc < maxDoc; doc++) {
          long k0 = key0Values[doc];
          int gm = grpCap - 1;
          int gs = Long.hashCode(k0) & gm;
          while (grpKeys[gs] != k0) gs = (gs + 1) & gm;
          if (grpSets[gs] == null) grpSets[gs] = new LongOpenHashSet(1024);
          grpSets[gs].add(key1Values[doc]);
        }
      }
    } else {
      // DELETED-DOCS FALLBACK: per-doc nextDoc() iteration
      int dvDoc0 = dv0.nextDoc(); int dvDoc1 = dv1.nextDoc();
      for (int doc = 0; doc < maxDoc; doc++) {
        long k0 = 0;
        if (dvDoc0 == doc) { k0 = dv0.nextValue(); dvDoc0 = dv0.nextDoc(); }
        long k1 = 0;
        if (dvDoc1 == doc) { k1 = dv1.nextValue(); dvDoc1 = dv1.nextDoc(); }
        // ... open-addressing insert + resize (same pattern as above) ...
        // grpSets[gs].add(k1);
      }
    }
  } else if (weight != null) {
    // FILTERED QUERY PATH: scorer-based iteration with advanceExact
    Scorer scorer = weight.scorer(leafCtx);
    if (scorer != null) {
      DocIdSetIterator disi = scorer.iterator();
      int doc;
      while ((doc = disi.nextDoc()) != DocIdSetIterator.NO_MORE_DOCS) {
        long k0 = 0;
        if (dv0 != null && dv0.advanceExact(doc)) k0 = dv0.nextValue();
        long k1 = 0;
        if (dv1 != null && dv1.advanceExact(doc)) k1 = dv1.nextValue();
        // ... open-addressing insert + resize (same pattern) ...
        // grpSets[gs].add(k1);
      }
    }
  }

  // Convert open-addressing arrays → HashMap
  Map<Long, LongOpenHashSet> result = new HashMap<>(grpSize);
  for (int g = 0; g < grpCap; g++) {
    if (grpOcc[g]) result.put(grpKeys[g], grpSets[g]);
  }
  return result;
}
```

## 3. Helper: mergeHashSets (line ~1360)

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

## 4. Helper: loadNumericColumn (FusedGroupByAggregate.java:14541)

```java
public static long[] loadNumericColumn(LeafReaderContext leafCtx, String fieldName)
    throws IOException {
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

## Key Patterns to Port to NKey Path

| Pattern | 2-Key Implementation | NKey Adaptation Needed |
|---------|---------------------|----------------------|
| Columnar loading | `loadNumericColumn()` per key → `long[]` | Loop over N keys, load `long[][]` |
| Open-addressing map | `grpKeys[]/grpOcc[]` with `Long.hashCode(k0)` | Composite hash of N-1 group keys, use `LongArrayKey` or packed long |
| Cache-friendly partition | 3-pass: discover → scatter → sequential insert | Same 3-pass but scatter dedup-key per composite group |
| Parallel segments | `getParallelPool()` + `CountDownLatch` | Same pattern, change per-segment scanner signature |
| Merge | `mergeHashSets()` smaller-into-larger | Same — merge `LongOpenHashSet` per composite group key |
