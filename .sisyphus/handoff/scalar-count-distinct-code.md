# Scalar COUNT(DISTINCT) Methods in FusedScanAggregate.java

File: `dqe/src/main/java/org/opensearch/sql/dqe/shard/source/FusedScanAggregate.java`

## Method 1: `collectDistinctValuesRaw` (lines ~1511–1588)

```java
  /**
   * Execute a fused scan to collect distinct values for a single numeric column, returning the raw
   * LongOpenHashSet directly (no Page construction). This avoids the overhead of building a
   * BlockBuilder with millions of entries and then extracting them again at the coordinator.
   *
   * @param columnName the column to collect distinct values from
   * @param shard the index shard
   * @param query the compiled Lucene query
   * @return the raw LongOpenHashSet containing all distinct values in this shard
   */
  public static LongOpenHashSet collectDistinctValuesRaw(
      String columnName, IndexShard shard, Query query) throws Exception {
    LongOpenHashSet distinctSet;

    try (org.opensearch.index.engine.Engine.Searcher engineSearcher =
        shard.acquireSearcher("dqe-fused-distinct-values-raw")) {
      // Pre-size based on maxDoc to avoid resize storms for high-cardinality columns
      int totalDocs = engineSearcher.getIndexReader().maxDoc();
      distinctSet =
          totalDocs > 0
              ? new LongOpenHashSet(Math.min(totalDocs, 32_000_000))
              : new LongOpenHashSet();
      if (query instanceof MatchAllDocsQuery) {
        // Sequential scan using nextDoc() for dense columns — avoids binary search overhead
        // of advanceExact(). For high-cardinality columns like UserID (~18M distinct),
        // parallel per-segment scanning creates too many large HashSets causing memory pressure.
        // Benchmarked: parallel with merge was 4.3x SLOWER due to LongOpenHashSet merge cost.
        for (LeafReaderContext leafCtx : engineSearcher.getIndexReader().leaves()) {
          LeafReader reader = leafCtx.reader();
          Bits liveDocs = reader.getLiveDocs();
          SortedNumericDocValues dv = reader.getSortedNumericDocValues(columnName);
          if (dv == null) continue;
          if (liveDocs == null) {
            // Dense column, no deletes: use nextDoc() for sequential access
            int doc = dv.nextDoc();
            while (doc != org.apache.lucene.search.DocIdSetIterator.NO_MORE_DOCS) {
              distinctSet.add(dv.nextValue());
              doc = dv.nextDoc();
            }
          } else {
            int maxDoc = reader.maxDoc();
            for (int doc = 0; doc < maxDoc; doc++) {
              if (liveDocs.get(doc) && dv.advanceExact(doc)) {
                distinctSet.add(dv.nextValue());
              }
            }
          }
        }
      } else {
        engineSearcher.search(
            query,
            new Collector() {
              @Override
              public LeafCollector getLeafCollector(LeafReaderContext context) throws IOException {
                SortedNumericDocValues dv = context.reader().getSortedNumericDocValues(columnName);
                return new LeafCollector() {
                  @Override
                  public void setScorer(Scorable scorer) {}

                  @Override
                  public void collect(int doc) throws IOException {
                    if (dv != null && dv.advanceExact(doc)) {
                      distinctSet.add(dv.nextValue());
                    }
                  }
                };
              }

              @Override
              public ScoreMode scoreMode() {
                return ScoreMode.COMPLETE_NO_SCORES;
              }
            });
      }
    }
    return distinctSet;
  }
```

## Method 2: `collectDistinctStringsRaw` (lines ~1588–1749)

```java
  /**
   * Execute a fused scan to collect distinct strings for a single VARCHAR column, returning the raw
   * HashSet directly (no Page construction). This avoids BlockBuilder overhead.
   *
   * @param columnName the VARCHAR column to collect distinct values from
   * @param shard the index shard
   * @param query the compiled Lucene query
   * @return the raw HashSet containing all distinct string values in this shard
   */
  public static java.util.HashSet<String> collectDistinctStringsRaw(
      String columnName, IndexShard shard, Query query) throws Exception {
    java.util.HashSet<String> distinctStrings = new java.util.HashSet<>();

    try (org.opensearch.index.engine.Engine.Searcher engineSearcher =
        shard.acquireSearcher("dqe-fused-distinct-strings-raw")) {
      List<LeafReaderContext> leaves = engineSearcher.getIndexReader().leaves();

      if (query instanceof MatchAllDocsQuery) {
        // Separate segments with and without deletes
        List<LeafReaderContext> noDeleteLeaves = new java.util.ArrayList<>();
        List<LeafReaderContext> hasDeleteLeaves = new java.util.ArrayList<>();
        for (LeafReaderContext leafCtx : leaves) {
          if (leafCtx.reader().getLiveDocs() == null) {
            noDeleteLeaves.add(leafCtx);
          } else {
            hasDeleteLeaves.add(leafCtx);
          }
        }

        // Parallelize the no-deletes path across segments (ordinal iteration is O(valueCount))
        int numWorkers =
            Math.min(
                Math.max(
                    1,
                    Runtime.getRuntime().availableProcessors()
                        / Integer.getInteger("dqe.numLocalShards", 4)),
                noDeleteLeaves.size());

        if (numWorkers > 1 && noDeleteLeaves.size() > 1) {
          // Parallel path: partition segments among workers using largest-first assignment
          @SuppressWarnings("unchecked")
          List<LeafReaderContext>[] workerSegments = new List[numWorkers];
          long[] workerValueCounts = new long[numWorkers];
          for (int i = 0; i < numWorkers; i++) {
            workerSegments[i] = new java.util.ArrayList<>();
          }

          // Sort segments by valueCount descending, assign largest-first to lightest worker
          java.util.List<LeafReaderContext> sortedLeaves =
              new java.util.ArrayList<>(noDeleteLeaves);
          sortedLeaves.sort(
              (a, b) -> {
                long vcA = 0, vcB = 0;
                try {
                  SortedSetDocValues dvA = a.reader().getSortedSetDocValues(columnName);
                  if (dvA != null) vcA = dvA.getValueCount();
                  SortedSetDocValues dvB = b.reader().getSortedSetDocValues(columnName);
                  if (dvB != null) vcB = dvB.getValueCount();
                } catch (java.io.IOException e) {
                  throw new java.io.UncheckedIOException(e);
                }
                return Long.compare(vcB, vcA);
              });
          for (LeafReaderContext leaf : sortedLeaves) {
            int lightest = 0;
            for (int i = 1; i < numWorkers; i++) {
              if (workerValueCounts[i] < workerValueCounts[lightest]) lightest = i;
            }
            workerSegments[lightest].add(leaf);
            try {
              SortedSetDocValues dvLeaf = leaf.reader().getSortedSetDocValues(columnName);
              if (dvLeaf != null) workerValueCounts[lightest] += dvLeaf.getValueCount();
            } catch (java.io.IOException e) {
              throw new java.io.UncheckedIOException(e);
            }
          }

          // Each worker builds a local HashSet<String>, then we merge
          @SuppressWarnings("unchecked")
          java.util.concurrent.CompletableFuture<java.util.HashSet<String>>[] futures =
              new java.util.concurrent.CompletableFuture[numWorkers];

          for (int w = 0; w < numWorkers; w++) {
            final List<LeafReaderContext> mySegments = workerSegments[w];
            futures[w] =
                java.util.concurrent.CompletableFuture.supplyAsync(
                    () -> {
                      java.util.HashSet<String> localSet = new java.util.HashSet<>();
                      try {
                        for (LeafReaderContext leafCtx : mySegments) {
                          SortedSetDocValues dv =
                              leafCtx.reader().getSortedSetDocValues(columnName);
                          if (dv == null) continue;
                          long valueCount = dv.getValueCount();
                          for (long ord = 0; ord < valueCount; ord++) {
                            localSet.add(dv.lookupOrd(ord).utf8ToString());
                          }
                        }
                      } catch (java.io.IOException e) {
                        throw new java.io.UncheckedIOException(e);
                      }
                      return localSet;
                    },
                    FusedGroupByAggregate.getParallelPool());
          }

          // Merge worker results
          java.util.concurrent.CompletableFuture.allOf(futures).join();
          for (var future : futures) {
            distinctStrings.addAll(future.join());
          }
        } else {
          // Sequential path for no-delete segments (single segment or single worker)
          for (LeafReaderContext leafCtx : noDeleteLeaves) {
            SortedSetDocValues dv = leafCtx.reader().getSortedSetDocValues(columnName);
            if (dv == null) continue;
            long valueCount = dv.getValueCount();
            for (long ord = 0; ord < valueCount; ord++) {
              distinctStrings.add(dv.lookupOrd(ord).utf8ToString());
            }
          }
        }

        // Sequential path for segments with deletes (liveDocs)
        for (LeafReaderContext leafCtx : hasDeleteLeaves) {
          LeafReader reader = leafCtx.reader();
          Bits liveDocs = reader.getLiveDocs();
          SortedSetDocValues dv = reader.getSortedSetDocValues(columnName);
          if (dv == null) continue;
          int maxDoc = reader.maxDoc();
          FixedBitSet usedOrdinals =
              new FixedBitSet((int) Math.min(dv.getValueCount(), Integer.MAX_VALUE));
          for (int doc = 0; doc < maxDoc; doc++) {
            if (liveDocs.get(doc) && dv.advanceExact(doc)) {
              usedOrdinals.set((int) dv.nextOrd());
            }
          }
          for (int ord = usedOrdinals.nextSetBit(0);
              ord != -1;
              ord = (ord + 1 < usedOrdinals.length()) ? usedOrdinals.nextSetBit(ord + 1) : -1) {
            distinctStrings.add(dv.lookupOrd(ord).utf8ToString());
          }
        }
      } else {
        // Filtered query: use FixedBitSet ordinal collection per segment
        for (LeafReaderContext leafCtx : leaves) {
          LeafReader reader = leafCtx.reader();
          SortedSetDocValues dv = reader.getSortedSetDocValues(columnName);
          if (dv == null) continue;
          int maxDoc = reader.maxDoc();
          Bits liveDocs = reader.getLiveDocs();
          long valueCount = dv.getValueCount();
          FixedBitSet usedOrdinals =
              new FixedBitSet((int) Math.min(valueCount, Integer.MAX_VALUE));
          for (int doc = 0; doc < maxDoc; doc++) {
            boolean isLive = liveDocs == null || liveDocs.get(doc);
            if (isLive && dv.advanceExact(doc)) {
              usedOrdinals.set((int) dv.nextOrd());
            }
          }
          for (int ord = usedOrdinals.nextSetBit(0);
              ord != -1;
              ord = (ord + 1 < usedOrdinals.length()) ? usedOrdinals.nextSetBit(ord + 1) : -1) {
            distinctStrings.add(dv.lookupOrd(ord).utf8ToString());
          }
        }
      }
    }
    return distinctStrings;
  }
```
