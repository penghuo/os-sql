# Q04 Execution Path — Actual Code

## 1. Dispatch (TransportShardExecuteAction.java:268-272)

```java
    // bare TableScanNode. For scalar COUNT(DISTINCT numericCol), the shard collects distinct
    // values into a raw LongOpenHashSet and attaches it to the response, avoiding Page
    // construction overhead for millions of entries. The coordinator unions the raw sets.
    if (scanFactory == null && isBareSingleNumericColumnScan(plan)) {
      ShardExecuteResponse resp = executeDistinctValuesScanWithRawSet(plan, req);
      return resp;
    }
```

## 2. executeDistinctValuesScanWithRawSet (TransportShardExecuteAction.java:3328-3358)

```java
  private ShardExecuteResponse executeDistinctValuesScanWithRawSet(
      DqePlanNode plan, ShardExecuteRequest req) throws Exception {
    TableScanNode scanNode = (TableScanNode) plan;
    String indexName = scanNode.getIndexName();
    String columnName = scanNode.getColumns().get(0);
    CachedIndexMeta cachedMeta = getOrBuildIndexMeta(indexName);

    IndexMetadata indexMeta = clusterService.state().metadata().index(indexName);
    IndexShard shard = indicesService.indexService(indexMeta.getIndex()).getShard(req.getShardId());

    Query luceneQuery =
        compileOrCacheLuceneQuery(scanNode.getDslFilter(), cachedMeta.fieldTypeMap());

    org.opensearch.sql.dqe.operator.LongOpenHashSet rawSet =
        FusedScanAggregate.collectDistinctValuesRaw(columnName, shard, luceneQuery);

    // Build a minimal 1-row Page with the count (fallback for non-local paths)
    BlockBuilder builder = BigintType.BIGINT.createBlockBuilder(null, 1);
    BigintType.BIGINT.writeLong(builder, rawSet.size());
    List<Type> columnTypes = List.of(BigintType.BIGINT);
    ShardExecuteResponse resp =
        new ShardExecuteResponse(List.of(new Page(builder.build())), columnTypes);
    resp.setScalarDistinctSet(rawSet);
    return resp;
  }
```

## 3. collectDistinctValuesRaw (FusedScanAggregate.java:1674-1898)

```java
  public static LongOpenHashSet collectDistinctValuesRaw(
      String columnName, IndexShard shard, Query query) throws Exception {
    LongOpenHashSet distinctSet;

    try (org.opensearch.index.engine.Engine.Searcher engineSearcher =
        shard.acquireSearcher("dqe-fused-distinct-values-raw")) {
      // Pre-size based on total docs to minimize resizes. For high-cardinality columns
      // (e.g., UserID with ~4.25M distinct per shard), starting at 65536 causes 7 resizes,
      // each rehashing all entries. Pre-sizing to ~8M avoids all resizes.
      long totalDocs = 0;
      for (LeafReaderContext leaf : engineSearcher.getIndexReader().leaves()) {
        totalDocs += leaf.reader().maxDoc();
      }
      // Estimate distinct count as min(totalDocs/4, 8M) — conservative for high-cardinality
      int estimatedDistinct = (int) Math.min(totalDocs / 4, 8_000_000);
      distinctSet = new LongOpenHashSet(Math.max(65536, estimatedDistinct));

      if (query instanceof MatchAllDocsQuery) {
        // ===== MATCHALL PATH =====
        // Phase 1: Parallel columnar load — read DocValues into flat long[] arrays per segment.
        // Phase 2: Parallel per-segment hash set insertion, then merge.
        // Two-phase approach is faster than fused (DocValues read + hash insert) because
        // Phase 1 has sequential memory access and Phase 2 has sequential array reads,
        // while fused interleaves random hash probes with sequential DocValues reads.
        java.util.List<LeafReaderContext> leaves = engineSearcher.getIndexReader().leaves();
        long[][] segArrays = new long[leaves.size()][];
        int[] segCounts = new int[leaves.size()];

        if (leaves.size() > 1) {
          // ----- MULTI-SEGMENT: parallel columnar load -----
          java.util.concurrent.CompletableFuture<?>[] futures =
              new java.util.concurrent.CompletableFuture[leaves.size()];
          for (int s = 0; s < leaves.size(); s++) {
            final int segIdx = s;
            futures[s] =
                java.util.concurrent.CompletableFuture.runAsync(
                    () -> {
                      try {
                        LeafReader reader = leaves.get(segIdx).reader();
                        SortedNumericDocValues dv =
                            reader.getSortedNumericDocValues(columnName);
                        if (dv == null) {
                          segArrays[segIdx] = new long[0];
                          return;
                        }
                        int maxDoc = reader.maxDoc();
                        long[] vals = new long[maxDoc];
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
                        segArrays[segIdx] = vals;
                        segCounts[segIdx] = count;
                      } catch (Exception e) {
                        throw new RuntimeException(e);
                      }
                    },
                    FusedGroupByAggregate.getParallelPool());
          }
          java.util.concurrent.CompletableFuture.allOf(futures).join();

        } else {
          // ----- SINGLE SEGMENT -----
          LeafReaderContext leafCtx = leaves.get(0);
          int maxDoc = leafCtx.reader().maxDoc();

          if (maxDoc > 1_000_000) {
            // Large single segment — parallel doc-range scanning via columnar load.
            // After force-merge to 1 segment, the sequential fused path is ~4x slower
            // than the multi-segment parallel path. Recover by splitting the flat array
            // into THREADS_PER_SHARD ranges with per-worker hash sets, then merging.
            long[] column = FusedGroupByAggregate.loadNumericColumn(leafCtx, columnName);
            int numWorkers = FusedGroupByAggregate.THREADS_PER_SHARD;
            int docsPerWorker = (maxDoc + numWorkers - 1) / numWorkers;
            @SuppressWarnings("unchecked")
            java.util.concurrent.CompletableFuture<LongOpenHashSet>[] workerFutures =
                new java.util.concurrent.CompletableFuture[numWorkers];
            for (int w = 0; w < numWorkers; w++) {
              final int start = w * docsPerWorker;
              final int end = Math.min(start + docsPerWorker, maxDoc);
              workerFutures[w] =
                  java.util.concurrent.CompletableFuture.supplyAsync(
                      () -> {
                        int rangeLen = end - start;
                        LongOpenHashSet set =
                            new LongOpenHashSet((int) Math.min(rangeLen / 4, 8_000_000));
                        set.addAllBatched(column, start, rangeLen);
                        return set;
                      },
                      FusedGroupByAggregate.getParallelPool());
            }
            // Merge worker sets: find largest, addAll smaller into largest
            LongOpenHashSet[] workerSets = new LongOpenHashSet[numWorkers];
            for (int w = 0; w < numWorkers; w++) {
              workerSets[w] = workerFutures[w].join();
            }
            int largestIdx = 0;
            for (int w = 1; w < numWorkers; w++) {
              if (workerSets[w].size() > workerSets[largestIdx].size()) {
                largestIdx = w;
              }
            }
            for (int w = 0; w < numWorkers; w++) {
              if (w != largestIdx) {
                workerSets[largestIdx].addAll(workerSets[w]);
              }
            }
            distinctSet = workerSets[largestIdx];

          } else {
            // Small single segment — fused single-pass: read DocValues directly into hash set
            // with inline run-length dedup, avoiding temp long[maxDoc] allocation.
            LeafReader reader = leafCtx.reader();
            SortedNumericDocValues dv = reader.getSortedNumericDocValues(columnName);
            if (dv != null) {
              Bits liveDocs = reader.getLiveDocs();
              if (liveDocs == null) {
                // Fused single-pass: read DocValues directly into hash set with run-length dedup
                long prev = Long.MIN_VALUE;
                boolean hasPrev = false;
                int doc = dv.nextDoc();
                while (doc != DocIdSetIterator.NO_MORE_DOCS) {
                  long v = dv.nextValue();
                  if (!hasPrev || v != prev) {
                    distinctSet.add(v);
                    prev = v;
                    hasPrev = true;
                  }
                  doc = dv.nextDoc();
                }
              } else {
                // With liveDocs: advanceExact per live doc
                int smallMaxDoc = reader.maxDoc();
                long prev = Long.MIN_VALUE;
                boolean hasPrev = false;
                for (int doc = 0; doc < smallMaxDoc; doc++) {
                  if (liveDocs.get(doc) && dv.advanceExact(doc)) {
                    long v = dv.nextValue();
                    if (!hasPrev || v != prev) {
                      distinctSet.add(v);
                      prev = v;
                      hasPrev = true;
                    }
                  }
                }
              }
            }
          }
        }

        // Phase 2: Parallel per-segment hash insertion, then merge (multi-segment only)
        if (leaves.size() > 1) {
          @SuppressWarnings("unchecked")
          java.util.concurrent.CompletableFuture<LongOpenHashSet>[] hashFutures =
              new java.util.concurrent.CompletableFuture[leaves.size()];
          for (int s = 0; s < leaves.size(); s++) {
            final int segIdx = s;
            hashFutures[s] =
                java.util.concurrent.CompletableFuture.supplyAsync(
                    () -> {
                      long[] vals = segArrays[segIdx];
                      if (vals == null || segCounts[segIdx] == 0) {
                        return new LongOpenHashSet();
                      }
                      LongOpenHashSet set = new LongOpenHashSet(Math.min(segCounts[segIdx], 8_000_000));
                      int count = segCounts[segIdx];
                      // Use prefetch-batched insertion for better cache behavior
                      set.addAllBatched(vals, 0, count);
                      return set;
                    },
                    FusedGroupByAggregate.getParallelPool());
          }
          LongOpenHashSet[] segSets = new LongOpenHashSet[leaves.size()];
          for (int s = 0; s < leaves.size(); s++) {
            segSets[s] = hashFutures[s].join();
          }
          // Merge: find largest set, addAll smaller sets into it
          int largestIdx = 0;
          for (int s = 1; s < segSets.length; s++) {
            if (segSets[s].size() > segSets[largestIdx].size()) {
              largestIdx = s;
            }
          }
          for (int s = 0; s < segSets.length; s++) {
            if (s != largestIdx) {
              segSets[largestIdx].addAll(segSets[s]);
            }
          }
          distinctSet = segSets[largestIdx];
        } else {
          // Single segment — already inserted directly into distinctSet in fused Phase 1
        }

      } else {
        // ===== FILTERED QUERY PATH =====
        final LongOpenHashSet filteredSet = distinctSet;
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
                      filteredSet.add(dv.nextValue());
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
