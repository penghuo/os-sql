# Scalar Aggregate Code Extraction

Source: `dqe/src/main/java/org/opensearch/sql/dqe/shard/source/FusedScanAggregate.java`
and `dqe/src/main/java/org/opensearch/sql/dqe/shard/source/FusedGroupByAggregate.java`

---

## 1. collectDistinctValuesRaw (L1505-L1564)

```java
  public static LongOpenHashSet collectDistinctValuesRaw(
      String columnName, IndexShard shard, Query query) throws Exception {
    LongOpenHashSet distinctSet = new LongOpenHashSet();

    try (org.opensearch.index.engine.Engine.Searcher engineSearcher =
        shard.acquireSearcher("dqe-fused-distinct-values-raw")) {
      if (query instanceof MatchAllDocsQuery) {
        // Sequential scan using nextDoc() for dense columns — avoids binary search overhead
        // of advanceExact(). For high-cardinality columns like UserID (~18M distinct),
        // parallel per-segment scanning creates too many large HashSets causing memory pressure.
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

---

## 2. collectDistinctStringsRaw (L1575-L1627)

```java
  public static java.util.HashSet<String> collectDistinctStringsRaw(
      String columnName, IndexShard shard, Query query) throws Exception {
    java.util.HashSet<String> distinctStrings = new java.util.HashSet<>();

    try (org.opensearch.index.engine.Engine.Searcher engineSearcher =
        shard.acquireSearcher("dqe-fused-distinct-strings-raw")) {
      List<LeafReaderContext> leaves = engineSearcher.getIndexReader().leaves();

      for (LeafReaderContext leafCtx : leaves) {
        LeafReader reader = leafCtx.reader();
        SortedSetDocValues dv = reader.getSortedSetDocValues(columnName);
        if (dv == null) continue;

        int maxDoc = reader.maxDoc();
        Bits liveDocs = reader.getLiveDocs();
        long valueCount = dv.getValueCount();

        if (query instanceof MatchAllDocsQuery) {
          if (liveDocs == null) {
            for (long ord = 0; ord < valueCount; ord++) {
              distinctStrings.add(dv.lookupOrd(ord).utf8ToString());
            }
            continue;
          }
          FixedBitSet usedOrdinals = new FixedBitSet((int) Math.min(valueCount, Integer.MAX_VALUE));
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
        } else {
          FixedBitSet usedOrdinals = new FixedBitSet((int) Math.min(valueCount, Integer.MAX_VALUE));
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

---

## 3. tryFlatArrayPath (L767-L1036)

```java
  private static boolean tryFlatArrayPath(
      List<AggSpec> specs,
      List<DirectAccumulator> accumulators,
      org.opensearch.index.engine.Engine.Searcher engineSearcher)
      throws IOException {
    // Check eligibility: no DISTINCT, no double, no varchar, no string columns
    for (AggSpec spec : specs) {
      if (spec.isDistinct()) {
        return false;
      }
      if (spec.argType() instanceof DoubleType || spec.argType() instanceof VarcharType) {
        return false;
      }
    }
    // Check for deleted docs
    for (LeafReaderContext leafCtx : engineSearcher.getIndexReader().leaves()) {
      if (leafCtx.reader().getLiveDocs() != null) {
        return false;
      }
    }

    // Collect unique column names (excluding "*" for COUNT(*))
    List<String> uniqueColumns = new ArrayList<>();
    for (AggSpec spec : specs) {
      if (!"*".equals(spec.arg()) && !uniqueColumns.contains(spec.arg())) {
        uniqueColumns.add(spec.arg());
      }
    }

    int numCols = uniqueColumns.size();
    // Per-column accumulators: [sum, count, min, max] per column
    long[] colSum = new long[numCols];
    // Note: no double[] needed — all columns in this path are integer-family,
    // and long accumulation is sufficient for SUM/AVG.
    long[] colCount = new long[numCols];
    long[] colMin = new long[numCols];
    long[] colMax = new long[numCols];
    for (int i = 0; i < numCols; i++) {
      colMin[i] = Long.MAX_VALUE;
      colMax[i] = Long.MIN_VALUE;
    }
    long totalDocs = 0;
    String[] colArray = uniqueColumns.toArray(new String[0]);

    // Determine which aggregate types are actually needed to skip unnecessary work.
    // Note: colNeedsDouble is always false within this path because DoubleType columns
    // are excluded by the eligibility check above. For integer-family columns used with AVG,
    // long accumulation is sufficient — the SUM fits in a long (e.g., short max 65535 * 100M
    // rows = ~6.5e12, well within long range). The conversion to double happens only once
    // in AvgDirectAccumulator.addLongSumCount().
    boolean needMin = false, needMax = false;
    for (AggSpec spec : specs) {
      if ("MIN".equals(spec.funcName())) needMin = true;
      if ("MAX".equals(spec.funcName())) needMax = true;
    }

    // Column-major iteration: process one column at a time across all docs per segment.
    // Uses nextDoc() sequential iteration instead of advanceExact(doc) random access.
    // For dense columns (all docs have values, typical in ClickBench), nextDoc() is
    // significantly faster because it reads the compressed doc value stream sequentially
    // instead of seeking to each doc position individually. This also improves CPU cache
    // utilization since we're reading one column's data contiguously before moving to the next.

    // Parallelize across segments using ForkJoinPool
    List<LeafReaderContext> leaves = engineSearcher.getIndexReader().leaves();
    int numWorkers =
        Math.min(
            Math.max(
                1,
                Runtime.getRuntime().availableProcessors()
                    / Integer.getInteger("dqe.numLocalShards", 4)),
            leaves.size());

    if (numWorkers > 1 && leaves.size() > 1) {
      // Parallel path: partition segments among workers using largest-first assignment
      @SuppressWarnings("unchecked")
      List<LeafReaderContext>[] workerSegments = new List[numWorkers];
      long[] workerDocCounts = new long[numWorkers];
      for (int i = 0; i < numWorkers; i++) {
        workerSegments[i] = new java.util.ArrayList<>();
      }

      // Sort segments by doc count descending, assign largest-first to lightest worker
      java.util.List<LeafReaderContext> sortedLeaves = new java.util.ArrayList<>(leaves);
      sortedLeaves.sort((a, b) -> Integer.compare(b.reader().maxDoc(), a.reader().maxDoc()));
      for (LeafReaderContext leaf : sortedLeaves) {
        int lightest = 0;
        for (int i = 1; i < numWorkers; i++) {
          if (workerDocCounts[i] < workerDocCounts[lightest]) lightest = i;
        }
        workerSegments[lightest].add(leaf);
        workerDocCounts[lightest] += leaf.reader().maxDoc();
      }

      // Capture effectively-final locals for lambda
      final int nc = numCols;
      final boolean needMinF = needMin, needMaxF = needMax;

      // Each worker packs results into a long[]:
      //   [totalDocs, colSum[0..nc-1], colCount[0..nc-1], colMin[0..nc-1], colMax[0..nc-1]]
      // No double accumulation needed: DoubleType columns are excluded by the eligibility
      // check, and for integer-family columns, long accumulation is sufficient.
      @SuppressWarnings("unchecked")
      java.util.concurrent.CompletableFuture<long[]>[] futures =
          new java.util.concurrent.CompletableFuture[numWorkers];

      for (int w = 0; w < numWorkers; w++) {
        final List<LeafReaderContext> mySegments = workerSegments[w];
        futures[w] =
            java.util.concurrent.CompletableFuture.supplyAsync(
                () -> {
                  long[] wColSum = new long[nc];
                  long[] wColCount = new long[nc];
                  long[] wColMin = new long[nc];
                  long[] wColMax = new long[nc];
                  for (int i = 0; i < nc; i++) {
                    wColMin[i] = Long.MAX_VALUE;
                    wColMax[i] = Long.MIN_VALUE;
                  }
                  long wTotalDocs = 0;

                  try {
                    for (LeafReaderContext leafCtx : mySegments) {
                      LeafReader reader = leafCtx.reader();
                      wTotalDocs += reader.maxDoc();

                      for (int c = 0; c < nc; c++) {
                        SortedNumericDocValues dv = reader.getSortedNumericDocValues(colArray[c]);
                        if (dv == null) continue;
                        long localSum = 0;
                        long localCount = 0;

                        if (needMinF || needMaxF) {
                          long localMin = Long.MAX_VALUE, localMax = Long.MIN_VALUE;
                          int doc = dv.nextDoc();
                          while (doc != DocIdSetIterator.NO_MORE_DOCS) {
                            long val = dv.nextValue();
                            localSum += val;
                            localCount++;
                            if (val < localMin) localMin = val;
                            if (val > localMax) localMax = val;
                            doc = dv.nextDoc();
                          }
                          if (localMin < wColMin[c]) wColMin[c] = localMin;
                          if (localMax > wColMax[c]) wColMax[c] = localMax;
                        } else {
                          int doc = dv.nextDoc();
                          while (doc != DocIdSetIterator.NO_MORE_DOCS) {
                            localSum += dv.nextValue();
                            localCount++;
                            doc = dv.nextDoc();
                          }
                        }
                        wColSum[c] += localSum;
                        wColCount[c] += localCount;
                      }
                    }
                  } catch (java.io.IOException e) {
                    throw new java.io.UncheckedIOException(e);
                  }

                  // Pack results into long array
                  long[] pack = new long[1 + nc * 4];
                  pack[0] = wTotalDocs;
                  for (int c = 0; c < nc; c++) {
                    pack[1 + c] = wColSum[c];
                    pack[1 + nc + c] = wColCount[c];
                    pack[1 + 2 * nc + c] = wColMin[c];
                    pack[1 + 3 * nc + c] = wColMax[c];
                  }
                  return pack;
                },
                FusedGroupByAggregate.getParallelPool());
      }

      // Merge worker results
      java.util.concurrent.CompletableFuture.allOf(futures).join();
      for (var future : futures) {
        long[] pack = future.join();
        totalDocs += pack[0];
        for (int c = 0; c < numCols; c++) {
          colSum[c] += pack[1 + c];
          colCount[c] += pack[1 + numCols + c];
          if (pack[1 + 2 * numCols + c] < colMin[c]) colMin[c] = pack[1 + 2 * numCols + c];
          if (pack[1 + 3 * numCols + c] > colMax[c]) colMax[c] = pack[1 + 3 * numCols + c];
        }
      }
    } else {
      // Sequential path: single worker or single segment
      for (LeafReaderContext leafCtx : leaves) {
        LeafReader reader = leafCtx.reader();
        int maxDoc = reader.maxDoc();
        totalDocs += maxDoc;

        for (int c = 0; c < numCols; c++) {
          SortedNumericDocValues dv = reader.getSortedNumericDocValues(colArray[c]);
          if (dv == null) continue;

          long localSum = 0;
          long localCount = 0;

          if (needMin || needMax) {
            long localMin = Long.MAX_VALUE;
            long localMax = Long.MIN_VALUE;
            int doc = dv.nextDoc();
            while (doc != org.apache.lucene.search.DocIdSetIterator.NO_MORE_DOCS) {
              long val = dv.nextValue();
              localSum += val;
              localCount++;
              if (val < localMin) localMin = val;
              if (val > localMax) localMax = val;
              doc = dv.nextDoc();
            }
            if (localMin < colMin[c]) colMin[c] = localMin;
            if (localMax > colMax[c]) colMax[c] = localMax;
          } else {
            int doc = dv.nextDoc();
            while (doc != org.apache.lucene.search.DocIdSetIterator.NO_MORE_DOCS) {
              localSum += dv.nextValue();
              localCount++;
              doc = dv.nextDoc();
            }
          }
          colSum[c] += localSum;
          colCount[c] += localCount;
        }
      }
    }

    // Map flat-array results back to accumulators
    for (int i = 0; i < specs.size(); i++) {
      AggSpec spec = specs.get(i);
      DirectAccumulator acc = accumulators.get(i);
      if ("COUNT".equals(spec.funcName()) && "*".equals(spec.arg())) {
        if (acc instanceof CountStarDirectAccumulator csa) {
          csa.addCount(totalDocs);
        }
      } else {
        int colIdx = uniqueColumns.indexOf(spec.arg());
        switch (spec.funcName()) {
          case "SUM":
            if (acc instanceof SumDirectAccumulator sa) {
              sa.addLongSum(colSum[colIdx], colCount[colIdx]);
            }
            break;
          case "COUNT":
            if (acc instanceof CountStarDirectAccumulator csa) {
              csa.addCount(colCount[colIdx]);
            }
            break;
          case "AVG":
            if (acc instanceof AvgDirectAccumulator aa) {
              aa.addLongSumCount(colSum[colIdx], colCount[colIdx]);
            }
            break;
          case "MIN":
            if (acc instanceof MinDirectAccumulator ma) {
              ma.mergeMin(colMin[colIdx], colCount[colIdx] > 0);
            }
            break;
          case "MAX":
            if (acc instanceof MaxDirectAccumulator ma) {
              ma.mergeMax(colMax[colIdx], colCount[colIdx] > 0);
            }
            break;
        }
      }
    }
    return true;
  }
```

---

## 4a. canFuseWithEval (L101-L141)

```java
  public static boolean canFuseWithEval(AggregationNode aggNode) {
    if (!aggNode.getGroupByKeys().isEmpty()) {
      return false;
    }
    if (!(aggNode.getChild() instanceof EvalNode evalNode)) {
      return false;
    }
    if (!(evalNode.getChild() instanceof TableScanNode)) {
      return false;
    }
    // All aggregate functions must be SUM, COUNT, or AVG (non-distinct)
    for (String func : aggNode.getAggregateFunctions()) {
      Matcher m = AGG_FUNCTION.matcher(func);
      if (!m.matches()) {
        return false;
      }
      String funcName = m.group(1).toUpperCase(Locale.ROOT);
      boolean isDistinct = m.group(2) != null;
      if (isDistinct) {
        return false;
      }
      if (!"SUM".equals(funcName) && !"COUNT".equals(funcName) && !"AVG".equals(funcName)) {
        return false;
      }
    }
    // All eval expressions must be plain columns or (column + integer_constant)
    for (String expr : evalNode.getExpressions()) {
      String trimmed = expr.trim();
      // Plain column reference (single identifier)
      if (trimmed.matches("\\w+")) {
        continue;
      }
      // (col + N) pattern
      Matcher cm = COL_PLUS_CONST.matcher(trimmed);
      if (cm.matches()) {
        continue;
      }
      return false; // Unsupported expression
    }
    return true;
  }
```

---

## 4b. executeWithEval (L154-L445)

```java
  public static List<Page> executeWithEval(
      AggregationNode aggNode, IndexShard shard, Query query, Map<String, Type> columnTypeMap)
      throws Exception {
    EvalNode evalNode = (EvalNode) aggNode.getChild();
    List<String> evalExprs = evalNode.getExpressions();
    List<String> evalNames = evalNode.getOutputColumnNames();
    List<String> aggFunctions = aggNode.getAggregateFunctions();

    // Build mapping: eval column name -> (physicalColumn, offset)
    // Also collect unique physical columns that need SUM
    Map<String, String> nameToPhysical = new java.util.LinkedHashMap<>();
    Map<String, Long> nameToOffset = new java.util.LinkedHashMap<>();
    Set<String> uniquePhysicalColumns = new java.util.LinkedHashSet<>();

    for (int i = 0; i < evalExprs.size(); i++) {
      String expr = evalExprs.get(i).trim();
      String name = evalNames.get(i);
      Matcher cm = COL_PLUS_CONST.matcher(expr);
      if (cm.matches()) {
        String col = cm.group(1);
        long offset = Long.parseLong(cm.group(2));
        nameToPhysical.put(name, col);
        nameToOffset.put(name, offset);
        uniquePhysicalColumns.add(col);
      } else {
        // Plain column reference
        nameToPhysical.put(name, expr);
        nameToOffset.put(name, 0L);
        uniquePhysicalColumns.add(expr);
      }
    }

    // Compute SUM and COUNT for each unique physical column
    // Also include physical columns referenced directly by COUNT(col) and AVG(col) aggregates
    for (String func : aggFunctions) {
      Matcher m = AGG_FUNCTION.matcher(func);
      if (m.matches()) {
        String arg = m.group(3).trim();
        if (!"*".equals(arg) && !nameToPhysical.containsKey(arg)) {
          // Direct physical column reference (not through EvalNode)
          nameToPhysical.put(arg, arg);
          nameToOffset.put(arg, 0L);
          uniquePhysicalColumns.add(arg);
        }
      }
    }
    Map<String, long[]> colSumCount = new java.util.LinkedHashMap<>();
    for (String col : uniquePhysicalColumns) {
      colSumCount.put(col, new long[2]); // [sum, count]
    }

    try (org.opensearch.index.engine.Engine.Searcher engineSearcher =
        shard.acquireSearcher("dqe-fused-eval-agg")) {
      String[] colArray = uniquePhysicalColumns.toArray(new String[0]);
      // Pre-resolve Map entries to flat arrays for zero-alloc inner loop
      long[][] scArrays = new long[colArray.length][];
      for (int i = 0; i < colArray.length; i++) {
        scArrays[i] = colSumCount.get(colArray[i]);
      }

      if (query instanceof MatchAllDocsQuery) {
        // Fast path: column-major iteration using nextDoc() for sequential access.
        // Avoids advanceExact() overhead and Map lookups in the inner loop.
        boolean noDeletes = true;
        for (LeafReaderContext leafCtx : engineSearcher.getIndexReader().leaves()) {
          if (leafCtx.reader().getLiveDocs() != null) {
            noDeletes = false;
            break;
          }
        }

        if (noDeletes) {
          // Parallel path: distribute segments across workers for segment-level parallelism.
          // Each worker accumulates sum/count for all columns across its assigned segments,
          // then results are merged. This is the same strategy as tryFlatArrayPath.
          List<LeafReaderContext> leaves = engineSearcher.getIndexReader().leaves();
          int numWorkers =
              Math.min(
                  Math.max(
                      1,
                      Runtime.getRuntime().availableProcessors()
                          / Integer.getInteger("dqe.numLocalShards", 4)),
                  leaves.size());

          if (numWorkers > 1 && leaves.size() > 1) {
            // Partition segments among workers using largest-first assignment
            @SuppressWarnings("unchecked")
            List<LeafReaderContext>[] workerSegments = new List[numWorkers];
            long[] workerDocCounts = new long[numWorkers];
            for (int i = 0; i < numWorkers; i++) {
              workerSegments[i] = new java.util.ArrayList<>();
            }
            java.util.List<LeafReaderContext> sortedLeaves = new java.util.ArrayList<>(leaves);
            sortedLeaves.sort((a, b) -> Integer.compare(b.reader().maxDoc(), a.reader().maxDoc()));
            for (LeafReaderContext leaf : sortedLeaves) {
              int lightest = 0;
              for (int i = 1; i < numWorkers; i++) {
                if (workerDocCounts[i] < workerDocCounts[lightest]) lightest = i;
              }
              workerSegments[lightest].add(leaf);
              workerDocCounts[lightest] += leaf.reader().maxDoc();
            }

            final int nc = colArray.length;
            @SuppressWarnings("unchecked")
            java.util.concurrent.CompletableFuture<long[]>[] futures =
                new java.util.concurrent.CompletableFuture[numWorkers];

            for (int w = 0; w < numWorkers; w++) {
              final List<LeafReaderContext> mySegments = workerSegments[w];
              futures[w] =
                  java.util.concurrent.CompletableFuture.supplyAsync(
                      () -> {
                        // Pack: [sum0, count0, sum1, count1, ...]
                        long[] pack = new long[nc * 2];
                        try {
                          for (LeafReaderContext leafCtx : mySegments) {
                            LeafReader reader = leafCtx.reader();
                            for (int i = 0; i < nc; i++) {
                              SortedNumericDocValues dv =
                                  reader.getSortedNumericDocValues(colArray[i]);
                              if (dv == null) continue;
                              long localSum = 0;
                              long localCount = 0;
                              int doc = dv.nextDoc();
                              while (doc != DocIdSetIterator.NO_MORE_DOCS) {
                                localSum += dv.nextValue();
                                localCount++;
                                doc = dv.nextDoc();
                              }
                              pack[i * 2] += localSum;
                              pack[i * 2 + 1] += localCount;
                            }
                          }
                        } catch (IOException e) {
                          throw new java.io.UncheckedIOException(e);
                        }
                        return pack;
                      },
                      FusedGroupByAggregate.getParallelPool());
            }

            // Merge worker results
            for (var future : futures) {
              long[] pack = future.join();
              for (int i = 0; i < nc; i++) {
                scArrays[i][0] += pack[i * 2];
                scArrays[i][1] += pack[i * 2 + 1];
              }
            }
          } else {
            // Sequential fallback for single segment or single worker
            for (LeafReaderContext leafCtx : leaves) {
              LeafReader reader = leafCtx.reader();
              for (int i = 0; i < colArray.length; i++) {
                SortedNumericDocValues dv = reader.getSortedNumericDocValues(colArray[i]);
                if (dv == null) continue;
                long localSum = 0;
                long localCount = 0;
                int doc = dv.nextDoc();
                while (doc != DocIdSetIterator.NO_MORE_DOCS) {
                  localSum += dv.nextValue();
                  localCount++;
                  doc = dv.nextDoc();
                }
                scArrays[i][0] += localSum;
                scArrays[i][1] += localCount;
              }
            }
          }
        } else {
          // Has deleted docs: row-major with advanceExact
          for (LeafReaderContext leafCtx : engineSearcher.getIndexReader().leaves()) {
            LeafReader reader = leafCtx.reader();
            int maxDoc = reader.maxDoc();
            Bits liveDocs = reader.getLiveDocs();
            SortedNumericDocValues[] dvs = new SortedNumericDocValues[colArray.length];
            for (int i = 0; i < colArray.length; i++) {
              dvs[i] = reader.getSortedNumericDocValues(colArray[i]);
            }
            for (int doc = 0; doc < maxDoc; doc++) {
              if (liveDocs.get(doc)) {
                for (int i = 0; i < colArray.length; i++) {
                  SortedNumericDocValues dv = dvs[i];
                  if (dv != null && dv.advanceExact(doc)) {
                    scArrays[i][0] += dv.nextValue();
                    scArrays[i][1]++;
                  }
                }
              }
            }
          }
        }
      } else {
        // General path: use Lucene's search framework
        engineSearcher.search(
            query,
            new Collector() {
              @Override
              public LeafCollector getLeafCollector(LeafReaderContext context) throws IOException {
                SortedNumericDocValues[] dvs = new SortedNumericDocValues[colArray.length];
                for (int i = 0; i < colArray.length; i++) {
                  dvs[i] = context.reader().getSortedNumericDocValues(colArray[i]);
                }
                return new LeafCollector() {
                  @Override
                  public void setScorer(Scorable scorer) {}

                  @Override
                  public void collect(int doc) throws IOException {
                    for (int i = 0; i < colArray.length; i++) {
                      SortedNumericDocValues dv = dvs[i];
                      if (dv != null && dv.advanceExact(doc)) {
                        long[] sc = colSumCount.get(colArray[i]);
                        sc[0] += dv.nextValue();
                        sc[1]++;
                      }
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

    // Derive results: SUM(col + k) = SUM(col) + k * COUNT(col)
    // COUNT(*) = count of any physical column, AVG(col + k) = (SUM(col) + k * COUNT) / COUNT
    int numAggs = aggFunctions.size();
    BlockBuilder[] builders = new BlockBuilder[numAggs];
    for (int i = 0; i < numAggs; i++) {
      Matcher m = AGG_FUNCTION.matcher(aggFunctions.get(i));
      String funcName = m.matches() ? m.group(1).toUpperCase(Locale.ROOT) : "SUM";
      builders[i] =
          "AVG".equals(funcName)
              ? DoubleType.DOUBLE.createBlockBuilder(null, 1)
              : BigintType.BIGINT.createBlockBuilder(null, 1);
    }

    for (int i = 0; i < numAggs; i++) {
      Matcher m = AGG_FUNCTION.matcher(aggFunctions.get(i));
      if (!m.matches()) {
        throw new IllegalArgumentException("Unsupported aggregate: " + aggFunctions.get(i));
      }
      String funcName = m.group(1).toUpperCase(Locale.ROOT);
      String arg = m.group(3).trim(); // The eval column name or "*"

      if ("COUNT".equals(funcName)) {
        // COUNT(*) or COUNT(col): use count from any available physical column
        if ("*".equals(arg)) {
          // Use count from the first physical column
          long count = 0;
          for (long[] sc : colSumCount.values()) {
            count = sc[1];
            break;
          }
          BigintType.BIGINT.writeLong(builders[i], count);
        } else {
          String physCol = nameToPhysical.getOrDefault(arg, arg);
          long[] sc = colSumCount.get(physCol);
          BigintType.BIGINT.writeLong(builders[i], sc != null ? sc[1] : 0);
        }
      } else if ("AVG".equals(funcName)) {
        String physCol = nameToPhysical.getOrDefault(arg, arg);
        long offset = nameToOffset.getOrDefault(arg, 0L);
        long[] sc = colSumCount.get(physCol);
        if (sc != null && sc[1] > 0) {
          double avg = (double) (sc[0] + offset * sc[1]) / sc[1];
          DoubleType.DOUBLE.writeDouble(builders[i], avg);
        } else {
          builders[i].appendNull();
        }
      } else {
        // SUM: original algebraic identity
        String physCol = nameToPhysical.get(arg);
        long offset = nameToOffset.getOrDefault(arg, 0L);
        long[] sc = colSumCount.get(physCol);
        long result = sc[0] + offset * sc[1]; // SUM(col) + k * COUNT
        BigintType.BIGINT.writeLong(builders[i], result);
      }
    }

    Block[] blocks = new Block[numAggs];
    for (int i = 0; i < numAggs; i++) {
      blocks[i] = builders[i].build();
    }
    return List.of(new Page(blocks));
  }
```

---

## 5. Parallel Pool Setup from FusedGroupByAggregate.java (L115-L137)

```java

  // === Intra-shard parallelism configuration ===
  private static final String PARALLELISM_MODE = System.getProperty("dqe.parallelism", "docrange");
  private static final int THREADS_PER_SHARD =
      Math.max(
          1,
          Runtime.getRuntime().availableProcessors() / Integer.getInteger("dqe.numLocalShards", 4));

  private static final java.util.concurrent.ForkJoinPool PARALLEL_POOL =
      new java.util.concurrent.ForkJoinPool(
          Runtime.getRuntime().availableProcessors(),
          java.util.concurrent.ForkJoinPool.defaultForkJoinWorkerThreadFactory,
          null,
          true); // asyncMode=true for work-stealing

  private FusedGroupByAggregate() {}

  /** Expose the shared ForkJoinPool for intra-shard parallelism in other fast paths. */
  public static java.util.concurrent.ForkJoinPool getParallelPool() {
    return PARALLEL_POOL;
  }

  /**
```

---

## Summary of Key Observations

### collectDistinctValuesRaw (L1505-L1564)
- **Sequential only** — iterates segments one at a time using `nextDoc()` for dense columns
- Comment says: "parallel per-segment scanning creates too many large HashSets causing memory pressure"
- Uses `LongOpenHashSet` (custom primitive set, no boxing)
- For MatchAll+no-deletes: uses `nextDoc()` sequential access
- For MatchAll+deletes: uses `advanceExact()` with liveDocs check
- For filtered: uses Lucene Collector framework

### collectDistinctStringsRaw (L1575-L1627)
- **Sequential only** — iterates segments one at a time
- Uses `java.util.HashSet<String>` (boxed strings)
- For MatchAll+no-deletes: iterates ordinals directly via `dv.lookupOrd(ord)` — O(valueCount)
- For MatchAll+deletes: uses `FixedBitSet` to collect used ordinals, then resolves
- For filtered: uses `FixedBitSet` ordinal collection approach

### tryFlatArrayPath (L767-L1036)
- **Already parallelized** — uses `FusedGroupByAggregate.getParallelPool()` with CompletableFuture
- Column-major iteration: processes one column at a time across all docs per segment
- Uses `nextDoc()` sequential iteration (not `advanceExact()`)
- Parallel path: partitions segments across workers using largest-first assignment
- Each worker packs results into `long[]`: `[totalDocs, colSum[0..nc-1], colCount[0..nc-1], colMin[0..nc-1], colMax[0..nc-1]]`
- Worker count: `min(availableProcessors / dqe.numLocalShards, numSegments)`

### canFuseWithEval + executeWithEval (L101-L445)
- Handles `SUM(col + k)` pattern using algebraic identity: `SUM(col+k) = SUM(col) + k*COUNT(*)`
- `canFuseWithEval`: checks AggNode -> EvalNode -> TableScanNode pattern, all aggs must be SUM/COUNT/AVG (non-distinct)
- `executeWithEval`: builds mapping of eval column names to (physicalColumn, offset), collects unique physical columns
- **Already parallelized** for MatchAll+no-deletes path — same segment-partitioning strategy as tryFlatArrayPath
- For MatchAll+deletes: row-major with `advanceExact()` (sequential, not parallel)
- For filtered: uses Lucene Collector (sequential, not parallel)
- COL_PLUS_CONST pattern: `^\(?\s*(\w+)\s*\+\s*(\d+)\s*\)?$` — only handles `+` with positive integer constants

### Parallel Pool (FusedGroupByAggregate L115-L137)
- `THREADS_PER_SHARD = max(1, availableProcessors / dqe.numLocalShards)` where `dqe.numLocalShards` defaults to 4
- `PARALLEL_POOL` = shared `ForkJoinPool` with `availableProcessors` threads, `asyncMode=true` for work-stealing
- Exposed via `getParallelPool()` static method — used by both FusedScanAggregate and FusedGroupByAggregate
