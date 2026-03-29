# Q28 Parallelization Source Code Extraction

File: `dqe/src/main/java/org/opensearch/sql/dqe/shard/source/FusedGroupByAggregate.java` (13,892 lines)

## Table of Contents
1. [ForkJoinPool Definition (lines 118-128)](#1-forkjoinpool-definition)
2. [executeWithExpressionKey — public entry point (lines 393-527)](#2-executewithexpressionkey-entry-point)
3. [executeWithExpressionKeyImpl — single-threaded Collector path (lines 528-882)](#3-executewithexpressionkeyimpl-single-threaded)
4. [executeSingleVarcharGeneric — parallel segment scanning reference (lines 1948-2812)](#4-executesinglevarchargeneric-parallel-reference)
5. [executeInternal — dispatch method (lines 939-1136)](#5-executeinternal-dispatch)
6. [System.gc() and applyHavingFilter — NOT FOUND](#6-not-found)

---

## 1. ForkJoinPool Definition (lines 118-128)

```java
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
```

## 2. executeWithExpressionKey — Public Entry Point (lines 393-527)

```java
  public static List<Page> executeWithExpressionKey(
      AggregationNode aggNode, IndexShard shard, Query query, Map<String, Type> columnTypeMap)
      throws Exception {

    EvalNode evalNode = (EvalNode) aggNode.getChild();
    TableScanNode scanNode = (TableScanNode) evalNode.getChild();
    List<String> groupByKeys = aggNode.getGroupByKeys();
    List<String> aggFunctions = aggNode.getAggregateFunctions();
    String groupByKey = groupByKeys.get(0);

    // Find the group-by key expression index in EvalNode
    int keyExprIdx = evalNode.getOutputColumnNames().indexOf(groupByKey);
    String keyExprStr = evalNode.getExpressions().get(keyExprIdx);

    // Find the source VARCHAR column
    List<String> scanColumns = scanNode.getColumns();
    String sourceVarcharCol = null;
    for (String col : scanColumns) {
      Type colType = columnTypeMap.get(col);
      if (colType instanceof VarcharType && keyExprStr.contains(col)) {
        sourceVarcharCol = col;
        break;
      }
    }

    // Parse aggregate specs
    final int numAggs = aggFunctions.size();
    List<AggSpec> specs = new ArrayList<>();
    for (String func : aggFunctions) {
      Matcher m = AGG_FUNCTION.matcher(func);
      m.matches();
      String funcName = m.group(1).toUpperCase(Locale.ROOT);
      boolean isDistinct = m.group(2) != null;
      String arg = m.group(3).trim();
      Type argType = arg.equals("*") ? null : columnTypeMap.getOrDefault(arg, BigintType.BIGINT);
      specs.add(new AggSpec(funcName, isDistinct, arg, argType));
    }

    // Identify which aggregate args are EvalNode-computed (vs physical columns)
    // For computed args that depend on sourceVarcharCol, we can cache them per ordinal too
    boolean[] isComputedArg = new boolean[numAggs];
    String[] computedArgExpr = new String[numAggs];
    boolean[] isCountStar = new boolean[numAggs];
    boolean[] isDoubleArg = new boolean[numAggs];
    boolean[] isVarcharArg = new boolean[numAggs];
    int[] accType = new int[numAggs];

    for (int i = 0; i < numAggs; i++) {
      AggSpec spec = specs.get(i);
      isCountStar[i] = "*".equals(spec.arg);

      if (!isCountStar[i]) {
        // Check if the arg is a computed EvalNode column
        int evalIdx = evalNode.getOutputColumnNames().indexOf(spec.arg);
        if (evalIdx >= 0 && !columnTypeMap.containsKey(spec.arg)) {
          isComputedArg[i] = true;
          computedArgExpr[i] = evalNode.getExpressions().get(evalIdx);
          // Computed args like length(Referer) produce BigintType
          isDoubleArg[i] = false;
          isVarcharArg[i] = false;
        } else {
          isDoubleArg[i] = spec.argType instanceof DoubleType;
          isVarcharArg[i] = spec.argType instanceof VarcharType;
        }
      }

      if (isCountStar[i]) {
        accType[i] = 0;
      } else {
        switch (spec.funcName) {
          case "COUNT":
            accType[i] = spec.isDistinct ? 5 : 0;
            break;
          case "SUM":
            accType[i] = isDoubleArg[i] ? 6 : 1;
            break;
          case "AVG":
            accType[i] = isDoubleArg[i] ? 7 : 2;
            break;
          case "MIN":
            accType[i] = 3;
            break;
          case "MAX":
            accType[i] = 4;
            break;
        }
      }
    }

    // Build expression evaluators for ordinal caching
    // We parse and compile expressions using the function registry
    org.opensearch.sql.dqe.function.FunctionRegistry registry =
        org.opensearch.sql.dqe.function.BuiltinFunctions.createRegistry();
    io.trino.sql.parser.SqlParser sqlParser = new io.trino.sql.parser.SqlParser();

    // For the key expression, we need a function: String -> String
    // Parse: "regexp_replace(Referer, '^pattern$', '$1')" with Referer at column index 0
    Map<String, Integer> colIndexMap = new HashMap<>();
    colIndexMap.put(sourceVarcharCol, 0);
    org.opensearch.sql.dqe.function.expression.ExpressionCompiler compiler =
        new org.opensearch.sql.dqe.function.expression.ExpressionCompiler(
            registry, colIndexMap, columnTypeMap);

    // Compile group-by key expression
    io.trino.sql.tree.Expression keyAst = sqlParser.createExpression(keyExprStr);
    org.opensearch.sql.dqe.function.expression.BlockExpression keyBlockExpr =
        compiler.compile(keyAst);

    // Compile computed aggregate arg expressions
    org.opensearch.sql.dqe.function.expression.BlockExpression[] computedArgBlockExprs =
        new org.opensearch.sql.dqe.function.expression.BlockExpression[numAggs];
    for (int i = 0; i < numAggs; i++) {
      if (isComputedArg[i]) {
        io.trino.sql.tree.Expression argAst = sqlParser.createExpression(computedArgExpr[i]);
        computedArgBlockExprs[i] = compiler.compile(argAst);
      }
    }

    return executeWithExpressionKeyImpl(
        shard,
        query,
        sourceVarcharCol,
        specs,
        numAggs,
        isCountStar,
        isComputedArg,
        isDoubleArg,
        isVarcharArg,
        accType,
        keyBlockExpr,
        computedArgBlockExprs,
        columnTypeMap);
  }

  /** Internal implementation of ordinal-cached expression GROUP BY using a global groups map. */
```

## 3. executeWithExpressionKeyImpl — Single-Threaded Collector Path (lines 528-882)

This is the method that needs to be refactored from `engineSearcher.search(query, Collector)` to parallel segment scanning.

```java
  private static List<Page> executeWithExpressionKeyImpl(
      IndexShard shard,
      Query query,
      String srcCol,
      List<AggSpec> specs,
      int numAggs,
      boolean[] isCountStar,
      boolean[] isComputedArg,
      boolean[] isDoubleArg,
      boolean[] isVarcharArg,
      int[] accType,
      org.opensearch.sql.dqe.function.expression.BlockExpression keyBlockExpr,
      org.opensearch.sql.dqe.function.expression.BlockExpression[] computedArgBlockExprs,
      Map<String, Type> columnTypeMap)
      throws Exception {

    try (org.opensearch.index.engine.Engine.Searcher engineSearcher =
        shard.acquireSearcher("dqe-fused-expr-groupby")) {

      // Global groups keyed by the computed expression result string.
      // Since Lucene processes segments sequentially in a single collector thread,
      // we can safely accumulate into this global map without synchronization.
      HashMap<String, AccumulatorGroup> globalGroups = new HashMap<>();

      engineSearcher.search(
          query,
          new Collector() {
            @Override
            public LeafCollector getLeafCollector(LeafReaderContext context) throws IOException {
              LeafReader reader = context.reader();
              SortedSetDocValues dv = reader.getSortedSetDocValues(srcCol);
              if (dv == null) {
                return new LeafCollector() {
                  @Override
                  public void setScorer(Scorable scorer) {}

                  @Override
                  public void collect(int doc) {}
                };
              }

              long ordCount = dv.getValueCount();
              if (ordCount <= 0 || ordCount > 10_000_000) {
                return new LeafCollector() {
                  @Override
                  public void setScorer(Scorable scorer) {}

                  @Override
                  public void collect(int doc) {}
                };
              }

              int ordCountInt = (int) ordCount;

              // Pre-compute expressions for each ordinal in this segment
              String[] ordToGroupKey = new String[ordCountInt];
              long[][] ordToComputedArg = new long[numAggs][];
              for (int i = 0; i < numAggs; i++) {
                if (isComputedArg[i]) {
                  ordToComputedArg[i] = new long[ordCountInt];
                }
              }

              // Check if any VARCHAR agg arg references the same source column
              // If so, cache the raw string per ordinal to avoid per-doc lookupOrd
              boolean[] isVarcharSameCol = new boolean[numAggs];
              for (int i = 0; i < numAggs; i++) {
                if (!isCountStar[i]
                    && !isComputedArg[i]
                    && isVarcharArg[i]
                    && specs.get(i).arg.equals(srcCol)) {
                  isVarcharSameCol[i] = true;
                }
              }
              boolean needRawStringCache = false;
              for (int i = 0; i < numAggs; i++) {
                if (isVarcharSameCol[i]) {
                  needRawStringCache = true;
                  break;
                }
              }
              String[] ordToRawString = needRawStringCache ? new String[ordCountInt] : null;

              // Process ordinals in batches for expression evaluation
              int batchSize = Math.min(ordCountInt, 4096);
              for (int batchStart = 0; batchStart < ordCountInt; batchStart += batchSize) {
                int batchEnd = Math.min(batchStart + batchSize, ordCountInt);
                int batchLen = batchEnd - batchStart;

                BlockBuilder inputBuilder = VarcharType.VARCHAR.createBlockBuilder(null, batchLen);
                for (int ord = batchStart; ord < batchEnd; ord++) {
                  BytesRef bytes = dv.lookupOrd(ord);
                  VarcharType.VARCHAR.writeSlice(
                      inputBuilder, Slices.wrappedBuffer(bytes.bytes, bytes.offset, bytes.length));
                  // Cache raw string if needed for VARCHAR agg args
                  if (ordToRawString != null) {
                    ordToRawString[ord] = bytes.utf8ToString();
                  }
                }
                Block inputBlock = inputBuilder.build();
                Page inputPage = new Page(inputBlock);

                Block keyResultBlock = keyBlockExpr.evaluate(inputPage);
                for (int j = 0; j < batchLen; j++) {
                  if (keyResultBlock.isNull(j)) {
                    ordToGroupKey[batchStart + j] = null;
                  } else {
                    ordToGroupKey[batchStart + j] =
                        VarcharType.VARCHAR.getSlice(keyResultBlock, j).toStringUtf8();
                  }
                }

                for (int i = 0; i < numAggs; i++) {
                  if (isComputedArg[i]) {
                    Block argResultBlock = computedArgBlockExprs[i].evaluate(inputPage);
                    for (int j = 0; j < batchLen; j++) {
                      if (!argResultBlock.isNull(j)) {
                        ordToComputedArg[i][batchStart + j] =
                            computedArgBlockExprs[i].getType().getLong(argResultBlock, j);
                      }
                    }
                  }
                }
              }

              // Pre-wire ordinal -> AccumulatorGroup using global groups map
              // Multiple ordinals may map to the same expression result string
              AccumulatorGroup[] ordGroups = new AccumulatorGroup[ordCountInt];
              for (int ord = 0; ord < ordCountInt; ord++) {
                String gk = ordToGroupKey[ord];
                if (gk != null) {
                  AccumulatorGroup existing = globalGroups.get(gk);
                  if (existing == null) {
                    existing = createAccumulatorGroup(specs);
                    globalGroups.put(gk, existing);
                  }
                  ordGroups[ord] = existing;
                }
              }

              // Open doc values for physical aggregate args (skip same-col varchar args)
              SortedNumericDocValues[] colNumDvs = new SortedNumericDocValues[numAggs];
              SortedSetDocValues[] colVarDvs = new SortedSetDocValues[numAggs];
              for (int i = 0; i < numAggs; i++) {
                if (!isCountStar[i] && !isComputedArg[i] && !isVarcharSameCol[i]) {
                  AggSpec spec = specs.get(i);
                  if (isVarcharArg[i]) {
                    colVarDvs[i] = reader.getSortedSetDocValues(spec.arg);
                  } else {
                    colNumDvs[i] = reader.getSortedNumericDocValues(spec.arg);
                  }
                }
              }

              final long[][] finalOrdToComputedArg = ordToComputedArg;
              final String[] finalOrdToRawString = ordToRawString;
              final boolean[] finalIsVarcharSameCol = isVarcharSameCol;

              return new LeafCollector() {
                @Override
                public void setScorer(Scorable scorer) {}

                @Override
                public void collect(int doc) throws IOException {
                  if (!dv.advanceExact(doc)) return;
                  int ord = (int) dv.nextOrd();
                  if (ord >= ordGroups.length) return;
                  AccumulatorGroup accGroup = ordGroups[ord];
                  if (accGroup == null) return;

                  for (int i = 0; i < numAggs; i++) {
                    MergeableAccumulator acc = accGroup.accumulators[i];
                    if (isCountStar[i]) {
                      ((CountStarAccum) acc).count++;
                      continue;
                    }
                    // Fast path: VARCHAR agg arg referencing the source column
                    // Use pre-cached string from ordinal (avoids advanceExact + lookupOrd)
                    if (finalIsVarcharSameCol[i]) {
                      String val = finalOrdToRawString[ord];
                      switch (accType[i]) {
                        case 5:
                          ((CountDistinctAccum) acc).objectDistinctValues.add(val);
                          break;
                        case 3:
                          MinAccum ma = (MinAccum) acc;
                          if (!ma.hasValue || val.compareTo((String) ma.objectVal) < 0) {
                            ma.objectVal = val;
                            ma.hasValue = true;
                          }
                          break;
                        case 4:
                          MaxAccum xa = (MaxAccum) acc;
                          if (!xa.hasValue || val.compareTo((String) xa.objectVal) > 0) {
                            xa.objectVal = val;
                            xa.hasValue = true;
                          }
                          break;
                      }
                      continue;
                    }
                    if (isComputedArg[i]) {
                      long val = finalOrdToComputedArg[i][ord];
                      switch (accType[i]) {
                        case 0:
                          ((CountStarAccum) acc).count++;
                          break;
                        case 1:
                          SumAccum sa = (SumAccum) acc;
                          sa.hasValue = true;
                          sa.longSum += val;
                          break;
                        case 2:
                          AvgAccum aa = (AvgAccum) acc;
                          aa.count++;
                          aa.longSum += val;
                          break;
                        case 3:
                          MinAccum mna = (MinAccum) acc;
                          mna.hasValue = true;
                          if (val < mna.longVal) mna.longVal = val;
                          break;
                        case 4:
                          MaxAccum mxa = (MaxAccum) acc;
                          mxa.hasValue = true;
                          if (val > mxa.longVal) mxa.longVal = val;
                          break;
                        case 5:
                          CountDistinctAccum cda = (CountDistinctAccum) acc;
                          if (cda.usePrimitiveLong) cda.longDistinctValues.add(val);
                          break;
                      }
                      continue;
                    }
                    if (isVarcharArg[i]) {
                      SortedSetDocValues varcharDv = colVarDvs[i];
                      if (varcharDv != null && varcharDv.advanceExact(doc)) {
                        BytesRef bytes = varcharDv.lookupOrd(varcharDv.nextOrd());
                        String val = bytes.utf8ToString();
                        switch (accType[i]) {
                          case 5:
                            ((CountDistinctAccum) acc).objectDistinctValues.add(val);
                            break;
                          case 3:
                            MinAccum ma = (MinAccum) acc;
                            if (!ma.hasValue || val.compareTo((String) ma.objectVal) < 0) {
                              ma.objectVal = val;
                              ma.hasValue = true;
                            }
                            break;
                          case 4:
                            MaxAccum xa = (MaxAccum) acc;
                            if (!xa.hasValue || val.compareTo((String) xa.objectVal) > 0) {
                              xa.objectVal = val;
                              xa.hasValue = true;
                            }
                            break;
                        }
                      }
                      continue;
                    }
                    SortedNumericDocValues aggDv = colNumDvs[i];
                    if (aggDv != null && aggDv.advanceExact(doc)) {
                      long rawVal = aggDv.nextValue();
                      switch (accType[i]) {
                        case 0:
                          ((CountStarAccum) acc).count++;
                          break;
                        case 1:
                          SumAccum sa = (SumAccum) acc;
                          sa.hasValue = true;
                          sa.longSum += rawVal;
                          break;
                        case 2:
                          AvgAccum aa = (AvgAccum) acc;
                          aa.count++;
                          aa.longSum += rawVal;
                          break;
                        case 3:
                          MinAccum mna = (MinAccum) acc;
                          mna.hasValue = true;
                          if (isDoubleArg[i]) {
                            double d = Double.longBitsToDouble(rawVal);
                            if (d < mna.doubleVal) mna.doubleVal = d;
                          } else {
                            if (rawVal < mna.longVal) mna.longVal = rawVal;
                          }
                          break;
                        case 4:
                          MaxAccum mxa = (MaxAccum) acc;
                          mxa.hasValue = true;
                          if (isDoubleArg[i]) {
                            double d = Double.longBitsToDouble(rawVal);
                            if (d > mxa.doubleVal) mxa.doubleVal = d;
                          } else {
                            if (rawVal > mxa.longVal) mxa.longVal = rawVal;
                          }
                          break;
                        case 5:
                          CountDistinctAccum cda = (CountDistinctAccum) acc;
                          if (cda.usePrimitiveLong) cda.longDistinctValues.add(rawVal);
                          else cda.objectDistinctValues.add(Double.longBitsToDouble(rawVal));
                          break;
                        case 6:
                          SumAccum sad = (SumAccum) acc;
                          sad.hasValue = true;
                          sad.doubleSum += Double.longBitsToDouble(rawVal);
                          break;
                        case 7:
                          AvgAccum aad = (AvgAccum) acc;
                          aad.count++;
                          aad.doubleSum += Double.longBitsToDouble(rawVal);
                          break;
                      }
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

      // Build output Page
      if (globalGroups.isEmpty()) {
        return List.of();
      }

      int groupCount = globalGroups.size();
      int totalColumns = 1 + numAggs;
      BlockBuilder[] builders = new BlockBuilder[totalColumns];
      builders[0] = VarcharType.VARCHAR.createBlockBuilder(null, groupCount);
      for (int i = 0; i < numAggs; i++) {
        builders[1 + i] =
            resolveAggOutputType(specs.get(i), columnTypeMap).createBlockBuilder(null, groupCount);
      }

      for (Map.Entry<String, AccumulatorGroup> entry : globalGroups.entrySet()) {
        VarcharType.VARCHAR.writeSlice(builders[0], Slices.utf8Slice(entry.getKey()));
        AccumulatorGroup accGroup = entry.getValue();
        for (int a = 0; a < numAggs; a++) {
          accGroup.accumulators[a].writeTo(builders[1 + a]);
        }
      }

      Block[] blocks = new Block[totalColumns];
      for (int i = 0; i < totalColumns; i++) blocks[i] = builders[i].build();
      return List.of(new Page(blocks));
    }
  }

  /** Check if a type is a numeric or timestamp type suitable for group-by keys. */
```

## 4. executeSingleVarcharGeneric — Parallel Segment Scanning Reference (lines 1948-2812)

This method demonstrates the 3-tier pattern to replicate:
1. **Single-segment fast path** (lines 1999-2503): ordinal-indexed array, no HashMap
2. **Global ordinals multi-segment path** (lines 2504-2527): OrdinalMap for cross-segment merge
3. **Parallel multi-segment path** (lines 2528-2700): ForkJoinPool + CompletableFuture
4. **Fallback multi-segment Collector path** (lines 2700-2812): sequential Collector

```java
  private static List<Page> executeSingleVarcharGeneric(
      IndexShard shard,
      Query query,
      List<KeyInfo> keyInfos,
      List<AggSpec> specs,
      Map<String, Type> columnTypeMap,
      List<String> groupByKeys,
      int sortAggIndex,
      boolean sortAscending,
      long topN)
      throws Exception {

    final String columnName = keyInfos.get(0).name();

    // Pre-compute aggregate dispatch types
    final int numAggs = specs.size();
    final boolean[] isCountStar = new boolean[numAggs];
    final boolean[] isDoubleArg = new boolean[numAggs];
    final boolean[] isVarcharArg = new boolean[numAggs];
    final int[] accType = new int[numAggs];
    for (int i = 0; i < numAggs; i++) {
      AggSpec spec = specs.get(i);
      isCountStar[i] = "*".equals(spec.arg);
      isDoubleArg[i] = spec.argType instanceof DoubleType;
      isVarcharArg[i] = spec.argType instanceof VarcharType;
      if (isCountStar[i]) {
        accType[i] = 0;
      } else {
        switch (spec.funcName) {
          case "COUNT":
            accType[i] = spec.isDistinct ? 5 : 0;
            break;
          case "SUM":
            accType[i] = isDoubleArg[i] ? 6 : 1;
            break;
          case "AVG":
            accType[i] = isDoubleArg[i] ? 7 : 2;
            break;
          case "MIN":
            accType[i] = 3;
            break;
          case "MAX":
            accType[i] = 4;
            break;
        }
      }
    }

    try (org.opensearch.index.engine.Engine.Searcher engineSearcher =
        shard.acquireSearcher("dqe-fused-groupby-varchar-generic")) {

      List<LeafReaderContext> leaves = engineSearcher.getIndexReader().leaves();

      if (leaves.size() == 1) {
        // === Single-segment fast path ===
        // Use ordinal-indexed AccumulatorGroup array — no HashMap, no hash computation.
        LeafReaderContext leafCtx = leaves.get(0);
        LeafReader reader = leafCtx.reader();
        SortedSetDocValues dv = reader.getSortedSetDocValues(columnName);
        long ordCountLong = (dv != null) ? dv.getValueCount() : 0;

        if (ordCountLong > 0 && ordCountLong <= 10_000_000) {
          AccumulatorGroup[] ordGroups = new AccumulatorGroup[(int) ordCountLong];

          // Open agg doc values with typed arrays
          final SortedNumericDocValues[] numericAggDvs = new SortedNumericDocValues[numAggs];
          final SortedSetDocValues[] varcharAggDvs = new SortedSetDocValues[numAggs];
          for (int i = 0; i < numAggs; i++) {
            if (!isCountStar[i]) {
              AggSpec spec = specs.get(i);
              if (isVarcharArg[i]) {
                varcharAggDvs[i] = reader.getSortedSetDocValues(spec.arg);
              } else {
                numericAggDvs[i] = reader.getSortedNumericDocValues(spec.arg);
              }
            }
          }

          // === MatchAllDocsQuery fast path: iterate docs directly ===
          if (query instanceof MatchAllDocsQuery) {
            Bits liveDocs = reader.getLiveDocs();
            if (liveDocs == null && dv != null) {
              // Ultra-fast path: use forward-only nextDoc() iteration.
              // For single-valued fields, unwrap to SortedDocValues and use
              // ordValue() for less overhead than nextOrd().
              SortedDocValues sdv = DocValues.unwrapSingleton(dv);
              if (sdv != null) {
                int doc;
                while ((doc = sdv.nextDoc()) != DocIdSetIterator.NO_MORE_DOCS) {
                  int ord = sdv.ordValue();
                  AccumulatorGroup accGroup = ordGroups[ord];
                  if (accGroup == null) {
                    accGroup = createAccumulatorGroup(specs);
                    ordGroups[ord] = accGroup;
                  }
                  collectVarcharGenericAccumulate(
                      doc,
                      accGroup,
                      numAggs,
                      isCountStar,
                      isVarcharArg,
                      isDoubleArg,
                      accType,
                      numericAggDvs,
                      varcharAggDvs);
                }
              } else {
                int doc;
                while ((doc = dv.nextDoc()) != DocIdSetIterator.NO_MORE_DOCS) {
                  int ord = (int) dv.nextOrd();
                  AccumulatorGroup accGroup = ordGroups[ord];
                  if (accGroup == null) {
                    accGroup = createAccumulatorGroup(specs);
                    ordGroups[ord] = accGroup;
                  }
                  collectVarcharGenericAccumulate(
                      doc,
                      accGroup,
                      numAggs,
                      isCountStar,
                      isVarcharArg,
                      isDoubleArg,
                      accType,
                      numericAggDvs,
                      varcharAggDvs);
                }
              }
            } else {
              int maxDoc = reader.maxDoc();
              for (int doc = 0; doc < maxDoc; doc++) {
                if (liveDocs != null && !liveDocs.get(doc)) continue;
                if (dv == null || !dv.advanceExact(doc)) continue;
                int ord = (int) dv.nextOrd();
                AccumulatorGroup accGroup = ordGroups[ord];
                if (accGroup == null) {
                  accGroup = createAccumulatorGroup(specs);
                  ordGroups[ord] = accGroup;
                }
                collectVarcharGenericAccumulate(
                    doc,
                    accGroup,
                    numAggs,
                    isCountStar,
                    isVarcharArg,
                    isDoubleArg,
                    accType,
                    numericAggDvs,
                    varcharAggDvs);
              }
            }
          } else {
            // General path: use Lucene's search framework with Collector
            engineSearcher.search(
                query,
                new Collector() {
                  @Override
                  public LeafCollector getLeafCollector(LeafReaderContext context)
                      throws IOException {
                    return new LeafCollector() {
                      @Override
                      public void setScorer(Scorable scorer) {}

                      @Override
                      public void collect(int doc) throws IOException {
                        if (dv == null || !dv.advanceExact(doc)) return;
                        int ord = (int) dv.nextOrd();
                        AccumulatorGroup accGroup = ordGroups[ord];
                        if (accGroup == null) {
                          accGroup = createAccumulatorGroup(specs);
                          ordGroups[ord] = accGroup;
                        }
                        collectVarcharGenericAccumulate(
                            doc,
                            accGroup,
                            numAggs,
                            isCountStar,
                            isVarcharArg,
                            isDoubleArg,
                            accType,
                            numericAggDvs,
                            varcharAggDvs);
                      }
                    };
                  }

                  @Override
                  public ScoreMode scoreMode() {
                    return ScoreMode.COMPLETE_NO_SCORES;
                  }
                });
          }

          // Count non-null entries
          int groupCount = 0;
          for (int i = 0; i < ordGroups.length; i++) {
            if (ordGroups[i] != null) groupCount++;
          }
          if (groupCount == 0) return List.of();

          int numGroupKeys = groupByKeys.size();
          int totalColumns = numGroupKeys + numAggs;

          // === Top-N selection: build only top-N entries ===
          if (sortAggIndex >= 0 && topN > 0 && topN < groupCount) {
            int n = (int) Math.min(topN, groupCount);
            int[] heap = new int[n];
            long[] heapVals = new long[n];
            int heapSize = 0;

            for (int i = 0; i < ordGroups.length; i++) {
              if (ordGroups[i] == null) continue;
              long val = ordGroups[i].accumulators[sortAggIndex].getSortValue();
              if (heapSize < n) {
                heap[heapSize] = i;
                heapVals[heapSize] = val;
                heapSize++;
                int k = heapSize - 1;
                while (k > 0) {
                  int parent = (k - 1) >>> 1;
                  boolean swap =
                      sortAscending
                          ? (heapVals[k] > heapVals[parent])
                          : (heapVals[k] < heapVals[parent]);
                  if (swap) {
                    int tmpI = heap[parent];
                    heap[parent] = heap[k];
                    heap[k] = tmpI;
                    long tmpV = heapVals[parent];
                    heapVals[parent] = heapVals[k];
                    heapVals[k] = tmpV;
                    k = parent;
                  } else break;
                }
              } else {
                boolean better = sortAscending ? (val < heapVals[0]) : (val > heapVals[0]);
                if (better) {
                  heap[0] = i;
                  heapVals[0] = val;
                  int k = 0;
                  while (true) {
                    int left = 2 * k + 1;
                    if (left >= heapSize) break;
                    int right = left + 1;
                    int target = left;
                    if (right < heapSize) {
                      boolean pickRight =
                          sortAscending
                              ? (heapVals[right] > heapVals[left])
                              : (heapVals[right] < heapVals[left]);
                      if (pickRight) target = right;
                    }
                    boolean swap =
                        sortAscending
                            ? (heapVals[target] > heapVals[k])
                            : (heapVals[target] < heapVals[k]);
                    if (swap) {
                      int tmpI = heap[k];
                      heap[k] = heap[target];
                      heap[target] = tmpI;
                      long tmpV = heapVals[k];
                      heapVals[k] = heapVals[target];
                      heapVals[target] = tmpV;
                      k = target;
                    } else break;
                  }
                }
              }
            }

            BlockBuilder[] builders = new BlockBuilder[totalColumns];
            builders[0] = VarcharType.VARCHAR.createBlockBuilder(null, heapSize);
            for (int i = 0; i < numAggs; i++) {
              builders[numGroupKeys + i] =
                  resolveAggOutputType(specs.get(i), columnTypeMap)
                      .createBlockBuilder(null, heapSize);
            }
            for (int h = 0; h < heapSize; h++) {
              int ord = heap[h];
              BytesRef bytes = dv.lookupOrd(ord);
              VarcharType.VARCHAR.writeSlice(
                  builders[0], Slices.wrappedBuffer(bytes.bytes, bytes.offset, bytes.length));
              AccumulatorGroup accGroup = ordGroups[ord];
              for (int a = 0; a < numAggs; a++) {
                accGroup.accumulators[a].writeTo(builders[numGroupKeys + a]);
              }
            }
            Block[] blocks = new Block[totalColumns];
            for (int i = 0; i < totalColumns; i++) blocks[i] = builders[i].build();
            return List.of(new Page(blocks));
          }

          // No top-N: output all groups
          BlockBuilder[] builders = new BlockBuilder[totalColumns];
          builders[0] = VarcharType.VARCHAR.createBlockBuilder(null, groupCount);
          for (int i = 0; i < numAggs; i++) {
            builders[numGroupKeys + i] =
                resolveAggOutputType(specs.get(i), columnTypeMap)
                    .createBlockBuilder(null, groupCount);
          }

          for (int i = 0; i < ordGroups.length; i++) {
            if (ordGroups[i] != null) {
              BytesRef bytes = dv.lookupOrd(i);
              VarcharType.VARCHAR.writeSlice(
                  builders[0], Slices.wrappedBuffer(bytes.bytes, bytes.offset, bytes.length));
              AccumulatorGroup accGroup = ordGroups[i];
              for (int a = 0; a < numAggs; a++) {
                accGroup.accumulators[a].writeTo(builders[numGroupKeys + a]);
              }
            }
          }

          Block[] blocks = new Block[totalColumns];
          for (int i = 0; i < totalColumns; i++) blocks[i] = builders[i].build();
          return List.of(new Page(blocks));
        }
        // Fall through to multi-segment path
      }

      // === Global ordinals multi-segment path ===
      // Use global ordinals for MatchAll or low-cardinality VARCHAR fields
      {
        OrdinalMap ordinalMap = null;
        if (query instanceof MatchAllDocsQuery) {
          ordinalMap = buildGlobalOrdinalMap(leaves, columnName);
        } else {
          SortedSetDocValues checkDv = leaves.get(0).reader().getSortedSetDocValues(columnName);
          if (checkDv != null && checkDv.getValueCount() <= 1_000_000) {
            ordinalMap = buildGlobalOrdinalMap(leaves, columnName);
          }
        }
        if (ordinalMap != null) {
          long globalOrdCount = ordinalMap.getValueCount();
          if (globalOrdCount > 0 && globalOrdCount <= 10_000_000) {
            AccumulatorGroup[] globalGroups = new AccumulatorGroup[(int) globalOrdCount];

            // Build Weight for filtered queries
            IndexSearcher luceneSearcher = new IndexSearcher(engineSearcher.getIndexReader());
            Weight weight =
                (query instanceof MatchAllDocsQuery)
                    ? null
                    : luceneSearcher.createWeight(
                        luceneSearcher.rewrite(query), ScoreMode.COMPLETE_NO_SCORES, 1.0f);

            for (int segIdx = 0; segIdx < leaves.size(); segIdx++) {
              LeafReaderContext leafCtx = leaves.get(segIdx);

              // For filtered queries, check scorer BEFORE opening DocValues
              if (weight != null) {
                Scorer scorer = weight.scorer(leafCtx);
                if (scorer == null) continue; // No matching docs — skip segment entirely

                SortedSetDocValues dv = leafCtx.reader().getSortedSetDocValues(columnName);
                if (dv == null) continue;
                LongValues segToGlobal = ordinalMap.getGlobalOrds(segIdx);

                SortedNumericDocValues[] numericAggDvs = new SortedNumericDocValues[numAggs];
                SortedSetDocValues[] varcharAggDvs = new SortedSetDocValues[numAggs];
                for (int i = 0; i < numAggs; i++) {
                  if (!isCountStar[i]) {
                    AggSpec spec = specs.get(i);
                    if (isVarcharArg[i]) {
                      varcharAggDvs[i] = leafCtx.reader().getSortedSetDocValues(spec.arg);
                    } else {
                      numericAggDvs[i] = leafCtx.reader().getSortedNumericDocValues(spec.arg);
                    }
                  }
                }

                DocIdSetIterator docIt = scorer.iterator();
                int doc;
                while ((doc = docIt.nextDoc()) != DocIdSetIterator.NO_MORE_DOCS) {
                  if (!dv.advanceExact(doc)) continue;
                  int globalOrd = (int) segToGlobal.get(dv.nextOrd());
                  AccumulatorGroup accGroup = globalGroups[globalOrd];
                  if (accGroup == null) {
                    accGroup = createAccumulatorGroup(specs);
                    globalGroups[globalOrd] = accGroup;
                  }
                  collectVarcharGenericAccumulate(
                      doc,
                      accGroup,
                      numAggs,
                      isCountStar,
                      isVarcharArg,
                      isDoubleArg,
                      accType,
                      numericAggDvs,
                      varcharAggDvs);
                }
              } else {
                // MatchAll: open DocValues and iterate all docs
                SortedSetDocValues dv = leafCtx.reader().getSortedSetDocValues(columnName);
                if (dv == null) continue;
                LongValues segToGlobal = ordinalMap.getGlobalOrds(segIdx);

                SortedNumericDocValues[] numericAggDvs = new SortedNumericDocValues[numAggs];
                SortedSetDocValues[] varcharAggDvs = new SortedSetDocValues[numAggs];
                for (int i = 0; i < numAggs; i++) {
                  if (!isCountStar[i]) {
                    AggSpec spec = specs.get(i);
                    if (isVarcharArg[i]) {
                      varcharAggDvs[i] = leafCtx.reader().getSortedSetDocValues(spec.arg);
                    } else {
                      numericAggDvs[i] = leafCtx.reader().getSortedNumericDocValues(spec.arg);
                    }
                  }
                }

                SortedDocValues sdv = DocValues.unwrapSingleton(dv);
                if (sdv != null) {
                  int doc;
                  while ((doc = sdv.nextDoc()) != DocIdSetIterator.NO_MORE_DOCS) {
                    int globalOrd = (int) segToGlobal.get(sdv.ordValue());
                    AccumulatorGroup accGroup = globalGroups[globalOrd];
                    if (accGroup == null) {
                      accGroup = createAccumulatorGroup(specs);
                      globalGroups[globalOrd] = accGroup;
                    }
                    collectVarcharGenericAccumulate(
                        doc,
                        accGroup,
                        numAggs,
                        isCountStar,
                        isVarcharArg,
                        isDoubleArg,
                        accType,
                        numericAggDvs,
                        varcharAggDvs);
                  }
                } else {
                  int doc;
                  while ((doc = dv.nextDoc()) != DocIdSetIterator.NO_MORE_DOCS) {
                    int globalOrd = (int) segToGlobal.get(dv.nextOrd());
                    AccumulatorGroup accGroup = globalGroups[globalOrd];
                    if (accGroup == null) {
                      accGroup = createAccumulatorGroup(specs);
                      globalGroups[globalOrd] = accGroup;
                    }
                    collectVarcharGenericAccumulate(
                        doc,
                        accGroup,
                        numAggs,
                        isCountStar,
                        isVarcharArg,
                        isDoubleArg,
                        accType,
                        numericAggDvs,
                        varcharAggDvs);
                  }
                }
              }
            }

            // Count non-null groups
            int groupCount = 0;
            for (AccumulatorGroup g : globalGroups) {
              if (g != null) groupCount++;
            }
            if (groupCount == 0) return List.of();

            // Prepare segment DVs for ordinal lookup
            SortedSetDocValues[] segDvs = new SortedSetDocValues[leaves.size()];
            for (int i = 0; i < leaves.size(); i++) {
              segDvs[i] = leaves.get(i).reader().getSortedSetDocValues(columnName);
            }

            int numGroupKeys = groupByKeys.size();
            int totalColumns = numGroupKeys + numAggs;

            // Top-N selection
            if (sortAggIndex >= 0 && topN > 0 && topN < groupCount) {
              int n = (int) Math.min(topN, groupCount);
              int[] heap = new int[n];
              long[] heapVals = new long[n];
              int heapSize = 0;

              for (int i = 0; i < globalGroups.length; i++) {
                if (globalGroups[i] == null) continue;
                long val = globalGroups[i].accumulators[sortAggIndex].getSortValue();
                if (heapSize < n) {
                  heap[heapSize] = i;
                  heapVals[heapSize] = val;
                  heapSize++;
                  int k = heapSize - 1;
                  while (k > 0) {
                    int parent = (k - 1) >>> 1;
                    boolean swap =
                        sortAscending
                            ? (heapVals[k] > heapVals[parent])
                            : (heapVals[k] < heapVals[parent]);
                    if (swap) {
                      int tmpI = heap[parent];
                      heap[parent] = heap[k];
                      heap[k] = tmpI;
                      long tmpV = heapVals[parent];
                      heapVals[parent] = heapVals[k];
                      heapVals[k] = tmpV;
                      k = parent;
                    } else break;
                  }
                } else {
                  boolean better = sortAscending ? (val < heapVals[0]) : (val > heapVals[0]);
                  if (better) {
                    heap[0] = i;
                    heapVals[0] = val;
                    int k = 0;
                    while (true) {
                      int left = 2 * k + 1;
                      if (left >= heapSize) break;
                      int right = left + 1;
                      int target = left;
                      if (right < heapSize) {
                        boolean pickRight =
                            sortAscending
                                ? (heapVals[right] > heapVals[left])
                                : (heapVals[right] < heapVals[left]);
                        if (pickRight) target = right;
                      }
                      boolean swap =
                          sortAscending
                              ? (heapVals[target] > heapVals[k])
                              : (heapVals[target] < heapVals[k]);
                      if (swap) {
                        int tmpI = heap[k];
                        heap[k] = heap[target];
                        heap[target] = tmpI;
                        long tmpV = heapVals[k];
                        heapVals[k] = heapVals[target];
                        heapVals[target] = tmpV;
                        k = target;
                      } else break;
                    }
                  }
                }
              }

              BlockBuilder[] builders = new BlockBuilder[totalColumns];
              builders[0] = VarcharType.VARCHAR.createBlockBuilder(null, heapSize);
              for (int i = 0; i < numAggs; i++) {
                builders[numGroupKeys + i] =
                    resolveAggOutputType(specs.get(i), columnTypeMap)
                        .createBlockBuilder(null, heapSize);
              }
              for (int i = 0; i < heapSize; i++) {
                BytesRef bytes = lookupGlobalOrd(ordinalMap, segDvs, heap[i]);
                VarcharType.VARCHAR.writeSlice(
                    builders[0], Slices.wrappedBuffer(bytes.bytes, bytes.offset, bytes.length));
                for (int a = 0; a < numAggs; a++) {
                  globalGroups[heap[i]].accumulators[a].writeTo(builders[numGroupKeys + a]);
                }
              }
              Block[] blocks = new Block[totalColumns];
              for (int j = 0; j < totalColumns; j++) blocks[j] = builders[j].build();
              return List.of(new Page(blocks));
            }

            // No top-N: output all groups
            BlockBuilder[] builders = new BlockBuilder[totalColumns];
            builders[0] = VarcharType.VARCHAR.createBlockBuilder(null, groupCount);
            for (int i = 0; i < numAggs; i++) {
              builders[numGroupKeys + i] =
                  resolveAggOutputType(specs.get(i), columnTypeMap)
                      .createBlockBuilder(null, groupCount);
            }
            for (int i = 0; i < globalGroups.length; i++) {
              if (globalGroups[i] == null) continue;
              BytesRef bytes = lookupGlobalOrd(ordinalMap, segDvs, i);
              VarcharType.VARCHAR.writeSlice(
                  builders[0], Slices.wrappedBuffer(bytes.bytes, bytes.offset, bytes.length));
              for (int a = 0; a < numAggs; a++) {
                globalGroups[i].accumulators[a].writeTo(builders[numGroupKeys + a]);
              }
            }
            Block[] blocks = new Block[totalColumns];
            for (int j = 0; j < totalColumns; j++) blocks[j] = builders[j].build();
            return List.of(new Page(blocks));
          }
        }
      }

      // === Parallel multi-segment path ===
      // Only parallelize when ordinal counts are manageable (< 500K per segment).
      boolean canParallelizeGeneric =
          !"off".equals(PARALLELISM_MODE) && THREADS_PER_SHARD > 1 && leaves.size() > 1;
      if (canParallelizeGeneric) {
        SortedSetDocValues checkDv = leaves.get(0).reader().getSortedSetDocValues(columnName);
        if (checkDv != null && checkDv.getValueCount() > 500_000) {
          canParallelizeGeneric = false;
        }
      }
      if (canParallelizeGeneric) {
        int numWorkers = Math.min(THREADS_PER_SHARD, leaves.size());
        @SuppressWarnings("unchecked")
        List<LeafReaderContext>[] workerSegments = new List[numWorkers];
        long[] workerDocCounts = new long[numWorkers];
        for (int i = 0; i < numWorkers; i++) workerSegments[i] = new ArrayList<>();
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

        IndexSearcher luceneSearcher = new IndexSearcher(engineSearcher.getIndexReader());
        Weight weight =
            luceneSearcher.createWeight(
                luceneSearcher.rewrite(query), ScoreMode.COMPLETE_NO_SCORES, 1.0f);

        @SuppressWarnings("unchecked")
        java.util.concurrent.CompletableFuture<HashMap<BytesRefKey, AccumulatorGroup>>[] futures =
            new java.util.concurrent.CompletableFuture[numWorkers];

        for (int w = 0; w < numWorkers; w++) {
          final List<LeafReaderContext> mySegments = workerSegments[w];
          futures[w] =
              java.util.concurrent.CompletableFuture.supplyAsync(
                  () -> {
                    HashMap<BytesRefKey, AccumulatorGroup> workerGroups = new HashMap<>();
                    try {
                      for (LeafReaderContext leafCtx : mySegments) {
                        Scorer scorer = weight.scorer(leafCtx);
                        if (scorer == null) continue;
                        SortedSetDocValues dv = leafCtx.reader().getSortedSetDocValues(columnName);
                        if (dv == null) continue;
                        long ordCount = dv.getValueCount();
                        AccumulatorGroup[] ordGroups =
                            (ordCount > 0 && ordCount <= 1_000_000)
                                ? new AccumulatorGroup[(int) ordCount]
                                : null;
                        if (ordGroups == null) continue;

                        // Open agg doc values
                        SortedNumericDocValues[] numericAggDvs =
                            new SortedNumericDocValues[numAggs];
                        SortedSetDocValues[] varcharAggDvs = new SortedSetDocValues[numAggs];
                        for (int i = 0; i < numAggs; i++) {
                          if (!isCountStar[i]) {
                            AggSpec spec = specs.get(i);
                            if (isVarcharArg[i]) {
                              varcharAggDvs[i] = leafCtx.reader().getSortedSetDocValues(spec.arg);
                            } else {
                              numericAggDvs[i] =
                                  leafCtx.reader().getSortedNumericDocValues(spec.arg);
                            }
                          }
                        }

                        DocIdSetIterator docIt = scorer.iterator();
                        int doc;
                        while ((doc = docIt.nextDoc()) != DocIdSetIterator.NO_MORE_DOCS) {
                          if (!dv.advanceExact(doc)) continue;
                          int ord = (int) dv.nextOrd();
                          AccumulatorGroup accGroup = ordGroups[ord];
                          if (accGroup == null) {
                            accGroup = createAccumulatorGroup(specs);
                            ordGroups[ord] = accGroup;
                          }
                          collectVarcharGenericAccumulate(
                              doc,
                              accGroup,
                              numAggs,
                              isCountStar,
                              isVarcharArg,
                              isDoubleArg,
                              accType,
                              numericAggDvs,
                              varcharAggDvs);
                        }

                        // Resolve and merge into worker map
                        for (int i = 0; i < ordGroups.length; i++) {
                          if (ordGroups[i] != null) {
                            BytesRef bytes = dv.lookupOrd(i);
                            BytesRefKey key = new BytesRefKey(bytes);
                            AccumulatorGroup existing = workerGroups.get(key);
                            if (existing == null) {
                              workerGroups.put(key, ordGroups[i]);
                            } else {
                              existing.merge(ordGroups[i]);
                            }
                          }
                        }
                      }
                    } catch (IOException e) {
                      throw new java.io.UncheckedIOException(e);
                    }
                    return workerGroups;
                  },
                  PARALLEL_POOL);
        }

        java.util.concurrent.CompletableFuture.allOf(futures).join();

        HashMap<BytesRefKey, AccumulatorGroup> globalGroups = new HashMap<>();
        for (var future : futures) {
          HashMap<BytesRefKey, AccumulatorGroup> wMap = future.join();
          for (Map.Entry<BytesRefKey, AccumulatorGroup> e : wMap.entrySet()) {
            AccumulatorGroup existing = globalGroups.get(e.getKey());
            if (existing == null) {
              globalGroups.put(e.getKey(), e.getValue());
            } else {
              existing.merge(e.getValue());
            }
          }
        }

        if (globalGroups.isEmpty()) return List.of();

        // Build output
        int numGroupKeys = groupByKeys.size();
        int totalColumns = numGroupKeys + numAggs;
        int groupCount = globalGroups.size();
        BlockBuilder[] builders = new BlockBuilder[totalColumns];
        builders[0] = VarcharType.VARCHAR.createBlockBuilder(null, groupCount);
        for (int i = 0; i < numAggs; i++) {
          builders[numGroupKeys + i] =
              resolveAggOutputType(specs.get(i), columnTypeMap)
                  .createBlockBuilder(null, groupCount);
        }
        for (Map.Entry<BytesRefKey, AccumulatorGroup> entry : globalGroups.entrySet()) {
          VarcharType.VARCHAR.writeSlice(builders[0], Slices.wrappedBuffer(entry.getKey().bytes));
          AccumulatorGroup accGroup = entry.getValue();
          for (int a = 0; a < numAggs; a++) {
            accGroup.accumulators[a].writeTo(builders[numGroupKeys + a]);
          }
        }
        Block[] blocks = new Block[totalColumns];
        for (int i = 0; i < totalColumns; i++) blocks[i] = builders[i].build();
        return List.of(new Page(blocks));
      }

      // === Multi-segment path ===
      // Use BytesRefKey-based HashMap for cross-segment merge
      HashMap<BytesRefKey, AccumulatorGroup> globalGroups = new HashMap<>();

      engineSearcher.search(
          query,
          new Collector() {
            @Override
            public LeafCollector getLeafCollector(LeafReaderContext context) throws IOException {
              SortedSetDocValues dv = context.reader().getSortedSetDocValues(columnName);
              long ordCount = (dv != null) ? dv.getValueCount() : 0;
              AccumulatorGroup[] ordGroups =
                  (ordCount > 0 && ordCount <= 1_000_000)
                      ? new AccumulatorGroup[(int) ordCount]
                      : null;

              // Open agg doc values with typed arrays
              final SortedNumericDocValues[] numericAggDvs = new SortedNumericDocValues[numAggs];
              final SortedSetDocValues[] varcharAggDvs = new SortedSetDocValues[numAggs];
              for (int i = 0; i < numAggs; i++) {
                if (!isCountStar[i]) {
                  AggSpec spec = specs.get(i);
                  if (isVarcharArg[i]) {
                    varcharAggDvs[i] = context.reader().getSortedSetDocValues(spec.arg);
                  } else {
                    numericAggDvs[i] = context.reader().getSortedNumericDocValues(spec.arg);
                  }
                }
              }

              return new LeafCollector() {
                @Override
                public void setScorer(Scorable scorer) {}

                @Override
                public void collect(int doc) throws IOException {
                  if (dv == null || !dv.advanceExact(doc)) return;
                  int ord = (int) dv.nextOrd();
                  AccumulatorGroup accGroup;
                  if (ordGroups != null) {
                    accGroup = ordGroups[ord];
                    if (accGroup == null) {
                      accGroup = createAccumulatorGroup(specs);
                      ordGroups[ord] = accGroup;
                    }
                  } else {
                    // Fallback for very large ordinal spaces - should not happen in practice
                    return;
                  }
                  collectVarcharGenericAccumulate(
                      doc,
                      accGroup,
                      numAggs,
                      isCountStar,
                      isVarcharArg,
                      isDoubleArg,
                      accType,
                      numericAggDvs,
                      varcharAggDvs);
                }

                @Override
                public void finish() throws IOException {
                  if (dv == null) return;
                  if (ordGroups != null) {
                    for (int i = 0; i < ordGroups.length; i++) {
                      if (ordGroups[i] != null) {
                        BytesRef bytes = dv.lookupOrd(i);
                        BytesRefKey key = new BytesRefKey(bytes);
                        AccumulatorGroup existing = globalGroups.get(key);
                        if (existing == null) {
                          globalGroups.put(key, ordGroups[i]);
                        } else {
                          existing.merge(ordGroups[i]);
                        }
                      }
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

      if (globalGroups.isEmpty()) {
        return List.of();
      }

      int numGroupKeys = groupByKeys.size();
      int totalColumns = numGroupKeys + numAggs;
      int groupCount = globalGroups.size();
      BlockBuilder[] builders = new BlockBuilder[totalColumns];
      builders[0] = VarcharType.VARCHAR.createBlockBuilder(null, groupCount);
      for (int i = 0; i < numAggs; i++) {
        builders[numGroupKeys + i] =
            resolveAggOutputType(specs.get(i), columnTypeMap).createBlockBuilder(null, groupCount);
      }

      for (Map.Entry<BytesRefKey, AccumulatorGroup> entry : globalGroups.entrySet()) {
        VarcharType.VARCHAR.writeSlice(builders[0], Slices.wrappedBuffer(entry.getKey().bytes));
        AccumulatorGroup accGroup = entry.getValue();
        for (int a = 0; a < numAggs; a++) {
          accGroup.accumulators[a].writeTo(builders[numGroupKeys + a]);
        }
      }

      Block[] blocks = new Block[totalColumns];
      for (int i = 0; i < totalColumns; i++) blocks[i] = builders[i].build();
      return List.of(new Page(blocks));
    }
  }

  /**
   * Fast path for numeric-only GROUP BY keys. Since all keys are raw long values (no segment-local
   * ordinals), we aggregate directly into a global map across all segments without
   * per-segment/cross-segment merge overhead. Supports DATE_TRUNC expressions on timestamp columns
   * by applying truncation during key value extraction.
   *
   * <p>Uses a reusable {@link NumericProbeKey} for HashMap lookups to avoid per-doc allocation. An
   * immutable {@link SegmentGroupKey} is only created when inserting a new group (cache miss),
   * eliminating ~4 array allocations per doc for the common case where the group already exists.
   *
   * <p>For exactly two numeric keys, dispatches to {@link #executeTwoKeyNumeric} which uses an
   * open-addressing hash map with parallel arrays, eliminating all per-group object allocation.
   */
```

## 5. executeInternal — Dispatch Method (lines 939-1136)

```java
  private static List<Page> executeInternal(
      AggregationNode aggNode,
      IndexShard shard,
      Query query,
      Map<String, Type> columnTypeMap,
      int sortAggIndex,
      boolean sortAscending,
      long topN)
      throws Exception {
    TableScanNode scanNode = findChildTableScan(aggNode);
    List<String> groupByKeys = aggNode.getGroupByKeys();
    List<String> aggFunctions = aggNode.getAggregateFunctions();

    // Parse aggregate specs
    List<AggSpec> specs = new ArrayList<>();
    for (String func : aggFunctions) {
      Matcher m = AGG_FUNCTION.matcher(func);
      if (!m.matches()) {
        throw new IllegalArgumentException("Unsupported aggregate: " + func);
      }
      String funcName = m.group(1).toUpperCase(Locale.ROOT);
      boolean isDistinct = m.group(2) != null;
      String arg = m.group(3).trim();
      // Handle length(col): resolve to underlying VARCHAR column, flag for inline computation
      boolean applyLength = false;
      if (arg.startsWith("length(") && arg.endsWith(")")) {
        String innerCol = arg.substring(7, arg.length() - 1).trim();
        if (columnTypeMap.containsKey(innerCol)
            && columnTypeMap.get(innerCol) instanceof VarcharType) {
          arg = innerCol;
          applyLength = true;
        }
      }
      Type argType;
      if (applyLength) {
        argType = BigintType.BIGINT; // length() returns long
      } else {
        argType = arg.equals("*") ? null : columnTypeMap.getOrDefault(arg, BigintType.BIGINT);
      }
      AggSpec spec = new AggSpec(funcName, isDistinct, arg, argType);
      spec.applyLength = applyLength;
      specs.add(spec);
    }

    // Classify group-by keys, detecting DATE_TRUNC expressions
    List<KeyInfo> keyInfos = new ArrayList<>();
    boolean hasVarchar = false;
    for (String key : groupByKeys) {
      Type type = columnTypeMap.get(key);
      if (type != null) {
        // Plain column reference
        boolean isVarchar = type instanceof VarcharType;
        keyInfos.add(new KeyInfo(key, type, isVarchar, null, null));
        if (isVarchar) {
          hasVarchar = true;
        }
      } else {
        // Check for DATE_TRUNC expression
        Matcher dtm = DATE_TRUNC_PATTERN.matcher(key);
        if (dtm.matches()) {
          String unit = dtm.group(1).toLowerCase(Locale.ROOT);
          String sourceCol = dtm.group(2);
          // Output type is TimestampType, source column is read from DocValues
          keyInfos.add(
              new KeyInfo(sourceCol, TimestampType.TIMESTAMP_MILLIS, false, "date_trunc", unit));
        } else {
          // Check for EXTRACT expression: EXTRACT(FIELD FROM col)
          Matcher em = EXTRACT_PATTERN.matcher(key);
          if (em.matches()) {
            String field = em.group(1).toUpperCase(Locale.ROOT);
            String sourceCol = em.group(2);
            // Output type is BigintType (extract returns an integer)
            keyInfos.add(new KeyInfo(sourceCol, BigintType.BIGINT, false, "extract", field));
          } else {
            // Check for arithmetic expression: (col OP constant)
            Matcher am = ARITH_EXPR_PATTERN.matcher(key);
            if (am.matches()) {
              String sourceCol = am.group(1);
              String op = am.group(2);
              String constant = am.group(3);
              // Use BigintType for arithmetic output to match Trino's expectations
              // (arithmetic on integers produces long in the Trino execution model)
              keyInfos.add(
                  new KeyInfo(sourceCol, BigintType.BIGINT, false, "arith", op + ":" + constant));
            } else if (aggNode.getChild() instanceof EvalNode evalNode
                && evalNode.getOutputColumnNames().contains(key)) {
              // EvalNode-computed key (e.g., CASE WHEN expression).
              // Mark as "eval" expression — will be compiled and evaluated per-document.
              // Determine the output type by compiling the expression.
              int exprIdx = evalNode.getOutputColumnNames().indexOf(key);
              String exprStr = evalNode.getExpressions().get(exprIdx);
              Type evalOutputType = compileAndGetType(exprStr, columnTypeMap);
              boolean isEvalVarchar = evalOutputType instanceof VarcharType;
              // Use the expression key name (not a physical column name) with "eval" exprFunc.
              // The exprUnit carries the expression index in the EvalNode.
              keyInfos.add(
                  new KeyInfo(key, evalOutputType, isEvalVarchar, "eval", String.valueOf(exprIdx)));
              if (isEvalVarchar) {
                hasVarchar = true;
              }
            } else {
              throw new IllegalArgumentException("Unsupported group-by expression: " + key);
            }
          }
        }
      }
    }

    // Dispatch to specialized path based on key types
    if (hasVarchar) {
      // Fast path: single VARCHAR key with COUNT(*) only — uses ordinal array directly,
      // avoiding MergedGroupKey wrapper and AccumulatorGroup object allocation per group.
      if (keyInfos.size() == 1
          && keyInfos.get(0).isVarchar
          && specs.size() == 1
          && "COUNT".equals(specs.get(0).funcName)
          && "*".equals(specs.get(0).arg)) {
        return executeSingleVarcharCountStar(
            shard,
            query,
            keyInfos.get(0).name,
            columnTypeMap,
            groupByKeys,
            sortAggIndex,
            sortAscending,
            topN);
      }
      // Fast path: single VARCHAR key with general aggregates — uses ordinal-indexed
      // AccumulatorGroup array, eliminating HashMap lookups for single-segment case.
      if (keyInfos.size() == 1 && keyInfos.get(0).isVarchar) {
        return executeSingleVarcharGeneric(
            shard,
            query,
            keyInfos,
            specs,
            columnTypeMap,
            groupByKeys,
            sortAggIndex,
            sortAscending,
            topN);
      }
      // Check if any key is an eval expression — use eval-aware path
      boolean hasEvalKey = false;
      for (KeyInfo ki : keyInfos) {
        if ("eval".equals(ki.exprFunc)) {
          hasEvalKey = true;
          break;
        }
      }
      if (hasEvalKey) {
        return executeWithEvalKeys(
            aggNode,
            shard,
            query,
            keyInfos,
            specs,
            columnTypeMap,
            groupByKeys,
            sortAggIndex,
            sortAscending,
            topN);
      }
      return executeWithVarcharKeys(
          shard,
          query,
          keyInfos,
          specs,
          columnTypeMap,
          groupByKeys,
          sortAggIndex,
          sortAscending,
          topN);
    } else {
      return executeNumericOnly(
          shard,
          query,
          keyInfos,
          specs,
          columnTypeMap,
          groupByKeys,
          sortAggIndex,
          sortAscending,
          topN);
    }
  }

  /**
   * Apply DATE_TRUNC transformation to an epoch-millis value. Truncates to the specified unit
   * boundary directly in millis, avoiding ZonedDateTime allocation for the common 'minute' case.
   *
   * @param millis epoch milliseconds from DocValues
   * @param unit the truncation unit (second, minute, hour, day, month, year)
   * @return truncated epoch milliseconds
   */
  /**
   * Extract equality comparisons from a condition expression tree (AND of col == const). Returns
   * false if the tree contains anything other than AND and EQUAL comparisons.
   */
```

## 6. System.gc() and applyHavingFilter — NOT FOUND

- **System.gc()**: No occurrences found in FusedGroupByAggregate.java
- **applyHavingFilter**: No method with this name found in FusedGroupByAggregate.java (searched both exact and case-insensitive "having" — zero matches)

## Key Architectural Differences for Refactoring

### executeWithExpressionKeyImpl (current — single-threaded)
- Uses `engineSearcher.search(query, Collector)` — Lucene drives segment iteration sequentially
- Accumulates into a single `HashMap<String, AccumulatorGroup> globalGroups` — no synchronization needed
- Expression evaluation (keyBlockExpr, computedArgBlockExprs) is done per-ordinal in batches during `getLeafCollector()`
- Group key is `String` (expression result), not raw ordinal

### executeSingleVarcharGeneric (reference — parallel)
- Uses `Weight.scorer(leafCtx)` + `DocIdSetIterator` — manual segment iteration
- Each worker gets its own `HashMap<BytesRefKey, AccumulatorGroup>`
- Workers scan assigned segments, accumulate per-segment with ordinal arrays, then resolve ordinals to BytesRefKey
- After `CompletableFuture.allOf(futures).join()`, merges worker maps via `AccumulatorGroup.merge()`
- Segment partitioning: largest-first greedy assignment to lightest worker

### Key challenge for Q28 parallelization
The expression evaluation (`keyBlockExpr.evaluate()`) currently happens once per segment in `getLeafCollector()` 
and produces `ordToGroupKey[ord]` (String). In a parallel version, each worker would need to:
1. Re-evaluate expressions for its assigned segments' ordinals
2. Use `String` keys (not BytesRefKey) since the group key is an expression result, not a raw column value
3. Merge `HashMap<String, AccumulatorGroup>` across workers
