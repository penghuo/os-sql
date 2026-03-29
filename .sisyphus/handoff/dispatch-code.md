# TransportShardExecuteAction - Dispatch & Execution Methods

File: `dqe/src/main/java/org/opensearch/sql/dqe/shard/transport/TransportShardExecuteAction.java`

## Dispatch Logic (lines 240-360)

The dispatch happens in the main execute method. Key decision points:

```java
// Line 250-253: Scalar COUNT(DISTINCT) numeric path
if (scanFactory == null && isBareSingleNumericColumnScan(plan)) {
    ShardExecuteResponse resp = executeDistinctValuesScanWithRawSet(plan, req);
    return resp;
}

// Line 257-261: Scalar COUNT(DISTINCT) varchar path
if (scanFactory == null && isBareSingleVarcharColumnScan(plan)) {
    ShardExecuteResponse resp = executeDistinctValuesScanVarcharWithRawSet(plan, req);
    return resp;
}

// Line 265-270: Fused eval-aggregate for SUM(col + constant) patterns
if (scanFactory == null
    && effectivePlan instanceof AggregationNode aggEvalNode
    && FusedScanAggregate.canFuseWithEval(aggEvalNode)) {
    List<Page> pages = executeFusedEvalAggregate(aggEvalNode, req);
    List<Type> columnTypes = FusedScanAggregate.resolveEvalAggOutputTypes(aggEvalNode);
    return applyTopProject(pages, columnTypes, topProject, aggEvalNode);
}

// Line 276-360: COUNT(DISTINCT) dedup plan with N numeric keys
// Dispatches to:
//   - executeCountDistinctWithHashSets (2 numeric keys, COUNT(*) only)
//   - executeVarcharCountDistinctWithHashSets (varchar key0 + numeric key1, Q13/Q14)
//   - executeNKeyCountDistinctWithHashSets (3+ numeric keys)
//   - executeMixedDedupWithHashSets (2 keys, mixed SUM/COUNT aggs, Q09/Q10)
```

---

## Method 1: executeDistinctValuesScanWithRawSet (lines 2180-2208)

Scalar COUNT(DISTINCT) for numeric columns. Collects distinct values into a raw LongOpenHashSet.

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

---

## Method 2: executeDistinctValuesScanVarcharWithRawSet (lines 2210-2237)

Scalar COUNT(DISTINCT) for VARCHAR columns. Collects distinct strings into a HashSet<String>.

```java
  private ShardExecuteResponse executeDistinctValuesScanVarcharWithRawSet(
      DqePlanNode plan, ShardExecuteRequest req) throws Exception {
    TableScanNode scanNode = (TableScanNode) plan;
    String indexName = scanNode.getIndexName();
    String columnName = scanNode.getColumns().get(0);
    CachedIndexMeta cachedMeta = getOrBuildIndexMeta(indexName);

    IndexMetadata indexMeta = clusterService.state().metadata().index(indexName);
    IndexShard shard = indicesService.indexService(indexMeta.getIndex()).getShard(req.getShardId());

    Query luceneQuery =
        compileOrCacheLuceneQuery(scanNode.getDslFilter(), cachedMeta.fieldTypeMap());

    java.util.HashSet<String> rawStrings =
        FusedScanAggregate.collectDistinctStringsRaw(columnName, shard, luceneQuery);

    // Build a minimal 1-row Page with the count (fallback for non-local paths)
    BlockBuilder builder = BigintType.BIGINT.createBlockBuilder(null, 1);
    BigintType.BIGINT.writeLong(builder, rawStrings.size());
    List<Type> columnTypes = List.of(io.trino.spi.type.VarcharType.VARCHAR);
    ShardExecuteResponse resp =
        new ShardExecuteResponse(List.of(new Page(builder.build())), columnTypes);
    resp.setScalarDistinctStrings(rawStrings);
    return resp;
  }
```

---

## Method 3: executeMixedDedupWithHashSets (lines 1537-1796)

Mixed dedup path (Q09/Q10): GROUP BY key0 with per-group LongOpenHashSet for key1 + SUM/COUNT accumulators.

```java
  private ShardExecuteResponse executeMixedDedupWithHashSets(
      AggregationNode aggNode,
      ShardExecuteRequest req,
      String keyName0,
      String keyName1,
      Type type0,
      Type type1)
      throws Exception {
    TableScanNode scanNode = findTableScanNode(aggNode);
    String indexName = scanNode.getIndexName();
    CachedIndexMeta cachedMeta = getOrBuildIndexMeta(indexName);

    IndexMetadata indexMeta = clusterService.state().metadata().index(indexName);
    IndexShard shard = indicesService.indexService(indexMeta.getIndex()).getShard(req.getShardId());

    Query luceneQuery =
        compileOrCacheLuceneQuery(scanNode.getDslFilter(), cachedMeta.fieldTypeMap());

    List<String> aggFunctions = aggNode.getAggregateFunctions();
    int numAggs = aggFunctions.size();

    // Open-addressing hash map: key0 → (LongOpenHashSet for key1, long[] for accumulators)
    int grpCap = 256;
    long[] grpKeys = new long[grpCap];
    org.opensearch.sql.dqe.operator.LongOpenHashSet[] grpSets =
        new org.opensearch.sql.dqe.operator.LongOpenHashSet[grpCap];
    long[][] grpAccs = new long[grpCap][numAggs]; // per-group accumulator values
    boolean[] grpOcc = new boolean[grpCap];
    int grpSize = 0;
    int grpThreshold = (int) (grpCap * 0.7f);

    // Resolve aggregate argument column names and function types
    String[] aggArgNames = new String[numAggs];
    boolean[] aggIsCountStar = new boolean[numAggs];
    boolean[] aggIsCount = new boolean[numAggs]; // COUNT(col) — increment by 1, not by value
    for (int i = 0; i < numAggs; i++) {
      String f = aggFunctions.get(i);
      java.util.regex.Matcher m =
          java.util.regex.Pattern.compile("(?i)^(sum|count)\\((.+?)\\)$").matcher(f);
      if (m.matches()) {
        String funcName = m.group(1).toUpperCase();
        String arg = m.group(2).trim();
        aggIsCountStar[i] = "*".equals(arg);
        aggIsCount[i] = "COUNT".equals(funcName) && !"*".equals(arg);
        aggArgNames[i] = arg;
      }
    }
```


```java
    // (continued - shard scan loop)
    try (org.opensearch.index.engine.Engine.Searcher engineSearcher =
        shard.acquireSearcher("dqe-mixed-dedup-hashset")) {
      for (org.apache.lucene.index.LeafReaderContext leafCtx :
          engineSearcher.getIndexReader().leaves()) {
        org.apache.lucene.index.LeafReader reader = leafCtx.reader();
        org.apache.lucene.index.SortedNumericDocValues dv0 =
            reader.getSortedNumericDocValues(keyName0);
        org.apache.lucene.index.SortedNumericDocValues dv1 =
            reader.getSortedNumericDocValues(keyName1);

        // Open aggregate argument DocValues
        org.apache.lucene.index.SortedNumericDocValues[] aggDvs =
            new org.apache.lucene.index.SortedNumericDocValues[numAggs];
        for (int i = 0; i < numAggs; i++) {
          if (!aggIsCountStar[i]) {
            aggDvs[i] = reader.getSortedNumericDocValues(aggArgNames[i]);
          }
        }

        boolean isMatchAll = luceneQuery instanceof org.apache.lucene.search.MatchAllDocsQuery;

        if (isMatchAll && dv0 != null && dv1 != null) {
          int maxDoc = reader.maxDoc();
          int dvDoc0 = dv0.nextDoc();
          int dvDoc1 = dv1.nextDoc();
          int[] aggDvDocs = new int[numAggs];
          for (int i = 0; i < numAggs; i++) {
            if (aggDvs[i] != null) aggDvDocs[i] = aggDvs[i].nextDoc();
            else aggDvDocs[i] = org.apache.lucene.search.DocIdSetIterator.NO_MORE_DOCS;
          }

          for (int doc = 0; doc < maxDoc; doc++) {
            long k0 = 0;
            if (dvDoc0 == doc) { k0 = dv0.nextValue(); dvDoc0 = dv0.nextDoc(); }
            long k1 = 0;
            if (dvDoc1 == doc) { k1 = dv1.nextValue(); dvDoc1 = dv1.nextDoc(); }

            // Open-addressing probe for key0
            int gm = grpCap - 1;
            int gs = Long.hashCode(k0) & gm;
            while (grpOcc[gs] && grpKeys[gs] != k0) gs = (gs + 1) & gm;
            if (!grpOcc[gs]) {
              grpKeys[gs] = k0;
              grpSets[gs] = new org.opensearch.sql.dqe.operator.LongOpenHashSet(16);
              grpAccs[gs] = new long[numAggs];
              grpOcc[gs] = true;
              grpSize++;
              if (grpSize > grpThreshold) {
                // Resize hash map (doubles capacity, rehashes all entries)
                int nc = grpCap * 2;
                long[] nk = new long[nc];
                var ns = new org.opensearch.sql.dqe.operator.LongOpenHashSet[nc];
                long[][] na = new long[nc][numAggs];
                boolean[] no = new boolean[nc];
                int nm = nc - 1;
                for (int g = 0; g < grpCap; g++) {
                  if (grpOcc[g]) {
                    int s = Long.hashCode(grpKeys[g]) & nm;
                    while (no[s]) s = (s + 1) & nm;
                    nk[s] = grpKeys[g]; ns[s] = grpSets[g]; na[s] = grpAccs[g]; no[s] = true;
                  }
                }
                grpKeys = nk; grpSets = ns; grpAccs = na; grpOcc = no;
                grpCap = nc; grpThreshold = (int) (nc * 0.7f);
                gm = grpCap - 1;
                gs = Long.hashCode(k0) & gm;
                while (grpOcc[gs] && grpKeys[gs] != k0) gs = (gs + 1) & gm;
              }
            }
            grpSets[gs].add(k1);

            // Accumulate SUM/COUNT for each aggregate
            for (int i = 0; i < numAggs; i++) {
              if (aggIsCountStar[i]) {
                grpAccs[gs][i]++;
              } else if (aggDvDocs[i] == doc) {
                long val = aggDvs[i].nextValue();
                aggDvDocs[i] = aggDvs[i].nextDoc();
                grpAccs[gs][i] += aggIsCount[i] ? 1 : val;
              }
            }
          }
```


        } else {
          // Filtered path
          org.apache.lucene.search.Weight weight =
              engineSearcher.createWeight(
                  engineSearcher.rewrite(luceneQuery),
                  org.apache.lucene.search.ScoreMode.COMPLETE_NO_SCORES, 1.0f);
          org.apache.lucene.search.Scorer scorer = weight.scorer(leafCtx);
          if (scorer == null) continue;
          org.apache.lucene.search.DocIdSetIterator disi = scorer.iterator();
          int doc;
          while ((doc = disi.nextDoc()) != org.apache.lucene.search.DocIdSetIterator.NO_MORE_DOCS) {
            long k0 = 0;
            if (dv0 != null && dv0.advanceExact(doc)) k0 = dv0.nextValue();
            long k1 = 0;
            if (dv1 != null && dv1.advanceExact(doc)) k1 = dv1.nextValue();

            // Same open-addressing probe + resize logic as MatchAll path (omitted for brevity)
            int gm = grpCap - 1;
            int gs = Long.hashCode(k0) & gm;
            while (grpOcc[gs] && grpKeys[gs] != k0) gs = (gs + 1) & gm;
            if (!grpOcc[gs]) {
              grpKeys[gs] = k0;
              grpSets[gs] = new org.opensearch.sql.dqe.operator.LongOpenHashSet(16);
              grpAccs[gs] = new long[numAggs];
              grpOcc[gs] = true;
              grpSize++;
              if (grpSize > grpThreshold) { /* resize - identical to MatchAll path */ }
            }
            grpSets[gs].add(k1);
            for (int i = 0; i < numAggs; i++) {
              if (aggIsCountStar[i]) {
                grpAccs[gs][i]++;
              } else if (aggDvs[i] != null && aggDvs[i].advanceExact(doc)) {
                grpAccs[gs][i] += aggIsCount[i] ? 1 : aggDvs[i].nextValue();
              }
            }
          }
        }
      }
    }

    // Build output page: (key0, key1_placeholder=0, agg0, agg1, ...)
    int numOutputCols = 2 + numAggs;
    List<Type> colTypes = new java.util.ArrayList<>();
    colTypes.add(type0 instanceof io.trino.spi.type.IntegerType
        ? io.trino.spi.type.IntegerType.INTEGER : io.trino.spi.type.BigintType.BIGINT);
    colTypes.add(type1 instanceof io.trino.spi.type.IntegerType
        ? io.trino.spi.type.IntegerType.INTEGER : io.trino.spi.type.BigintType.BIGINT);
    for (int i = 0; i < numAggs; i++) colTypes.add(io.trino.spi.type.BigintType.BIGINT);

    io.trino.spi.block.BlockBuilder[] builders = new io.trino.spi.block.BlockBuilder[numOutputCols];
    for (int i = 0; i < numOutputCols; i++)
      builders[i] = colTypes.get(i).createBlockBuilder(null, grpSize);

    java.util.Map<Long, org.opensearch.sql.dqe.operator.LongOpenHashSet> distinctSets =
        new java.util.HashMap<>(grpSize);

    for (int g = 0; g < grpCap; g++) {
      if (!grpOcc[g]) continue;
      distinctSets.put(grpKeys[g], grpSets[g]);
      colTypes.get(0).writeLong(builders[0], grpKeys[g]);
      colTypes.get(1).writeLong(builders[1], 0L); // placeholder for key1
      for (int i = 0; i < numAggs; i++)
        io.trino.spi.type.BigintType.BIGINT.writeLong(builders[2 + i], grpAccs[g][i]);
    }

    Block[] blocks = new Block[numOutputCols];
    for (int i = 0; i < numOutputCols; i++) blocks[i] = builders[i].build();
    Page page = new Page(blocks);
    ShardExecuteResponse resp = new ShardExecuteResponse(List.of(page), colTypes);
    resp.setDistinctSets(distinctSets);
    return resp;
  }
```

---

## Method 4: executeVarcharCountDistinctWithHashSets (lines 1798-2005)

VARCHAR key + COUNT(DISTINCT numeric) pattern (Q13/Q14). Uses ordinal-indexed arrays per segment.

```java
  private ShardExecuteResponse executeVarcharCountDistinctWithHashSets(
      AggregationNode aggNode,
      ShardExecuteRequest req,
      String varcharKeyName,
      String numericKeyName,
      Type numericKeyType)
      throws Exception {
    TableScanNode scanNode = findTableScanNode(aggNode);
    String indexName = scanNode.getIndexName();
    CachedIndexMeta cachedMeta = getOrBuildIndexMeta(indexName);

    IndexMetadata indexMeta = clusterService.state().metadata().index(indexName);
    IndexShard shard = indicesService.indexService(indexMeta.getIndex()).getShard(req.getShardId());

    Query luceneQuery =
        compileOrCacheLuceneQuery(scanNode.getDslFilter(), cachedMeta.fieldTypeMap());

    // Per-group accumulators: ordinal → LongOpenHashSet of distinct values.
    java.util.Map<String, org.opensearch.sql.dqe.operator.LongOpenHashSet> varcharDistinctSets =
        new java.util.HashMap<>();

    try (org.opensearch.index.engine.Engine.Searcher engineSearcher =
        shard.acquireSearcher("dqe-varchar-count-distinct-hashset")) {

      for (org.apache.lucene.index.LeafReaderContext leafCtx :
          engineSearcher.getIndexReader().leaves()) {
        org.apache.lucene.index.LeafReader reader = leafCtx.reader();
        org.apache.lucene.index.SortedSetDocValues varcharDv =
            reader.getSortedSetDocValues(varcharKeyName);
        org.apache.lucene.index.SortedNumericDocValues numericDv =
            reader.getSortedNumericDocValues(numericKeyName);
        if (varcharDv == null) continue;

        long ordCount = varcharDv.getValueCount();
        org.opensearch.sql.dqe.operator.LongOpenHashSet[] ordSets =
            new org.opensearch.sql.dqe.operator.LongOpenHashSet
                [(int) Math.min(ordCount, 10_000_000)];

        boolean isMatchAll = luceneQuery instanceof org.apache.lucene.search.MatchAllDocsQuery;

        if (isMatchAll) {
          // Sequential scan: iterate DocValues directly
          org.apache.lucene.index.SortedDocValues sdv =
              org.apache.lucene.index.DocValues.unwrapSingleton(varcharDv);
          if (sdv != null) {
            int doc;
            while ((doc = sdv.nextDoc())
                != org.apache.lucene.search.DocIdSetIterator.NO_MORE_DOCS) {
              int ord = sdv.ordValue();
              if (ordSets[ord] == null)
                ordSets[ord] = new org.opensearch.sql.dqe.operator.LongOpenHashSet(16);
              long val = 0;
              if (numericDv != null && numericDv.advanceExact(doc)) val = numericDv.nextValue();
              ordSets[ord].add(val);
            }
          } else {
            int doc;
            while ((doc = varcharDv.nextDoc())
                != org.apache.lucene.search.DocIdSetIterator.NO_MORE_DOCS) {
              int ord = (int) varcharDv.nextOrd();
              if (ordSets[ord] == null)
                ordSets[ord] = new org.opensearch.sql.dqe.operator.LongOpenHashSet(16);
              long val = 0;
              if (numericDv != null && numericDv.advanceExact(doc)) val = numericDv.nextValue();
              ordSets[ord].add(val);
            }
          }
```


        } else {
          // Collect-then-sequential-scan: collect matching doc IDs first, then scan sequentially.
          org.apache.lucene.search.Weight weight =
              engineSearcher.createWeight(
                  engineSearcher.rewrite(luceneQuery),
                  org.apache.lucene.search.ScoreMode.COMPLETE_NO_SCORES, 1.0f);
          org.apache.lucene.search.Scorer scorer = weight.scorer(leafCtx);
          if (scorer == null) {
            mergeOrdSetsIntoMap(varcharDv, ordSets, varcharDistinctSets);
            continue;
          }

          // Step 1: Collect matching doc IDs into sorted array
          org.apache.lucene.search.DocIdSetIterator disi = scorer.iterator();
          int[] matchDocs = new int[1024];
          int matchCount = 0;
          int doc;
          while ((doc = disi.nextDoc()) != org.apache.lucene.search.DocIdSetIterator.NO_MORE_DOCS) {
            if (matchCount == matchDocs.length)
              matchDocs = java.util.Arrays.copyOf(matchDocs, matchDocs.length * 2);
            matchDocs[matchCount++] = doc;
          }

          // Step 2: Sequential scan of varchar DocValues, matching against collected doc IDs
          org.apache.lucene.index.SortedDocValues sdv =
              org.apache.lucene.index.DocValues.unwrapSingleton(varcharDv);
          if (sdv != null && matchCount > 0) {
            int matchIdx = 0;
            int dvDoc;
            while ((dvDoc = sdv.nextDoc())
                != org.apache.lucene.search.DocIdSetIterator.NO_MORE_DOCS) {
              if (matchIdx >= matchCount) break;
              if (dvDoc == matchDocs[matchIdx]) {
                int ord = sdv.ordValue();
                if (ordSets[ord] == null)
                  ordSets[ord] = new org.opensearch.sql.dqe.operator.LongOpenHashSet(16);
                long val = 0;
                if (numericDv != null && numericDv.advanceExact(dvDoc)) val = numericDv.nextValue();
                ordSets[ord].add(val);
                matchIdx++;
              }
            }
          } else if (matchCount > 0) {
            // Multi-valued path
            int matchIdx = 0;
            int dvDoc;
            while ((dvDoc = varcharDv.nextDoc())
                != org.apache.lucene.search.DocIdSetIterator.NO_MORE_DOCS) {
              if (matchIdx >= matchCount) break;
              if (dvDoc == matchDocs[matchIdx]) {
                int ord = (int) varcharDv.nextOrd();
                if (ordSets[ord] == null)
                  ordSets[ord] = new org.opensearch.sql.dqe.operator.LongOpenHashSet(16);
                long val = 0;
                if (numericDv != null && numericDv.advanceExact(dvDoc)) val = numericDv.nextValue();
                ordSets[ord].add(val);
                matchIdx++;
              }
            }
          }
        }

        // Merge this segment's ordinal-indexed sets into the cross-segment String-keyed map
        mergeOrdSetsIntoMap(varcharDv, ordSets, varcharDistinctSets);
      }
    }

    // Build output page: (varcharKey, placeholder=0, COUNT(*)=distinct_count)
    List<Type> colTypes = List.of(
        io.trino.spi.type.VarcharType.VARCHAR,
        numericKeyType instanceof io.trino.spi.type.IntegerType
            ? io.trino.spi.type.IntegerType.INTEGER : io.trino.spi.type.BigintType.BIGINT,
        io.trino.spi.type.BigintType.BIGINT);

    int groupCount = varcharDistinctSets.size();
    io.trino.spi.block.BlockBuilder b0 = colTypes.get(0).createBlockBuilder(null, groupCount);
    io.trino.spi.block.BlockBuilder b1 = colTypes.get(1).createBlockBuilder(null, groupCount);
    io.trino.spi.block.BlockBuilder b2 = colTypes.get(2).createBlockBuilder(null, groupCount);

    for (java.util.Map.Entry<String, org.opensearch.sql.dqe.operator.LongOpenHashSet> e :
        varcharDistinctSets.entrySet()) {
      io.airlift.slice.Slice keySlice = io.airlift.slice.Slices.utf8Slice(e.getKey());
      io.trino.spi.type.VarcharType.VARCHAR.writeSlice(b0, keySlice);
      colTypes.get(1).writeLong(b1, 0L);
      io.trino.spi.type.BigintType.BIGINT.writeLong(b2, e.getValue().size());
    }

    Page page = new Page(b0.build(), b1.build(), b2.build());
    ShardExecuteResponse resp = new ShardExecuteResponse(List.of(page), colTypes);
    resp.setVarcharDistinctSets(varcharDistinctSets);
    return resp;
  }
```

### Helper: mergeOrdSetsIntoMap (used by method 4)

```java
  private static void mergeOrdSetsIntoMap(
      org.apache.lucene.index.SortedSetDocValues dv,
      org.opensearch.sql.dqe.operator.LongOpenHashSet[] ordSets,
      java.util.Map<String, org.opensearch.sql.dqe.operator.LongOpenHashSet> target)
      throws java.io.IOException {
    for (int ord = 0; ord < ordSets.length; ord++) {
      if (ordSets[ord] == null) continue;
      org.apache.lucene.util.BytesRef bytes = dv.lookupOrd(ord);
      String key = bytes.utf8ToString();
      org.opensearch.sql.dqe.operator.LongOpenHashSet existing = target.get(key);
      if (existing == null) {
        target.put(key, ordSets[ord]);
      } else {
        long[] srcKeys = ordSets[ord].keys();
        for (long v : srcKeys) {
          if (v != ordSets[ord].emptyMarker()) existing.add(v);
        }
        if (ordSets[ord].hasZeroValue()) existing.add(0L);
        if (ordSets[ord].hasSentinelValue()) existing.add(Long.MIN_VALUE);
      }
    }
  }
```

---

## Method 5: executeFusedEvalAggregate (line 930) + canFuseWithEval (FusedScanAggregate.java:101)

### executeFusedEvalAggregate (TransportShardExecuteAction.java lines 930-948)

Fused eval-aggregate for SUM(col + constant) patterns. Delegates to FusedScanAggregate.executeWithEval.

```java
  private List<Page> executeFusedEvalAggregate(AggregationNode aggNode, ShardExecuteRequest req)
      throws Exception {
    // Walk through EvalNode to find the TableScanNode
    EvalNode evalNode = (EvalNode) aggNode.getChild();
    TableScanNode scanNode = (TableScanNode) evalNode.getChild();
    String indexName = scanNode.getIndexName();
    CachedIndexMeta cachedMeta = getOrBuildIndexMeta(indexName);

    // Resolve IndexShard
    IndexMetadata indexMeta = clusterService.state().metadata().index(indexName);
    IndexShard shard = indicesService.indexService(indexMeta.getIndex()).getShard(req.getShardId());

    // Compile Lucene query (cached across concurrent shard executions)
    Query luceneQuery =
        compileOrCacheLuceneQuery(scanNode.getDslFilter(), cachedMeta.fieldTypeMap());

    return FusedScanAggregate.executeWithEval(
        aggNode, shard, luceneQuery, cachedMeta.columnTypeMap());
  }
```

### canFuseWithEval (FusedScanAggregate.java lines 101-145)

Guards the fused eval-aggregate path. Requires: no GROUP BY, child is EvalNode→TableScanNode,
all aggs are SUM/COUNT/AVG (non-distinct), all eval expressions are plain columns or (col + int_constant).

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
      if (trimmed.matches("\\w+")) {
        continue;  // Plain column reference
      }
      Matcher cm = COL_PLUS_CONST.matcher(trimmed);
      if (cm.matches()) {
        continue;  // (col + N) pattern
      }
      return false; // Unsupported expression
    }
    return true;
  }
```

### FusedScanAggregate.executeWithEval (line ~155) — the algebraic shortcut

Uses identity: `SUM(col + k) = SUM(col) + k * COUNT(*)` to avoid per-row expression evaluation.

---

## Summary of Line Ranges

| Method | Lines | Purpose |
|--------|-------|---------|
| `executeDistinctValuesScanWithRawSet` | 2180-2208 | Scalar COUNT(DISTINCT) numeric |
| `executeDistinctValuesScanVarcharWithRawSet` | 2210-2237 | Scalar COUNT(DISTINCT) varchar |
| `executeMixedDedupWithHashSets` | 1537-1796 | Mixed dedup (Q09): GROUP BY key0 + HashSet key1 + SUM/COUNT accumulators |
| `executeVarcharCountDistinctWithHashSets` | 1798-2005 | VARCHAR COUNT(DISTINCT) (Q13): ordinal-indexed per-segment + cross-segment merge |
| `executeFusedEvalAggregate` | 930-948 | Fused eval-aggregate: delegates to FusedScanAggregate.executeWithEval |
| `canFuseWithEval` | FusedScanAggregate:101-145 | Guard: no GROUP BY, SUM/COUNT/AVG only, plain col or col+const evals |
