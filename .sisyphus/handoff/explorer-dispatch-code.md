# TransportShardExecuteAction.java — COUNT(DISTINCT) Dispatch Chain

File: `dqe/src/main/java/org/opensearch/sql/dqe/shard/transport/TransportShardExecuteAction.java`

---

## Section 1: Bare scan detection for scalar COUNT(DISTINCT) — Lines 246-265

```java
 246:     // Fast path: bare TableScanNode with single numeric column — pre-dedup for COUNT(DISTINCT).
 247:     // In the SINGLE aggregation path, the PlanFragmenter strips the AggregationNode, leaving a
 248:     // bare TableScanNode. For scalar COUNT(DISTINCT numericCol), the shard collects distinct
 249:     // values into a raw LongOpenHashSet and attaches it to the response, avoiding Page
 250:     // construction overhead for millions of entries. The coordinator unions the raw sets.
 251:     if (scanFactory == null && isBareSingleNumericColumnScan(plan)) {
 252:       ShardExecuteResponse resp = executeDistinctValuesScanWithRawSet(plan, req);
 253:       return resp;
 254:     }
 255: 
 256:     // Fast path: bare TableScanNode with single VARCHAR column — pre-dedup for COUNT(DISTINCT).
 257:     // Uses ordinal-based dedup via FixedBitSet for fast ordinal collection, then attaches the
 258:     // raw string set to the response for direct coordinator merge.
 259:     if (scanFactory == null && isBareSingleVarcharColumnScan(plan)) {
 260:       ShardExecuteResponse resp = executeDistinctValuesScanVarcharWithRawSet(plan, req);
 261:       return resp;
 262:     }
 263: 
 264:     // Try fused eval-aggregate for SUM(col + constant) patterns
 265:     if (scanFactory == null
```

---

## Section 2: 2-key dedup detection and routing — Lines 266-340

```java
 266:         && effectivePlan instanceof AggregationNode aggEvalNode
 267:         && FusedScanAggregate.canFuseWithEval(aggEvalNode)) {
 268:       List<Page> pages = executeFusedEvalAggregate(aggEvalNode, req);
 269:       List<Type> columnTypes = FusedScanAggregate.resolveEvalAggOutputTypes(aggEvalNode);
 270:       return applyTopProject(pages, columnTypes, topProject, aggEvalNode);
 271:     }
 272: 
 273:     // Fast path: COUNT(DISTINCT) dedup plan with 2 numeric keys and COUNT(*).
 274:     // Instead of GROUP BY (key0, key1) producing ~10K rows, builds per-group HashSets
 275:     // directly during DocValues iteration. Outputs compact pages (~450 rows) + attached
 276:     // HashSets for the coordinator to union across shards.
 277:     if (scanFactory == null
 278:         && effectivePlan instanceof AggregationNode aggDedupNode
 279:         && aggDedupNode.getStep() == AggregationNode.Step.PARTIAL
 280:         && aggDedupNode.getGroupByKeys().size() == 2
 281:         && FusedGroupByAggregate.canFuse(
 282:             aggDedupNode, getOrBuildIndexMeta(findIndexName(plan)).columnTypeMap())) {
 283:       // Check if it's a pure COUNT(*)-only dedup (Q9) or mixed dedup (Q10)
 284:       boolean isSingleCountStar =
 285:           aggDedupNode.getAggregateFunctions().size() == 1
 286:               && "COUNT(*)".equals(aggDedupNode.getAggregateFunctions().get(0));
 287:       // For mixed dedup: all aggregates must be SUM/COUNT (decomposable) — no DISTINCT, no AVG
 288:       boolean isMixedDedup =
 289:           !isSingleCountStar
 290:               && aggDedupNode.getAggregateFunctions().stream()
 291:                   .allMatch(f -> f.matches("(?i)^(sum|count)\\(.*\\)$"));
 292:       if (isSingleCountStar) {
 293:         Map<String, Type> ctm = getOrBuildIndexMeta(findIndexName(plan)).columnTypeMap();
 294:         String keyName0 = aggDedupNode.getGroupByKeys().get(0);
 295:         String keyName1 = aggDedupNode.getGroupByKeys().get(1);
 296:         Type t0 = ctm.get(keyName0);
 297:         Type t1 = ctm.get(keyName1);
 298:         if (t0 != null
 299:             && !(t0 instanceof io.trino.spi.type.VarcharType)
 300:             && t1 != null
 301:             && !(t1 instanceof io.trino.spi.type.VarcharType)) {
 302:           // Execute with HashSet-per-group: single-key GROUP BY with LongOpenHashSet accumulators
 303:           ShardExecuteResponse resp =
 304:               executeCountDistinctWithHashSets(aggDedupNode, req, keyName0, keyName1, t0, t1);
 305:           return resp;
 306:         }
 307:         // VARCHAR key0 + numeric key1: Q14 pattern (GROUP BY SearchPhrase, COUNT(DISTINCT UserID))
 308:         if (t0 instanceof io.trino.spi.type.VarcharType
 309:             && t1 != null
 310:             && !(t1 instanceof io.trino.spi.type.VarcharType)) {
 311:           ShardExecuteResponse resp =
 312:               executeVarcharCountDistinctWithHashSets(aggDedupNode, req, keyName0, keyName1, t1);
 313:           return resp;
 314:         }
 315:       } else if (isMixedDedup) {
 316:         // Q10 pattern: GROUP BY (key0, key1) with mixed SUM/COUNT aggregates.
 317:         // Native path: GROUP BY key0 only with per-group HashSet for key1 + accumulators for
 318:         // SUM/COUNT.
 319:         // Reduces shard output from ~25K (key0×key1) rows to ~400 (key0) rows.
 320:         Map<String, Type> ctm = getOrBuildIndexMeta(findIndexName(plan)).columnTypeMap();
 321:         String keyName0 = aggDedupNode.getGroupByKeys().get(0);
 322:         String keyName1 = aggDedupNode.getGroupByKeys().get(1);
 323:         Type t0 = ctm.get(keyName0);
 324:         Type t1 = ctm.get(keyName1);
 325:         if (t0 != null
 326:             && !(t0 instanceof io.trino.spi.type.VarcharType)
 327:             && t1 != null
 328:             && !(t1 instanceof io.trino.spi.type.VarcharType)) {
 329:           ShardExecuteResponse resp =
 330:               executeMixedDedupWithHashSets(aggDedupNode, req, keyName0, keyName1, t0, t1);
 331:           return resp;
 332:         }
 333:       }
 334:     }
 335: 
 336:     // Try ordinal-cached expression GROUP BY: AggregationNode -> EvalNode -> TableScanNode
 337:     // where the group-by key is a computed expression (e.g., REGEXP_REPLACE) over a single
 338:     // VARCHAR column. Pre-computes the expression once per unique ordinal (~16K evaluations
 339:     // instead of ~921K), giving ~58x reduction in expression evaluations for Q29.
 340:     // NOTE: This check MUST come before the generic canFuse() check below, because canFuse()
```

---

## Section 3: Fallback paths after dedup checks — Lines 341-400

```java
 341:     // also matches expression keys via EvalNode but routes to a path that doesn't handle
 342:     // expression GROUP BY correctly when there's no Sort/Limit wrapping (HAVING case).
 343:     if (scanFactory == null
 344:         && effectivePlan instanceof AggregationNode aggExprNode
 345:         && FusedGroupByAggregate.canFuseWithExpressionKey(
 346:             aggExprNode, getOrBuildIndexMeta(findIndexName(plan)).columnTypeMap())) {
 347:       List<Page> pages = executeFusedExprGroupByAggregate(aggExprNode, req);
 348:       List<Type> columnTypes =
 349:           resolveColumnTypes(
 350:               effectivePlan, getOrBuildIndexMeta(findIndexName(plan)).columnTypeMap());
 351:       return applyTopProject(pages, columnTypes, topProject, aggExprNode);
 352:     }
 353: 
 354:     // Try fused ordinal-based GROUP BY for aggregations with string group keys
 355:     if (scanFactory == null
 356:         && effectivePlan instanceof AggregationNode aggGroupNode
 357:         && FusedGroupByAggregate.canFuse(
 358:             aggGroupNode, getOrBuildIndexMeta(findIndexName(plan)).columnTypeMap())) {
 359:       List<Page> pages = executeFusedGroupByAggregate(aggGroupNode, req);
 360:       List<Type> columnTypes =
 361:           FusedGroupByAggregate.resolveOutputTypes(
 362:               aggGroupNode, getOrBuildIndexMeta(findIndexName(plan)).columnTypeMap());
 363:       return applyTopProject(pages, columnTypes, topProject, aggGroupNode);
 364:     }
 365: 
 366:     // Try fused GROUP BY with sort+limit: detect LimitNode -> [ProjectNode] -> SortNode ->
 367:     // AggregationNode pattern and use FusedGroupByAggregate for the aggregation, then apply
 368:     // sort+limit in-process. This avoids the generic operator pipeline (ScanOperator ->
 369:     // HashAggregationOperator) which is much slower than the fused DocValues path.
 370:     if (scanFactory == null) {
 371:       AggregationNode innerAgg = extractAggFromSortedLimit(plan);
 372:       if (innerAgg != null
 373:           && FusedGroupByAggregate.canFuse(
 374:               innerAgg, getOrBuildIndexMeta(findIndexName(plan)).columnTypeMap())) {
 375:         Map<String, Type> colTypeMap = getOrBuildIndexMeta(findIndexName(plan)).columnTypeMap();
 376:         // Apply sort+limit using SortOperator on the fused result
 377:         SortNode sortNode = extractSortNode(plan);
 378:         LimitNode limitNode = extractLimitNode(plan);
 379:         if (sortNode != null && limitNode != null) {
 380:           // Resolve sort column indices against the aggregation output columns
 381:           List<String> aggOutputColumns = new ArrayList<>();
 382:           aggOutputColumns.addAll(innerAgg.getGroupByKeys());
 383:           aggOutputColumns.addAll(innerAgg.getAggregateFunctions());
 384:           List<Integer> sortIndices = new ArrayList<>();
 385:           for (String sortKey : sortNode.getSortKeys()) {
 386:             int idx = aggOutputColumns.indexOf(sortKey);
 387:             if (idx < 0) {
 388:               sortIndices = null;
 389:               break;
 390:             }
 391:             sortIndices.add(idx);
 392:           }
 393:           if (sortIndices != null) {
 394:             long topN = limitNode.getCount() + limitNode.getOffset();
 395:             int numGroupByCols = innerAgg.getGroupByKeys().size();
 396: 
 397:             // HAVING: when a FilterNode is present between SortNode and AggregationNode,
 398:             // we must aggregate all groups first, apply the HAVING filter, then sort+limit.
 399:             // Top-N pre-filtering cannot be used because HAVING may eliminate groups.
 400:             FilterNode havingFilter = extractFilterFromSortedLimit(plan);
```

---

## Section 4: executeCountDistinctWithHashSets — Lines 936-1000

```java
 936:   private ShardExecuteResponse executeCountDistinctWithHashSets(
 937:       AggregationNode aggNode,
 938:       ShardExecuteRequest req,
 939:       String keyName0,
 940:       String keyName1,
 941:       Type type0,
 942:       Type type1)
 943:       throws Exception {
 944:     TableScanNode scanNode = findTableScanNode(aggNode);
 945:     String indexName = scanNode.getIndexName();
 946:     CachedIndexMeta cachedMeta = getOrBuildIndexMeta(indexName);
 947: 
 948:     IndexMetadata indexMeta = clusterService.state().metadata().index(indexName);
 949:     IndexShard shard = indicesService.indexService(indexMeta.getIndex()).getShard(req.getShardId());
 950: 
 951:     Query luceneQuery =
 952:         compileOrCacheLuceneQuery(scanNode.getDslFilter(), cachedMeta.fieldTypeMap());
 953: 
 954:     // Parallel segment scanning: each segment builds its own per-group HashSet map,
 955:     // then we merge across segments. This exploits multiple CPU cores within a single shard.
 956:     java.util.Map<Long, org.opensearch.sql.dqe.operator.LongOpenHashSet> finalSets;
 957: 
 958:     try (org.opensearch.index.engine.Engine.Searcher engineSearcher =
 959:         shard.acquireSearcher("dqe-count-distinct-hashset")) {
 960:       java.util.List<org.apache.lucene.index.LeafReaderContext> leaves =
 961:           engineSearcher.getIndexReader().leaves();
 962:       boolean isMatchAll = luceneQuery instanceof org.apache.lucene.search.MatchAllDocsQuery;
 963:       org.apache.lucene.search.Weight weight =
 964:           isMatchAll
 965:               ? null
 966:               : engineSearcher.createWeight(
 967:                   engineSearcher.rewrite(luceneQuery),
 968:                   org.apache.lucene.search.ScoreMode.COMPLETE_NO_SCORES,
 969:                   1.0f);
 970: 
 971:       if (leaves.size() <= 1) {
 972:         // Single segment: direct scan (no parallelism overhead)
 973:         finalSets =
 974:             scanSegmentForCountDistinct(
 975:                 leaves.isEmpty() ? null : leaves.get(0), weight, keyName0, keyName1, isMatchAll);
 976:       } else {
 977:         // Multi-segment: parallel scan using ForkJoinPool
 978:         @SuppressWarnings("unchecked")
 979:         java.util.Map<Long, org.opensearch.sql.dqe.operator.LongOpenHashSet>[] segResults =
 980:             new java.util.Map[leaves.size()];
 981:         java.util.concurrent.CountDownLatch segLatch =
 982:             new java.util.concurrent.CountDownLatch(leaves.size() - 1);
 983:         Exception[] segError = new Exception[1];
 984: 
 985:         // Dispatch all but last segment to thread pool
 986:         for (int s = 0; s < leaves.size() - 1; s++) {
 987:           final int segIdx = s;
 988:           final org.apache.lucene.index.LeafReaderContext leafCtx = leaves.get(s);
 989:           FusedGroupByAggregate.getParallelPool()
 990:               .execute(
 991:                   () -> {
 992:                     try {
 993:                       segResults[segIdx] =
 994:                           scanSegmentForCountDistinct(
 995:                               leafCtx, weight, keyName0, keyName1, isMatchAll);
 996:                     } catch (Exception e) {
 997:                       segError[0] = e;
 998:                     }
 999:                     segLatch.countDown();
1000:                   });
```

---

## Section 5: executeMixedDedupWithHashSets — Lines 1224-1280

```java
1224:   private ShardExecuteResponse executeMixedDedupWithHashSets(
1225:       AggregationNode aggNode,
1226:       ShardExecuteRequest req,
1227:       String keyName0,
1228:       String keyName1,
1229:       Type type0,
1230:       Type type1)
1231:       throws Exception {
1232:     TableScanNode scanNode = findTableScanNode(aggNode);
1233:     String indexName = scanNode.getIndexName();
1234:     CachedIndexMeta cachedMeta = getOrBuildIndexMeta(indexName);
1235: 
1236:     IndexMetadata indexMeta = clusterService.state().metadata().index(indexName);
1237:     IndexShard shard = indicesService.indexService(indexMeta.getIndex()).getShard(req.getShardId());
1238: 
1239:     Query luceneQuery =
1240:         compileOrCacheLuceneQuery(scanNode.getDslFilter(), cachedMeta.fieldTypeMap());
1241: 
1242:     List<String> aggFunctions = aggNode.getAggregateFunctions();
1243:     int numAggs = aggFunctions.size();
1244: 
1245:     // Open-addressing hash map: key0 → (LongOpenHashSet for key1, long[] for accumulators)
1246:     int grpCap = 256;
1247:     long[] grpKeys = new long[grpCap];
1248:     org.opensearch.sql.dqe.operator.LongOpenHashSet[] grpSets =
1249:         new org.opensearch.sql.dqe.operator.LongOpenHashSet[grpCap];
1250:     long[][] grpAccs = new long[grpCap][numAggs]; // per-group accumulator values
1251:     boolean[] grpOcc = new boolean[grpCap];
1252:     int grpSize = 0;
1253:     int grpThreshold = (int) (grpCap * 0.7f);
1254: 
1255:     // Resolve aggregate argument column names and function types
1256:     String[] aggArgNames = new String[numAggs];
1257:     boolean[] aggIsCountStar = new boolean[numAggs];
1258:     boolean[] aggIsCount = new boolean[numAggs]; // COUNT(col) — increment by 1, not by value
1259:     for (int i = 0; i < numAggs; i++) {
1260:       String f = aggFunctions.get(i);
1261:       java.util.regex.Matcher m =
1262:           java.util.regex.Pattern.compile("(?i)^(sum|count)\\((.+?)\\)$").matcher(f);
1263:       if (m.matches()) {
1264:         String funcName = m.group(1).toUpperCase();
1265:         String arg = m.group(2).trim();
1266:         aggIsCountStar[i] = "*".equals(arg);
1267:         aggIsCount[i] = "COUNT".equals(funcName) && !"*".equals(arg);
1268:         aggArgNames[i] = arg;
1269:       }
1270:     }
1271: 
1272:     try (org.opensearch.index.engine.Engine.Searcher engineSearcher =
1273:         shard.acquireSearcher("dqe-mixed-dedup-hashset")) {
1274:       for (org.apache.lucene.index.LeafReaderContext leafCtx :
1275:           engineSearcher.getIndexReader().leaves()) {
1276:         org.apache.lucene.index.LeafReader reader = leafCtx.reader();
1277:         org.apache.lucene.index.SortedNumericDocValues dv0 =
1278:             reader.getSortedNumericDocValues(keyName0);
1279:         org.apache.lucene.index.SortedNumericDocValues dv1 =
1280:             reader.getSortedNumericDocValues(keyName1);
```

---

## Section 6: executeVarcharCountDistinctWithHashSets — Lines 1485-1540

```java
1485:   private ShardExecuteResponse executeVarcharCountDistinctWithHashSets(
1486:       AggregationNode aggNode,
1487:       ShardExecuteRequest req,
1488:       String varcharKeyName,
1489:       String numericKeyName,
1490:       Type numericKeyType)
1491:       throws Exception {
1492:     TableScanNode scanNode = findTableScanNode(aggNode);
1493:     String indexName = scanNode.getIndexName();
1494:     CachedIndexMeta cachedMeta = getOrBuildIndexMeta(indexName);
1495: 
1496:     IndexMetadata indexMeta = clusterService.state().metadata().index(indexName);
1497:     IndexShard shard = indicesService.indexService(indexMeta.getIndex()).getShard(req.getShardId());
1498: 
1499:     Query luceneQuery =
1500:         compileOrCacheLuceneQuery(scanNode.getDslFilter(), cachedMeta.fieldTypeMap());
1501: 
1502:     // Per-group accumulators: ordinal → LongOpenHashSet of distinct values.
1503:     // Using ordinal-indexed array (no hash computation for VARCHAR keys).
1504:     java.util.Map<String, org.opensearch.sql.dqe.operator.LongOpenHashSet> varcharDistinctSets =
1505:         new java.util.HashMap<>();
1506: 
1507:     try (org.opensearch.index.engine.Engine.Searcher engineSearcher =
1508:         shard.acquireSearcher("dqe-varchar-count-distinct-hashset")) {
1509: 
1510:       for (org.apache.lucene.index.LeafReaderContext leafCtx :
1511:           engineSearcher.getIndexReader().leaves()) {
1512:         org.apache.lucene.index.LeafReader reader = leafCtx.reader();
1513:         org.apache.lucene.index.SortedSetDocValues varcharDv =
1514:             reader.getSortedSetDocValues(varcharKeyName);
1515:         org.apache.lucene.index.SortedNumericDocValues numericDv =
1516:             reader.getSortedNumericDocValues(numericKeyName);
1517:         if (varcharDv == null) continue;
1518: 
1519:         long ordCount = varcharDv.getValueCount();
1520:         // Ordinal-indexed array for this segment
1521:         org.opensearch.sql.dqe.operator.LongOpenHashSet[] ordSets =
1522:             new org.opensearch.sql.dqe.operator.LongOpenHashSet
1523:                 [(int) Math.min(ordCount, 10_000_000)];
1524: 
1525:         boolean isMatchAll = luceneQuery instanceof org.apache.lucene.search.MatchAllDocsQuery;
1526: 
1527:         if (isMatchAll) {
1528:           // Sequential scan: iterate DocValues directly
1529:           org.apache.lucene.index.SortedDocValues sdv =
1530:               org.apache.lucene.index.DocValues.unwrapSingleton(varcharDv);
1531:           if (sdv != null) {
1532:             int doc;
1533:             while ((doc = sdv.nextDoc())
1534:                 != org.apache.lucene.search.DocIdSetIterator.NO_MORE_DOCS) {
1535:               int ord = sdv.ordValue();
1536:               if (ordSets[ord] == null)
1537:                 ordSets[ord] = new org.opensearch.sql.dqe.operator.LongOpenHashSet(16);
1538:               long val = 0;
1539:               if (numericDv != null && numericDv.advanceExact(doc)) val = numericDv.nextValue();
1540:               ordSets[ord].add(val);
```
