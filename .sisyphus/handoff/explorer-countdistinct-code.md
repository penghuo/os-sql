# COUNT(DISTINCT) Dispatch Chain — TransportShardExecuteAction.java

File: `dqe/src/main/java/org/opensearch/sql/dqe/shard/transport/TransportShardExecuteAction.java`

---

## 1. Dispatch Chain (Lines 250–420)

### Bare single-column fast paths (lines ~268–283)

```java
    // Fast path: bare TableScanNode with single numeric column — pre-dedup for COUNT(DISTINCT).
    // In the SINGLE aggregation path, the PlanFragmenter strips the AggregationNode, leaving a
    // bare TableScanNode. For scalar COUNT(DISTINCT numericCol), the shard collects distinct
    // values into a raw LongOpenHashSet and attaches it to the response, avoiding Page
    // construction overhead for millions of entries. The coordinator unions the raw sets.
    if (scanFactory == null && isBareSingleNumericColumnScan(plan)) {
      ShardExecuteResponse resp = executeDistinctValuesScanWithRawSet(plan, req);
      return resp;
    }

    // Fast path: bare TableScanNode with single VARCHAR column — pre-dedup for COUNT(DISTINCT).
    // Uses ordinal-based dedup via FixedBitSet for fast ordinal collection, then attaches the
    // raw string set to the response for direct coordinator merge.
    if (scanFactory == null && isBareSingleVarcharColumnScan(plan)) {
      ShardExecuteResponse resp = executeDistinctValuesScanVarcharWithRawSet(plan, req);
      return resp;
    }
```

### N-key COUNT(DISTINCT) dedup dispatch (lines ~293–398)

Entry condition:
```java
    if (scanFactory == null
        && effectivePlan instanceof AggregationNode aggDedupNode
        && aggDedupNode.getStep() == AggregationNode.Step.PARTIAL
        && aggDedupNode.getGroupByKeys().size() >= 2
        && FusedGroupByAggregate.canFuse(
            aggDedupNode, getOrBuildIndexMeta(findIndexName(plan)).columnTypeMap())) {
```

Dispatch logic (simplified):
- `isSingleCountStar` = exactly 1 aggregate, `COUNT(*)`
- `isMixedDedup` = not single COUNT(*), all aggregates match `SUM|COUNT(...)`

#### isSingleCountStar path:
- **2 numeric keys** → `executeCountDistinctWithHashSets(...)` (line 327)
- **VARCHAR key0 + numeric key1** → `executeVarcharCountDistinctWithHashSets(...)` (line 335)
- **3+ all-numeric keys** → `executeNKeyCountDistinctWithHashSets(...)` (line 353)
- **3+ mixed-type keys (last numeric)** → `executeMixedTypeCountDistinctWithHashSets(...)` (line 373)

#### isMixedDedup path (2 keys only):
- **2 numeric keys** → `executeMixedDedupWithHashSets(...)` (line 394)

---

## 2. executeCountDistinctWithHashSets (line 1000)

```java
  private ShardExecuteResponse executeCountDistinctWithHashSets(
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

    // Parallel segment scanning: each segment builds its own per-group HashSet map,
    // then we merge across segments. This exploits multiple CPU cores within a single shard.
    java.util.Map<Long, org.opensearch.sql.dqe.operator.LongOpenHashSet> finalSets;

    try (org.opensearch.index.engine.Engine.Searcher engineSearcher =
        shard.acquireSearcher("dqe-count-distinct-hashset")) {
      java.util.List<org.apache.lucene.index.LeafReaderContext> leaves =
          engineSearcher.getIndexReader().leaves();
      boolean isMatchAll = luceneQuery instanceof org.apache.lucene.search.MatchAllDocsQuery;
      org.apache.lucene.search.Weight weight =
          isMatchAll
              ? null
              : engineSearcher.createWeight(
                  engineSearcher.rewrite(luceneQuery),
                  org.apache.lucene.search.ScoreMode.COMPLETE_NO_SCORES,
                  1.0f);
      // ... parallel segment iteration, per-group LongOpenHashSet accumulation ...
```

---

## 3. executeVarcharCountDistinctWithHashSets (line 2410)

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

      java.util.List<org.apache.lucene.index.LeafReaderContext> leaves =
          engineSearcher.getIndexReader().leaves();
      boolean isMatchAll = luceneQuery instanceof org.apache.lucene.search.MatchAllDocsQuery;

      // Try global ordinals path: avoids per-segment String resolution
      org.apache.lucene.index.OrdinalMap ordinalMap = null;
      if (leaves.size() > 1) {
        ordinalMap = FusedGroupByAggregate.buildGlobalOrdinalMap(leaves, varcharKeyName);
      }
      // ... ordinal-based or fallback String-keyed accumulation ...
```

---

## 4. executeNKeyCountDistinctWithHashSets (line 1329)

```java
  private ShardExecuteResponse executeNKeyCountDistinctWithHashSets(
      AggregationNode aggNode,
      ShardExecuteRequest req,
      List<String> keyNames,
      Type[] keyTypes)
      throws Exception {
    int numKeys = keyNames.size();
    int numGroupKeys = numKeys - 1; // first N-1 are GROUP BY keys, last is the dedup key

    TableScanNode scanNode = findTableScanNode(aggNode);
    String indexName = scanNode.getIndexName();
    CachedIndexMeta cachedMeta = getOrBuildIndexMeta(indexName);

    IndexMetadata indexMeta = clusterService.state().metadata().index(indexName);
    IndexShard shard = indicesService.indexService(indexMeta.getIndex()).getShard(req.getShardId());

    Query luceneQuery =
        compileOrCacheLuceneQuery(scanNode.getDslFilter(), cachedMeta.fieldTypeMap());

    // Parallel segment scanning: each segment builds its own composite-group → HashSet map,
    // then we merge across segments.
    java.util.HashMap<LongArrayKey, org.opensearch.sql.dqe.operator.LongOpenHashSet> finalSets;

    try (org.opensearch.index.engine.Engine.Searcher engineSearcher =
        shard.acquireSearcher("dqe-nkey-count-distinct-hashset")) {
      java.util.List<org.apache.lucene.index.LeafReaderContext> leaves =
          engineSearcher.getIndexReader().leaves();
      boolean isMatchAll = luceneQuery instanceof org.apache.lucene.search.MatchAllDocsQuery;
      org.apache.lucene.search.Weight weight =
          isMatchAll
              ? null
              : engineSearcher.createWeight(
                  engineSearcher.rewrite(luceneQuery),
                  org.apache.lucene.search.ScoreMode.COMPLETE_NO_SCORES,
                  1.0f);
      // ... composite LongArrayKey grouping with per-group HashSet ...
```

---

## 5. executeMixedTypeCountDistinctWithHashSets (line 1649)

```java
  private ShardExecuteResponse executeMixedTypeCountDistinctWithHashSets(
      AggregationNode aggNode,
      ShardExecuteRequest req,
      List<String> keyNames,
      Type[] keyTypes)
      throws Exception {
    int numKeys = keyNames.size();
    int numGroupKeys = numKeys - 1;

    TableScanNode scanNode = findTableScanNode(aggNode);
    String indexName = scanNode.getIndexName();
    CachedIndexMeta cachedMeta = getOrBuildIndexMeta(indexName);

    IndexMetadata indexMeta = clusterService.state().metadata().index(indexName);
    IndexShard shard = indicesService.indexService(indexMeta.getIndex()).getShard(req.getShardId());

    Query luceneQuery =
        compileOrCacheLuceneQuery(scanNode.getDslFilter(), cachedMeta.fieldTypeMap());

    // Determine which group keys are VARCHAR vs numeric
    boolean[] isVarchar = new boolean[numGroupKeys];
    for (int i = 0; i < numGroupKeys; i++) {
      isVarchar[i] = keyTypes[i] instanceof io.trino.spi.type.VarcharType;
    }

    // Parallel segment scanning with cross-segment merge via String-keyed map
    java.util.HashMap<ObjectArrayKey, org.opensearch.sql.dqe.operator.LongOpenHashSet> finalSets;

    try (org.opensearch.index.engine.Engine.Searcher engineSearcher =
        shard.acquireSearcher("dqe-mixed-nkey-count-distinct-hashset")) {
      java.util.List<org.apache.lucene.index.LeafReaderContext> leaves =
          engineSearcher.getIndexReader().leaves();
      boolean isMatchAll = luceneQuery instanceof org.apache.lucene.search.MatchAllDocsQuery;
      // ... ObjectArrayKey grouping for mixed VARCHAR/numeric keys ...
```

---

## 6. executeMixedDedupWithHashSets (line 1931)

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
    // ... open-addressing hash map iteration with per-group HashSet + SUM/COUNT accumulators ...
```

---

## Summary of Key Patterns

| Method | Line | Group Key Type | Dedup Key Type | Data Structure |
|--------|------|---------------|----------------|----------------|
| `executeCountDistinctWithHashSets` | 1000 | 1 numeric (Long) | 1 numeric | `Map<Long, LongOpenHashSet>` |
| `executeVarcharCountDistinctWithHashSets` | 2410 | 1 VARCHAR (String) | 1 numeric | `Map<String, LongOpenHashSet>` |
| `executeNKeyCountDistinctWithHashSets` | 1329 | N-1 numeric (LongArrayKey) | 1 numeric | `HashMap<LongArrayKey, LongOpenHashSet>` |
| `executeMixedTypeCountDistinctWithHashSets` | 1649 | N-1 mixed (ObjectArrayKey) | 1 numeric | `HashMap<ObjectArrayKey, LongOpenHashSet>` |
| `executeMixedDedupWithHashSets` | 1931 | 1 numeric (open-addressing) | 1 numeric | Custom open-addressing arrays + LongOpenHashSet[] + long[][] accumulators |

All methods share the same skeleton:
1. Resolve `TableScanNode` → index name → shard
2. Compile Lucene query from DSL filter
3. Acquire searcher, iterate leaf segments (parallel for first 4, inline for mixed dedup)
4. Build per-group `LongOpenHashSet` for the dedup key
5. Return `ShardExecuteResponse` with attached hash sets or pages
