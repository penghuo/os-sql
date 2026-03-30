# Scalar COUNT(DISTINCT) Dispatch and Execute Methods

## Source: `TransportShardExecuteAction.java`

---

### Dispatch Logic (lines 240–280)

```java
      List<Type> columnTypes =
          FusedScanAggregate.resolveOutputTypes(
              aggNode, getOrBuildIndexMeta(findIndexName(plan)).columnTypeMap());
      return applyTopProject(pages, columnTypes, topProject, aggNode);
    }

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

    // Try fused eval-aggregate for SUM(col + constant) patterns
    if (scanFactory == null
        && effectivePlan instanceof AggregationNode aggEvalNode
        && FusedScanAggregate.canFuseWithEval(aggEvalNode)) {
      List<Page> pages = executeFusedEvalAggregate(aggEvalNode, req);
      List<Type> columnTypes = FusedScanAggregate.resolveEvalAggOutputTypes(aggEvalNode);
      return applyTopProject(pages, columnTypes, topProject, aggEvalNode);
    }

    // Fast path: COUNT(DISTINCT) dedup plan with N numeric keys (N >= 2) and COUNT(*).
    // Instead of GROUP BY (key0, ..., keyN-1) producing many rows, builds per-group HashSets
    // directly during DocValues iteration. For 2 keys: outputs compact pages + attached
    // HashSets for the coordinator to union across shards. For 3+ keys: outputs full dedup
    // tuples for the coordinator's generic merge path.
    if (scanFactory == null
        && effectivePlan instanceof AggregationNode aggDedupNode
        && aggDedupNode.getStep() == AggregationNode.Step.PARTIAL
```

---

### `executeDistinctValuesScanWithRawSet` (lines ~2842–2870)

```java
  /**
   * Execute a distinct-values scan for a single numeric column and return the raw LongOpenHashSet
   * as an attachment on the response. Avoids building a Page with millions of entries — the
   * coordinator merges the raw sets directly.
   */
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

### `executeDistinctValuesScanVarcharWithRawSet` (lines ~2872–2900)

```java
  /**
   * Execute a distinct-values scan for a single VARCHAR column and return the raw string HashSet as
   * an attachment on the response. Avoids building a Page with thousands of strings.
   */
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
