/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.dqe.shard.transport;

import io.trino.spi.Page;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.type.BigintType;
import io.trino.spi.type.DoubleType;
import io.trino.spi.type.Type;
import java.io.ByteArrayInputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.apache.lucene.index.ReaderUtil;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Query;
import org.opensearch.action.ActionRequest;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.HandledTransportAction;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.inject.Inject;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.common.io.stream.InputStreamStreamInput;
import org.opensearch.index.shard.IndexShard;
import org.opensearch.indices.IndicesService;
import org.opensearch.sql.dqe.common.config.DqeSettings;
import org.opensearch.sql.dqe.coordinator.metadata.OpenSearchMetadata;
import org.opensearch.sql.dqe.coordinator.metadata.TableInfo;
import org.opensearch.sql.dqe.coordinator.metadata.TableInfo.ColumnInfo;
import org.opensearch.sql.dqe.function.BuiltinFunctions;
import org.opensearch.sql.dqe.function.FunctionRegistry;
import org.opensearch.sql.dqe.function.expression.BlockExpression;
import org.opensearch.sql.dqe.function.expression.ExpressionCompiler;
import org.opensearch.sql.dqe.operator.Operator;
import org.opensearch.sql.dqe.planner.plan.AggregationNode;
import org.opensearch.sql.dqe.planner.plan.DqePlanNode;
import org.opensearch.sql.dqe.planner.plan.DqePlanVisitor;
import org.opensearch.sql.dqe.planner.plan.EvalNode;
import org.opensearch.sql.dqe.planner.plan.FilterNode;
import org.opensearch.sql.dqe.planner.plan.LimitNode;
import org.opensearch.sql.dqe.planner.plan.ProjectNode;
import org.opensearch.sql.dqe.planner.plan.SortNode;
import org.opensearch.sql.dqe.planner.plan.TableScanNode;
import org.opensearch.sql.dqe.shard.executor.LocalExecutionPlanner;
import org.opensearch.sql.dqe.shard.source.ColumnHandle;
import org.opensearch.sql.dqe.shard.source.FusedGroupByAggregate;
import org.opensearch.sql.dqe.shard.source.FusedScanAggregate;
import org.opensearch.sql.dqe.shard.source.LucenePageSource;
import org.opensearch.sql.dqe.shard.source.LuceneQueryCompiler;
import org.opensearch.sql.dqe.trino.parser.DqeSqlParser;
import org.opensearch.tasks.Task;
import org.opensearch.transport.TransportService;
import org.opensearch.transport.client.node.NodeClient;

/**
 * Transport action that executes a DQE plan fragment on a shard. The coordinator sends a serialized
 * plan fragment to this action, which deserializes it, builds an operator pipeline via {@link
 * LocalExecutionPlanner}, drains all pages, and returns the result as serialized Trino Pages.
 *
 * <p>The production {@link Inject @Inject} constructor takes only standard Guice-injectable
 * dependencies ({@link NodeClient}, {@link ClusterService}). The scan factory and column type map
 * are built dynamically inside {@link #doExecute} from the request's index metadata, so no custom
 * Guice bindings are required.
 */
public class TransportShardExecuteAction
    extends HandledTransportAction<ActionRequest, ShardExecuteResponse> {

  /** Thread pool name for shard-level DQE execution. */
  public static final String DQE_THREAD_POOL_NAME = "dqe-shard-executor";

  /**
   * Cached resolved metadata per index. When 8 shards of the same index execute concurrently, the
   * first one resolves TableInfo / type maps / field type maps and the rest reuse the cached
   * result. The cache is small (typically one entry per active query) and entries are evicted on
   * each new query via metadata version check.
   */
  private static final ConcurrentHashMap<String, CachedIndexMeta> INDEX_META_CACHE =
      new ConcurrentHashMap<>();

  /**
   * Cache for compiled Lucene queries. When 8 shards of the same index execute concurrently with
   * the same DSL filter, only the first shard compiles the filter; the rest reuse the compiled
   * Query. The cache is small (typically one entry) and is cleared after each batch of queries by
   * reusing the same key. Thread-safe via ConcurrentHashMap.
   */
  private static final ConcurrentHashMap<String, Query> LUCENE_QUERY_CACHE =
      new ConcurrentHashMap<>();

  /** Holder for pre-computed index metadata used by shard execution. */
  private record CachedIndexMeta(
      TableInfo tableInfo,
      Map<String, Type> columnTypeMap,
      Map<String, String> fieldTypeMap,
      long metadataVersion) {}

  /** ClusterService for resolving index metadata (production path). */
  private final ClusterService clusterService;

  /** IndicesService for resolving IndexShard (production path, Lucene native reader). */
  private final IndicesService indicesService;

  /**
   * Scan factory supplied directly (test path only). When non-null, the action uses this factory
   * and the companion {@link #columnTypeMap} instead of building them from cluster metadata.
   */
  private final Function<TableScanNode, Operator> scanFactory;

  /** Column type map supplied directly (test path only). */
  private final Map<String, Type> columnTypeMap;

  /**
   * Production constructor for plugin wiring with Guice dependency injection. All parameters are
   * standard OpenSearch injectable types. Execution is routed to the {@value #DQE_THREAD_POOL_NAME}
   * thread pool.
   *
   * @param transportService the transport service
   * @param actionFilters action filters
   * @param client the node-local client for executing search requests
   * @param clusterService cluster service for resolving index metadata
   * @param indicesService indices service for resolving IndexShard
   */
  @Inject
  public TransportShardExecuteAction(
      TransportService transportService,
      ActionFilters actionFilters,
      NodeClient client,
      ClusterService clusterService,
      IndicesService indicesService) {
    super(
        ShardExecuteAction.NAME,
        transportService,
        actionFilters,
        ShardExecuteRequest::new,
        DQE_THREAD_POOL_NAME);
    this.clusterService = clusterService;
    this.indicesService = indicesService;
    this.scanFactory = null;
    this.columnTypeMap = null;
  }

  /**
   * Test constructor that accepts a scan factory and column type map directly, bypassing the need
   * for a real NodeClient and ClusterService.
   *
   * @param transportService the transport service
   * @param actionFilters action filters
   * @param scanFactory factory that creates a leaf Operator for a given TableScanNode
   * @param columnTypeMap mapping from column name to Trino Type
   */
  TransportShardExecuteAction(
      TransportService transportService,
      ActionFilters actionFilters,
      Function<TableScanNode, Operator> scanFactory,
      Map<String, Type> columnTypeMap) {
    super(ShardExecuteAction.NAME, transportService, actionFilters, ShardExecuteRequest::new);
    this.clusterService = null;
    this.indicesService = null;
    this.scanFactory = scanFactory;
    this.columnTypeMap = columnTypeMap;
  }

  /**
   * Execute a shard plan fragment locally without serialization/deserialization overhead. This is
   * called by the coordinator when all shards are on the same node, bypassing the transport layer
   * entirely. The plan node is passed directly (not serialized), and the response Pages are
   * returned as-is (not serialized to bytes and back).
   *
   * @param plan the deserialized plan fragment (already constructed, no need to deserialize)
   * @param req the shard execute request (contains index name, shard ID, timeout)
   * @return the shard execution result with pages and column types
   * @throws Exception if execution fails
   */
  public ShardExecuteResponse executeLocal(DqePlanNode plan, ShardExecuteRequest req)
      throws Exception {
    return executePlan(plan, req);
  }

  /**
   * Core plan execution logic shared by both the transport path ({@link #doExecute}) and the
   * local-node shortcut ({@link #executeLocal}). Handles all fused paths, pipeline construction,
   * and result collection.
   */
  private ShardExecuteResponse executePlan(DqePlanNode plan, ShardExecuteRequest req)
      throws Exception {
    // Try short-circuit for scalar COUNT(*) — avoids pipeline construction entirely
    if (scanFactory == null && isScalarCountStar(plan)) {
      List<Page> pages = executeScalarCountStar(plan, req);
      List<Type> columnTypes = List.of(BigintType.BIGINT);
      return new ShardExecuteResponse(pages, columnTypes);
    }

    // Fast path: Lucene-native sorted scan for LimitNode -> [ProjectNode] -> SortNode ->
    // TableScanNode patterns. Uses IndexSearcher.search(query, topN, Sort) which leverages
    // Lucene's early-termination and segment-level competition to find the top N docs
    // without scanning all matching docs. Critical for queries like:
    //   SELECT col FROM t WHERE col <> '' ORDER BY col LIMIT 10
    // where millions of docs match but only the first N in sort order are needed.
    if (scanFactory == null) {
      SortedScanSpec sortedSpec = extractSortedScanSpec(plan);
      if (sortedSpec != null) {
        List<Page> pages = executeSortedScan(sortedSpec, req);
        List<Type> columnTypes =
            resolveColumnTypes(plan, getOrBuildIndexMeta(findIndexName(plan)).columnTypeMap());
        return new ShardExecuteResponse(pages, columnTypes);
      }
    }

    // Try fused scan-aggregate for scalar aggregations (no GROUP BY)
    if (scanFactory == null
        && plan instanceof AggregationNode aggNode
        && FusedScanAggregate.canFuse(aggNode)) {
      List<Page> pages = executeFusedScanAggregate(aggNode, req);
      List<Type> columnTypes =
          FusedScanAggregate.resolveOutputTypes(
              aggNode, getOrBuildIndexMeta(findIndexName(plan)).columnTypeMap());
      return new ShardExecuteResponse(pages, columnTypes);
    }

    // Fast path: bare TableScanNode with single numeric column — pre-dedup for COUNT(DISTINCT).
    // In the SINGLE aggregation path, the PlanFragmenter strips the AggregationNode, leaving a
    // bare TableScanNode. For scalar COUNT(DISTINCT numericCol), the shard deduplicates locally
    // (~25K distinct values instead of ~125K raw rows), reducing coordinator merge work by ~5x.
    if (scanFactory == null && isBareSingleNumericColumnScan(plan)) {
      List<Page> pages = executeDistinctValuesScan(plan, req);
      List<Type> columnTypes = List.of(BigintType.BIGINT);
      return new ShardExecuteResponse(pages, columnTypes);
    }

    // Fast path: bare TableScanNode with single VARCHAR column — pre-dedup for COUNT(DISTINCT).
    // Uses ordinal-based dedup via FixedBitSet for fast ordinal collection, then resolves strings
    // in bulk from the term dictionary. Sends ~50K unique strings instead of ~125K raw rows.
    if (scanFactory == null && isBareSingleVarcharColumnScan(plan)) {
      List<Page> pages = executeDistinctValuesScanVarchar(plan, req);
      List<Type> columnTypes = List.of(io.trino.spi.type.VarcharType.VARCHAR);
      return new ShardExecuteResponse(pages, columnTypes);
    }

    // Try fused eval-aggregate for SUM(col + constant) patterns
    if (scanFactory == null
        && plan instanceof AggregationNode aggEvalNode
        && FusedScanAggregate.canFuseWithEval(aggEvalNode)) {
      List<Page> pages = executeFusedEvalAggregate(aggEvalNode, req);
      List<Type> columnTypes = FusedScanAggregate.resolveEvalAggOutputTypes(aggEvalNode);
      return new ShardExecuteResponse(pages, columnTypes);
    }

    // Try fused ordinal-based GROUP BY for aggregations with string group keys
    if (scanFactory == null
        && plan instanceof AggregationNode aggGroupNode
        && FusedGroupByAggregate.canFuse(
            aggGroupNode, getOrBuildIndexMeta(findIndexName(plan)).columnTypeMap())) {
      List<Page> pages = executeFusedGroupByAggregate(aggGroupNode, req);
      List<Type> columnTypes =
          FusedGroupByAggregate.resolveOutputTypes(
              aggGroupNode, getOrBuildIndexMeta(findIndexName(plan)).columnTypeMap());
      return new ShardExecuteResponse(pages, columnTypes);
    }

    // Try ordinal-cached expression GROUP BY: AggregationNode -> EvalNode -> TableScanNode
    // where the group-by key is a computed expression (e.g., REGEXP_REPLACE) over a single
    // VARCHAR column. Pre-computes the expression once per unique ordinal (~16K evaluations
    // instead of ~921K), giving ~58x reduction in expression evaluations for Q29.
    if (scanFactory == null
        && plan instanceof AggregationNode aggExprNode
        && FusedGroupByAggregate.canFuseWithExpressionKey(
            aggExprNode, getOrBuildIndexMeta(findIndexName(plan)).columnTypeMap())) {
      List<Page> pages = executeFusedExprGroupByAggregate(aggExprNode, req);
      List<Type> columnTypes =
          resolveColumnTypes(plan, getOrBuildIndexMeta(findIndexName(plan)).columnTypeMap());
      return new ShardExecuteResponse(pages, columnTypes);
    }

    // Try fused GROUP BY with sort+limit: detect LimitNode -> [ProjectNode] -> SortNode ->
    // AggregationNode pattern and use FusedGroupByAggregate for the aggregation, then apply
    // sort+limit in-process. This avoids the generic operator pipeline (ScanOperator ->
    // HashAggregationOperator) which is much slower than the fused DocValues path.
    if (scanFactory == null) {
      AggregationNode innerAgg = extractAggFromSortedLimit(plan);
      if (innerAgg != null
          && FusedGroupByAggregate.canFuse(
              innerAgg, getOrBuildIndexMeta(findIndexName(plan)).columnTypeMap())) {
        Map<String, Type> colTypeMap = getOrBuildIndexMeta(findIndexName(plan)).columnTypeMap();
        // Apply sort+limit using SortOperator on the fused result
        SortNode sortNode = extractSortNode(plan);
        LimitNode limitNode = extractLimitNode(plan);
        if (sortNode != null && limitNode != null) {
          // Resolve sort column indices against the aggregation output columns
          List<String> aggOutputColumns = new ArrayList<>();
          aggOutputColumns.addAll(innerAgg.getGroupByKeys());
          aggOutputColumns.addAll(innerAgg.getAggregateFunctions());
          List<Integer> sortIndices = new ArrayList<>();
          for (String sortKey : sortNode.getSortKeys()) {
            int idx = aggOutputColumns.indexOf(sortKey);
            if (idx < 0) {
              sortIndices = null;
              break;
            }
            sortIndices.add(idx);
          }
          if (sortIndices != null) {
            long topN = limitNode.getCount() + limitNode.getOffset();
            int numGroupByCols = innerAgg.getGroupByKeys().size();

            // Try fused top-N: when sorting by a single aggregate column (BigintType),
            // the top-N selection can be done directly on the flat accData array inside
            // FusedGroupByAggregate, avoiding full ordinal resolution and Page construction
            // for all groups. This is critical for high-cardinality GROUP BY with small LIMIT.
            if (sortIndices.size() == 1 && sortIndices.get(0) >= numGroupByCols) {
              int sortAggIndex = sortIndices.get(0) - numGroupByCols;
              boolean sortAsc = sortNode.getAscending().get(0);
              List<Page> aggPages =
                  executeFusedGroupByAggregateWithTopN(innerAgg, req, sortAggIndex, sortAsc, topN);
              List<Type> aggColumnTypes =
                  FusedGroupByAggregate.resolveOutputTypes(innerAgg, colTypeMap);
              // The pages are already sorted by the fused path — apply project if needed
              List<Page> sortedPages = aggPages;
              ProjectNode projNode = extractProjectNode(plan);
              if (projNode != null) {
                List<String> projColumns = projNode.getOutputColumns();
                if (!projColumns.equals(aggOutputColumns)) {
                  List<Integer> projIndices = new ArrayList<>();
                  for (String col : projColumns) {
                    int idx = aggOutputColumns.indexOf(col);
                    projIndices.add(idx >= 0 ? idx : 0);
                  }
                  List<Page> projectedPages = new ArrayList<>();
                  for (Page sp : sortedPages) {
                    Block[] newBlocks = new Block[projIndices.size()];
                    for (int i = 0; i < projIndices.size(); i++) {
                      newBlocks[i] = sp.getBlock(projIndices.get(i));
                    }
                    projectedPages.add(new Page(newBlocks));
                  }
                  sortedPages = projectedPages;
                  List<Type> projTypes = new ArrayList<>();
                  for (int idx : projIndices) {
                    projTypes.add(aggColumnTypes.get(idx));
                  }
                  aggColumnTypes = projTypes;
                }
              }
              return new ShardExecuteResponse(sortedPages, aggColumnTypes);
            }

            // Fallback: aggregation (with optional shard-level top-N pre-filter) + SortOperator.
            // When the primary sort key is an aggregate column, use top-N to reduce the
            // output size before the SortOperator applies the full multi-key sort. This is
            // critical for high-cardinality GROUP BY with small LIMIT (e.g., Q33 with ~100M
            // groups where only the top-10 by COUNT(*) matter). The SortOperator will then
            // apply the full multi-key sort on the reduced set for correct output ordering.
            List<Page> aggPages;
            if (sortIndices.get(0) >= numGroupByCols) {
              int primarySortAggIndex = sortIndices.get(0) - numGroupByCols;
              boolean primarySortAsc = sortNode.getAscending().get(0);
              aggPages =
                  executeFusedGroupByAggregateWithTopN(
                      innerAgg, req, primarySortAggIndex, primarySortAsc, topN);
            } else {
              aggPages = executeFusedGroupByAggregate(innerAgg, req);
            }
            List<Type> aggColumnTypes =
                FusedGroupByAggregate.resolveOutputTypes(innerAgg, colTypeMap);
            if (!aggPages.isEmpty()) {
              org.opensearch.sql.dqe.operator.SortOperator sortOp =
                  new org.opensearch.sql.dqe.operator.SortOperator(
                      new org.opensearch.sql.dqe.operator.Operator() {
                        int idx = 0;

                        @Override
                        public Page processNextBatch() {
                          return idx < aggPages.size() ? aggPages.get(idx++) : null;
                        }

                        @Override
                        public void close() {}
                      },
                      sortIndices,
                      sortNode.getAscending(),
                      sortNode.getNullsFirst(),
                      aggColumnTypes,
                      topN);
              List<Page> sortedPages = new ArrayList<>();
              Page p;
              while ((p = sortOp.processNextBatch()) != null) {
                sortedPages.add(p);
              }
              ProjectNode projNode = extractProjectNode(plan);
              if (projNode != null) {
                List<String> projColumns = projNode.getOutputColumns();
                if (!projColumns.equals(aggOutputColumns)) {
                  List<Integer> projIndices = new ArrayList<>();
                  for (String col : projColumns) {
                    int idx = aggOutputColumns.indexOf(col);
                    projIndices.add(idx >= 0 ? idx : 0);
                  }
                  List<Page> projectedPages = new ArrayList<>();
                  for (Page sp : sortedPages) {
                    Block[] newBlocks = new Block[projIndices.size()];
                    for (int i = 0; i < projIndices.size(); i++) {
                      newBlocks[i] = sp.getBlock(projIndices.get(i));
                    }
                    projectedPages.add(new Page(newBlocks));
                  }
                  sortedPages = projectedPages;
                  List<Type> projTypes = new ArrayList<>();
                  for (int idx : projIndices) {
                    projTypes.add(aggColumnTypes.get(idx));
                  }
                  aggColumnTypes = projTypes;
                }
              }
              return new ShardExecuteResponse(sortedPages, aggColumnTypes);
            }
          }
        }
      }
    }

    // Fast path: LimitNode -> AggregationNode (no Sort) with FusedGroupByAggregate.
    // For queries like Q18 (GROUP BY UserID, SearchPhrase LIMIT 10), this avoids building
    // the full output Page for all groups and instead returns just the first N groups.
    if (scanFactory == null) {
      AggregationNode limitedAgg = extractAggFromLimit(plan);
      if (limitedAgg != null
          && FusedGroupByAggregate.canFuse(
              limitedAgg, getOrBuildIndexMeta(findIndexName(plan)).columnTypeMap())) {
        LimitNode limitNode = extractLimitNode(plan);
        if (limitNode != null) {
          long topN = limitNode.getCount() + limitNode.getOffset();
          List<Page> aggPages =
              executeFusedGroupByAggregateWithTopN(limitedAgg, req, -1, false, topN);
          Map<String, Type> colTypeMap = getOrBuildIndexMeta(findIndexName(plan)).columnTypeMap();
          List<Type> aggColumnTypes =
              FusedGroupByAggregate.resolveOutputTypes(limitedAgg, colTypeMap);
          return new ShardExecuteResponse(aggPages, aggColumnTypes);
        }
      }
    }

    // Resolve scan factory and column types
    final Function<TableScanNode, Operator> effectiveScanFactory;
    final Map<String, Type> effectiveColumnTypeMap;

    if (scanFactory != null) {
      effectiveScanFactory = scanFactory;
      effectiveColumnTypeMap = columnTypeMap != null ? columnTypeMap : Map.of();
    } else {
      String indexName = findIndexName(plan);
      CachedIndexMeta cachedMeta = getOrBuildIndexMeta(indexName);
      effectiveColumnTypeMap = cachedMeta.columnTypeMap();

      Settings nodeSettings = clusterService.getSettings();
      int batchSize = DqeSettings.PAGE_BATCH_SIZE.get(nodeSettings);
      effectiveScanFactory =
          buildLuceneScanFactory(
              req,
              cachedMeta.tableInfo(),
              effectiveColumnTypeMap,
              cachedMeta.fieldTypeMap(),
              batchSize);
    }

    // Build operator pipeline
    LocalExecutionPlanner planner =
        new LocalExecutionPlanner(effectiveScanFactory, effectiveColumnTypeMap);
    Operator pipeline = plan.accept(planner, null);

    // Execute: drain pages
    List<Page> pages = new ArrayList<>();
    Page page;
    while ((page = pipeline.processNextBatch()) != null) {
      pages.add(page);
    }
    pipeline.close();

    // Resolve column types from the plan
    List<Type> columnTypes = resolveColumnTypes(plan, effectiveColumnTypeMap);

    return new ShardExecuteResponse(pages, columnTypes);
  }

  @Override
  protected void doExecute(
      Task task, ActionRequest request, ActionListener<ShardExecuteResponse> listener) {
    ShardExecuteRequest req = ShardExecuteRequest.fromActionRequest(request);
    try {
      // 1. Deserialize plan fragment
      DqePlanNode plan =
          DqePlanNode.readPlanNode(
              new InputStreamStreamInput(new ByteArrayInputStream(req.getSerializedFragment())));

      // 2. Execute the plan using shared logic
      ShardExecuteResponse response = executePlan(plan, req);
      listener.onResponse(response);
    } catch (Exception e) {
      listener.onFailure(e);
    }
  }

  /**
   * Build a scan factory that creates a {@link LucenePageSource} for the given shard. Reads doc
   * values directly from Lucene segments instead of using the scroll API.
   *
   * @param req the shard execute request
   * @param tableInfo table metadata including OpenSearch field types
   * @param typeMap mapping from column name to Trino Type
   * @param fieldTypeMap pre-computed field name to OS type string mapping
   * @param batchSize number of rows per page
   */
  private Function<TableScanNode, Operator> buildLuceneScanFactory(
      ShardExecuteRequest req,
      TableInfo tableInfo,
      Map<String, Type> typeMap,
      Map<String, String> fieldTypeMap,
      int batchSize) {
    return node -> {
      // 1. Resolve IndexShard
      IndexMetadata indexMeta = clusterService.state().metadata().index(node.getIndexName());
      IndexShard shard =
          indicesService.indexService(indexMeta.getIndex()).getShard(req.getShardId());

      // 2. Compile Lucene query from DSL filter (cached across concurrent shard executions)
      Query query = compileOrCacheLuceneQuery(node.getDslFilter(), fieldTypeMap);

      // 3. Build column handles
      List<ColumnHandle> columns =
          node.getColumns().stream()
              .map(col -> new ColumnHandle(col, typeMap.getOrDefault(col, BigintType.BIGINT)))
              .collect(Collectors.toList());

      // 4. Create LucenePageSource
      return new LucenePageSource(shard, query, columns, batchSize);
    };
  }

  /**
   * Get or build cached index metadata. When multiple shards of the same index execute
   * concurrently, only the first one resolves TableInfo and builds type maps; the rest reuse the
   * cached result. The cache is invalidated when the cluster metadata version changes.
   */
  private CachedIndexMeta getOrBuildIndexMeta(String indexName) {
    long currentVersion = clusterService.state().metadata().version();
    CachedIndexMeta cached = INDEX_META_CACHE.get(indexName);
    if (cached != null && cached.metadataVersion() == currentVersion) {
      return cached;
    }
    // Build fresh metadata
    OpenSearchMetadata metadata = new OpenSearchMetadata(clusterService);
    TableInfo tableInfo = metadata.getTableInfo(indexName);
    Map<String, Type> typeMap =
        tableInfo.columns().stream()
            .collect(Collectors.toMap(ColumnInfo::name, ColumnInfo::trinoType));
    Map<String, String> fieldTypeMap =
        tableInfo.columns().stream()
            .collect(Collectors.toMap(ColumnInfo::name, ColumnInfo::openSearchType));
    CachedIndexMeta fresh = new CachedIndexMeta(tableInfo, typeMap, fieldTypeMap, currentVersion);
    INDEX_META_CACHE.put(indexName, fresh);
    return fresh;
  }

  /**
   * Compile a Lucene query from a DSL filter string, caching the result so that concurrent shard
   * executions on the same node reuse the compiled query. The DSL filter string itself is used as
   * the cache key. The cache is bounded (size 1 effectively) since all concurrent shards share the
   * same filter. Thread-safe via ConcurrentHashMap.computeIfAbsent.
   *
   * @param dslFilter the OpenSearch DSL filter JSON string, or null for match-all
   * @param fieldTypeMap field name to OS type string mapping for query compilation
   * @return compiled Lucene Query
   */
  private Query compileOrCacheLuceneQuery(String dslFilter, Map<String, String> fieldTypeMap) {
    if (dslFilter == null) {
      return new MatchAllDocsQuery();
    }
    // Evict stale entries when cache grows beyond a reasonable size (one per unique query)
    if (LUCENE_QUERY_CACHE.size() > 100) {
      LUCENE_QUERY_CACHE.clear();
    }
    return LUCENE_QUERY_CACHE.computeIfAbsent(
        dslFilter, filter -> new LuceneQueryCompiler(fieldTypeMap).compile(filter));
  }

  /**
   * Check if the shard plan is a scalar COUNT(*) pattern: AggregationNode(PARTIAL, groupBy=[],
   * aggs=["count(*)"]) -> TableScanNode with empty or no columns. This pattern can be
   * short-circuited with a direct Lucene count.
   */
  static boolean isScalarCountStar(DqePlanNode plan) {
    if (!(plan instanceof AggregationNode aggNode)) {
      return false;
    }
    if (!aggNode.getGroupByKeys().isEmpty()) {
      return false;
    }
    List<String> aggs = aggNode.getAggregateFunctions();
    if (aggs.size() != 1) {
      return false;
    }
    if (!"count(*)".equals(aggs.get(0).toLowerCase(Locale.ROOT))) {
      return false;
    }
    return aggNode.getChild() instanceof TableScanNode;
  }

  /**
   * Execute a scalar COUNT(*) by directly calling searcher.count(query), bypassing the full
   * pipeline construction (FunctionRegistry, LocalExecutionPlanner, operator chain). Returns a
   * single-row Page with the count value.
   */
  private List<Page> executeScalarCountStar(DqePlanNode plan, ShardExecuteRequest req)
      throws Exception {
    AggregationNode aggNode = (AggregationNode) plan;
    TableScanNode scanNode = (TableScanNode) aggNode.getChild();

    // Resolve IndexShard
    IndexMetadata indexMeta = clusterService.state().metadata().index(scanNode.getIndexName());
    IndexShard shard = indicesService.indexService(indexMeta.getIndex()).getShard(req.getShardId());

    // Compile Lucene query from DSL filter (cached across concurrent shard executions)
    CachedIndexMeta cachedMeta2 = getOrBuildIndexMeta(scanNode.getIndexName());
    Query luceneQuery =
        compileOrCacheLuceneQuery(scanNode.getDslFilter(), cachedMeta2.fieldTypeMap());

    // Execute count directly
    long count;
    try (org.opensearch.index.engine.Engine.Searcher engineSearcher =
        shard.acquireSearcher("dqe-count-star")) {
      count = engineSearcher.count(luceneQuery);
    }

    // Build single-row result Page with the count
    BlockBuilder builder = BigintType.BIGINT.createBlockBuilder(null, 1);
    BigintType.BIGINT.writeLong(builder, count);
    Block block = builder.build();
    return List.of(new Page(block));
  }

  /**
   * Execute a fused scan-aggregate by aggregating directly from Lucene DocValues without building
   * intermediate Trino Pages. This is used for scalar aggregations (no GROUP BY) like SUM(col),
   * AVG(col), MIN(col), MAX(col), COUNT(DISTINCT col), and combinations thereof.
   */
  private List<Page> executeFusedScanAggregate(AggregationNode aggNode, ShardExecuteRequest req)
      throws Exception {
    TableScanNode scanNode = (TableScanNode) aggNode.getChild();
    String indexName = scanNode.getIndexName();
    CachedIndexMeta cachedMeta = getOrBuildIndexMeta(indexName);

    // Resolve IndexShard
    IndexMetadata indexMeta = clusterService.state().metadata().index(indexName);
    IndexShard shard = indicesService.indexService(indexMeta.getIndex()).getShard(req.getShardId());

    // Compile Lucene query (cached across concurrent shard executions)
    Query luceneQuery =
        compileOrCacheLuceneQuery(scanNode.getDslFilter(), cachedMeta.fieldTypeMap());

    return FusedScanAggregate.execute(aggNode, shard, luceneQuery, cachedMeta.columnTypeMap());
  }

  /**
   * Execute a fused eval-aggregate using the algebraic shortcut for SUM(col + constant) patterns.
   * Reads each unique physical column once from DocValues and derives all results using the
   * identity: SUM(col + k) = SUM(col) + k * COUNT(*).
   */
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

  /**
   * Execute a fused ordinal-based GROUP BY aggregation using SortedSetDocValues ordinals as hash
   * keys during grouping. This avoids the expensive lookupOrd() per row, deferring string
   * resolution to the final output phase.
   */
  private List<Page> executeFusedGroupByAggregate(AggregationNode aggNode, ShardExecuteRequest req)
      throws Exception {
    // Walk through optional EvalNode to find the TableScanNode
    TableScanNode scanNode = findTableScanNode(aggNode);
    String indexName = scanNode.getIndexName();
    CachedIndexMeta cachedMeta = getOrBuildIndexMeta(indexName);

    // Resolve IndexShard
    IndexMetadata indexMeta = clusterService.state().metadata().index(indexName);
    IndexShard shard = indicesService.indexService(indexMeta.getIndex()).getShard(req.getShardId());

    // Compile Lucene query (cached across concurrent shard executions)
    Query luceneQuery =
        compileOrCacheLuceneQuery(scanNode.getDslFilter(), cachedMeta.fieldTypeMap());

    return FusedGroupByAggregate.execute(aggNode, shard, luceneQuery, cachedMeta.columnTypeMap());
  }

  /**
   * Execute ordinal-cached expression GROUP BY aggregation. Pre-computes the group-by expression
   * once per unique ordinal in SortedSetDocValues, then uses the cached result during the scan.
   * This is critical for queries like Q29 where REGEXP_REPLACE on a VARCHAR column is the GROUP BY
   * key: ~16K ordinals vs ~921K docs = ~58x reduction in regex evaluations.
   */
  private List<Page> executeFusedExprGroupByAggregate(
      AggregationNode aggNode, ShardExecuteRequest req) throws Exception {
    TableScanNode scanNode = findTableScanNode(aggNode);
    String indexName = scanNode.getIndexName();
    CachedIndexMeta cachedMeta = getOrBuildIndexMeta(indexName);

    IndexMetadata indexMeta = clusterService.state().metadata().index(indexName);
    IndexShard shard = indicesService.indexService(indexMeta.getIndex()).getShard(req.getShardId());

    Query luceneQuery =
        compileOrCacheLuceneQuery(scanNode.getDslFilter(), cachedMeta.fieldTypeMap());

    return FusedGroupByAggregate.executeWithExpressionKey(
        aggNode, shard, luceneQuery, cachedMeta.columnTypeMap());
  }

  /**
   * Execute fused GROUP BY aggregation with an inline top-N selection. Instead of building a full
   * Page for all groups and then sorting, the top-N selection happens directly on the flat
   * accumulator data inside FusedGroupByAggregate. This avoids ordinal resolution and Page
   * construction for groups outside the top-N, critical for high-cardinality GROUP BY with small
   * LIMIT (e.g., Q17: ~98K groups with LIMIT 10).
   */
  private List<Page> executeFusedGroupByAggregateWithTopN(
      AggregationNode aggNode,
      ShardExecuteRequest req,
      int sortAggIndex,
      boolean sortAscending,
      long topN)
      throws Exception {
    TableScanNode scanNode = findTableScanNode(aggNode);
    String indexName = scanNode.getIndexName();
    CachedIndexMeta cachedMeta = getOrBuildIndexMeta(indexName);

    IndexMetadata indexMeta = clusterService.state().metadata().index(indexName);
    IndexShard shard = indicesService.indexService(indexMeta.getIndex()).getShard(req.getShardId());

    Query luceneQuery =
        compileOrCacheLuceneQuery(scanNode.getDslFilter(), cachedMeta.fieldTypeMap());

    return FusedGroupByAggregate.executeWithTopN(
        aggNode, shard, luceneQuery, cachedMeta.columnTypeMap(), sortAggIndex, sortAscending, topN);
  }

  /**
   * Check if the shard plan is a bare TableScanNode with exactly one numeric (long-representable)
   * column. This pattern results from the PlanFragmenter stripping a SINGLE COUNT(DISTINCT col)
   * aggregation. In this case, the shard pre-deduplicates values locally to reduce coordinator
   * merge work.
   */
  private boolean isBareSingleNumericColumnScan(DqePlanNode plan) {
    if (!(plan instanceof TableScanNode scanNode)) {
      return false;
    }
    List<String> columns = scanNode.getColumns();
    if (columns.size() != 1) {
      return false;
    }
    CachedIndexMeta cachedMeta = getOrBuildIndexMeta(scanNode.getIndexName());
    Type colType = cachedMeta.columnTypeMap().get(columns.get(0));
    return colType instanceof BigintType
        || colType instanceof io.trino.spi.type.IntegerType
        || colType instanceof io.trino.spi.type.SmallintType
        || colType instanceof io.trino.spi.type.TinyintType
        || colType instanceof io.trino.spi.type.TimestampType;
  }

  /**
   * Check if the shard plan is a bare TableScanNode with exactly one VARCHAR column. This pattern
   * results from the PlanFragmenter stripping a SINGLE COUNT(DISTINCT col) aggregation for varchar
   * columns. In this case, the shard pre-deduplicates values using ordinal-based dedup.
   */
  private boolean isBareSingleVarcharColumnScan(DqePlanNode plan) {
    if (!(plan instanceof TableScanNode scanNode)) {
      return false;
    }
    List<String> columns = scanNode.getColumns();
    if (columns.size() != 1) {
      return false;
    }
    CachedIndexMeta cachedMeta = getOrBuildIndexMeta(scanNode.getIndexName());
    Type colType = cachedMeta.columnTypeMap().get(columns.get(0));
    return colType instanceof io.trino.spi.type.VarcharType;
  }

  /**
   * Execute a fused distinct-values scan for VARCHAR: read SortedSetDocValues for the single column
   * and return only the distinct string values as a VarcharType Page. Uses ordinal-based dedup via
   * FixedBitSet for efficient collection.
   */
  private List<Page> executeDistinctValuesScanVarchar(DqePlanNode plan, ShardExecuteRequest req)
      throws Exception {
    TableScanNode scanNode = (TableScanNode) plan;
    String indexName = scanNode.getIndexName();
    String columnName = scanNode.getColumns().get(0);
    CachedIndexMeta cachedMeta = getOrBuildIndexMeta(indexName);

    // Resolve IndexShard
    IndexMetadata indexMeta = clusterService.state().metadata().index(indexName);
    IndexShard shard = indicesService.indexService(indexMeta.getIndex()).getShard(req.getShardId());

    // Compile Lucene query (cached across concurrent shard executions)
    Query luceneQuery =
        compileOrCacheLuceneQuery(scanNode.getDslFilter(), cachedMeta.fieldTypeMap());

    return FusedScanAggregate.executeDistinctValuesVarchar(columnName, shard, luceneQuery);
  }

  /**
   * Execute a fused distinct-values scan: read DocValues for the single column in the TableScanNode
   * and return only the distinct values as a Page. This drastically reduces the data sent to the
   * coordinator by pre-deduplicating at the shard level.
   */
  private List<Page> executeDistinctValuesScan(DqePlanNode plan, ShardExecuteRequest req)
      throws Exception {
    TableScanNode scanNode = (TableScanNode) plan;
    String indexName = scanNode.getIndexName();
    String columnName = scanNode.getColumns().get(0);
    CachedIndexMeta cachedMeta = getOrBuildIndexMeta(indexName);

    // Resolve IndexShard
    IndexMetadata indexMeta = clusterService.state().metadata().index(indexName);
    IndexShard shard = indicesService.indexService(indexMeta.getIndex()).getShard(req.getShardId());

    // Compile Lucene query (cached across concurrent shard executions)
    Query luceneQuery =
        compileOrCacheLuceneQuery(scanNode.getDslFilter(), cachedMeta.fieldTypeMap());

    return FusedScanAggregate.executeDistinctValues(columnName, shard, luceneQuery);
  }

  /**
   * Walk the plan tree to find the index name from the leaf {@link TableScanNode}.
   *
   * @throws IllegalStateException if no TableScanNode is found
   */
  static String findIndexName(DqePlanNode node) {
    if (node instanceof TableScanNode) {
      return ((TableScanNode) node).getIndexName();
    }
    List<DqePlanNode> children = node.getChildren();
    if (!children.isEmpty()) {
      return findIndexName(children.get(0));
    }
    throw new IllegalStateException("Plan tree contains no TableScanNode");
  }

  /**
   * Resolve column types from the plan node by walking the tree to determine output column names
   * and mapping them to types. For computed expression column names (e.g., arithmetic expressions
   * like "(count_long * price_double)"), the result type is inferred by compiling the expression
   * and checking the output type.
   */
  private List<Type> resolveColumnTypes(DqePlanNode node, Map<String, Type> typeMap) {
    List<String> columnNames = resolveColumnNames(node);

    // Check if any column name is a computed expression (not in the type map)
    boolean hasComputed = false;
    for (String col : columnNames) {
      if (!typeMap.containsKey(col)) {
        hasComputed = true;
        break;
      }
    }

    if (!hasComputed) {
      // Fast path: all columns are plain column references
      List<Type> types = new ArrayList<>();
      for (String col : columnNames) {
        types.add(typeMap.getOrDefault(col, BigintType.BIGINT));
      }
      return types;
    }

    // Find the EvalNode in the plan to get expression strings and their output column names
    EvalNode evalNode = findEvalNode(node);
    Map<String, String> columnNameToExpression = new HashMap<>();
    if (evalNode != null) {
      List<String> evalOutputNames = evalNode.getOutputColumnNames();
      List<String> evalExpressions = evalNode.getExpressions();
      for (int i = 0; i < evalOutputNames.size(); i++) {
        columnNameToExpression.put(evalOutputNames.get(i), evalExpressions.get(i));
      }
    }

    // Build column index and type maps for expression compilation
    TableScanNode scanNode = findTableScanNode(node);
    List<String> tableColumns = scanNode != null ? scanNode.getColumns() : List.of();
    Map<String, Integer> columnIndexMap = new HashMap<>();
    for (int i = 0; i < tableColumns.size(); i++) {
      columnIndexMap.put(tableColumns.get(i), i);
    }

    FunctionRegistry registry = BuiltinFunctions.createRegistry();
    ExpressionCompiler compiler = new ExpressionCompiler(registry, columnIndexMap, typeMap);
    DqeSqlParser exprParser = new DqeSqlParser();

    List<Type> types = new ArrayList<>();
    for (String col : columnNames) {
      if (typeMap.containsKey(col)) {
        types.add(typeMap.get(col));
      } else {
        // Try aggregate output type inference first (e.g., MIN(URL) → VarcharType)
        Type aggType = inferAggregateOutputType(col, typeMap);
        if (aggType != null) {
          types.add(aggType);
          continue;
        }
        // Try to infer the type by compiling the expression
        String exprStr = columnNameToExpression.getOrDefault(col, col);
        try {
          io.trino.sql.tree.Expression expr = exprParser.parseExpression(exprStr);
          BlockExpression blockExpr = compiler.compile(expr);
          types.add(blockExpr.getType());
        } catch (Exception e) {
          // If parsing/compilation fails, fall back to BIGINT
          types.add(BigintType.BIGINT);
        }
      }
    }
    return types;
  }

  /**
   * Infer the output type of an aggregate function expression like "count(*)", "min(URL)",
   * "sum(amount)". Returns null if the column name is not an aggregate expression.
   */
  private static final java.util.regex.Pattern SHARD_AGG_TYPE_PATTERN =
      java.util.regex.Pattern.compile(
          "^(COUNT|SUM|MIN|MAX|AVG)\\((DISTINCT\\s+)?(.+?)\\)$",
          java.util.regex.Pattern.CASE_INSENSITIVE);

  private static Type inferAggregateOutputType(String colName, Map<String, Type> columnTypeMap) {
    java.util.regex.Matcher m = SHARD_AGG_TYPE_PATTERN.matcher(colName);
    if (!m.matches()) {
      return null;
    }
    String funcName = m.group(1).toUpperCase(java.util.Locale.ROOT);
    String arg = m.group(3).trim();

    switch (funcName) {
      case "COUNT":
        return BigintType.BIGINT;
      case "AVG":
        return DoubleType.DOUBLE;
      case "SUM":
        {
          Type inputType = columnTypeMap.getOrDefault(arg, BigintType.BIGINT);
          return inputType instanceof DoubleType ? DoubleType.DOUBLE : BigintType.BIGINT;
        }
      case "MIN":
      case "MAX":
        return columnTypeMap.getOrDefault(arg, BigintType.BIGINT);
      default:
        return null;
    }
  }

  /** Walk the plan tree to find the EvalNode. Returns null if none. */
  private static EvalNode findEvalNode(DqePlanNode plan) {
    return plan.accept(
        new DqePlanVisitor<EvalNode, Void>() {
          @Override
          public EvalNode visitEval(EvalNode node, Void context) {
            return node;
          }

          @Override
          public EvalNode visitPlan(DqePlanNode node, Void context) {
            for (DqePlanNode child : node.getChildren()) {
              EvalNode result = child.accept(this, context);
              if (result != null) {
                return result;
              }
            }
            return null;
          }
        },
        null);
  }

  /** Walk the plan tree to find the TableScanNode. Returns null if none. */
  private static TableScanNode findTableScanNode(DqePlanNode plan) {
    return plan.accept(
        new DqePlanVisitor<TableScanNode, Void>() {
          @Override
          public TableScanNode visitTableScan(TableScanNode node, Void context) {
            return node;
          }

          @Override
          public TableScanNode visitPlan(DqePlanNode node, Void context) {
            for (DqePlanNode child : node.getChildren()) {
              TableScanNode result = child.accept(this, context);
              if (result != null) {
                return result;
              }
            }
            return null;
          }
        },
        null);
  }

  /**
   * Resolve column names from the root plan node. Walks down through unary nodes to find the
   * effective output column list.
   */
  static List<String> resolveColumnNames(DqePlanNode node) {
    if (node instanceof TableScanNode) {
      return ((TableScanNode) node).getColumns();
    }
    if (node instanceof ProjectNode) {
      return ((ProjectNode) node).getOutputColumns();
    }
    if (node instanceof AggregationNode) {
      AggregationNode agg = (AggregationNode) node;
      List<String> names = new ArrayList<>(agg.getGroupByKeys());
      names.addAll(agg.getAggregateFunctions());
      return names;
    }
    // For filter, limit, sort: delegate to child
    List<DqePlanNode> children = node.getChildren();
    if (!children.isEmpty()) {
      return resolveColumnNames(children.get(0));
    }
    return List.of();
  }

  /**
   * Extract the inner AggregationNode from a LimitNode -> [ProjectNode] -> SortNode ->
   * AggregationNode pattern. Returns null if the plan doesn't match this pattern.
   */
  private static AggregationNode extractAggFromSortedLimit(DqePlanNode plan) {
    if (!(plan instanceof LimitNode limit)) return null;
    DqePlanNode child = limit.getChild();
    if (child instanceof ProjectNode proj) child = proj.getChild();
    if (!(child instanceof SortNode sort)) return null;
    DqePlanNode sortChild = sort.getChild();
    if (sortChild instanceof AggregationNode agg) return agg;
    return null;
  }

  /** Extract the SortNode from a LimitNode -> [ProjectNode] -> SortNode -> ... pattern. */
  private static SortNode extractSortNode(DqePlanNode plan) {
    if (!(plan instanceof LimitNode limit)) return null;
    DqePlanNode child = limit.getChild();
    if (child instanceof ProjectNode proj) child = proj.getChild();
    if (child instanceof SortNode sort) return sort;
    return null;
  }

  /** Extract the LimitNode if the plan root is a LimitNode. */
  private static LimitNode extractLimitNode(DqePlanNode plan) {
    return plan instanceof LimitNode limit ? limit : null;
  }

  /** Extract the ProjectNode from a LimitNode -> ProjectNode -> ... pattern. */
  private static ProjectNode extractProjectNode(DqePlanNode plan) {
    if (!(plan instanceof LimitNode limit)) return null;
    if (limit.getChild() instanceof ProjectNode proj) return proj;
    return null;
  }

  /**
   * Extract AggregationNode from a LimitNode -> [ProjectNode ->] AggregationNode pattern (no
   * SortNode). This handles queries like Q18: GROUP BY ... LIMIT N without ORDER BY.
   */
  private static AggregationNode extractAggFromLimit(DqePlanNode plan) {
    if (!(plan instanceof LimitNode limit)) return null;
    DqePlanNode child = limit.getChild();
    if (child instanceof ProjectNode proj) child = proj.getChild();
    if (child instanceof AggregationNode agg) return agg;
    return null;
  }

  /**
   * Specification for a Lucene-native sorted scan: captures the plan components needed to execute a
   * query using {@code IndexSearcher.search(query, topN, Sort)}.
   */
  private record SortedScanSpec(
      TableScanNode scanNode,
      List<String> sortKeys,
      List<Boolean> ascending,
      List<Boolean> nullsFirst,
      long topN,
      List<String> outputColumns) {}

  /**
   * Extract a {@link SortedScanSpec} from a LimitNode -> [ProjectNode] -> SortNode -> [FilterNode]
   * -> TableScanNode pattern (no aggregation). Returns null if the pattern doesn't match or the
   * sort keys cannot be handled by Lucene's native Sort.
   *
   * <p>All sort keys must be physical columns available as Lucene SortField types. Computed
   * expressions are not supported.
   */
  private SortedScanSpec extractSortedScanSpec(DqePlanNode plan) {
    if (!(plan instanceof LimitNode limit)) return null;
    long topN = limit.getCount() + limit.getOffset();
    if (topN <= 0 || topN > 10000) return null; // Only use for small limits

    DqePlanNode child = limit.getChild();
    ProjectNode projNode = null;
    if (child instanceof ProjectNode proj) {
      projNode = proj;
      child = proj.getChild();
    }
    if (!(child instanceof SortNode sort)) return null;

    // The sort child must lead to TableScanNode (possibly through FilterNode)
    DqePlanNode sortChild = sort.getChild();
    if (sortChild instanceof FilterNode filterNode) {
      sortChild = filterNode.getChild();
    }
    if (!(sortChild instanceof TableScanNode scanNode)) return null;

    // All sort keys must be physical columns with sortable types
    CachedIndexMeta cachedMeta = getOrBuildIndexMeta(scanNode.getIndexName());
    Map<String, Type> typeMap = cachedMeta.columnTypeMap();
    Map<String, String> fieldTypeMap = cachedMeta.fieldTypeMap();
    for (String key : sort.getSortKeys()) {
      Type type = typeMap.get(key);
      if (type == null) return null; // Not a physical column (expression)
      String osType = fieldTypeMap.getOrDefault(key, "keyword");
      // Supported: keyword, long, integer, short, byte, date, double, float
      if (!isSortableFieldType(osType)) return null;
    }

    List<String> outputColumns =
        projNode != null ? projNode.getOutputColumns() : scanNode.getColumns();

    return new SortedScanSpec(
        scanNode,
        sort.getSortKeys(),
        sort.getAscending(),
        sort.getNullsFirst(),
        topN,
        outputColumns);
  }

  /** Check if an OpenSearch field type supports efficient Lucene-native sorting. */
  private static boolean isSortableFieldType(String osType) {
    return switch (osType) {
      case "keyword",
          "long",
          "integer",
          "short",
          "byte",
          "date",
          "double",
          "float",
          "half_float",
          "boolean" ->
          true;
      default -> false;
    };
  }

  /**
   * Execute a Lucene-native sorted scan using {@code IndexSearcher.search(query, topN, Sort)}.
   * Instead of collecting all matching docs and sorting in memory, this leverages Lucene's
   * TopFieldCollector with early termination and segment-level competition.
   *
   * @param spec the sorted scan specification
   * @param req the shard execute request
   * @return pages with the top N rows in sort order
   */
  private List<Page> executeSortedScan(SortedScanSpec spec, ShardExecuteRequest req)
      throws Exception {
    TableScanNode scanNode = spec.scanNode();
    String indexName = scanNode.getIndexName();
    CachedIndexMeta cachedMeta = getOrBuildIndexMeta(indexName);

    // Resolve IndexShard
    IndexMetadata indexMeta = clusterService.state().metadata().index(indexName);
    IndexShard shard = indicesService.indexService(indexMeta.getIndex()).getShard(req.getShardId());

    // Compile Lucene query
    Query luceneQuery =
        compileOrCacheLuceneQuery(scanNode.getDslFilter(), cachedMeta.fieldTypeMap());

    // Build Lucene Sort from sort keys
    org.apache.lucene.search.SortField[] sortFields =
        new org.apache.lucene.search.SortField[spec.sortKeys().size()];
    for (int i = 0; i < spec.sortKeys().size(); i++) {
      String key = spec.sortKeys().get(i);
      boolean asc = spec.ascending().get(i);
      boolean nf = spec.nullsFirst().get(i);
      String osType = cachedMeta.fieldTypeMap().getOrDefault(key, "keyword");
      sortFields[i] = buildLuceneSortField(key, osType, !asc, nf);
    }
    org.apache.lucene.search.Sort luceneSort = new org.apache.lucene.search.Sort(sortFields);

    int topN = (int) Math.min(spec.topN(), Integer.MAX_VALUE);

    // Execute sorted search
    try (org.opensearch.index.engine.Engine.Searcher engineSearcher =
        shard.acquireSearcher("dqe-sorted-scan")) {
      org.apache.lucene.search.TopFieldDocs topDocs =
          engineSearcher.search(luceneQuery, topN, luceneSort);

      if (topDocs.scoreDocs.length == 0) {
        return List.of();
      }

      // Read doc values for only the top N docs
      // Group by segment for efficient doc values access
      Map<String, Type> typeMap = cachedMeta.columnTypeMap();
      List<String> outputCols = spec.outputColumns();
      List<ColumnHandle> columns = new ArrayList<>();
      for (String col : outputCols) {
        columns.add(new ColumnHandle(col, typeMap.getOrDefault(col, BigintType.BIGINT)));
      }

      int numDocs = topDocs.scoreDocs.length;

      // Read doc values for only the top N docs, resolving per-segment.
      // TopFieldDocs returns global doc IDs in sort order. For each doc we:
      // 1. Resolve to segment + segment-local doc ID
      // 2. Read doc values directly (re-opening iterators per doc since N is small)
      List<org.apache.lucene.index.LeafReaderContext> leaves =
          engineSearcher.getIndexReader().leaves();

      io.trino.spi.block.BlockBuilder[] builders =
          new io.trino.spi.block.BlockBuilder[columns.size()];
      for (int c = 0; c < columns.size(); c++) {
        builders[c] = columns.get(c).type().createBlockBuilder(null, numDocs);
      }

      for (org.apache.lucene.search.ScoreDoc scoreDoc : topDocs.scoreDocs) {
        int globalDocId = scoreDoc.doc;
        int leafIdx = ReaderUtil.subIndex(globalDocId, leaves);
        org.apache.lucene.index.LeafReaderContext leaf = leaves.get(leafIdx);
        int segmentDocId = globalDocId - leaf.docBase;

        for (int c = 0; c < columns.size(); c++) {
          readSingleDocValue(leaf, segmentDocId, columns.get(c), builders[c]);
        }
      }

      Block[] blocks = new Block[builders.length];
      for (int i = 0; i < builders.length; i++) {
        blocks[i] = builders[i].build();
      }
      return List.of(new Page(blocks));
    }
  }

  /**
   * Build a Lucene SortField for the given column name and OpenSearch type. Uses {@code
   * SortedSetSortField} for keyword fields (which use SORTED_SET doc values) and standard {@code
   * SortField} for numeric types.
   */
  private static org.apache.lucene.search.SortField buildLuceneSortField(
      String fieldName, String osType, boolean reverse, boolean nullsFirst) {
    switch (osType) {
      case "keyword":
        {
          // Keyword fields use SORTED_SET doc values — must use SortedSetSortField
          org.apache.lucene.search.SortedSetSortField sf =
              new org.apache.lucene.search.SortedSetSortField(fieldName, reverse);
          if (nullsFirst) {
            sf.setMissingValue(org.apache.lucene.search.SortField.STRING_FIRST);
          } else {
            sf.setMissingValue(org.apache.lucene.search.SortField.STRING_LAST);
          }
          return sf;
        }
      case "long":
      case "date":
        {
          // OpenSearch stores numeric fields with SortedNumericDocValues
          org.apache.lucene.search.SortedNumericSortField sf =
              new org.apache.lucene.search.SortedNumericSortField(
                  fieldName, org.apache.lucene.search.SortField.Type.LONG, reverse);
          sf.setMissingValue(
              nullsFirst
                  ? (reverse ? Long.MIN_VALUE : Long.MAX_VALUE)
                  : (reverse ? Long.MAX_VALUE : Long.MIN_VALUE));
          return sf;
        }
      case "integer":
      case "short":
      case "byte":
        {
          org.apache.lucene.search.SortedNumericSortField sf =
              new org.apache.lucene.search.SortedNumericSortField(
                  fieldName, org.apache.lucene.search.SortField.Type.INT, reverse);
          sf.setMissingValue(
              nullsFirst
                  ? (reverse ? Integer.MIN_VALUE : Integer.MAX_VALUE)
                  : (reverse ? Integer.MAX_VALUE : Integer.MIN_VALUE));
          return sf;
        }
      case "double":
      case "float":
      case "half_float":
        {
          org.apache.lucene.search.SortedNumericSortField sf =
              new org.apache.lucene.search.SortedNumericSortField(
                  fieldName, org.apache.lucene.search.SortField.Type.DOUBLE, reverse);
          sf.setMissingValue(
              nullsFirst
                  ? (reverse ? -Double.MAX_VALUE : Double.MAX_VALUE)
                  : (reverse ? Double.MAX_VALUE : -Double.MAX_VALUE));
          return sf;
        }
      case "boolean":
        {
          org.apache.lucene.search.SortedNumericSortField sf =
              new org.apache.lucene.search.SortedNumericSortField(
                  fieldName, org.apache.lucene.search.SortField.Type.LONG, reverse);
          sf.setMissingValue(
              nullsFirst
                  ? (reverse ? Long.MIN_VALUE : Long.MAX_VALUE)
                  : (reverse ? Long.MAX_VALUE : Long.MIN_VALUE));
          return sf;
        }
      default:
        {
          org.apache.lucene.search.SortedSetSortField sf =
              new org.apache.lucene.search.SortedSetSortField(fieldName, reverse);
          if (nullsFirst) {
            sf.setMissingValue(org.apache.lucene.search.SortField.STRING_FIRST);
          } else {
            sf.setMissingValue(org.apache.lucene.search.SortField.STRING_LAST);
          }
          return sf;
        }
    }
  }

  /** Read a single doc value from a specific segment and doc ID into a BlockBuilder. */
  private static void readSingleDocValue(
      org.apache.lucene.index.LeafReaderContext leaf,
      int docId,
      ColumnHandle column,
      io.trino.spi.block.BlockBuilder builder)
      throws java.io.IOException {
    Type type = column.type();
    String name = column.name();

    if (type instanceof io.trino.spi.type.VarcharType) {
      org.apache.lucene.index.SortedSetDocValues dv = leaf.reader().getSortedSetDocValues(name);
      if (dv != null && dv.advanceExact(docId)) {
        long ord = dv.nextOrd();
        org.apache.lucene.util.BytesRef bytes = dv.lookupOrd(ord);
        io.trino.spi.type.VarcharType.VARCHAR.writeSlice(
            builder,
            io.airlift.slice.Slices.wrappedBuffer(bytes.bytes, bytes.offset, bytes.length));
      } else {
        builder.appendNull();
      }
    } else if (type instanceof DoubleType) {
      org.apache.lucene.index.SortedNumericDocValues dv =
          leaf.reader().getSortedNumericDocValues(name);
      if (dv != null && dv.advanceExact(docId)) {
        DoubleType.DOUBLE.writeDouble(builder, Double.longBitsToDouble(dv.nextValue()));
      } else {
        builder.appendNull();
      }
    } else if (type instanceof io.trino.spi.type.TimestampType) {
      org.apache.lucene.index.SortedNumericDocValues dv =
          leaf.reader().getSortedNumericDocValues(name);
      if (dv != null && dv.advanceExact(docId)) {
        long epochMillis = dv.nextValue();
        io.trino.spi.type.TimestampType.TIMESTAMP_MILLIS.writeLong(builder, epochMillis * 1000L);
      } else {
        builder.appendNull();
      }
    } else if (type instanceof io.trino.spi.type.BooleanType) {
      org.apache.lucene.index.SortedNumericDocValues dv =
          leaf.reader().getSortedNumericDocValues(name);
      if (dv != null && dv.advanceExact(docId)) {
        io.trino.spi.type.BooleanType.BOOLEAN.writeBoolean(builder, dv.nextValue() == 1);
      } else {
        builder.appendNull();
      }
    } else {
      // Numeric types (BigintType, IntegerType, SmallintType, TinyintType)
      org.apache.lucene.index.SortedNumericDocValues dv =
          leaf.reader().getSortedNumericDocValues(name);
      if (dv != null && dv.advanceExact(docId)) {
        type.writeLong(builder, dv.nextValue());
      } else {
        builder.appendNull();
      }
    }
  }
}
