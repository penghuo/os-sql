/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.dqe.coordinator.transport;

import io.trino.spi.Page;
import io.trino.spi.block.Block;
import io.trino.spi.type.BigintType;
import io.trino.spi.type.BooleanType;
import io.trino.spi.type.DoubleType;
import io.trino.spi.type.IntegerType;
import io.trino.spi.type.RealType;
import io.trino.spi.type.SmallintType;
import io.trino.spi.type.TimestampType;
import io.trino.spi.type.TinyintType;
import io.trino.spi.type.Type;
import io.trino.spi.type.VarcharType;
import io.trino.sql.tree.Query;
import io.trino.sql.tree.QuerySpecification;
import io.trino.sql.tree.Statement;
import java.io.IOException;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.action.ActionRequest;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.GroupedActionListener;
import org.opensearch.action.support.HandledTransportAction;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.inject.Inject;
import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.sql.dqe.common.config.DqeSettings;
import org.opensearch.sql.dqe.coordinator.fragment.PlanFragment;
import org.opensearch.sql.dqe.coordinator.fragment.PlanFragmenter;
import org.opensearch.sql.dqe.coordinator.merge.ResultMerger;
import org.opensearch.sql.dqe.coordinator.metadata.OpenSearchMetadata;
import org.opensearch.sql.dqe.coordinator.metadata.TableInfo;
import org.opensearch.sql.dqe.function.BuiltinFunctions;
import org.opensearch.sql.dqe.function.FunctionRegistry;
import org.opensearch.sql.dqe.function.expression.BlockExpression;
import org.opensearch.sql.dqe.function.expression.ExpressionCompiler;
import org.opensearch.sql.dqe.operator.Operator;
import org.opensearch.sql.dqe.planner.LogicalPlanner;
import org.opensearch.sql.dqe.planner.optimizer.PlanOptimizer;
import org.opensearch.sql.dqe.planner.plan.AggregationNode;
import org.opensearch.sql.dqe.planner.plan.DqePlanNode;
import org.opensearch.sql.dqe.planner.plan.DqePlanVisitor;
import org.opensearch.sql.dqe.planner.plan.EvalNode;
import org.opensearch.sql.dqe.planner.plan.FilterNode;
import org.opensearch.sql.dqe.planner.plan.LimitNode;
import org.opensearch.sql.dqe.planner.plan.ProjectNode;
import org.opensearch.sql.dqe.planner.plan.SortNode;
import org.opensearch.sql.dqe.planner.plan.TableScanNode;
import org.opensearch.sql.dqe.shard.transport.ShardExecuteAction;
import org.opensearch.sql.dqe.shard.transport.ShardExecuteRequest;
import org.opensearch.sql.dqe.shard.transport.ShardExecuteResponse;
import org.opensearch.sql.dqe.trino.parser.DqeSqlParser;
import org.opensearch.tasks.Task;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.TransportException;
import org.opensearch.transport.TransportResponseHandler;
import org.opensearch.transport.TransportService;

/**
 * Coordinator transport action that orchestrates the full DQE query pipeline: parse, plan,
 * fragment, dispatch to shards via transport, merge Pages, and format the response.
 *
 * <p>Shard plan fragments are dispatched to their target nodes via {@link TransportService}. Each
 * shard executes its fragment locally and returns serialized Trino Pages via {@link
 * ShardExecuteResponse}.
 */
public class TransportTrinoSqlAction
    extends HandledTransportAction<ActionRequest, TrinoSqlResponse> {

  private static final Logger LOG = LogManager.getLogger();

  /**
   * Singleton reference set during Guice construction. Used by {@link
   * org.opensearch.sql.dqe.coordinator.rest.RestTrinoSqlAction} to bypass the transport action
   * framework for reduced per-query overhead.
   */
  private static volatile TransportTrinoSqlAction INSTANCE;

  /** Return the singleton instance (available after Guice initialization). */
  public static TransportTrinoSqlAction getInstance() {
    return INSTANCE;
  }

  /**
   * Cache for compiled SQL query plans. Eliminates the parse-plan-optimize-fragment overhead for
   * repeated queries. Keyed by (SQL string, cluster metadata version) so that schema changes
   * automatically invalidate the cache. Bounded to 128 entries to limit memory usage.
   */
  private static final java.util.concurrent.ConcurrentHashMap<String, CachedQueryPlan>
      QUERY_PLAN_CACHE = new java.util.concurrent.ConcurrentHashMap<>();

  /** Holder for a compiled query plan cached across repeated executions of the same SQL. */
  private record CachedQueryPlan(
      PlanFragmenter.FragmentResult fragments,
      DqePlanNode optimizedPlan,
      DqePlanNode shardPlan,
      DqePlanNode coordinatorPlan,
      List<String> columnNames,
      List<String> internalColumnNames,
      List<Type> columnTypes,
      Map<String, Type> columnTypeMap,
      long metadataVersion,
      String schemaJsonPrefix,
      Type[] columnTypeArray) {}

  private final ClusterService clusterService;
  private final TransportService transportService;
  private final org.opensearch.sql.dqe.shard.transport.TransportShardExecuteAction shardAction;

  /**
   * Constructor for plugin wiring with dependency injection.
   *
   * @param transportService the transport service for dispatching shard requests
   * @param actionFilters action filters
   * @param clusterService the cluster service for metadata and routing
   * @param shardAction the shard execute action for local-node execution shortcut
   */
  @Inject
  public TransportTrinoSqlAction(
      TransportService transportService,
      ActionFilters actionFilters,
      ClusterService clusterService,
      org.opensearch.sql.dqe.shard.transport.TransportShardExecuteAction shardAction) {
    super(TrinoSqlAction.NAME, transportService, actionFilters, TrinoSqlRequest::new);
    this.clusterService = clusterService;
    this.transportService = transportService;
    this.shardAction = shardAction;
    INSTANCE = this;
  }

  /**
   * Execute a Trino SQL query directly, bypassing the transport action framework. This avoids
   * action filter chain, task creation, action map lookup, and TrinoSqlRequest object allocation
   * overhead.
   *
   * @param queryStr the SQL query string
   * @param isExplain whether this is an explain request
   * @param listener the response listener
   */
  public void executeDirect(
      String queryStr, boolean isExplain, ActionListener<TrinoSqlResponse> listener) {
    executeInternal(queryStr, isExplain, listener);
  }

  @Override
  protected void doExecute(
      Task task, ActionRequest request, ActionListener<TrinoSqlResponse> listener) {
    TrinoSqlRequest sqlReq = TrinoSqlRequest.fromActionRequest(request);
    executeInternal(sqlReq.getQuery(), sqlReq.isExplain(), listener);
  }

  private void executeInternal(
      String queryStr, boolean isExplain, ActionListener<TrinoSqlResponse> listener) {
    try {
      // Check query plan cache first (skips parse/plan/optimize/fragment for repeated queries)
      long currentMetaVersion = clusterService.state().metadata().version();

      // Explain mode cannot use the cache (needs the unoptimized plan for display)
      CachedQueryPlan cached = isExplain ? null : QUERY_PLAN_CACHE.get(queryStr);
      if (cached != null && cached.metadataVersion() != currentMetaVersion) {
        cached = null; // Stale cache entry — schema may have changed
      }

      final PlanFragmenter.FragmentResult fragments;
      final DqePlanNode optimizedPlan;
      final List<String> columnNames;
      final List<String> internalColumnNames;
      final List<Type> columnTypes;
      final Map<String, Type> columnTypeMap;
      final String schemaJsonPrefix;
      final Type[] columnTypeArray;

      if (cached != null) {
        // === Cache hit: reuse pre-compiled plan ===
        fragments = cached.fragments();
        optimizedPlan = cached.optimizedPlan();
        columnNames = cached.columnNames();
        internalColumnNames = cached.internalColumnNames();
        columnTypes = cached.columnTypes();
        columnTypeMap = cached.columnTypeMap();
        schemaJsonPrefix = cached.schemaJsonPrefix();
        columnTypeArray = cached.columnTypeArray();
      } else {
        // === Cache miss: full compilation pipeline ===

        // 1. Parse
        DqeSqlParser parser = new DqeSqlParser();
        Statement stmt = parser.parse(queryStr);

        // 2. Resolve metadata (cache TableInfo to avoid redundant resolution)
        OpenSearchMetadata metadata = new OpenSearchMetadata(clusterService);
        Map<String, TableInfo> tableInfoCache = new HashMap<>();
        java.util.function.Function<String, TableInfo> cachingResolver =
            name -> tableInfoCache.computeIfAbsent(name, metadata::getTableInfo);

        // 3. Plan
        DqePlanNode plan = LogicalPlanner.plan(stmt, cachingResolver);

        // 4. Optimize (resolve field types for predicate pushdown)
        String indexName = findIndexName(plan);
        TableInfo tableInfo = cachingResolver.apply(indexName);
        List<TableInfo.ColumnInfo> columnInfoList = tableInfo.columns();
        Map<String, String> fieldTypeMap = new HashMap<>(columnInfoList.size());
        Map<String, Type> compiledColumnTypeMap = new HashMap<>(columnInfoList.size());
        List<String> allColumnNames = new ArrayList<>(columnInfoList.size());
        for (TableInfo.ColumnInfo col : columnInfoList) {
          fieldTypeMap.put(col.name(), col.openSearchType());
          compiledColumnTypeMap.put(col.name(), col.trinoType());
          allColumnNames.add(col.name());
        }
        PlanOptimizer optimizer = new PlanOptimizer(fieldTypeMap);
        DqePlanNode compiledOptimizedPlan = optimizer.optimize(plan);

        // 5. Fragment (pass column type map for shard-level dedup optimization)
        PlanFragmenter fragmenter = new PlanFragmenter();
        PlanFragmenter.FragmentResult compiledFragments =
            fragmenter.fragment(
                compiledOptimizedPlan, clusterService.state(), compiledColumnTypeMap);

        // 6. Explain mode: return logical plan, optimized plan, and fragments
        if (isExplain) {
          listener.onResponse(
              new TrinoSqlResponse(formatExplain(plan, compiledOptimizedPlan, compiledFragments)));
          return;
        }

        // Resolve column names and types
        List<String> compiledInternalColumnNames = resolveColumnNames(compiledOptimizedPlan);
        List<String> compiledColumnNames;
        if (stmt instanceof Query query2
            && query2.getQueryBody() instanceof QuerySpecification querySpec2) {
          compiledColumnNames =
              LogicalPlanner.extractDisplayColumnNames(querySpec2, allColumnNames);
        } else {
          compiledColumnNames = compiledInternalColumnNames;
        }
        List<Type> compiledColumnTypes =
            resolveColumnTypes(
                compiledInternalColumnNames, compiledColumnTypeMap, compiledOptimizedPlan);

        // Store in cache (bounded to 128 entries to limit memory)
        if (QUERY_PLAN_CACHE.size() > 128) {
          QUERY_PLAN_CACHE.clear();
        }
        List<PlanFragment> shardFrags = compiledFragments.shardFragments();
        DqePlanNode cachedShardPlan = shardFrags.isEmpty() ? null : shardFrags.get(0).shardPlan();
        // Pre-build schema JSON prefix and type array for response formatting
        String schemaPrefix = buildSchemaJsonPrefix(compiledColumnNames, compiledColumnTypes);
        Type[] typeArray = compiledColumnTypes.toArray(new Type[0]);
        QUERY_PLAN_CACHE.put(
            queryStr,
            new CachedQueryPlan(
                compiledFragments,
                compiledOptimizedPlan,
                cachedShardPlan,
                compiledFragments.coordinatorPlan(),
                compiledColumnNames,
                compiledInternalColumnNames,
                compiledColumnTypes,
                compiledColumnTypeMap,
                currentMetaVersion,
                schemaPrefix,
                typeArray));

        // Assign to final variables for the execution phase
        fragments = compiledFragments;
        optimizedPlan = compiledOptimizedPlan;
        columnNames = compiledColumnNames;
        internalColumnNames = compiledInternalColumnNames;
        columnTypes = compiledColumnTypes;
        columnTypeMap = compiledColumnTypeMap;
        schemaJsonPrefix = schemaPrefix;
        columnTypeArray = typeArray;
      }

      // 8. Dispatch to shards via transport
      List<PlanFragment> shardFragments = fragments.shardFragments();
      DqePlanNode coordinatorPlan = fragments.coordinatorPlan();

      long timeoutMillis = DqeSettings.QUERY_TIMEOUT.get(clusterService.getSettings()).millis();

      GroupedActionListener<ShardExecuteResponse> groupedListener =
          new GroupedActionListener<>(
              ActionListener.wrap(
                  responses -> {
                    try {
                      // 9. Merge Page-based results
                      List<List<Page>> shardPages =
                          responses.stream()
                              .map(ShardExecuteResponse::getPages)
                              .collect(Collectors.toList());

                      ResultMerger merger = new ResultMerger();
                      List<Page> mergedPages;
                      if (coordinatorPlan instanceof AggregationNode aggNode
                          && isScalarPartialMerge(aggNode)) {
                        // Fast path: scalar aggregation merge (no GROUP BY).
                        // Just sum/merge the partial results from each shard directly,
                        // bypassing HashAggregationOperator construction.
                        mergedPages = mergeScalarAggregation(shardPages, aggNode, columnTypes);
                        // No sort/having needed for scalar aggregation
                      } else if (coordinatorPlan instanceof AggregationNode singleCdAgg
                          && singleCdAgg.getStep() == AggregationNode.Step.SINGLE
                          && isScalarCountDistinctLong(singleCdAgg, columnTypeMap)) {
                        // Fast path: scalar COUNT(DISTINCT numericCol) — shards already pre-deduped
                        // values. Merge distinct value sets using LongOpenHashSet and count.
                        mergedPages = mergeCountDistinctValues(shardPages);
                      } else if (coordinatorPlan instanceof AggregationNode singleCdVarcharAgg
                          && singleCdVarcharAgg.getStep() == AggregationNode.Step.SINGLE
                          && isScalarCountDistinctVarchar(singleCdVarcharAgg, columnTypeMap)) {
                        // Fast path: scalar COUNT(DISTINCT varcharCol) — shards already pre-deduped
                        // values using ordinal-based collection. Merge by unioning string sets.
                        mergedPages = mergeCountDistinctVarcharValues(shardPages);
                      } else if (coordinatorPlan instanceof AggregationNode singleAgg
                          && singleAgg.getStep() == AggregationNode.Step.SINGLE
                          && isShardDedupCountDistinct(
                              shardFragments.get(0).shardPlan(), singleAgg, columnTypeMap)) {
                        // Fast path: shard-deduped COUNT(DISTINCT) with GROUP BY.
                        // Two-stage merge: FINAL dedup merge + re-aggregate.
                        mergedPages =
                            mergeDedupCountDistinct(
                                shardPages,
                                singleAgg,
                                shardFragments.get(0).shardPlan(),
                                columnTypes,
                                columnTypeMap,
                                merger);
                        mergedPages =
                            applyCoordinatorSort(
                                mergedPages, singleAgg, optimizedPlan, columnTypes, merger);
                      } else if (coordinatorPlan instanceof AggregationNode singleMixed
                          && singleMixed.getStep() == AggregationNode.Step.SINGLE
                          && isShardMixedDedup(shardFragments.get(0).shardPlan(), singleMixed)) {
                        // Fast path: mixed-aggregate dedup (e.g., Q10).
                        // Shards did GROUP BY (original_keys + distinct_cols) with partial aggs.
                        // Coordinator merges partials then re-aggregates to final result.
                        mergedPages =
                            mergeMixedDedup(
                                shardPages,
                                singleMixed,
                                shardFragments.get(0).shardPlan(),
                                columnTypes,
                                columnTypeMap,
                                merger);
                        mergedPages =
                            applyCoordinatorSort(
                                mergedPages, singleMixed, optimizedPlan, columnTypes, merger);
                      } else if (coordinatorPlan instanceof AggregationNode singleAgg2
                          && singleAgg2.getStep() == AggregationNode.Step.SINGLE) {
                        // SINGLE aggregation: shards sent raw data, coordinator aggregates
                        List<String> shardColumnNames =
                            resolveColumnNames(shardFragments.get(0).shardPlan());
                        List<Page> rawPages = merger.mergePassthrough(shardPages);
                        mergedPages =
                            runCoordinatorAggregation(
                                singleAgg2, rawPages, shardColumnNames, columnTypeMap);
                        mergedPages =
                            applyCoordinatorSort(
                                mergedPages, singleAgg2, optimizedPlan, columnTypes, merger);
                      } else if (coordinatorPlan instanceof AggregationNode aggNode) {
                        // Check if we can use the fused merge+sort path (no HAVING clause)
                        FilterNode havingNode = findHavingNode(optimizedPlan);
                        SortNode sortNodeForFuse = findSortNode(optimizedPlan);
                        long fusedLimit = findGlobalLimit(optimizedPlan);
                        if (havingNode == null
                            && sortNodeForFuse != null
                            && fusedLimit > 0
                            && aggNode.getStep() == AggregationNode.Step.FINAL) {
                          // Fused merge+sort: avoids building full Page for all groups
                          List<String> aggOutputCols = new ArrayList<>(aggNode.getGroupByKeys());
                          aggOutputCols.addAll(aggNode.getAggregateFunctions());
                          List<Integer> sortIndicesForFuse =
                              sortNodeForFuse.getSortKeys().stream()
                                  .map(aggOutputCols::indexOf)
                                  .collect(Collectors.toList());
                          long sortLimitForFuse = fusedLimit + findGlobalOffset(optimizedPlan);
                          mergedPages =
                              merger.mergeAggregationAndSort(
                                  shardPages,
                                  aggNode,
                                  columnTypes,
                                  sortIndicesForFuse,
                                  sortNodeForFuse.getAscending(),
                                  sortNodeForFuse.getNullsFirst(),
                                  sortLimitForFuse);
                        } else {
                          // Fallback: separate merge + HAVING + sort
                          mergedPages = merger.mergeAggregation(shardPages, aggNode, columnTypes);
                          mergedPages =
                              applyCoordinatorHaving(
                                  mergedPages, optimizedPlan, aggNode, columnTypeMap);
                          mergedPages =
                              applyCoordinatorSort(
                                  mergedPages, aggNode, optimizedPlan, columnTypes, merger);
                        }
                      } else {
                        // Check if we need sorted merge
                        SortNode sortNode = findSortNode(optimizedPlan);
                        if (sortNode != null) {
                          // Use internal column names (which include sort-only columns
                          // appended by LogicalPlanner) to resolve sort key indices.
                          List<Integer> sortIndices =
                              sortNode.getSortKeys().stream()
                                  .map(internalColumnNames::indexOf)
                                  .collect(Collectors.toList());
                          // The sort limit must account for the global OFFSET so
                          // that enough rows survive the merge-sort for the
                          // subsequent applyGlobalOffset to skip correctly.
                          long rawLimit = findGlobalLimit(optimizedPlan);
                          long sortLimit =
                              rawLimit >= 0
                                  ? rawLimit + findGlobalOffset(optimizedPlan)
                                  : Long.MAX_VALUE;
                          mergedPages =
                              merger.mergeSorted(
                                  shardPages,
                                  sortIndices,
                                  sortNode.getAscending(),
                                  sortNode.getNullsFirst(),
                                  columnTypes,
                                  sortLimit);
                        } else {
                          mergedPages = merger.mergePassthrough(shardPages);
                        }
                      }

                      // 10. Apply coordinator-level OFFSET + LIMIT
                      long globalOffset = findGlobalOffset(optimizedPlan);
                      if (globalOffset > 0) {
                        mergedPages = applyGlobalOffset(mergedPages, globalOffset);
                      }
                      long globalLimit = findGlobalLimit(optimizedPlan);
                      if (globalLimit >= 0) {
                        mergedPages = applyGlobalLimit(mergedPages, globalLimit);
                      }

                      // 11. Format response (Page -> JSON for REST client)
                      String responseJson =
                          formatResponse(
                              mergedPages,
                              columnNames,
                              columnTypes,
                              schemaJsonPrefix,
                              columnTypeArray);
                      listener.onResponse(new TrinoSqlResponse(responseJson));
                    } catch (Exception e) {
                      listener.onFailure(e);
                    }
                  },
                  listener::onFailure),
              shardFragments.size());

      // Check if all shards are on the local node for direct execution shortcut.
      // This bypasses plan serialization, transport layer, and response serialization
      // for every shard — a significant overhead reduction for single-node deployments.
      DiscoveryNode localNode = clusterService.localNode();
      String localNodeId = (localNode != null) ? localNode.getId() : null;
      boolean allLocal = localNodeId != null;
      if (allLocal) {
        for (PlanFragment frag : shardFragments) {
          if (!localNodeId.equals(frag.nodeId())) {
            allLocal = false;
            break;
          }
        }
      }

      if (allLocal && shardAction != null) {
        // === Local-node fast path ===
        // Execute all shard plans directly without transport serialization.
        // Dispatches N-1 shards to the thread pool and executes the last shard on the
        // coordinator thread itself, overlapping with the latch wait. This saves one
        // thread pool submission and eliminates the latch entirely for single-shard cases.
        LOG.debug("DQE: Using local-node fast path for {} shard fragments", shardFragments.size());
        DqePlanNode shardPlan = shardFragments.get(0).shardPlan();
        int numShards = shardFragments.size();
        ShardExecuteResponse[] shardResults = new ShardExecuteResponse[numShards];
        Exception[] shardErrors = new Exception[1]; // first error wins

        // Dispatch shards 0..N-2 to the thread pool; shard N-1 runs on this thread
        int remoteShards = numShards - 1;
        java.util.concurrent.CountDownLatch latch =
            new java.util.concurrent.CountDownLatch(remoteShards);

        if (remoteShards > 0) {
          java.util.concurrent.ExecutorService executor =
              transportService
                  .getThreadPool()
                  .executor(
                      org.opensearch.sql.dqe.shard.transport.TransportShardExecuteAction
                          .DQE_THREAD_POOL_NAME);
          for (int i = 0; i < remoteShards; i++) {
            PlanFragment frag = shardFragments.get(i);
            final int fragIdx = i;
            final int fragShardId = frag.shardId();
            final String fragIndexName = frag.indexName();
            executor.execute(
                () -> {
                  ShardExecuteRequest shardReq =
                      new ShardExecuteRequest(
                          new byte[0], fragIndexName, fragShardId, timeoutMillis);
                  try {
                    shardResults[fragIdx] = shardAction.executeLocal(shardPlan, shardReq);
                  } catch (Exception e) {
                    shardErrors[0] = e;
                  }
                  latch.countDown();
                });
          }
        }

        // Execute the last shard on the coordinator thread (overlaps with pool execution)
        {
          PlanFragment lastFrag = shardFragments.get(remoteShards);
          ShardExecuteRequest shardReq =
              new ShardExecuteRequest(
                  new byte[0], lastFrag.indexName(), lastFrag.shardId(), timeoutMillis);
          try {
            shardResults[remoteShards] = shardAction.executeLocal(shardPlan, shardReq);
          } catch (Exception e) {
            shardErrors[0] = e;
          }
        }

        // Wait for the pool-dispatched shards (the last shard is already done)
        if (remoteShards > 0) {
          try {
            latch.await(timeoutMillis, java.util.concurrent.TimeUnit.MILLISECONDS);
          } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
            listener.onFailure(ie);
            return;
          }
        }

        if (shardErrors[0] != null) {
          listener.onFailure(shardErrors[0]);
          return;
        }

        // Process results synchronously — avoids the GroupedActionListener callback chain
        try {
          // Build shardPages directly from the shared array (no stream/collect)
          List<List<Page>> shardPages = new ArrayList<>(numShards);
          for (ShardExecuteResponse resp : shardResults) {
            shardPages.add(resp.getPages());
          }

          ResultMerger merger = new ResultMerger();
          List<Page> mergedPages;
          if (coordinatorPlan instanceof AggregationNode aggNode && isScalarPartialMerge(aggNode)) {
            mergedPages = mergeScalarAggregation(shardPages, aggNode, columnTypes);
          } else if (coordinatorPlan instanceof AggregationNode singleCdAgg
              && singleCdAgg.getStep() == AggregationNode.Step.SINGLE
              && isScalarCountDistinctLong(singleCdAgg, columnTypeMap)) {
            mergedPages = mergeCountDistinctValues(shardPages);
          } else if (coordinatorPlan instanceof AggregationNode singleCdVarcharAgg2
              && singleCdVarcharAgg2.getStep() == AggregationNode.Step.SINGLE
              && isScalarCountDistinctVarchar(singleCdVarcharAgg2, columnTypeMap)) {
            mergedPages = mergeCountDistinctVarcharValues(shardPages);
          } else if (coordinatorPlan instanceof AggregationNode singleAgg
              && singleAgg.getStep() == AggregationNode.Step.SINGLE
              && isShardDedupCountDistinct(
                  shardFragments.get(0).shardPlan(), singleAgg, columnTypeMap)) {
            // Fast path: shard-deduped COUNT(DISTINCT) with GROUP BY.
            mergedPages =
                mergeDedupCountDistinct(
                    shardPages,
                    singleAgg,
                    shardFragments.get(0).shardPlan(),
                    columnTypes,
                    columnTypeMap,
                    merger);
            mergedPages =
                applyCoordinatorSort(mergedPages, singleAgg, optimizedPlan, columnTypes, merger);
          } else if (coordinatorPlan instanceof AggregationNode singleMixed2
              && singleMixed2.getStep() == AggregationNode.Step.SINGLE
              && isShardMixedDedup(shardFragments.get(0).shardPlan(), singleMixed2)) {
            mergedPages =
                mergeMixedDedup(
                    shardPages,
                    singleMixed2,
                    shardFragments.get(0).shardPlan(),
                    columnTypes,
                    columnTypeMap,
                    merger);
            mergedPages =
                applyCoordinatorSort(mergedPages, singleMixed2, optimizedPlan, columnTypes, merger);
          } else if (coordinatorPlan instanceof AggregationNode singleAgg2
              && singleAgg2.getStep() == AggregationNode.Step.SINGLE) {
            List<String> shardColumnNames = resolveColumnNames(shardFragments.get(0).shardPlan());
            List<Page> rawPages = merger.mergePassthrough(shardPages);
            mergedPages =
                runCoordinatorAggregation(singleAgg2, rawPages, shardColumnNames, columnTypeMap);
            mergedPages =
                applyCoordinatorSort(mergedPages, singleAgg2, optimizedPlan, columnTypes, merger);
          } else if (coordinatorPlan instanceof AggregationNode aggNode) {
            FilterNode havingNode = findHavingNode(optimizedPlan);
            SortNode sortNodeForFuse = findSortNode(optimizedPlan);
            long fusedLimit = findGlobalLimit(optimizedPlan);
            if (havingNode == null
                && sortNodeForFuse != null
                && fusedLimit > 0
                && aggNode.getStep() == AggregationNode.Step.FINAL) {
              List<String> aggOutputCols = new ArrayList<>(aggNode.getGroupByKeys());
              aggOutputCols.addAll(aggNode.getAggregateFunctions());
              List<Integer> sortIndicesForFuse =
                  sortNodeForFuse.getSortKeys().stream()
                      .map(aggOutputCols::indexOf)
                      .collect(Collectors.toList());
              long sortLimitForFuse = fusedLimit + findGlobalOffset(optimizedPlan);
              mergedPages =
                  merger.mergeAggregationAndSort(
                      shardPages,
                      aggNode,
                      columnTypes,
                      sortIndicesForFuse,
                      sortNodeForFuse.getAscending(),
                      sortNodeForFuse.getNullsFirst(),
                      sortLimitForFuse);
            } else {
              mergedPages = merger.mergeAggregation(shardPages, aggNode, columnTypes);
              mergedPages =
                  applyCoordinatorHaving(mergedPages, optimizedPlan, aggNode, columnTypeMap);
              mergedPages =
                  applyCoordinatorSort(mergedPages, aggNode, optimizedPlan, columnTypes, merger);
            }
          } else {
            SortNode sortNode = findSortNode(optimizedPlan);
            if (sortNode != null) {
              List<Integer> sortIndices =
                  sortNode.getSortKeys().stream()
                      .map(internalColumnNames::indexOf)
                      .collect(Collectors.toList());
              long rawLimit = findGlobalLimit(optimizedPlan);
              long sortLimit =
                  rawLimit >= 0 ? rawLimit + findGlobalOffset(optimizedPlan) : Long.MAX_VALUE;
              mergedPages =
                  merger.mergeSorted(
                      shardPages,
                      sortIndices,
                      sortNode.getAscending(),
                      sortNode.getNullsFirst(),
                      columnTypes,
                      sortLimit);
            } else {
              mergedPages = merger.mergePassthrough(shardPages);
            }
          }

          long globalOffset = findGlobalOffset(optimizedPlan);
          if (globalOffset > 0) {
            mergedPages = applyGlobalOffset(mergedPages, globalOffset);
          }
          long globalLimit = findGlobalLimit(optimizedPlan);
          if (globalLimit >= 0) {
            mergedPages = applyGlobalLimit(mergedPages, globalLimit);
          }
          String responseJson =
              formatResponse(
                  mergedPages, columnNames, columnTypes, schemaJsonPrefix, columnTypeArray);
          listener.onResponse(new TrinoSqlResponse(responseJson));
        } catch (Exception e) {
          listener.onFailure(e);
        }
        return; // skip the GroupedActionListener path below
      } else {
        // === Transport path ===
        // Serialize the shard plan once and reuse for all shards (all fragments share the
        // same plan object; only shardId and nodeId differ).
        byte[] serializedPlan;
        {
          BytesStreamOutput planOut = new BytesStreamOutput();
          DqePlanNode.writePlanNode(planOut, shardFragments.get(0).shardPlan());
          serializedPlan = planOut.bytes().toBytesRef().bytes;
        }

        // Dispatch each fragment to its target node
        for (PlanFragment frag : shardFragments) {
          ShardExecuteRequest shardReq =
              new ShardExecuteRequest(
                  serializedPlan, frag.indexName(), frag.shardId(), timeoutMillis);

          // Resolve target node
          DiscoveryNode targetNode = clusterService.state().nodes().get(frag.nodeId());

          // Send via transport
          transportService.sendRequest(
              targetNode,
              ShardExecuteAction.NAME,
              shardReq,
              new TransportResponseHandler<ShardExecuteResponse>() {
                @Override
                public ShardExecuteResponse read(StreamInput in) throws IOException {
                  return new ShardExecuteResponse(in);
                }

                @Override
                public void handleResponse(ShardExecuteResponse response) {
                  groupedListener.onResponse(response);
                }

                @Override
                public void handleException(TransportException exp) {
                  groupedListener.onFailure(exp);
                }

                @Override
                public String executor() {
                  return ThreadPool.Names.SAME;
                }
              });
        }
      }

    } catch (Exception e) {
      LOG.error("Error executing Trino SQL query: {}", queryStr, e);
      listener.onFailure(e);
    }
  }

  /**
   * Resolve column names from the root plan node by walking the tree to find effective output
   * columns.
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
   * Resolve Trino types for the given column names. For plain column names that exist in the type
   * map, the mapped type is used directly. For computed expression column names (e.g., arithmetic
   * expressions like "(count_long * price_double)"), the result type is inferred by compiling the
   * expression and checking the output type of the resulting {@link BlockExpression}.
   *
   * @param columnNames the internal column names (may include expression strings)
   * @param columnTypeMap mapping from physical column names to Trino types
   * @param plan the optimized plan tree (used to find EvalNode expressions)
   * @return list of resolved types, one per column name
   */
  static List<Type> resolveColumnTypes(
      List<String> columnNames, Map<String, Type> columnTypeMap, DqePlanNode plan) {
    // Check if any column name is a computed expression (not in the type map).
    // If so, we need to compile those expressions to infer their output types.
    boolean hasComputed = false;
    for (String col : columnNames) {
      if (!columnTypeMap.containsKey(col)) {
        hasComputed = true;
        break;
      }
    }

    if (!hasComputed) {
      // Fast path: all columns are plain column references
      List<Type> types = new ArrayList<>();
      for (String col : columnNames) {
        types.add(columnTypeMap.getOrDefault(col, BigintType.BIGINT));
      }
      return types;
    }

    // Find the EvalNode in the plan to get expression strings and their output column names
    EvalNode evalNode = findEvalNode(plan);
    Map<String, String> columnNameToExpression = new HashMap<>();
    if (evalNode != null) {
      List<String> evalOutputNames = evalNode.getOutputColumnNames();
      List<String> evalExpressions = evalNode.getExpressions();
      for (int i = 0; i < evalOutputNames.size(); i++) {
        columnNameToExpression.put(evalOutputNames.get(i), evalExpressions.get(i));
      }
    }

    // Build column index and type maps for expression compilation. The indices correspond
    // to the TableScanNode columns (all physical columns of the table).
    TableScanNode scanNode = findTableScanNode(plan);
    List<String> tableColumns = scanNode != null ? scanNode.getColumns() : List.of();
    Map<String, Integer> columnIndexMap = new HashMap<>();
    for (int i = 0; i < tableColumns.size(); i++) {
      columnIndexMap.put(tableColumns.get(i), i);
    }

    FunctionRegistry registry = BuiltinFunctions.createRegistry();
    ExpressionCompiler compiler = new ExpressionCompiler(registry, columnIndexMap, columnTypeMap);
    DqeSqlParser exprParser = new DqeSqlParser();

    List<Type> types = new ArrayList<>();
    for (String col : columnNames) {
      if (columnTypeMap.containsKey(col)) {
        types.add(columnTypeMap.get(col));
      } else {
        // Try aggregate function type inference first
        Type aggType = inferAggregateOutputType(col, columnTypeMap);
        if (aggType != null) {
          types.add(aggType);
        } else {
          // Try to infer the type by compiling the expression
          String exprStr = columnNameToExpression.getOrDefault(col, col);
          try {
            io.trino.sql.tree.Expression expr = exprParser.parseExpression(exprStr);
            BlockExpression blockExpr = compiler.compile(expr);
            types.add(blockExpr.getType());
          } catch (Exception e) {
            // If parsing/compilation fails, fall back to BIGINT
            LOG.warn(
                "Could not infer type for column '{}', defaulting to BIGINT: {}",
                col,
                e.getMessage());
            types.add(BigintType.BIGINT);
          }
        }
      }
    }
    return types;
  }

  private static final java.util.regex.Pattern AGG_TYPE_PATTERN =
      java.util.regex.Pattern.compile(
          "^(COUNT|SUM|MIN|MAX|AVG)\\((DISTINCT\\s+)?(.+?)\\)$",
          java.util.regex.Pattern.CASE_INSENSITIVE);

  /**
   * Infer the output type of an aggregate function expression like "COUNT(*)", "SUM(val)",
   * "AVG(val)". Returns null if the column name is not an aggregate function expression.
   */
  private static Type inferAggregateOutputType(String colName, Map<String, Type> columnTypeMap) {
    java.util.regex.Matcher m = AGG_TYPE_PATTERN.matcher(colName);
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
          // SUM output type: DOUBLE for double input, BIGINT for all integer types
          Type inputType = columnTypeMap.getOrDefault(arg, BigintType.BIGINT);
          return inputType instanceof DoubleType ? DoubleType.DOUBLE : BigintType.BIGINT;
        }
      case "MIN":
      case "MAX":
        // Use the input column type if known, otherwise BIGINT
        return columnTypeMap.getOrDefault(arg, BigintType.BIGINT);
      default:
        return null;
    }
  }

  /** Walk the plan tree to find the EvalNode. Returns null if none. */
  static EvalNode findEvalNode(DqePlanNode plan) {
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
  static TableScanNode findTableScanNode(DqePlanNode plan) {
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

  /** Walk the plan tree to find the TableScanNode and extract the index name. */
  private String findIndexName(DqePlanNode plan) {
    String indexName =
        plan.accept(
            new DqePlanVisitor<String, Void>() {
              @Override
              public String visitTableScan(TableScanNode node, Void context) {
                return node.getIndexName();
              }

              @Override
              public String visitPlan(DqePlanNode node, Void context) {
                for (DqePlanNode child : node.getChildren()) {
                  String result = child.accept(this, context);
                  if (result != null) {
                    return result;
                  }
                }
                return null;
              }
            },
            null);

    if (indexName == null) {
      throw new IllegalArgumentException("Plan does not contain a TableScanNode");
    }
    return indexName;
  }

  /**
   * Format the explain output showing all three plan stages: logical plan (before optimization),
   * optimized plan (after optimization), and per-shard fragments with coordinator merge plan.
   */
  static String formatExplain(
      DqePlanNode logicalPlan, DqePlanNode optimizedPlan, PlanFragmenter.FragmentResult fragments) {
    StringBuilder sb = new StringBuilder();
    sb.append("{");

    // 1. Logical plan (before optimization)
    sb.append("\"logical_plan\":");
    planToJson(sb, logicalPlan);

    // 2. Optimized plan (after optimization)
    sb.append(",\"optimized_plan\":");
    planToJson(sb, optimizedPlan);

    // 3. Fragments (per-shard plans + coordinator plan)
    sb.append(",\"fragments\":[");
    List<PlanFragment> frags = fragments.shardFragments();
    for (int i = 0; i < frags.size(); i++) {
      if (i > 0) {
        sb.append(",");
      }
      PlanFragment f = frags.get(i);
      sb.append("{\"shard_id\":").append(f.shardId());
      sb.append(",\"node_id\":\"").append(escapeJson(f.nodeId())).append("\"");
      sb.append(",\"index\":\"").append(escapeJson(f.indexName())).append("\"");
      sb.append(",\"plan\":");
      planToJson(sb, f.shardPlan());
      sb.append("}");
    }
    sb.append("]");

    // 4. Coordinator merge plan (null for non-aggregate queries)
    sb.append(",\"coordinator_plan\":");
    if (fragments.coordinatorPlan() != null) {
      planToJson(sb, fragments.coordinatorPlan());
    } else {
      sb.append("null");
    }

    sb.append("}");
    return sb.toString();
  }

  /** Convert a plan node tree to a JSON object recursively. */
  private static void planToJson(StringBuilder sb, DqePlanNode node) {
    sb.append("{\"node\":\"").append(node.getClass().getSimpleName()).append("\"");

    // Node-specific attributes
    if (node instanceof TableScanNode scan) {
      sb.append(",\"index\":\"").append(escapeJson(scan.getIndexName())).append("\"");
      sb.append(",\"columns\":").append(toJsonArray(scan.getColumns()));
      if (scan.getDslFilter() != null) {
        sb.append(",\"dsl_filter\":").append(scan.getDslFilter());
      }
    } else if (node instanceof FilterNode filter) {
      sb.append(",\"predicate\":\"").append(escapeJson(filter.getPredicateString())).append("\"");
    } else if (node instanceof ProjectNode proj) {
      sb.append(",\"columns\":").append(toJsonArray(proj.getOutputColumns()));
    } else if (node instanceof AggregationNode agg) {
      sb.append(",\"group_by\":").append(toJsonArray(agg.getGroupByKeys()));
      sb.append(",\"aggregates\":").append(toJsonArray(agg.getAggregateFunctions()));
      sb.append(",\"step\":\"").append(agg.getStep().name()).append("\"");
    } else if (node instanceof SortNode sort) {
      sb.append(",\"sort_keys\":").append(toJsonArray(sort.getSortKeys()));
      sb.append(",\"ascending\":").append(sort.getAscending());
      sb.append(",\"nulls_first\":").append(sort.getNullsFirst());
    } else if (node instanceof LimitNode limit) {
      sb.append(",\"count\":").append(limit.getCount());
    }

    // Children
    List<DqePlanNode> children = node.getChildren();
    if (!children.isEmpty()) {
      sb.append(",\"child\":");
      planToJson(sb, children.get(0));
    }

    sb.append("}");
  }

  /** Convert a list of strings to a JSON array. */
  private static String toJsonArray(List<String> items) {
    StringBuilder sb = new StringBuilder("[");
    for (int i = 0; i < items.size(); i++) {
      if (i > 0) {
        sb.append(",");
      }
      sb.append("\"").append(escapeJson(items.get(i))).append("\"");
    }
    sb.append("]");
    return sb.toString();
  }

  /**
   * Format the query result as a JSON response matching the OpenSearch SQL response format. Works
   * directly with Trino Pages.
   *
   * @param pages the merged result pages
   * @param columnNames the output column names
   * @param columnTypes the Trino types for each column
   * @return JSON response string
   */
  static String formatResponse(List<Page> pages, List<String> columnNames, List<Type> columnTypes) {
    return formatResponse(pages, columnNames, columnTypes, null, null);
  }

  /**
   * Build the schema JSON prefix: {@code {"schema":[...],"datarows":[}. This is identical for
   * repeated executions of the same query and can be cached alongside the query plan.
   */
  static String buildSchemaJsonPrefix(List<String> columnNames, List<Type> columnTypes) {
    int numCols = columnNames.size();
    StringBuilder sb = new StringBuilder(numCols * 40 + 32);
    sb.append("{\"schema\":[");
    for (int i = 0; i < numCols; i++) {
      if (i > 0) {
        sb.append(",");
      }
      sb.append("{\"name\":\"")
          .append(escapeJson(columnNames.get(i)))
          .append("\",\"type\":\"")
          .append(trinoTypeToOpenSearchType(columnTypes.get(i)))
          .append("\"}");
    }
    sb.append("],\"datarows\":[");
    return sb.toString();
  }

  /**
   * Overloaded formatResponse that accepts a pre-built schema JSON prefix and type array from the
   * plan cache to avoid re-computing the schema portion and List-to-array conversion per query.
   */
  static String formatResponse(
      List<Page> pages,
      List<String> columnNames,
      List<Type> columnTypes,
      String cachedSchemaPrefix,
      Type[] cachedTypes) {
    // Pre-compute total row count for StringBuilder sizing
    int totalRows = 0;
    for (Page page : pages) {
      totalRows += page.getPositionCount();
    }
    int numCols = columnNames.size();
    // Estimate ~40 bytes per cell for initial capacity
    StringBuilder sb = new StringBuilder(Math.max(256, totalRows * numCols * 40));

    // Use cached schema prefix if available, otherwise build it inline
    if (cachedSchemaPrefix != null) {
      sb.append(cachedSchemaPrefix);
    } else {
      sb.append("{\"schema\":[");
      for (int i = 0; i < numCols; i++) {
        if (i > 0) {
          sb.append(",");
        }
        sb.append("{\"name\":\"")
            .append(escapeJson(columnNames.get(i)))
            .append("\",\"type\":\"")
            .append(trinoTypeToOpenSearchType(columnTypes.get(i)))
            .append("\"}");
      }
      sb.append("],\"datarows\":[");
    }

    // Use cached type array if available, otherwise build from list
    Type[] types = cachedTypes != null ? cachedTypes : columnTypes.toArray(new Type[0]);
    boolean firstRow = true;
    for (Page page : pages) {
      int channelCount = page.getChannelCount();
      int positionCount = page.getPositionCount();
      // Pre-fetch blocks for this page to avoid repeated getBlock calls
      Block[] blocks = new Block[Math.min(numCols, channelCount)];
      for (int col = 0; col < blocks.length; col++) {
        blocks[col] = page.getBlock(col);
      }
      for (int pos = 0; pos < positionCount; pos++) {
        if (!firstRow) {
          sb.append(",");
        }
        firstRow = false;
        sb.append("[");
        for (int col = 0; col < numCols; col++) {
          if (col > 0) {
            sb.append(",");
          }
          if (col < channelCount) {
            appendExtractedValue(sb, blocks[col], pos, types[col]);
          } else {
            sb.append("null");
          }
        }
        sb.append("]");
      }
    }

    sb.append("],\"total\":").append(totalRows);
    sb.append(",\"size\":").append(totalRows);
    sb.append(",\"status\":200}");
    return sb.toString();
  }

  /**
   * Extract a value from a block and append directly to the StringBuilder, avoiding intermediate
   * Object boxing for numeric types.
   */
  private static void appendExtractedValue(StringBuilder sb, Block block, int position, Type type) {
    if (block.isNull(position)) {
      sb.append("null");
      return;
    }
    if (type instanceof BigintType) {
      sb.append(BigintType.BIGINT.getLong(block, position));
    } else if (type instanceof IntegerType) {
      sb.append((int) IntegerType.INTEGER.getLong(block, position));
    } else if (type instanceof DoubleType) {
      double val = DoubleType.DOUBLE.getDouble(block, position);
      if (val == Math.floor(val) && !Double.isInfinite(val) && Math.abs(val) < 1e15) {
        sb.append((long) val);
      } else {
        sb.append(val);
      }
    } else if (type instanceof VarcharType) {
      sb.append("\"")
          .append(escapeJson(VarcharType.VARCHAR.getSlice(block, position).toStringUtf8()))
          .append("\"");
    } else if (type instanceof BooleanType) {
      sb.append(BooleanType.BOOLEAN.getBoolean(block, position));
    } else if (type instanceof TimestampType) {
      long microsSinceEpoch = type.getLong(block, position);
      long millisSinceEpoch = microsSinceEpoch / 1000;
      java.time.LocalDateTime ldt =
          Instant.ofEpochMilli(millisSinceEpoch).atZone(ZoneOffset.UTC).toLocalDateTime();
      sb.append("\"");
      if (ldt.getHour() == 0 && ldt.getMinute() == 0 && ldt.getSecond() == 0) {
        sb.append(ldt.toLocalDate());
      } else {
        sb.append(ldt.format(java.time.format.DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")));
      }
      sb.append("\"");
    } else if (type instanceof SmallintType) {
      sb.append((short) SmallintType.SMALLINT.getLong(block, position));
    } else if (type instanceof TinyintType) {
      sb.append((byte) TinyintType.TINYINT.getLong(block, position));
    } else if (type instanceof RealType) {
      long bits = RealType.REAL.getLong(block, position);
      sb.append((double) Float.intBitsToFloat((int) bits));
    } else {
      // Fallback: use extractValue + appendJsonValue
      appendJsonValue(sb, extractValue(new Page(block), 0, position, type));
    }
  }

  /** Extract a typed value from a Page at the given column and row position. */
  private static Object extractValue(Page page, int channel, int position, Type type) {
    Block block = page.getBlock(channel);
    if (block.isNull(position)) {
      return null;
    }
    if (type instanceof BigintType) {
      return BigintType.BIGINT.getLong(block, position);
    } else if (type instanceof IntegerType) {
      return (int) IntegerType.INTEGER.getLong(block, position);
    } else if (type instanceof SmallintType) {
      return (short) SmallintType.SMALLINT.getLong(block, position);
    } else if (type instanceof TinyintType) {
      return (byte) TinyintType.TINYINT.getLong(block, position);
    } else if (type instanceof DoubleType) {
      double val = DoubleType.DOUBLE.getDouble(block, position);
      // Format integer-valued doubles without decimal point (e.g., 1638 instead of 1638.0)
      if (val == Math.floor(val) && !Double.isInfinite(val) && Math.abs(val) < 1e15) {
        return (long) val;
      }
      return val;
    } else if (type instanceof RealType) {
      // RealType stores as int bits of float
      long bits = RealType.REAL.getLong(block, position);
      return (double) Float.intBitsToFloat((int) bits);
    } else if (type instanceof BooleanType) {
      return BooleanType.BOOLEAN.getBoolean(block, position);
    } else if (type instanceof VarcharType) {
      return VarcharType.VARCHAR.getSlice(block, position).toStringUtf8();
    } else if (type instanceof TimestampType) {
      // Trino stores timestamps as microseconds since epoch.
      // Format as date or datetime string depending on time components.
      long microsSinceEpoch = type.getLong(block, position);
      long millisSinceEpoch = microsSinceEpoch / 1000;
      java.time.LocalDateTime ldt =
          Instant.ofEpochMilli(millisSinceEpoch).atZone(ZoneOffset.UTC).toLocalDateTime();
      if (ldt.getHour() == 0 && ldt.getMinute() == 0 && ldt.getSecond() == 0) {
        return ldt.toLocalDate().toString(); // YYYY-MM-DD
      }
      return ldt.format(java.time.format.DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"));
    } else {
      // Default: try getLong for other numeric types
      try {
        return type.getLong(block, position);
      } catch (Exception e) {
        return block.toString();
      }
    }
  }

  /** Map Trino Type to OpenSearch type name for schema output. */
  private static String trinoTypeToOpenSearchType(Type type) {
    if (type instanceof BigintType) {
      return "long";
    } else if (type instanceof IntegerType) {
      return "integer";
    } else if (type instanceof SmallintType) {
      return "short";
    } else if (type instanceof TinyintType) {
      return "byte";
    } else if (type instanceof DoubleType) {
      return "double";
    } else if (type instanceof RealType) {
      return "float";
    } else if (type instanceof BooleanType) {
      return "boolean";
    } else if (type instanceof VarcharType) {
      return "keyword";
    } else {
      return "keyword";
    }
  }

  private static void appendJsonValue(StringBuilder sb, Object value) {
    if (value == null) {
      sb.append("null");
    } else if (value instanceof Number) {
      sb.append(value);
    } else if (value instanceof Boolean) {
      sb.append(value);
    } else {
      sb.append("\"").append(escapeJson(value.toString())).append("\"");
    }
  }

  private static final char[] HEX_DIGITS = "0123456789abcdef".toCharArray();

  private static String escapeJson(String s) {
    if (s == null) {
      return "null";
    }
    // Fast-path: scan for characters that need escaping. Most strings (column names,
    // numeric values) never need escaping, so we can return the original string directly
    // and avoid StringBuilder allocation entirely.
    boolean needsEscape = false;
    for (int i = 0; i < s.length(); i++) {
      char c = s.charAt(i);
      if (c == '\\' || c == '"' || c == '\n' || c == '\r' || c == '\t' || c < 0x20) {
        needsEscape = true;
        break;
      }
    }
    if (!needsEscape) {
      return s;
    }
    StringBuilder sb = new StringBuilder(s.length() + 16);
    for (int i = 0; i < s.length(); i++) {
      char c = s.charAt(i);
      switch (c) {
        case '\\':
          sb.append("\\\\");
          break;
        case '"':
          sb.append("\\\"");
          break;
        case '\n':
          sb.append("\\n");
          break;
        case '\r':
          sb.append("\\r");
          break;
        case '\t':
          sb.append("\\t");
          break;
        default:
          if (c < 0x20) {
            // Escape control characters as JSON unicode escapes (manual hex to avoid
            // String.format overhead)
            sb.append("\\u00");
            sb.append(HEX_DIGITS[(c >> 4) & 0xF]);
            sb.append(HEX_DIGITS[c & 0xF]);
          } else {
            sb.append(c);
          }
      }
    }
    return sb.toString();
  }

  /**
   * Walk the plan tree to find a LimitNode and return its count. Returns -1 if no limit is present.
   */
  static long findGlobalLimit(DqePlanNode plan) {
    return plan.accept(
        new DqePlanVisitor<Long, Void>() {
          @Override
          public Long visitPlan(DqePlanNode node, Void context) {
            if (node instanceof LimitNode limitNode) {
              return limitNode.getCount();
            }
            for (DqePlanNode child : node.getChildren()) {
              Long result = child.accept(this, context);
              if (result >= 0) {
                return result;
              }
            }
            return -1L;
          }
        },
        null);
  }

  /** Walk the plan tree to find a SortNode. Returns null if none. */
  static SortNode findSortNode(DqePlanNode plan) {
    return plan.accept(
        new DqePlanVisitor<SortNode, Void>() {
          @Override
          public SortNode visitPlan(DqePlanNode node, Void context) {
            if (node instanceof SortNode sortNode) {
              return sortNode;
            }
            for (DqePlanNode child : node.getChildren()) {
              SortNode result = child.accept(this, context);
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
   * Apply HAVING filter at the coordinator after merging aggregation results from shards. The
   * HAVING (FilterNode above AggregationNode) was stripped from shard plans because per-shard
   * partial counts would incorrectly filter groups.
   */
  private static List<Page> applyCoordinatorHaving(
      List<Page> mergedPages,
      DqePlanNode optimizedPlan,
      AggregationNode aggNode,
      Map<String, Type> columnTypeMap) {
    // Find HAVING FilterNode in the original plan (above AggregationNode)
    FilterNode havingNode = findHavingNode(optimizedPlan);
    if (havingNode == null) {
      return mergedPages;
    }

    // Build column index map for the aggregation output
    List<String> aggOutputCols = new ArrayList<>(aggNode.getGroupByKeys());
    aggOutputCols.addAll(aggNode.getAggregateFunctions());
    Map<String, Integer> colIndexMap = new HashMap<>();
    for (int i = 0; i < aggOutputCols.size(); i++) {
      colIndexMap.put(aggOutputCols.get(i), i);
    }

    // Compile the HAVING predicate
    FunctionRegistry registry = BuiltinFunctions.createRegistry();
    ExpressionCompiler compiler = new ExpressionCompiler(registry, colIndexMap, columnTypeMap);
    DqeSqlParser parser = new DqeSqlParser();
    io.trino.sql.tree.Expression predExpr = parser.parseExpression(havingNode.getPredicateString());
    org.opensearch.sql.dqe.function.expression.BlockExpression blockPred =
        compiler.compile(predExpr);

    // Apply filter to each page
    List<Page> filtered = new ArrayList<>();
    org.opensearch.sql.dqe.operator.FilterOperator filterOp =
        new org.opensearch.sql.dqe.operator.FilterOperator(
            new org.opensearch.sql.dqe.operator.Operator() {
              private int idx = 0;

              @Override
              public Page processNextBatch() {
                return idx < mergedPages.size() ? mergedPages.get(idx++) : null;
              }

              @Override
              public void close() {}
            },
            blockPred);
    Page page;
    while ((page = filterOp.processNextBatch()) != null) {
      if (page.getPositionCount() > 0) {
        filtered.add(page);
      }
    }
    return filtered;
  }

  /** Find a FilterNode directly above an AggregationNode (HAVING clause). */
  private static FilterNode findHavingNode(DqePlanNode plan) {
    return plan.accept(
        new DqePlanVisitor<FilterNode, Void>() {
          @Override
          public FilterNode visitFilter(FilterNode node, Void context) {
            // Check if this filter's child (or child chain) leads to AggregationNode
            if (hasAggregationChild(node.getChild())) {
              return node;
            }
            return node.getChild().accept(this, context);
          }

          @Override
          public FilterNode visitPlan(DqePlanNode node, Void context) {
            for (DqePlanNode child : node.getChildren()) {
              FilterNode result = child.accept(this, context);
              if (result != null) return result;
            }
            return null;
          }

          private boolean hasAggregationChild(DqePlanNode node) {
            if (node instanceof AggregationNode) return true;
            if (node instanceof EvalNode) return hasAggregationChild(((EvalNode) node).getChild());
            return false;
          }
        },
        null);
  }

  /**
   * Apply Sort + Limit to merged aggregation results at the coordinator. The original plan's Sort
   * and Limit were stripped from the shard plan, so the coordinator must apply them after merging.
   */
  private static List<Page> applyCoordinatorSort(
      List<Page> mergedPages,
      AggregationNode aggNode,
      DqePlanNode optimizedPlan,
      List<Type> columnTypes,
      ResultMerger merger) {
    SortNode sortNode = findSortNode(optimizedPlan);
    if (sortNode == null) {
      return mergedPages;
    }
    // Resolve sort key indices in the aggregation output columns
    List<String> aggOutputCols = new ArrayList<>(aggNode.getGroupByKeys());
    aggOutputCols.addAll(aggNode.getAggregateFunctions());
    List<Integer> sortIndices =
        sortNode.getSortKeys().stream().map(aggOutputCols::indexOf).collect(Collectors.toList());
    long rawLimit = findGlobalLimit(optimizedPlan);
    long sortLimit = rawLimit >= 0 ? rawLimit + findGlobalOffset(optimizedPlan) : Long.MAX_VALUE;
    return merger.mergeSorted(
        List.of(mergedPages),
        sortIndices,
        sortNode.getAscending(),
        sortNode.getNullsFirst(),
        columnTypes,
        sortLimit);
  }

  /** Walk the plan tree to find a LimitNode and return its offset. Returns 0 if none. */
  static long findGlobalOffset(DqePlanNode plan) {
    return plan.accept(
        new DqePlanVisitor<Long, Void>() {
          @Override
          public Long visitPlan(DqePlanNode node, Void context) {
            if (node instanceof LimitNode limitNode) {
              return limitNode.getOffset();
            }
            for (DqePlanNode child : node.getChildren()) {
              Long result = child.accept(this, context);
              if (result > 0) {
                return result;
              }
            }
            return 0L;
          }
        },
        null);
  }

  /** Skip the first {@code offset} rows from merged pages. */
  static List<Page> applyGlobalOffset(List<Page> pages, long offset) {
    List<Page> result = new ArrayList<>();
    long remaining = offset;
    for (Page page : pages) {
      if (remaining <= 0) {
        result.add(page);
      } else if (page.getPositionCount() <= remaining) {
        remaining -= page.getPositionCount();
      } else {
        result.add(page.getRegion((int) remaining, page.getPositionCount() - (int) remaining));
        remaining = 0;
      }
    }
    return result;
  }

  /**
   * Apply a global row limit to merged pages. Trims the list of pages so that at most {@code limit}
   * total rows are retained.
   */
  static List<Page> applyGlobalLimit(List<Page> pages, long limit) {
    List<Page> result = new ArrayList<>();
    long remaining = limit;
    for (Page page : pages) {
      if (remaining <= 0) {
        break;
      }
      if (page.getPositionCount() <= remaining) {
        result.add(page);
        remaining -= page.getPositionCount();
      } else {
        // Trim this page to the remaining count
        result.add(page.getRegion(0, (int) remaining));
        remaining = 0;
      }
    }
    return result;
  }

  /**
   * Run full aggregation at the coordinator for queries that can't use PARTIAL/FINAL split (e.g.,
   * COUNT(DISTINCT)). Feeds raw shard pages through a HashAggregationOperator.
   *
   * @param aggNode the aggregation node (group-by keys + functions)
   * @param rawPages concatenated raw pages from all shards
   * @param rawColumnNames column names in the raw pages (from shard scan output)
   * @param columnTypeMap field name → Trino Type mapping
   */
  private static List<Page> runCoordinatorAggregation(
      AggregationNode aggNode,
      List<Page> rawPages,
      List<String> rawColumnNames,
      Map<String, Type> columnTypeMap) {
    if (rawPages.isEmpty()) {
      return List.of();
    }

    org.opensearch.sql.dqe.shard.executor.LocalExecutionPlanner planner =
        new org.opensearch.sql.dqe.shard.executor.LocalExecutionPlanner(
            scan -> null, columnTypeMap);

    Operator pageSource =
        new Operator() {
          private int pageIndex = 0;

          @Override
          public Page processNextBatch() {
            return pageIndex < rawPages.size() ? rawPages.get(pageIndex++) : null;
          }

          @Override
          public void close() {}
        };

    Operator aggOperator = planner.buildAggregationOperator(pageSource, aggNode, rawColumnNames);

    List<Page> result = new ArrayList<>();
    Page page;
    while ((page = aggOperator.processNextBatch()) != null) {
      result.add(page);
    }
    return result;
  }

  /**
   * Check if the coordinator aggregation node is a scalar (no GROUP BY) FINAL merge that can use
   * the fast merge path.
   */
  private static boolean isScalarPartialMerge(AggregationNode aggNode) {
    return aggNode.getStep() == AggregationNode.Step.FINAL && aggNode.getGroupByKeys().isEmpty();
  }

  /**
   * Check if the coordinator aggregation node is a scalar COUNT(DISTINCT numericCol) in SINGLE mode
   * where the column is a numeric (long-representable) type. Shards have already pre-deduped values
   * as longs, so the coordinator can merge with LongOpenHashSet. Returns false for non-numeric
   * columns (e.g., VARCHAR) to avoid ClassCastException when reading VariableWidthBlock as
   * LongArrayBlock.
   */
  private static boolean isScalarCountDistinctLong(
      AggregationNode aggNode, Map<String, Type> columnTypeMap) {
    if (!aggNode.getGroupByKeys().isEmpty()) {
      return false;
    }
    List<String> aggs = aggNode.getAggregateFunctions();
    if (aggs.size() != 1) {
      return false;
    }
    String originalAgg = aggs.get(0);
    String aggUpper = originalAgg.toUpperCase(java.util.Locale.ROOT);
    if (!aggUpper.startsWith("COUNT(DISTINCT ")) {
      return false;
    }
    // Extract the column name from the original aggregate string using the known
    // prefix length. The prefix "COUNT(DISTINCT " is 15 chars regardless of case.
    String colName = originalAgg.substring(15, originalAgg.length() - 1).trim();
    // Verify the column is a numeric (long-representable) type.
    // VARCHAR columns (like SearchPhrase) must NOT use this fast path — the shards
    // will not have pre-deduped them as longs, so mergeCountDistinctValues() would
    // throw ClassCastException (VariableWidthBlock cannot be cast to LongArrayBlock).
    Type colType = columnTypeMap.get(colName);
    return colType instanceof BigintType
        || colType instanceof IntegerType
        || colType instanceof SmallintType
        || colType instanceof TinyintType
        || colType instanceof TimestampType;
  }

  /**
   * Check if the coordinator aggregation node is a scalar COUNT(DISTINCT varcharCol) in SINGLE mode
   * where the column is a VARCHAR type. Shards have already pre-deduped values as strings, so the
   * coordinator can merge distinct string sets.
   */
  private static boolean isScalarCountDistinctVarchar(
      AggregationNode aggNode, Map<String, Type> columnTypeMap) {
    if (!aggNode.getGroupByKeys().isEmpty()) {
      return false;
    }
    List<String> aggs = aggNode.getAggregateFunctions();
    if (aggs.size() != 1) {
      return false;
    }
    String originalAgg = aggs.get(0);
    String aggUpper = originalAgg.toUpperCase(java.util.Locale.ROOT);
    if (!aggUpper.startsWith("COUNT(DISTINCT ")) {
      return false;
    }
    String colName = originalAgg.substring(15, originalAgg.length() - 1).trim();
    Type colType = columnTypeMap.get(colName);
    return colType instanceof VarcharType;
  }

  /**
   * Merge pre-deduplicated distinct VARCHAR value pages from all shards into a single
   * COUNT(DISTINCT) result. Each shard sends a page of unique string values; the coordinator unions
   * them via a zero-allocation open-addressing hash set and returns the count.
   *
   * <p>Uses {@link org.opensearch.sql.dqe.operator.SliceRangeHashSet} which operates directly on
   * raw byte ranges from VariableWidthBlock, avoiding all per-entry object allocation: no String
   * conversion (UTF-16 decoding), no Slice copying, no boxing. Pre-sizes the hash set based on
   * total values across all shards to avoid resize overhead.
   *
   * @param shardPages pages from each shard, each containing distinct values as a VarcharType
   *     column
   * @return single-row page with the global distinct count
   */
  private static List<Page> mergeCountDistinctVarcharValues(List<List<Page>> shardPages) {
    // Pre-compute total values to pre-size the hash set (avoids expensive resizing)
    int totalValues = 0;
    for (List<Page> pages : shardPages) {
      for (Page page : pages) {
        totalValues += page.getPositionCount();
      }
    }
    // Zero-allocation open-addressing hash set: stores (Slice ref, offset, length) triples
    // referencing the raw block bytes. No String or Slice object allocated per entry.
    org.opensearch.sql.dqe.operator.SliceRangeHashSet globalSet =
        new org.opensearch.sql.dqe.operator.SliceRangeHashSet(totalValues);
    for (List<Page> pages : shardPages) {
      for (Page page : pages) {
        Block block = page.getBlock(0);
        int positionCount = page.getPositionCount();
        if (block instanceof io.trino.spi.block.VariableWidthBlock vwb) {
          // Fast path: access raw slice directly — zero object allocation per position
          io.airlift.slice.Slice rawSlice = vwb.getRawSlice();
          if (!vwb.mayHaveNull()) {
            // No nulls: skip null check entirely
            for (int pos = 0; pos < positionCount; pos++) {
              globalSet.add(rawSlice, vwb.getRawSliceOffset(pos), vwb.getSliceLength(pos));
            }
          } else {
            for (int pos = 0; pos < positionCount; pos++) {
              if (!vwb.isNull(pos)) {
                globalSet.add(rawSlice, vwb.getRawSliceOffset(pos), vwb.getSliceLength(pos));
              }
            }
          }
        } else {
          // Fallback for other block types (e.g., DictionaryBlock)
          io.trino.spi.block.VariableWidthBlock underlying =
              (io.trino.spi.block.VariableWidthBlock) block.getUnderlyingValueBlock();
          io.airlift.slice.Slice rawSlice = underlying.getRawSlice();
          for (int pos = 0; pos < positionCount; pos++) {
            if (!block.isNull(pos)) {
              int uPos = block.getUnderlyingValuePosition(pos);
              globalSet.add(
                  rawSlice, underlying.getRawSliceOffset(uPos), underlying.getSliceLength(uPos));
            }
          }
        }
      }
    }
    io.trino.spi.block.BlockBuilder builder = BigintType.BIGINT.createBlockBuilder(null, 1);
    BigintType.BIGINT.writeLong(builder, globalSet.size());
    return List.of(new Page(builder.build()));
  }

  /**
   * Merge pre-deduplicated distinct value pages from all shards into a single COUNT(DISTINCT)
   * result. Each shard sends a page of unique long values; the coordinator unions them via
   * LongOpenHashSet and returns the count.
   *
   * @param shardPages pages from each shard, each containing distinct values as a BigintType column
   * @return single-row page with the global distinct count
   */
  private static List<Page> mergeCountDistinctValues(List<List<Page>> shardPages) {
    // Pre-compute total row count across all shards to pre-size the hash set.
    // This avoids expensive resizing when inserting ~200K values.
    int totalValues = 0;
    for (List<Page> pages : shardPages) {
      for (Page page : pages) {
        totalValues += page.getPositionCount();
      }
    }
    org.opensearch.sql.dqe.operator.LongOpenHashSet globalSet =
        new org.opensearch.sql.dqe.operator.LongOpenHashSet(totalValues);
    for (List<Page> pages : shardPages) {
      for (Page page : pages) {
        Block block = page.getBlock(0);
        int positionCount = page.getPositionCount();
        for (int pos = 0; pos < positionCount; pos++) {
          if (!block.isNull(pos)) {
            globalSet.add(BigintType.BIGINT.getLong(block, pos));
          }
        }
      }
    }
    io.trino.spi.block.BlockBuilder builder = BigintType.BIGINT.createBlockBuilder(null, 1);
    BigintType.BIGINT.writeLong(builder, globalSet.size());
    return List.of(new Page(builder.build()));
  }

  /**
   * Check if the shard plan uses dedup for COUNT(DISTINCT) queries. Returns true when the shard
   * plan is a PARTIAL AggregationNode (created by PlanFragmenter for dedup), the coordinator plan
   * is a SINGLE AggregationNode with GROUP BY + COUNT(DISTINCT), and the shard aggregates exactly
   * COUNT(*). This enables the two-stage merge (FINAL dedup + row counting) which avoids the
   * expensive HashSet-based COUNT(DISTINCT) accumulators in runCoordinatorAggregation.
   */
  private static boolean isShardDedupCountDistinct(
      DqePlanNode shardPlan, AggregationNode singleAgg, Map<String, Type> columnTypeMap) {
    if (!(shardPlan instanceof AggregationNode shardAgg)) {
      return false;
    }
    if (shardAgg.getStep() != AggregationNode.Step.PARTIAL) {
      return false;
    }
    if (singleAgg.getGroupByKeys().isEmpty()) {
      return false;
    }
    // Shard dedup has more group-by keys than the original (original keys + distinct columns)
    if (shardAgg.getGroupByKeys().size() <= singleAgg.getGroupByKeys().size()
        || shardAgg.getAggregateFunctions().size() != 1
        || !"COUNT(*)".equals(shardAgg.getAggregateFunctions().get(0))) {
      return false;
    }
    return true;
  }

  /**
   * Two-stage merge for shard-deduped COUNT(DISTINCT) queries. Shards produce pre-deduped (key +
   * distinct_col, COUNT(*)) tuples. The coordinator:
   *
   * <ol>
   *   <li>Stage 1: FINAL merge for dedup keys to remove cross-shard duplicates
   *   <li>Stage 2: GROUP BY original keys with COUNT(*) to get COUNT(DISTINCT)
   * </ol>
   *
   * This leverages the fast FINAL merge path (O(n) hash merge) instead of the generic
   * HashAggregationOperator with per-row CountDistinctAccumulator (HashSet per group).
   */
  private static List<Page> mergeDedupCountDistinct(
      List<List<Page>> shardPages,
      AggregationNode singleAgg,
      DqePlanNode shardPlan,
      List<Type> columnTypes,
      Map<String, Type> columnTypeMap,
      ResultMerger merger) {
    AggregationNode shardAgg = (AggregationNode) shardPlan;
    List<String> dedupKeys = shardAgg.getGroupByKeys();
    List<String> originalKeys = singleAgg.getGroupByKeys();

    // Stage 1: FINAL merge for dedup keys (removes cross-shard duplicates)
    // Build types for the dedup output: [dedupKey types..., BigintType for COUNT(*)]
    List<Type> dedupTypes = new ArrayList<>();
    for (String key : dedupKeys) {
      dedupTypes.add(columnTypeMap.getOrDefault(key, BigintType.BIGINT));
    }
    dedupTypes.add(BigintType.BIGINT); // COUNT(*) column

    int numOriginalKeys = originalKeys.size();
    int numDedupKeys = dedupKeys.size();
    int numCountDistinctAggs = singleAgg.getAggregateFunctions().size();

    // === Fused path: all-numeric dedup keys with single original numeric key ===
    // Combines Stage 1 (FINAL dedup merge) and Stage 2 (row counting by original keys)
    // into a single pass, eliminating intermediate Page construction.
    boolean allNumericDedup = true;
    for (int i = 0; i < numDedupKeys; i++) {
      Type t = dedupTypes.get(i);
      if (!(t instanceof BigintType
          || t instanceof IntegerType
          || t instanceof io.trino.spi.type.TimestampType
          || t instanceof io.trino.spi.type.SmallintType
          || t instanceof io.trino.spi.type.TinyintType
          || t instanceof io.trino.spi.type.BooleanType)) {
        allNumericDedup = false;
        break;
      }
    }

    if (allNumericDedup && numOriginalKeys == 1 && numDedupKeys == 2) {
      // Ultra-fast 2-key fused path: uses two flat long[] arrays instead of long[][]
      // to eliminate all per-entry array allocation and indirection. For Q9-style
      // GROUP BY RegionID COUNT(DISTINCT UserID), this avoids 80K long[2] allocations.
      return mergeDedupCountDistinct2Key(
          shardPages, dedupTypes, numOriginalKeys, numCountDistinctAggs);
    }

    if (allNumericDedup && numOriginalKeys == 1) {
      // Fused: Build dedup hashmap from shard pages, then count per original key.
      // Uses open-addressing hash map with long[] keys for zero-allocation inner loop.
      int capacity = 1024;
      float loadFactor = 0.7f;
      int threshold = (int) (capacity * loadFactor);
      int size = 0;
      long[][] mapKeys = new long[capacity][];
      long[] mapCounts = new long[capacity];
      boolean[] mapOccupied = new boolean[capacity];
      long[] tmpKey = new long[numDedupKeys];

      for (java.util.List<Page> shardPageList : shardPages) {
        for (Page page : shardPageList) {
          int positionCount = page.getPositionCount();
          Block[] keyBlocks = new Block[numDedupKeys];
          for (int k = 0; k < numDedupKeys; k++) {
            keyBlocks[k] = page.getBlock(k);
          }
          Block countBlock = page.getBlock(numDedupKeys); // COUNT(*) column

          for (int pos = 0; pos < positionCount; pos++) {
            for (int k = 0; k < numDedupKeys; k++) {
              tmpKey[k] = dedupTypes.get(k).getLong(keyBlocks[k], pos);
            }
            long cnt = BigintType.BIGINT.getLong(countBlock, pos);
            int hash = 1;
            for (int k = 0; k < numDedupKeys; k++) {
              hash = hash * 31 + Long.hashCode(tmpKey[k]);
            }
            int mask = capacity - 1;
            int slot = hash & mask;
            while (true) {
              if (!mapOccupied[slot]) {
                mapKeys[slot] = tmpKey.clone();
                mapCounts[slot] = cnt;
                mapOccupied[slot] = true;
                size++;
                if (size > threshold) {
                  int newCap = capacity * 2;
                  long[][] nk = new long[newCap][];
                  long[] nc = new long[newCap];
                  boolean[] no = new boolean[newCap];
                  int nm = newCap - 1;
                  for (int s = 0; s < capacity; s++) {
                    if (mapOccupied[s]) {
                      int h = 1;
                      for (int k = 0; k < numDedupKeys; k++) {
                        h = h * 31 + Long.hashCode(mapKeys[s][k]);
                      }
                      int ns = h & nm;
                      while (no[ns]) ns = (ns + 1) & nm;
                      nk[ns] = mapKeys[s];
                      nc[ns] = mapCounts[s];
                      no[ns] = true;
                    }
                  }
                  mapKeys = nk;
                  mapCounts = nc;
                  mapOccupied = no;
                  capacity = newCap;
                  mask = nm;
                  threshold = (int) (newCap * loadFactor);
                  slot = hash & mask;
                  while (mapOccupied[slot]) {
                    long[] existing = mapKeys[slot];
                    boolean match = true;
                    for (int k = 0; k < numDedupKeys; k++) {
                      if (existing[k] != tmpKey[k]) {
                        match = false;
                        break;
                      }
                    }
                    if (match) break;
                    slot = (slot + 1) & mask;
                  }
                }
                break;
              }
              long[] existing = mapKeys[slot];
              boolean match = true;
              for (int k = 0; k < numDedupKeys; k++) {
                if (existing[k] != tmpKey[k]) {
                  match = false;
                  break;
                }
              }
              if (match) {
                mapCounts[slot] += cnt; // SUM the partial counts
                break;
              }
              slot = (slot + 1) & mask;
            }
          }
        }
      }

      if (size == 0) {
        return List.of();
      }

      // Stage 2: Count entries per original key (first dedup key)
      java.util.HashMap<Long, Long> groupCounts = new java.util.HashMap<>();
      for (int s = 0; s < capacity; s++) {
        if (!mapOccupied[s]) continue;
        long origKey = mapKeys[s][0];
        groupCounts.merge(origKey, 1L, Long::sum);
      }

      // Build result page
      Type keyType = dedupTypes.get(0);
      int numOutputCols = numOriginalKeys + numCountDistinctAggs;
      io.trino.spi.block.BlockBuilder[] builders =
          new io.trino.spi.block.BlockBuilder[numOutputCols];
      builders[0] = keyType.createBlockBuilder(null, groupCounts.size());
      for (int i = numOriginalKeys; i < numOutputCols; i++) {
        builders[i] = BigintType.BIGINT.createBlockBuilder(null, groupCounts.size());
      }

      for (var entry : groupCounts.entrySet()) {
        long key = entry.getKey();
        if (keyType instanceof IntegerType) {
          IntegerType.INTEGER.writeLong(builders[0], (int) key);
        } else {
          keyType.writeLong(builders[0], key);
        }
        BigintType.BIGINT.writeLong(builders[numOriginalKeys], entry.getValue());
      }

      Block[] blocks = new Block[numOutputCols];
      for (int i = 0; i < numOutputCols; i++) {
        blocks[i] = builders[i].build();
      }
      return List.of(new Page(blocks));
    }

    // === Fused path: VARCHAR original key + numeric distinct column ===
    // For Q14-style queries: GROUP BY SearchPhrase COUNT(DISTINCT UserID)
    // Dedup keys are [SearchPhrase (VARCHAR), UserID (BIGINT)], original key is [SearchPhrase].
    // Uses SliceLongDedupMap for zero-copy dedup, then SliceCountMap for group counting.
    if (!allNumericDedup
        && numOriginalKeys == 1
        && numDedupKeys == 2
        && dedupTypes.get(0) instanceof VarcharType
        && isNumericType(dedupTypes.get(1))) {
      return mergeDedupCountDistinctVarcharKey(shardPages, dedupTypes, numCountDistinctAggs);
    }

    // === Fallback: two-stage merge via intermediate Pages ===
    AggregationNode finalDedupNode =
        new AggregationNode(null, dedupKeys, List.of("COUNT(*)"), AggregationNode.Step.FINAL);
    List<Page> dedupedPages = merger.mergeAggregation(shardPages, finalDedupNode, dedupTypes);

    Type[] keyTypes = new Type[numOriginalKeys];
    for (int i = 0; i < numOriginalKeys; i++) {
      keyTypes[i] = dedupTypes.get(i);
    }

    java.util.LinkedHashMap<Object, Long> groupCounts = new java.util.LinkedHashMap<>();

    for (Page page : dedupedPages) {
      if (numOriginalKeys == 1) {
        Block keyBlock = page.getBlock(0);
        Type keyType = keyTypes[0];
        for (int pos = 0; pos < page.getPositionCount(); pos++) {
          Object key = extractValue(page, 0, pos, keyType);
          groupCounts.merge(key, 1L, Long::sum);
        }
      } else {
        for (int pos = 0; pos < page.getPositionCount(); pos++) {
          List<Object> key = new ArrayList<>(numOriginalKeys);
          for (int i = 0; i < numOriginalKeys; i++) {
            key.add(extractValue(page, i, pos, keyTypes[i]));
          }
          groupCounts.merge(key, 1L, Long::sum);
        }
      }
    }

    if (groupCounts.isEmpty()) {
      return List.of();
    }

    int numOutputCols = numOriginalKeys + numCountDistinctAggs;
    io.trino.spi.block.BlockBuilder[] builders = new io.trino.spi.block.BlockBuilder[numOutputCols];
    for (int i = 0; i < numOriginalKeys; i++) {
      builders[i] = keyTypes[i].createBlockBuilder(null, groupCounts.size());
    }
    for (int i = numOriginalKeys; i < numOutputCols; i++) {
      builders[i] = BigintType.BIGINT.createBlockBuilder(null, groupCounts.size());
    }

    for (var entry : groupCounts.entrySet()) {
      Object key = entry.getKey();
      if (numOriginalKeys == 1) {
        appendTypedValue(builders[0], keyTypes[0], key);
      } else {
        @SuppressWarnings("unchecked")
        List<Object> multiKey = (List<Object>) key;
        for (int i = 0; i < numOriginalKeys; i++) {
          appendTypedValue(builders[i], keyTypes[i], multiKey.get(i));
        }
      }
      BigintType.BIGINT.writeLong(builders[numOriginalKeys], entry.getValue());
    }

    Block[] blocks = new Block[numOutputCols];
    for (int i = 0; i < numOutputCols; i++) {
      blocks[i] = builders[i].build();
    }
    return List.of(new Page(blocks));
  }

  /**
   * Ultra-fast 2-key fused merge for COUNT(DISTINCT) with exactly 2 numeric dedup keys and 1
   * numeric original key. Uses two flat long[] arrays instead of long[][] to eliminate all
   * per-entry array allocation. For Q9-style queries (GROUP BY RegionID, COUNT(DISTINCT UserID)),
   * this processes ~80K shard rows with zero allocation in the inner loop.
   *
   * <p>Stage 1: Build open-addressing hash map from (key0, key1) pairs (set insertion, ignoring
   * counts). Stage 2: Count entries per original key (key0) to get COUNT(DISTINCT).
   */
  private static List<Page> mergeDedupCountDistinct2Key(
      List<List<Page>> shardPages,
      List<Type> dedupTypes,
      int numOriginalKeys,
      int numCountDistinctAggs) {
    // Pre-size capacity: use largest single shard's row count as estimate (not total)
    // because dedup keys overlap across shards.
    int maxShardRows = 0;
    int totalRows = 0;
    for (List<Page> shardPageList : shardPages) {
      int shardRows = 0;
      for (Page page : shardPageList) {
        shardRows += page.getPositionCount();
      }
      totalRows += shardRows;
      if (shardRows > maxShardRows) maxShardRows = shardRows;
    }
    int estimatedUnique = Math.min(totalRows, maxShardRows * 2);
    int capacity = Integer.highestOneBit(Math.max(1024, (int) (estimatedUnique / 0.65f))) << 1;
    float loadFactor = 0.65f;
    int threshold = (int) (capacity * loadFactor);
    int size = 0;
    long[] mapKey0 = new long[capacity];
    long[] mapKey1 = new long[capacity];
    boolean[] mapOccupied = new boolean[capacity];
    // Track occupied slots for O(size) Stage 2 iteration
    int[] occupiedSlots = new int[Math.max(1024, estimatedUnique)];

    Type type0 = dedupTypes.get(0);
    Type type1 = dedupTypes.get(1);

    for (List<Page> shardPageList : shardPages) {
      for (Page page : shardPageList) {
        int positionCount = page.getPositionCount();
        Block block0 = page.getBlock(0);
        Block block1 = page.getBlock(1);

        for (int pos = 0; pos < positionCount; pos++) {
          long k0 = type0.getLong(block0, pos);
          long k1 = type1.getLong(block1, pos);
          // Murmur-style hash mix for better distribution
          int hash = Long.hashCode(k0) * 0x9E3779B9 + Long.hashCode(k1);
          int mask = capacity - 1;
          int slot = hash & mask;
          while (true) {
            if (!mapOccupied[slot]) {
              mapKey0[slot] = k0;
              mapKey1[slot] = k1;
              mapOccupied[slot] = true;
              if (size >= occupiedSlots.length) {
                occupiedSlots = java.util.Arrays.copyOf(occupiedSlots, occupiedSlots.length * 2);
              }
              occupiedSlots[size] = slot;
              size++;
              if (size > threshold) {
                // Resize
                int newCap = capacity * 2;
                long[] nk0 = new long[newCap];
                long[] nk1 = new long[newCap];
                boolean[] no = new boolean[newCap];
                int nm = newCap - 1;
                int rebuildIdx = 0;
                if (size > occupiedSlots.length) {
                  occupiedSlots = java.util.Arrays.copyOf(occupiedSlots, size * 2);
                }
                for (int si = 0; si < size; si++) {
                  int oldSlot = occupiedSlots[si];
                  int h =
                      Long.hashCode(mapKey0[oldSlot]) * 0x9E3779B9
                          + Long.hashCode(mapKey1[oldSlot]);
                  int ns = h & nm;
                  while (no[ns]) ns = (ns + 1) & nm;
                  nk0[ns] = mapKey0[oldSlot];
                  nk1[ns] = mapKey1[oldSlot];
                  no[ns] = true;
                  occupiedSlots[rebuildIdx++] = ns;
                }
                mapKey0 = nk0;
                mapKey1 = nk1;
                mapOccupied = no;
                capacity = newCap;
                mask = nm;
                threshold = (int) (newCap * loadFactor);
              }
              break;
            }
            if (mapKey0[slot] == k0 && mapKey1[slot] == k1) {
              break; // duplicate — skip
            }
            slot = (slot + 1) & mask;
          }
        }
      }
    }

    if (size == 0) {
      return List.of();
    }

    // Stage 2: Count entries per original key (key0)
    // Iterate only occupied slots (O(size) instead of O(capacity))
    int grpCap = 256;
    long[] grpKeys = new long[grpCap];
    long[] grpCounts = new long[grpCap];
    boolean[] grpOcc = new boolean[grpCap];
    int grpSize = 0;
    int grpThreshold = (int) (grpCap * 0.7f);

    for (int si = 0; si < size; si++) {
      int s = occupiedSlots[si];
      long origKey = mapKey0[s];
      int gm = grpCap - 1;
      int gs = Long.hashCode(origKey) & gm;
      while (grpOcc[gs] && grpKeys[gs] != origKey) {
        gs = (gs + 1) & gm;
      }
      if (grpOcc[gs]) {
        grpCounts[gs]++;
      } else {
        grpKeys[gs] = origKey;
        grpCounts[gs] = 1;
        grpOcc[gs] = true;
        grpSize++;
        if (grpSize > grpThreshold) {
          int newGC = grpCap * 2;
          long[] ngk = new long[newGC];
          long[] ngc = new long[newGC];
          boolean[] ngo = new boolean[newGC];
          int ngm = newGC - 1;
          for (int g = 0; g < grpCap; g++) {
            if (grpOcc[g]) {
              int ns = Long.hashCode(grpKeys[g]) & ngm;
              while (ngo[ns]) ns = (ns + 1) & ngm;
              ngk[ns] = grpKeys[g];
              ngc[ns] = grpCounts[g];
              ngo[ns] = true;
            }
          }
          grpKeys = ngk;
          grpCounts = ngc;
          grpOcc = ngo;
          grpCap = newGC;
          grpThreshold = (int) (newGC * 0.7f);
        }
      }
    }

    // Build result page
    Type keyType = dedupTypes.get(0);
    int numOutputCols = numOriginalKeys + numCountDistinctAggs;
    io.trino.spi.block.BlockBuilder[] builders = new io.trino.spi.block.BlockBuilder[numOutputCols];
    builders[0] = keyType.createBlockBuilder(null, grpSize);
    for (int i = numOriginalKeys; i < numOutputCols; i++) {
      builders[i] = BigintType.BIGINT.createBlockBuilder(null, grpSize);
    }

    for (int gs = 0; gs < grpCap; gs++) {
      if (!grpOcc[gs]) continue;
      long key = grpKeys[gs];
      if (keyType instanceof IntegerType) {
        IntegerType.INTEGER.writeLong(builders[0], (int) key);
      } else {
        keyType.writeLong(builders[0], key);
      }
      // Write the same count for all COUNT(DISTINCT) columns
      for (int i = numOriginalKeys; i < numOutputCols; i++) {
        BigintType.BIGINT.writeLong(builders[i], grpCounts[gs]);
      }
    }

    Block[] blocks = new Block[numOutputCols];
    for (int i = 0; i < numOutputCols; i++) {
      blocks[i] = builders[i].build();
    }
    return List.of(new Page(blocks));
  }

  /**
   * Check if the shard plan uses mixed-aggregate dedup. Returns true when the shard plan is a
   * PARTIAL AggregationNode whose GROUP BY keys are a superset of the original SINGLE aggregation's
   * GROUP BY keys, AND the shard has more aggregate functions than just COUNT(*) (distinguishing
   * from the COUNT-DISTINCT-only dedup path).
   */

  /**
   * Ultra-fast 2-key fused merge for mixed-aggregate dedup queries (Q10-style). Uses two flat
   * long[] arrays instead of long[][] to eliminate per-entry allocation. Combines Stage 1 (dedup
   * merge) and Stage 2 (re-aggregate by original key) into a single pass.
   */
  private static List<Page> mergeMixedDedup2Key(
      List<List<Page>> shardPages,
      List<Type> dedupTypes,
      int numDedupKeys,
      int numShardAggs,
      int numOriginalKeys,
      int numOrigAggs,
      boolean[] isCountDistinct,
      boolean[] isAvg,
      int[] shardAggIdx,
      int[] shardCountIdx,
      List<Type> columnTypes) {
    // Pre-size capacity: use largest single shard's row count as estimate (not total)
    // because dedup keys overlap across shards. This avoids massive over-allocation
    // (e.g., 8 shards x 10K rows = 80K total, but only ~15K unique pairs).
    int maxShardRows = 0;
    int totalRows = 0;
    for (List<Page> shardPageList : shardPages) {
      int shardRows = 0;
      for (Page page : shardPageList) {
        shardRows += page.getPositionCount();
      }
      totalRows += shardRows;
      if (shardRows > maxShardRows) maxShardRows = shardRows;
    }
    // Estimate unique as 2x the largest shard (to handle some non-overlap), capped at totalRows
    int estimatedUnique = Math.min(totalRows, maxShardRows * 2);
    int capacity = Integer.highestOneBit(Math.max(1024, (int) (estimatedUnique / 0.65f))) << 1;
    float loadFactor = 0.65f;
    int threshold = (int) (capacity * loadFactor);
    int size = 0;
    long[] mapKey0 = new long[capacity];
    long[] mapKey1 = new long[capacity];
    // Flatten agg values: aggs[slot * numShardAggs + a]
    long[] mapAggs = new long[capacity * numShardAggs];
    boolean[] mapOccupied = new boolean[capacity];
    // Track occupied slot indices for fast Stage 2 iteration
    int[] occupiedSlots = new int[Math.max(1024, estimatedUnique)];

    Type type0 = dedupTypes.get(0);
    Type type1 = dedupTypes.get(1);

    for (List<Page> shardPageList : shardPages) {
      for (Page page : shardPageList) {
        int positionCount = page.getPositionCount();
        Block block0 = page.getBlock(0);
        Block block1 = page.getBlock(1);
        Block[] aggBlocks = new Block[numShardAggs];
        for (int a = 0; a < numShardAggs; a++) {
          aggBlocks[a] = page.getBlock(numDedupKeys + a);
        }

        for (int pos = 0; pos < positionCount; pos++) {
          long k0 = type0.getLong(block0, pos);
          long k1 = type1.getLong(block1, pos);
          int hash = Long.hashCode(k0) * 0x9E3779B9 + Long.hashCode(k1);
          int mask = capacity - 1;
          int slot = hash & mask;
          while (true) {
            if (!mapOccupied[slot]) {
              mapKey0[slot] = k0;
              mapKey1[slot] = k1;
              int base = slot * numShardAggs;
              for (int a = 0; a < numShardAggs; a++) {
                if (!aggBlocks[a].isNull(pos)) {
                  mapAggs[base + a] = BigintType.BIGINT.getLong(aggBlocks[a], pos);
                }
              }
              mapOccupied[slot] = true;
              if (size >= occupiedSlots.length) {
                occupiedSlots = java.util.Arrays.copyOf(occupiedSlots, occupiedSlots.length * 2);
              }
              occupiedSlots[size] = slot;
              size++;
              if (size > threshold) {
                int newCap = capacity * 2;
                long[] nk0 = new long[newCap];
                long[] nk1 = new long[newCap];
                long[] na = new long[newCap * numShardAggs];
                boolean[] no = new boolean[newCap];
                int nm = newCap - 1;
                // Rebuild occupied slots list during resize
                int rebuildIdx = 0;
                if (size > occupiedSlots.length) {
                  occupiedSlots = java.util.Arrays.copyOf(occupiedSlots, size * 2);
                }
                for (int si = 0; si < size; si++) {
                  int oldSlot = occupiedSlots[si];
                  int h =
                      Long.hashCode(mapKey0[oldSlot]) * 0x9E3779B9
                          + Long.hashCode(mapKey1[oldSlot]);
                  int ns = h & nm;
                  while (no[ns]) ns = (ns + 1) & nm;
                  nk0[ns] = mapKey0[oldSlot];
                  nk1[ns] = mapKey1[oldSlot];
                  System.arraycopy(
                      mapAggs, oldSlot * numShardAggs, na, ns * numShardAggs, numShardAggs);
                  no[ns] = true;
                  occupiedSlots[rebuildIdx++] = ns;
                }
                mapKey0 = nk0;
                mapKey1 = nk1;
                mapAggs = na;
                mapOccupied = no;
                capacity = newCap;
                mask = nm;
                threshold = (int) (newCap * loadFactor);
              }
              break;
            }
            if (mapKey0[slot] == k0 && mapKey1[slot] == k1) {
              // Merge: SUM for all COUNT/SUM shard aggs
              int base = slot * numShardAggs;
              for (int a = 0; a < numShardAggs; a++) {
                if (!aggBlocks[a].isNull(pos)) {
                  mapAggs[base + a] += BigintType.BIGINT.getLong(aggBlocks[a], pos);
                }
              }
              break;
            }
            slot = (slot + 1) & mask;
          }
        }
      }
    }

    if (size == 0) {
      return List.of();
    }

    // Stage 2: Re-aggregate by original key (key0)
    // Iterate only occupied slots (O(size) instead of O(capacity))
    int numValues = numOrigAggs * 2;
    // Use primitive long→double[] open-addressing map
    int grpCap = 256;
    long[] grpKeys = new long[grpCap];
    double[][] grpVals = new double[grpCap][];
    boolean[] grpOcc = new boolean[grpCap];
    int grpSize = 0;
    int grpThreshold = (int) (grpCap * 0.7f);

    for (int si = 0; si < size; si++) {
      int s = occupiedSlots[si];
      long origKey = mapKey0[s];
      int gm = grpCap - 1;
      int gs = Long.hashCode(origKey) & gm;
      while (grpOcc[gs] && grpKeys[gs] != origKey) {
        gs = (gs + 1) & gm;
      }
      if (!grpOcc[gs]) {
        grpKeys[gs] = origKey;
        grpVals[gs] = new double[numValues];
        grpOcc[gs] = true;
        grpSize++;
        if (grpSize > grpThreshold) {
          int newGC = grpCap * 2;
          long[] ngk = new long[newGC];
          double[][] ngv = new double[newGC][];
          boolean[] ngo = new boolean[newGC];
          int ngm = newGC - 1;
          for (int g = 0; g < grpCap; g++) {
            if (grpOcc[g]) {
              int ns = Long.hashCode(grpKeys[g]) & ngm;
              while (ngo[ns]) ns = (ns + 1) & ngm;
              ngk[ns] = grpKeys[g];
              ngv[ns] = grpVals[g];
              ngo[ns] = true;
            }
          }
          grpKeys = ngk;
          grpVals = ngv;
          grpOcc = ngo;
          grpCap = newGC;
          grpThreshold = (int) (newGC * 0.7f);
          // Re-probe
          gm = grpCap - 1;
          gs = Long.hashCode(origKey) & gm;
          while (grpOcc[gs] && grpKeys[gs] != origKey) {
            gs = (gs + 1) & gm;
          }
        }
      }
      double[] vals = grpVals[gs];
      int base = s * numShardAggs;
      for (int a = 0; a < numOrigAggs; a++) {
        if (isCountDistinct[a]) {
          vals[a * 2]++;
        } else if (isAvg[a]) {
          vals[a * 2] += mapAggs[base + shardAggIdx[a]]; // sum
          vals[a * 2 + 1] += mapAggs[base + shardCountIdx[a]]; // count
        } else {
          vals[a * 2] += mapAggs[base + shardAggIdx[a]];
        }
      }
    }

    // Build result page
    int numOutputCols = numOriginalKeys + numOrigAggs;
    Type keyType = dedupTypes.get(0);
    io.trino.spi.block.BlockBuilder[] builders = new io.trino.spi.block.BlockBuilder[numOutputCols];
    builders[0] = keyType.createBlockBuilder(null, grpSize);
    for (int i = 0; i < numOrigAggs; i++) {
      Type outType = columnTypes.get(numOriginalKeys + i);
      builders[numOriginalKeys + i] = outType.createBlockBuilder(null, grpSize);
    }

    for (int gs = 0; gs < grpCap; gs++) {
      if (!grpOcc[gs]) continue;
      long key = grpKeys[gs];
      double[] vals = grpVals[gs];
      if (keyType instanceof IntegerType) {
        IntegerType.INTEGER.writeLong(builders[0], (int) key);
      } else {
        keyType.writeLong(builders[0], key);
      }
      for (int a = 0; a < numOrigAggs; a++) {
        Type outType = columnTypes.get(numOriginalKeys + a);
        if (isCountDistinct[a]) {
          BigintType.BIGINT.writeLong(builders[numOriginalKeys + a], (long) vals[a * 2]);
        } else if (isAvg[a]) {
          double sum = vals[a * 2];
          double count = vals[a * 2 + 1];
          DoubleType.DOUBLE.writeDouble(
              builders[numOriginalKeys + a], count > 0 ? sum / count : 0.0);
        } else if (outType instanceof DoubleType) {
          DoubleType.DOUBLE.writeDouble(builders[numOriginalKeys + a], vals[a * 2]);
        } else {
          BigintType.BIGINT.writeLong(builders[numOriginalKeys + a], (long) vals[a * 2]);
        }
      }
    }

    Block[] blocks = new Block[numOutputCols];
    for (int i = 0; i < numOutputCols; i++) {
      blocks[i] = builders[i].build();
    }
    return List.of(new Page(blocks));
  }

  private static boolean isShardMixedDedup(DqePlanNode shardPlan, AggregationNode singleAgg) {
    if (!(shardPlan instanceof AggregationNode shardAgg)) {
      return false;
    }
    if (shardAgg.getStep() != AggregationNode.Step.PARTIAL) {
      return false;
    }
    if (singleAgg.getGroupByKeys().isEmpty()) {
      return false;
    }
    // Mixed dedup has more group-by keys than the original (original keys + distinct columns)
    // and has MORE than just COUNT(*) as shard aggregates (unlike the COUNT-DISTINCT-only path)
    if (shardAgg.getGroupByKeys().size() <= singleAgg.getGroupByKeys().size()) {
      return false;
    }
    // Distinguish from COUNT-DISTINCT-only dedup: that path has exactly 1 shard agg (COUNT(*))
    if (shardAgg.getAggregateFunctions().size() <= 1) {
      return false;
    }
    // Verify original aggregate functions contain COUNT(DISTINCT)
    java.util.regex.Pattern aggPat =
        java.util.regex.Pattern.compile(
            "^(COUNT|SUM|MIN|MAX|AVG)\\((DISTINCT\\s+)?(.+?)\\)$",
            java.util.regex.Pattern.CASE_INSENSITIVE);
    boolean hasCountDistinct = false;
    for (String func : singleAgg.getAggregateFunctions()) {
      java.util.regex.Matcher m = aggPat.matcher(func);
      if (m.matches()
          && "COUNT".equals(m.group(1).toUpperCase(java.util.Locale.ROOT))
          && m.group(2) != null) {
        hasCountDistinct = true;
        break;
      }
    }
    return hasCountDistinct;
  }

  /**
   * Three-stage merge for mixed-aggregate dedup queries (e.g., Q10). Shards produce pre-deduped
   * (original_keys + distinct_cols, partial_agg0, partial_agg1, ...) tuples. The coordinator:
   *
   * <ol>
   *   <li>Stage 1: FINAL merge for dedup keys to remove cross-shard duplicates and merge partial
   *       aggregates (SUM→sum, COUNT→sum, MIN→min, MAX→max)
   *   <li>Stage 2: Re-aggregate by original keys: SUM the partial sums, SUM the partial counts,
   *       compute weighted AVG, COUNT rows for COUNT(DISTINCT)
   * </ol>
   */
  private static List<Page> mergeMixedDedup(
      List<List<Page>> shardPages,
      AggregationNode singleAgg,
      DqePlanNode shardPlan,
      List<Type> columnTypes,
      Map<String, Type> columnTypeMap,
      ResultMerger merger) {

    AggregationNode shardAgg = (AggregationNode) shardPlan;
    List<String> dedupKeys = shardAgg.getGroupByKeys();
    List<String> shardAggs = shardAgg.getAggregateFunctions();
    List<String> originalKeys = singleAgg.getGroupByKeys();
    List<String> originalAggs = singleAgg.getAggregateFunctions();
    int numOriginalKeys = originalKeys.size();

    // Stage 1: FINAL merge for dedup keys (merges partial aggregates across shards)
    // Build types for the dedup output: [dedupKey types..., shardAgg types...]
    List<Type> dedupTypes = new ArrayList<>();
    for (String key : dedupKeys) {
      dedupTypes.add(columnTypeMap.getOrDefault(key, BigintType.BIGINT));
    }
    // Determine types for shard aggregates
    java.util.regex.Pattern aggPat =
        java.util.regex.Pattern.compile(
            "^(COUNT|SUM|MIN|MAX|AVG)\\((DISTINCT\\s+)?(.+?)\\)$",
            java.util.regex.Pattern.CASE_INSENSITIVE);
    for (String func : shardAggs) {
      java.util.regex.Matcher m = aggPat.matcher(func);
      if (m.matches()) {
        String funcName = m.group(1).toUpperCase(java.util.Locale.ROOT);
        String arg = m.group(3).trim();
        if ("COUNT".equals(funcName)) {
          dedupTypes.add(BigintType.BIGINT);
        } else if ("SUM".equals(funcName)) {
          Type argType = columnTypeMap.getOrDefault(arg, BigintType.BIGINT);
          dedupTypes.add(argType instanceof DoubleType ? DoubleType.DOUBLE : BigintType.BIGINT);
        } else if ("MIN".equals(funcName) || "MAX".equals(funcName)) {
          dedupTypes.add(columnTypeMap.getOrDefault(arg, BigintType.BIGINT));
        } else {
          dedupTypes.add(BigintType.BIGINT);
        }
      } else {
        dedupTypes.add(BigintType.BIGINT);
      }
    }

    int numDedupKeys = dedupKeys.size();

    // Parse original aggregates to know what to produce
    int numOrigAggs = originalAggs.size();
    // For each original agg, track how to compute from shard agg columns
    // shardAggIdx[i] = index of shard agg column for original agg i (-1 for COUNT(DISTINCT))
    int[] shardAggIdx = new int[numOrigAggs];
    int[] shardCountIdx = new int[numOrigAggs]; // companion COUNT index for AVG
    boolean[] isCountDistinct = new boolean[numOrigAggs];
    boolean[] isAvg = new boolean[numOrigAggs];
    boolean[] isMin = new boolean[numOrigAggs];
    boolean[] isMax = new boolean[numOrigAggs];
    boolean[] isOutputDouble = new boolean[numOrigAggs];
    boolean[] isVarcharAgg = new boolean[numOrigAggs];
    java.util.Arrays.fill(shardAggIdx, -1);
    java.util.Arrays.fill(shardCountIdx, -1);

    int shardAggOffset = 0; // tracks position in shard agg list
    for (int i = 0; i < numOrigAggs; i++) {
      java.util.regex.Matcher m = aggPat.matcher(originalAggs.get(i));
      if (!m.matches()) continue;
      String funcName = m.group(1).toUpperCase(java.util.Locale.ROOT);
      boolean distinct = m.group(2) != null;

      if (distinct && "COUNT".equals(funcName)) {
        isCountDistinct[i] = true;
        // No shard agg column — COUNT(DISTINCT) = count of rows per group
      } else if ("AVG".equals(funcName)) {
        isAvg[i] = true;
        isOutputDouble[i] = true;
        // AVG was decomposed into SUM + COUNT in shard plan
        shardAggIdx[i] = shardAggOffset; // SUM column
        shardCountIdx[i] = shardAggOffset + 1; // COUNT column
        shardAggOffset += 2;
      } else if ("MIN".equals(funcName)) {
        isMin[i] = true;
        shardAggIdx[i] = shardAggOffset;
        Type aggType = dedupTypes.get(numDedupKeys + shardAggOffset);
        isOutputDouble[i] = aggType instanceof DoubleType;
        isVarcharAgg[i] = aggType instanceof VarcharType;
        shardAggOffset++;
      } else if ("MAX".equals(funcName)) {
        isMax[i] = true;
        shardAggIdx[i] = shardAggOffset;
        Type aggType = dedupTypes.get(numDedupKeys + shardAggOffset);
        isOutputDouble[i] = aggType instanceof DoubleType;
        isVarcharAgg[i] = aggType instanceof VarcharType;
        shardAggOffset++;
      } else {
        // SUM, COUNT(*)
        shardAggIdx[i] = shardAggOffset;
        Type aggType = dedupTypes.get(numDedupKeys + shardAggOffset);
        isOutputDouble[i] = aggType instanceof DoubleType;
        shardAggOffset++;
      }
    }

    // === Fused path: all-numeric dedup keys with COUNT/SUM-only shard aggs ===
    // Combines Stage 1 (FINAL dedup merge) and Stage 2 (re-aggregate by original keys)
    // into a single pass, eliminating intermediate Page construction.
    int numShardAggs = shardAggs.size();
    boolean allNumericDedupKeys = true;
    for (int i = 0; i < numDedupKeys; i++) {
      Type t = dedupTypes.get(i);
      if (!(t instanceof BigintType
          || t instanceof IntegerType
          || t instanceof io.trino.spi.type.TimestampType
          || t instanceof io.trino.spi.type.SmallintType
          || t instanceof io.trino.spi.type.TinyintType
          || t instanceof io.trino.spi.type.BooleanType)) {
        allNumericDedupKeys = false;
        break;
      }
    }
    boolean allShardAggsLong = true;
    for (int i = numDedupKeys; i < dedupTypes.size(); i++) {
      if (!(dedupTypes.get(i) instanceof BigintType)) {
        allShardAggsLong = false;
        break;
      }
    }
    // Use fused path when: all dedup keys numeric, all shard aggs are long (COUNT/SUM),
    // single numeric original key, no MIN/MAX/VARCHAR aggs in original
    boolean noMinMaxVarchar = true;
    for (int a = 0; a < numOrigAggs; a++) {
      if (isMin[a] || isMax[a] || isVarcharAgg[a]) {
        noMinMaxVarchar = false;
        break;
      }
    }
    if (allNumericDedupKeys
        && allShardAggsLong
        && numOriginalKeys == 1
        && numDedupKeys == 2
        && !(dedupTypes.get(0) instanceof VarcharType)
        && !(dedupTypes.get(0) instanceof DoubleType)
        && noMinMaxVarchar) {
      // Ultra-fast 2-key fused path for mixed dedup (Q10-style queries).
      // Uses two flat long[] arrays instead of long[][] to eliminate per-entry allocation.
      return mergeMixedDedup2Key(
          shardPages,
          dedupTypes,
          numDedupKeys,
          numShardAggs,
          numOriginalKeys,
          numOrigAggs,
          isCountDistinct,
          isAvg,
          shardAggIdx,
          shardCountIdx,
          columnTypes);
    }

    if (allNumericDedupKeys
        && allShardAggsLong
        && numOriginalKeys == 1
        && !(dedupTypes.get(0) instanceof VarcharType)
        && !(dedupTypes.get(0) instanceof DoubleType)
        && noMinMaxVarchar) {
      // Fused Stage 1+2: Build dedup hashmap directly from shard pages, then re-aggregate.
      // Uses open-addressing hash map with long[] keys for zero-allocation inner loop.
      int capacity = 1024;
      float loadFactor = 0.7f;
      int threshold = (int) (capacity * loadFactor);
      int size = 0;
      long[][] mapKeys = new long[capacity][];
      long[][] mapAggs = new long[capacity][];
      boolean[] mapOccupied = new boolean[capacity];
      long[] tmpKey = new long[numDedupKeys];

      for (java.util.List<Page> shardPageList : shardPages) {
        for (Page page : shardPageList) {
          int positionCount = page.getPositionCount();
          Block[] keyBlocks = new Block[numDedupKeys];
          for (int k = 0; k < numDedupKeys; k++) {
            keyBlocks[k] = page.getBlock(k);
          }
          Block[] aggBlocks = new Block[numShardAggs];
          for (int a = 0; a < numShardAggs; a++) {
            aggBlocks[a] = page.getBlock(numDedupKeys + a);
          }

          for (int pos = 0; pos < positionCount; pos++) {
            for (int k = 0; k < numDedupKeys; k++) {
              tmpKey[k] = dedupTypes.get(k).getLong(keyBlocks[k], pos);
            }
            int hash = 1;
            for (int k = 0; k < numDedupKeys; k++) {
              hash = hash * 31 + Long.hashCode(tmpKey[k]);
            }
            int mask = capacity - 1;
            int slot = hash & mask;
            while (true) {
              if (!mapOccupied[slot]) {
                // New entry
                mapKeys[slot] = tmpKey.clone();
                long[] aggs = new long[numShardAggs];
                for (int a = 0; a < numShardAggs; a++) {
                  if (!aggBlocks[a].isNull(pos)) {
                    aggs[a] = BigintType.BIGINT.getLong(aggBlocks[a], pos);
                  }
                }
                mapAggs[slot] = aggs;
                mapOccupied[slot] = true;
                size++;
                if (size > threshold) {
                  // Resize
                  int newCap = capacity * 2;
                  long[][] nk = new long[newCap][];
                  long[][] na = new long[newCap][];
                  boolean[] no = new boolean[newCap];
                  int nm = newCap - 1;
                  for (int s = 0; s < capacity; s++) {
                    if (mapOccupied[s]) {
                      int h = 1;
                      for (int k = 0; k < numDedupKeys; k++) {
                        h = h * 31 + Long.hashCode(mapKeys[s][k]);
                      }
                      int ns = h & nm;
                      while (no[ns]) ns = (ns + 1) & nm;
                      nk[ns] = mapKeys[s];
                      na[ns] = mapAggs[s];
                      no[ns] = true;
                    }
                  }
                  mapKeys = nk;
                  mapAggs = na;
                  mapOccupied = no;
                  capacity = newCap;
                  mask = nm;
                  threshold = (int) (newCap * loadFactor);
                  // Re-probe for current key after resize
                  slot = hash & mask;
                  while (mapOccupied[slot]) {
                    long[] existing = mapKeys[slot];
                    boolean match = true;
                    for (int k = 0; k < numDedupKeys; k++) {
                      if (existing[k] != tmpKey[k]) {
                        match = false;
                        break;
                      }
                    }
                    if (match) break;
                    slot = (slot + 1) & mask;
                  }
                  // If we found the just-inserted key, continue to next row
                  // (it was already inserted above during resize migration)
                }
                break;
              }
              long[] existing = mapKeys[slot];
              boolean match = true;
              for (int k = 0; k < numDedupKeys; k++) {
                if (existing[k] != tmpKey[k]) {
                  match = false;
                  break;
                }
              }
              if (match) {
                // Merge partial aggs (SUM for all COUNT/SUM)
                long[] aggs = mapAggs[slot];
                for (int a = 0; a < numShardAggs; a++) {
                  if (!aggBlocks[a].isNull(pos)) {
                    aggs[a] += BigintType.BIGINT.getLong(aggBlocks[a], pos);
                  }
                }
                break;
              }
              slot = (slot + 1) & mask;
            }
          }
        }
      }

      if (size == 0) {
        return List.of();
      }

      // Stage 2: Re-aggregate by original key (first dedup key = original group key)
      int numValues = numOrigAggs * 2;
      java.util.HashMap<Long, double[]> groups = new java.util.HashMap<>();

      for (int s = 0; s < capacity; s++) {
        if (!mapOccupied[s]) continue;
        long origKey = mapKeys[s][0];
        double[] vals = groups.get(origKey);
        if (vals == null) {
          vals = new double[numValues];
          groups.put(origKey, vals);
        }
        long[] dedupAggs = mapAggs[s];
        for (int a = 0; a < numOrigAggs; a++) {
          if (isCountDistinct[a]) {
            vals[a * 2]++;
          } else if (isAvg[a]) {
            // AVG decomposed to SUM + COUNT in shard aggs
            vals[a * 2] += dedupAggs[shardAggIdx[a]]; // sum
            vals[a * 2 + 1] += dedupAggs[shardCountIdx[a]]; // count
          } else {
            // SUM, COUNT(*)
            vals[a * 2] += dedupAggs[shardAggIdx[a]];
          }
        }
      }

      // Build result page
      int numOutputCols = numOriginalKeys + numOrigAggs;
      Type keyType = dedupTypes.get(0);
      io.trino.spi.block.BlockBuilder[] builders =
          new io.trino.spi.block.BlockBuilder[numOutputCols];
      builders[0] = keyType.createBlockBuilder(null, groups.size());
      for (int i = 0; i < numOrigAggs; i++) {
        Type outType = columnTypes.get(numOriginalKeys + i);
        builders[numOriginalKeys + i] = outType.createBlockBuilder(null, groups.size());
      }

      for (var entry : groups.entrySet()) {
        long key = entry.getKey();
        double[] vals = entry.getValue();
        if (keyType instanceof IntegerType) {
          IntegerType.INTEGER.writeLong(builders[0], (int) key);
        } else {
          keyType.writeLong(builders[0], key);
        }
        for (int a = 0; a < numOrigAggs; a++) {
          Type outType = columnTypes.get(numOriginalKeys + a);
          if (isCountDistinct[a]) {
            BigintType.BIGINT.writeLong(builders[numOriginalKeys + a], (long) vals[a * 2]);
          } else if (isAvg[a]) {
            double sum = vals[a * 2];
            double count = vals[a * 2 + 1];
            DoubleType.DOUBLE.writeDouble(
                builders[numOriginalKeys + a], count > 0 ? sum / count : 0.0);
          } else if (outType instanceof DoubleType) {
            DoubleType.DOUBLE.writeDouble(builders[numOriginalKeys + a], vals[a * 2]);
          } else {
            outType.writeLong(builders[numOriginalKeys + a], (long) vals[a * 2]);
          }
        }
      }

      Block[] blocks = new Block[numOutputCols];
      for (int i = 0; i < numOutputCols; i++) {
        blocks[i] = builders[i].build();
      }
      return List.of(new Page(blocks));
    }

    // === Fallback: two-stage merge via intermediate Pages ===
    AggregationNode finalDedupNode =
        new AggregationNode(null, dedupKeys, shardAggs, AggregationNode.Step.FINAL);
    List<Page> dedupedPages = merger.mergeAggregation(shardPages, finalDedupNode, dedupTypes);

    if (dedupedPages.isEmpty()) {
      return List.of();
    }

    // Accumulate per original group key
    // Use long key for single numeric key (common case)
    if (numOriginalKeys == 1
        && !(dedupTypes.get(0) instanceof VarcharType)
        && !(dedupTypes.get(0) instanceof DoubleType)) {
      // Single numeric key fast path
      Type keyType = dedupTypes.get(0);

      int numValues = numOrigAggs * 2;
      java.util.HashMap<Long, double[]> groups = new java.util.HashMap<>();
      java.util.HashMap<Long, String[]> varcharAccums =
          hasAnyVarcharAgg(isVarcharAgg) ? new java.util.HashMap<>() : null;

      for (Page page : dedupedPages) {
        Block keyBlock = page.getBlock(0);
        for (int pos = 0; pos < page.getPositionCount(); pos++) {
          long key = keyType.getLong(keyBlock, pos);
          double[] vals = groups.get(key);
          if (vals == null) {
            vals = new double[numValues];
            groups.put(key, vals);
          }

          for (int a = 0; a < numOrigAggs; a++) {
            if (isCountDistinct[a]) {
              vals[a * 2]++;
            } else if (isAvg[a]) {
              int sumCol = numDedupKeys + shardAggIdx[a];
              int cntCol = numDedupKeys + shardCountIdx[a];
              Block sumBlock = page.getBlock(sumCol);
              Block cntBlock = page.getBlock(cntCol);
              if (!sumBlock.isNull(pos) && !cntBlock.isNull(pos)) {
                Type sumType = dedupTypes.get(sumCol);
                if (sumType instanceof DoubleType) {
                  vals[a * 2] += DoubleType.DOUBLE.getDouble(sumBlock, pos);
                } else {
                  vals[a * 2] += sumType.getLong(sumBlock, pos);
                }
                vals[a * 2 + 1] += BigintType.BIGINT.getLong(cntBlock, pos);
              }
            } else if ((isMin[a] || isMax[a]) && isVarcharAgg[a]) {
              int col = numDedupKeys + shardAggIdx[a];
              Block valBlock = page.getBlock(col);
              if (!valBlock.isNull(pos)) {
                String sv = VarcharType.VARCHAR.getSlice(valBlock, pos).toStringUtf8();
                String[] vVals = varcharAccums.computeIfAbsent(key, k -> new String[numOrigAggs]);
                if (vVals[a] == null) {
                  vVals[a] = sv;
                } else if (isMin[a] ? sv.compareTo(vVals[a]) < 0 : sv.compareTo(vVals[a]) > 0) {
                  vVals[a] = sv;
                }
              }
            } else if (isMin[a] || isMax[a]) {
              int col = numDedupKeys + shardAggIdx[a];
              Block valBlock = page.getBlock(col);
              if (!valBlock.isNull(pos)) {
                Type valType = dedupTypes.get(col);
                double v =
                    isOutputDouble[a]
                        ? DoubleType.DOUBLE.getDouble(valBlock, pos)
                        : valType.getLong(valBlock, pos);
                if (vals[a * 2 + 1] == 0) {
                  vals[a * 2] = v;
                  vals[a * 2 + 1] = 1;
                } else if (isMin[a] ? v < vals[a * 2] : v > vals[a * 2]) {
                  vals[a * 2] = v;
                }
              }
            } else {
              int col = numDedupKeys + shardAggIdx[a];
              Block valBlock = page.getBlock(col);
              if (!valBlock.isNull(pos)) {
                Type valType = dedupTypes.get(col);
                if (isOutputDouble[a]) {
                  vals[a * 2] += DoubleType.DOUBLE.getDouble(valBlock, pos);
                } else {
                  vals[a * 2] += valType.getLong(valBlock, pos);
                }
              }
            }
          }
        }
      }

      // Build result page
      int numOutputCols = numOriginalKeys + numOrigAggs;
      io.trino.spi.block.BlockBuilder[] builders =
          new io.trino.spi.block.BlockBuilder[numOutputCols];
      builders[0] = dedupTypes.get(0).createBlockBuilder(null, groups.size());
      for (int i = 0; i < numOrigAggs; i++) {
        Type outType = columnTypes.get(numOriginalKeys + i);
        builders[numOriginalKeys + i] = outType.createBlockBuilder(null, groups.size());
      }

      for (var entry : groups.entrySet()) {
        long key = entry.getKey();
        double[] vals = entry.getValue();
        Type kt = dedupTypes.get(0);
        if (kt instanceof IntegerType) {
          IntegerType.INTEGER.writeLong(builders[0], (int) key);
        } else {
          kt.writeLong(builders[0], key);
        }
        String[] vVals = varcharAccums != null ? varcharAccums.get(key) : null;
        for (int a = 0; a < numOrigAggs; a++) {
          Type outType = columnTypes.get(numOriginalKeys + a);
          if (isCountDistinct[a]) {
            BigintType.BIGINT.writeLong(builders[numOriginalKeys + a], (long) vals[a * 2]);
          } else if (isAvg[a]) {
            double sum = vals[a * 2];
            double count = vals[a * 2 + 1];
            DoubleType.DOUBLE.writeDouble(
                builders[numOriginalKeys + a], count > 0 ? sum / count : 0.0);
          } else if (isVarcharAgg[a]) {
            String sv = vVals != null ? vVals[a] : null;
            if (sv != null) {
              VarcharType.VARCHAR.writeSlice(
                  builders[numOriginalKeys + a], io.airlift.slice.Slices.utf8Slice(sv));
            } else {
              builders[numOriginalKeys + a].appendNull();
            }
          } else if (outType instanceof DoubleType) {
            DoubleType.DOUBLE.writeDouble(builders[numOriginalKeys + a], vals[a * 2]);
          } else {
            outType.writeLong(builders[numOriginalKeys + a], (long) vals[a * 2]);
          }
        }
      }

      Block[] blocks = new Block[numOutputCols];
      for (int i = 0; i < numOutputCols; i++) {
        blocks[i] = builders[i].build();
      }
      return List.of(new Page(blocks));
    }

    // Generic fallback: use Object keys
    // Build types for original keys
    Type[] keyTypes = new Type[numOriginalKeys];
    for (int i = 0; i < numOriginalKeys; i++) {
      keyTypes[i] = dedupTypes.get(i);
    }

    int numValues = numOrigAggs * 2;
    java.util.LinkedHashMap<Object, double[]> groups = new java.util.LinkedHashMap<>();
    // Separate storage for VARCHAR MIN/MAX values (keyed by group key)
    java.util.LinkedHashMap<Object, String[]> varcharGroups =
        hasAnyVarcharAgg(isVarcharAgg) ? new java.util.LinkedHashMap<>() : null;

    for (Page page : dedupedPages) {
      for (int pos = 0; pos < page.getPositionCount(); pos++) {
        Object key;
        if (numOriginalKeys == 1) {
          key = extractValue(page, 0, pos, keyTypes[0]);
        } else {
          List<Object> multiKey = new ArrayList<>(numOriginalKeys);
          for (int i = 0; i < numOriginalKeys; i++) {
            multiKey.add(extractValue(page, i, pos, keyTypes[i]));
          }
          key = multiKey;
        }
        double[] vals = groups.computeIfAbsent(key, k -> new double[numValues]);

        for (int a = 0; a < numOrigAggs; a++) {
          if (isCountDistinct[a]) {
            vals[a * 2]++;
          } else if (isAvg[a]) {
            int sumCol = numDedupKeys + shardAggIdx[a];
            int cntCol = numDedupKeys + shardCountIdx[a];
            Block sumBlock = page.getBlock(sumCol);
            Block cntBlock = page.getBlock(cntCol);
            if (!sumBlock.isNull(pos) && !cntBlock.isNull(pos)) {
              Type sumType = dedupTypes.get(sumCol);
              if (sumType instanceof DoubleType) {
                vals[a * 2] += DoubleType.DOUBLE.getDouble(sumBlock, pos);
              } else {
                vals[a * 2] += sumType.getLong(sumBlock, pos);
              }
              vals[a * 2 + 1] += BigintType.BIGINT.getLong(cntBlock, pos);
            }
          } else if ((isMin[a] || isMax[a]) && isVarcharAgg[a]) {
            // VARCHAR MIN/MAX: compare as strings, store in separate map
            int col = numDedupKeys + shardAggIdx[a];
            Block valBlock = page.getBlock(col);
            if (!valBlock.isNull(pos)) {
              String sv = VarcharType.VARCHAR.getSlice(valBlock, pos).toStringUtf8();
              String[] vVals = varcharGroups.computeIfAbsent(key, k -> new String[numOrigAggs]);
              if (vVals[a] == null) {
                vVals[a] = sv;
              } else if (isMin[a] ? sv.compareTo(vVals[a]) < 0 : sv.compareTo(vVals[a]) > 0) {
                vVals[a] = sv;
              }
            }
          } else if (isMin[a] || isMax[a]) {
            int col = numDedupKeys + shardAggIdx[a];
            Block valBlock = page.getBlock(col);
            if (!valBlock.isNull(pos)) {
              Type valType = dedupTypes.get(col);
              double v =
                  isOutputDouble[a]
                      ? DoubleType.DOUBLE.getDouble(valBlock, pos)
                      : valType.getLong(valBlock, pos);
              if (vals[a * 2 + 1] == 0) {
                vals[a * 2] = v;
                vals[a * 2 + 1] = 1;
              } else if (isMin[a] ? v < vals[a * 2] : v > vals[a * 2]) {
                vals[a * 2] = v;
              }
            }
          } else {
            int col = numDedupKeys + shardAggIdx[a];
            Block valBlock = page.getBlock(col);
            if (!valBlock.isNull(pos)) {
              Type valType = dedupTypes.get(col);
              if (isOutputDouble[a]) {
                vals[a * 2] += DoubleType.DOUBLE.getDouble(valBlock, pos);
              } else {
                vals[a * 2] += valType.getLong(valBlock, pos);
              }
            }
          }
        }
      }
    }

    if (groups.isEmpty()) {
      return List.of();
    }

    int numOutputCols = numOriginalKeys + numOrigAggs;
    io.trino.spi.block.BlockBuilder[] builders = new io.trino.spi.block.BlockBuilder[numOutputCols];
    for (int i = 0; i < numOriginalKeys; i++) {
      builders[i] = keyTypes[i].createBlockBuilder(null, groups.size());
    }
    for (int i = 0; i < numOrigAggs; i++) {
      Type outType = columnTypes.get(numOriginalKeys + i);
      builders[numOriginalKeys + i] = outType.createBlockBuilder(null, groups.size());
    }

    for (var entry : groups.entrySet()) {
      Object key = entry.getKey();
      double[] vals = entry.getValue();
      if (numOriginalKeys == 1) {
        appendTypedValue(builders[0], keyTypes[0], key);
      } else {
        @SuppressWarnings("unchecked")
        List<Object> multiKey = (List<Object>) key;
        for (int i = 0; i < numOriginalKeys; i++) {
          appendTypedValue(builders[i], keyTypes[i], multiKey.get(i));
        }
      }
      String[] vVals = varcharGroups != null ? varcharGroups.get(key) : null;
      for (int a = 0; a < numOrigAggs; a++) {
        Type outType = columnTypes.get(numOriginalKeys + a);
        if (isCountDistinct[a]) {
          BigintType.BIGINT.writeLong(builders[numOriginalKeys + a], (long) vals[a * 2]);
        } else if (isAvg[a]) {
          double sum = vals[a * 2];
          double count = vals[a * 2 + 1];
          DoubleType.DOUBLE.writeDouble(
              builders[numOriginalKeys + a], count > 0 ? sum / count : 0.0);
        } else if (isVarcharAgg[a]) {
          String sv = vVals != null ? vVals[a] : null;
          if (sv != null) {
            VarcharType.VARCHAR.writeSlice(
                builders[numOriginalKeys + a], io.airlift.slice.Slices.utf8Slice(sv));
          } else {
            builders[numOriginalKeys + a].appendNull();
          }
        } else if (outType instanceof DoubleType) {
          DoubleType.DOUBLE.writeDouble(builders[numOriginalKeys + a], vals[a * 2]);
        } else {
          outType.writeLong(builders[numOriginalKeys + a], (long) vals[a * 2]);
        }
      }
    }

    Block[] blocks = new Block[numOutputCols];
    for (int i = 0; i < numOutputCols; i++) {
      blocks[i] = builders[i].build();
    }
    return List.of(new Page(blocks));
  }

  /** Check if any element in the boolean array is true. */
  private static boolean hasAnyVarcharAgg(boolean[] isVarcharAgg) {
    for (boolean v : isVarcharAgg) {
      if (v) return true;
    }
    return false;
  }

  /**
   * Fast path for merging scalar aggregation results from shards. Instead of constructing a
   * HashAggregationOperator, directly iterates over shard Pages and merges values. Supports
   * COUNT(*) (sum), SUM (sum), MIN (min), MAX (max), and AVG (weighted average).
   *
   * @param shardPages results from each shard (each shard returns a single-row Page)
   * @param aggNode the FINAL aggregation node
   * @param columnTypes types for the output columns
   * @return single-row merged result
   */
  private static List<Page> mergeScalarAggregation(
      List<List<Page>> shardPages, AggregationNode aggNode, List<Type> columnTypes) {
    List<String> aggFunctions = aggNode.getAggregateFunctions();
    int numAggs = aggFunctions.size();

    // Initialize accumulators
    double[] sumValues = new double[numAggs];
    long[] longSumValues = new long[numAggs];
    boolean[] isDouble = new boolean[numAggs];
    boolean[] isMinMax = new boolean[numAggs];
    boolean[] isMin = new boolean[numAggs];
    boolean[] isAvg = new boolean[numAggs];
    // For AVG weighting: track companion COUNT column index
    int countColIdx = -1;

    java.util.regex.Pattern AGG_PAT =
        java.util.regex.Pattern.compile(
            "^(COUNT|SUM|MIN|MAX|AVG)\\((DISTINCT\\s+)?(.+?)\\)$",
            java.util.regex.Pattern.CASE_INSENSITIVE);

    for (int a = 0; a < numAggs; a++) {
      java.util.regex.Matcher m = AGG_PAT.matcher(aggFunctions.get(a));
      String funcName = m.matches() ? m.group(1).toUpperCase(java.util.Locale.ROOT) : "SUM";
      if ("MIN".equals(funcName) || "MAX".equals(funcName)) {
        isMinMax[a] = true;
        isMin[a] = "MIN".equals(funcName);
      } else if ("AVG".equals(funcName)) {
        isAvg[a] = true;
        isDouble[a] = true;
      } else {
        // COUNT or SUM
        isDouble[a] = columnTypes.get(a) instanceof DoubleType;
      }
      if ("COUNT".equals(funcName) && (m.group(2) == null)) {
        countColIdx = a;
      }
    }

    // For MIN/MAX, track raw values to avoid type conversion issues.
    // Long types (integers, timestamps) use long comparison.
    // Double types use double comparison.
    // VarcharType uses Slice-based comparison.
    long[] minMaxLongValues = new long[numAggs];
    double[] minMaxDoubleValues = new double[numAggs];
    io.airlift.slice.Slice[] minMaxSliceValues = new io.airlift.slice.Slice[numAggs];
    boolean[] minMaxInitialized = new boolean[numAggs];
    boolean[] minMaxIsVarchar = new boolean[numAggs];
    boolean[] minMaxIsDouble = new boolean[numAggs];
    for (int a = 0; a < numAggs; a++) {
      if (isMinMax[a]) {
        if (columnTypes.get(a) instanceof VarcharType) {
          minMaxIsVarchar[a] = true;
        } else if (columnTypes.get(a) instanceof DoubleType) {
          minMaxIsDouble[a] = true;
        }
      }
    }

    // Merge values from all shards
    for (List<Page> pages : shardPages) {
      for (Page page : pages) {
        for (int pos = 0; pos < page.getPositionCount(); pos++) {
          for (int a = 0; a < numAggs; a++) {
            Block block = page.getBlock(a);
            if (block.isNull(pos)) continue;

            if (isMinMax[a]) {
              if (minMaxIsVarchar[a]) {
                // VARCHAR: compare Slices
                io.airlift.slice.Slice val = VarcharType.VARCHAR.getSlice(block, pos);
                if (!minMaxInitialized[a]) {
                  minMaxSliceValues[a] = val;
                  minMaxInitialized[a] = true;
                } else {
                  int cmp = val.compareTo(minMaxSliceValues[a]);
                  if (isMin[a] ? cmp < 0 : cmp > 0) {
                    minMaxSliceValues[a] = val;
                  }
                }
              } else if (minMaxIsDouble[a]) {
                // DOUBLE: compare doubles
                double val = DoubleType.DOUBLE.getDouble(block, pos);
                if (!minMaxInitialized[a]) {
                  minMaxDoubleValues[a] = val;
                  minMaxInitialized[a] = true;
                } else {
                  if (isMin[a] ? val < minMaxDoubleValues[a] : val > minMaxDoubleValues[a]) {
                    minMaxDoubleValues[a] = val;
                  }
                }
              } else {
                // Long types (including TimestampType): compare raw longs
                long val = columnTypes.get(a).getLong(block, pos);
                if (!minMaxInitialized[a]) {
                  minMaxLongValues[a] = val;
                  minMaxInitialized[a] = true;
                } else {
                  if (isMin[a] ? val < minMaxLongValues[a] : val > minMaxLongValues[a]) {
                    minMaxLongValues[a] = val;
                  }
                }
              }
            } else if (isAvg[a]) {
              sumValues[a] += DoubleType.DOUBLE.getDouble(block, pos);
            } else if (isDouble[a]) {
              sumValues[a] += DoubleType.DOUBLE.getDouble(block, pos);
            } else {
              longSumValues[a] += BigintType.BIGINT.getLong(block, pos);
            }
          }
        }
      }
    }

    // Handle AVG weighting: merge shard-level AVG values correctly.
    // Each shard computes a shard-local AVG. To merge, we need weighted averaging.
    for (int a = 0; a < numAggs; a++) {
      if (!isAvg[a]) continue;
      if (countColIdx >= 0) {
        // Weighted merge: use companion COUNT column as weight.
        // Correct merge: sum(avg_i * count_i) / sum(count_i).
        double weightedSum = 0;
        long totalCount = 0;
        for (List<Page> pages : shardPages) {
          for (Page page : pages) {
            for (int pos = 0; pos < page.getPositionCount(); pos++) {
              Block avgBlock = page.getBlock(a);
              Block cntBlock = page.getBlock(countColIdx);
              if (!avgBlock.isNull(pos) && !cntBlock.isNull(pos)) {
                double avg = DoubleType.DOUBLE.getDouble(avgBlock, pos);
                long cnt = BigintType.BIGINT.getLong(cntBlock, pos);
                weightedSum += avg * cnt;
                totalCount += cnt;
              }
            }
          }
        }
        sumValues[a] = totalCount > 0 ? weightedSum / totalCount : 0.0;
      } else {
        // No companion COUNT column (e.g., SELECT AVG(col) FROM t).
        // Simple average of shard AVGs — correct when shards have equal row counts,
        // which is the default for evenly distributed OpenSearch indices.
        int shardCount = 0;
        for (List<Page> pages : shardPages) {
          for (Page page : pages) {
            shardCount += page.getPositionCount();
          }
        }
        if (shardCount > 0) {
          sumValues[a] = sumValues[a] / shardCount;
        }
      }
    }

    // Build result Page
    io.trino.spi.block.BlockBuilder[] builders = new io.trino.spi.block.BlockBuilder[numAggs];
    for (int a = 0; a < numAggs; a++) {
      builders[a] = columnTypes.get(a).createBlockBuilder(null, 1);
      if (isMinMax[a]) {
        if (!minMaxInitialized[a]) {
          builders[a].appendNull();
        } else if (minMaxIsVarchar[a]) {
          VarcharType.VARCHAR.writeSlice(builders[a], minMaxSliceValues[a]);
        } else if (minMaxIsDouble[a]) {
          DoubleType.DOUBLE.writeDouble(builders[a], minMaxDoubleValues[a]);
        } else {
          columnTypes.get(a).writeLong(builders[a], minMaxLongValues[a]);
        }
      } else if (isDouble[a] || isAvg[a]) {
        DoubleType.DOUBLE.writeDouble(builders[a], sumValues[a]);
      } else {
        BigintType.BIGINT.writeLong(builders[a], longSumValues[a]);
      }
    }
    Block[] blocks = new Block[numAggs];
    for (int a = 0; a < numAggs; a++) {
      blocks[a] = builders[a].build();
    }
    return List.of(new Page(blocks));
  }

  /** Append a typed value to a BlockBuilder. */
  private static void appendTypedValue(
      io.trino.spi.block.BlockBuilder builder, Type type, Object value) {
    if (type instanceof BigintType) {
      BigintType.BIGINT.writeLong(builder, ((Number) value).longValue());
    } else if (type instanceof DoubleType) {
      DoubleType.DOUBLE.writeDouble(builder, ((Number) value).doubleValue());
    } else if (type instanceof BooleanType) {
      BooleanType.BOOLEAN.writeBoolean(builder, (Boolean) value);
    } else if (type instanceof VarcharType) {
      VarcharType.VARCHAR.writeSlice(builder, io.airlift.slice.Slices.utf8Slice(value.toString()));
    } else {
      type.writeLong(builder, ((Number) value).longValue());
    }
  }

  /** Check if a Type is a numeric type that supports getLong(). */
  private static boolean isNumericType(Type type) {
    return type instanceof BigintType
        || type instanceof IntegerType
        || type instanceof io.trino.spi.type.TimestampType
        || type instanceof io.trino.spi.type.SmallintType
        || type instanceof io.trino.spi.type.TinyintType
        || type instanceof io.trino.spi.type.BooleanType;
  }

  /**
   * Fused two-stage merge for COUNT(DISTINCT) with VARCHAR original key and numeric distinct
   * column. For Q14-style queries (GROUP BY SearchPhrase COUNT(DISTINCT UserID)), the dedup keys
   * are [SearchPhrase (VARCHAR), UserID (BIGINT)].
   *
   * <p>Stage 1: Insert all (SearchPhrase, UserID) pairs into a SliceLongDedupMap, which
   * deduplicates across shards using zero-copy Slice references and open-addressing hashing.
   *
   * <p>Stage 2: Count entries per SearchPhrase group using SliceCountMap, then build the result
   * page.
   *
   * <p>This eliminates the expensive intermediate merger.mergeAggregation call and avoids all
   * boxing/String conversion that the generic fallback path requires.
   */
  private static List<Page> mergeDedupCountDistinctVarcharKey(
      List<List<Page>> shardPages, List<Type> dedupTypes, int numCountDistinctAggs) {

    // Pre-compute total rows for pre-sizing
    int totalRows = 0;
    for (List<Page> shardPageList : shardPages) {
      for (Page page : shardPageList) {
        totalRows += page.getPositionCount();
      }
    }

    if (totalRows == 0) {
      return List.of();
    }

    Type numericType = dedupTypes.get(1);

    // Stage 1: Build global dedup set of (SearchPhrase, UserID) pairs
    org.opensearch.sql.dqe.operator.SliceLongDedupMap dedupMap =
        new org.opensearch.sql.dqe.operator.SliceLongDedupMap(totalRows);

    for (List<Page> shardPageList : shardPages) {
      for (Page page : shardPageList) {
        int positionCount = page.getPositionCount();
        Block varcharBlock = page.getBlock(0);
        Block numericBlock = page.getBlock(1);

        if (varcharBlock instanceof io.trino.spi.block.VariableWidthBlock vwb) {
          // Fast path: access raw slice directly
          io.airlift.slice.Slice rawSlice = vwb.getRawSlice();
          if (!vwb.mayHaveNull()) {
            for (int pos = 0; pos < positionCount; pos++) {
              long numVal = numericType.getLong(numericBlock, pos);
              dedupMap.add(rawSlice, vwb.getRawSliceOffset(pos), vwb.getSliceLength(pos), numVal);
            }
          } else {
            for (int pos = 0; pos < positionCount; pos++) {
              if (!vwb.isNull(pos)) {
                long numVal =
                    numericBlock.isNull(pos) ? 0L : numericType.getLong(numericBlock, pos);
                dedupMap.add(rawSlice, vwb.getRawSliceOffset(pos), vwb.getSliceLength(pos), numVal);
              }
            }
          }
        } else {
          // Fallback for DictionaryBlock etc.
          io.trino.spi.block.VariableWidthBlock underlying =
              (io.trino.spi.block.VariableWidthBlock) varcharBlock.getUnderlyingValueBlock();
          io.airlift.slice.Slice rawSlice = underlying.getRawSlice();
          for (int pos = 0; pos < positionCount; pos++) {
            if (!varcharBlock.isNull(pos)) {
              int uPos = varcharBlock.getUnderlyingValuePosition(pos);
              long numVal = numericBlock.isNull(pos) ? 0L : numericType.getLong(numericBlock, pos);
              dedupMap.add(
                  rawSlice,
                  underlying.getRawSliceOffset(uPos),
                  underlying.getSliceLength(uPos),
                  numVal);
            }
          }
        }
      }
    }

    if (dedupMap.size() == 0) {
      return List.of();
    }

    // Stage 2: Count entries per VARCHAR group key
    // Estimate unique groups (much fewer than dedup entries)
    org.opensearch.sql.dqe.operator.SliceCountMap groupCounts =
        new org.opensearch.sql.dqe.operator.SliceCountMap(Math.max(64, dedupMap.size() / 4));

    dedupMap.countPerGroup(groupCounts::increment);

    // Build result page: [SearchPhrase (VARCHAR), COUNT(DISTINCT) (BIGINT)]
    int numGroups = groupCounts.size();
    int numOutputCols = 1 + numCountDistinctAggs;
    io.trino.spi.block.BlockBuilder[] builders = new io.trino.spi.block.BlockBuilder[numOutputCols];
    builders[0] = VarcharType.VARCHAR.createBlockBuilder(null, numGroups);
    for (int i = 1; i < numOutputCols; i++) {
      builders[i] = BigintType.BIGINT.createBlockBuilder(null, numGroups);
    }

    groupCounts.forEach(
        (slice, offset, length, count) -> {
          VarcharType.VARCHAR.writeSlice(builders[0], slice, offset, length);
          BigintType.BIGINT.writeLong(builders[1], count);
        });

    Block[] blocks = new Block[numOutputCols];
    for (int i = 0; i < numOutputCols; i++) {
      blocks[i] = builders[i].build();
    }
    return List.of(new Page(blocks));
  }
}
