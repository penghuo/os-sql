/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dqe.plugin.execution;

import io.trino.spi.Page;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.transport.client.Client;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.dqe.analyzer.AnalyzedQuery;
import org.opensearch.dqe.analyzer.DqeAnalyzer;
import org.opensearch.dqe.analyzer.predicate.PredicateAnalysisResult;
import org.opensearch.dqe.analyzer.predicate.PushdownPredicate;
import org.opensearch.dqe.exchange.action.DqeStageExecuteRequest;
import org.opensearch.dqe.exchange.action.DqeStageExecuteRequest.ShardSplitInfo;
import org.opensearch.dqe.exchange.gather.GatherExchangeSink;
import org.opensearch.dqe.exchange.gather.TransportChunkSender;
import org.opensearch.dqe.exchange.serde.DqeExchangeChunk;
import org.opensearch.dqe.exchange.stage.StageExecutionHandler;
import org.opensearch.dqe.execution.expression.ExpressionEvaluator;
import org.opensearch.dqe.execution.operator.FilterOperator;
import org.opensearch.dqe.execution.operator.Operator;
import org.opensearch.dqe.execution.operator.OperatorContext;
import org.opensearch.dqe.execution.operator.ProjectOperator;
import org.opensearch.dqe.execution.operator.scan.SearchRequestBuilder;
import org.opensearch.dqe.execution.operator.scan.ShardScanOperator;
import org.opensearch.dqe.execution.pit.PitHandle;
import org.opensearch.dqe.execution.pit.PitManager;
import org.opensearch.dqe.execution.plan.PlanFragment;
import org.opensearch.dqe.execution.predicate.PredicateToQueryDslConverter;
import org.opensearch.dqe.memory.DqeMemoryTracker;
import org.opensearch.dqe.memory.QueryMemoryBudget;
import org.opensearch.dqe.metadata.DqeColumnHandle;
import org.opensearch.dqe.metadata.DqeMetadata;
import org.opensearch.dqe.parser.DqeErrorCode;
import org.opensearch.dqe.parser.DqeException;
import org.opensearch.dqe.parser.DqeSqlParser;
import org.opensearch.dqe.plugin.DqeEnginePlugin;
import org.opensearch.dqe.plugin.security.PermissiveSecurityContext;
import org.opensearch.dqe.types.converter.ColumnDescriptor;
import org.opensearch.dqe.types.converter.SearchHitToPageConverter;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.TransportService;

/**
 * Data-node-side {@link StageExecutionHandler.StageExecutionCallback} that builds shard scan
 * pipelines and drives execution, pushing results through a {@link GatherExchangeSink} to the
 * coordinator.
 */
public class ShardStageExecutor implements StageExecutionHandler.StageExecutionCallback {

  private static final Logger logger = LogManager.getLogger(ShardStageExecutor.class);

  private final DqeSqlParser parser;
  private final DqeAnalyzer analyzer;
  private final DqeMetadata metadata;
  private final PitManager pitManager;
  private final TransportService transportService;
  private final ClusterService clusterService;
  private final DqeMemoryTracker memoryTracker;
  private final ThreadPool threadPool;
  private final Client client;

  public ShardStageExecutor(
      DqeSqlParser parser,
      DqeAnalyzer analyzer,
      DqeMetadata metadata,
      PitManager pitManager,
      TransportService transportService,
      ClusterService clusterService,
      DqeMemoryTracker memoryTracker,
      ThreadPool threadPool,
      Client client) {
    this.parser = Objects.requireNonNull(parser, "parser must not be null");
    this.analyzer = Objects.requireNonNull(analyzer, "analyzer must not be null");
    this.metadata = Objects.requireNonNull(metadata, "metadata must not be null");
    this.pitManager = Objects.requireNonNull(pitManager, "pitManager must not be null");
    this.transportService =
        Objects.requireNonNull(transportService, "transportService must not be null");
    this.clusterService =
        Objects.requireNonNull(clusterService, "clusterService must not be null");
    this.memoryTracker = Objects.requireNonNull(memoryTracker, "memoryTracker must not be null");
    this.threadPool = Objects.requireNonNull(threadPool, "threadPool must not be null");
    this.client = Objects.requireNonNull(client, "client must not be null");
  }

  @Override
  public void execute(
      DqeStageExecuteRequest request, StageExecutionHandler.StageCompletionListener onComplete) {

    String queryId = request.getQueryId();
    int stageId = request.getStageId();

    threadPool
        .executor(DqeEnginePlugin.DQE_WORKER_POOL)
        .execute(
            () -> {
              try {
                executeStage(request);
                onComplete.onComplete(null);
              } catch (Exception e) {
                logger.error("[{}] Stage {} execution failed", queryId, stageId, e);
                onComplete.onComplete(e);
              }
            });
  }

  private void executeStage(DqeStageExecuteRequest request) {
    String queryId = request.getQueryId();
    int stageId = request.getStageId();
    String coordinatorNodeId = request.getCoordinatorNodeId();

    logger.debug(
        "[{}] Executing stage {} with {} splits",
        queryId,
        stageId,
        request.getShardSplitInfos().size());

    // 1. Deserialize PlanFragment
    PlanFragment planFragment = PlanFragment.deserialize(request.getSerializedPlanFragment());

    // 2. Re-parse SQL locally
    var statement = parser.parse(planFragment.getQueryText());

    // 3. Re-analyze with local cluster state
    var securityContext = new PermissiveSecurityContext("dqe-stage-executor");
    AnalyzedQuery analyzed = analyzer.analyze(statement, metadata, securityContext);

    // 4. Build pushdown query
    PredicateToQueryDslConverter predicateConverter = new PredicateToQueryDslConverter();
    org.opensearch.index.query.QueryBuilder pushdownQuery = null;
    List<ExpressionEvaluator> residualEvaluators = null;

    if (analyzed.getPredicateAnalysis().isPresent()) {
      PredicateAnalysisResult predicateResult = analyzed.getPredicateAnalysis().get();
      List<PushdownPredicate> pushdownPredicates = predicateResult.getPushdownPredicates();
      if (!pushdownPredicates.isEmpty()) {
        pushdownQuery = predicateConverter.convertAll(pushdownPredicates);
      }
      // Build residual predicate evaluators if present
      if (predicateResult.getResidualPredicates() != null
          && !predicateResult.getResidualPredicates().isEmpty()) {
        // Residual expressions will be handled via FilterOperator after scan
        // We build evaluators later once we know the column index map
      }
    }

    // 5. Build column descriptors for the scan
    Set<DqeColumnHandle> requiredColumnHandles =
        analyzed.getRequiredColumns().isAllColumns()
            ? Set.copyOf(metadata.getColumnHandles(clusterService.state(), analyzed.getTable()))
            : analyzed.getRequiredColumns().getColumns();

    // Build ordered list and column index map
    List<DqeColumnHandle> orderedColumns = new ArrayList<>(requiredColumnHandles);
    List<ColumnDescriptor> columnDescriptors =
        orderedColumns.stream()
            .map(col -> new ColumnDescriptor(col.getFieldPath(), col.getType()))
            .collect(Collectors.toList());
    List<String> requiredColumnPaths =
        orderedColumns.stream()
            .map(DqeColumnHandle::getFieldPath)
            .collect(Collectors.toList());

    // Column name -> channel index in scan output
    Map<String, Integer> inputColumnMap = new LinkedHashMap<>();
    for (int i = 0; i < orderedColumns.size(); i++) {
      inputColumnMap.put(orderedColumns.get(i).getFieldName(), i);
      // Also map by field path in case expressions reference dot-separated paths
      inputColumnMap.putIfAbsent(orderedColumns.get(i).getFieldPath(), i);
    }

    // 6. Build converter
    SearchHitToPageConverter converter =
        new SearchHitToPageConverter(columnDescriptors, planFragment.getBatchSize());

    // 7. Build exchange sink
    QueryMemoryBudget budget =
        new QueryMemoryBudget(queryId, request.getQueryMemoryBudgetBytes(), memoryTracker);
    TransportChunkSender chunkSender =
        new TransportChunkSender(
            transportService, clusterService, coordinatorNodeId, queryId, stageId);
    GatherExchangeSink sink =
        new GatherExchangeSink(
            queryId, stageId, 0, DqeExchangeChunk.DEFAULT_MAX_CHUNK_BYTES, chunkSender);

    List<PitHandle> pits = new ArrayList<>();

    try {
      // 8. Drive each shard pipeline
      int operatorId = 0;
      for (ShardSplitInfo splitInfo : request.getShardSplitInfos()) {
        // Create PIT for this shard
        PitHandle pitHandle =
            pitManager.createPit(
                splitInfo.getIndexName(),
                TimeValue.timeValueSeconds(planFragment.getPitKeepAliveSeconds()));
        pitManager.registerPit(queryId, pitHandle);
        pits.add(pitHandle);

        // Build scan operator
        SearchRequestBuilder searchReqBuilder =
            new SearchRequestBuilder(
                splitInfo.getIndexName(),
                splitInfo.getShardId(),
                requiredColumnPaths,
                pushdownQuery,
                null, // sort handled at coordinator
                planFragment.getBatchSize());

        OperatorContext scanCtx =
            new OperatorContext(
                queryId, stageId, 0, operatorId++, "ShardScan", budget);
        Operator pipeline =
            new ShardScanOperator(
                scanCtx, searchReqBuilder, pitHandle, client, planFragment.getBatchSize(), converter);

        // Add filter for residual predicates if present
        if (analyzed.getPredicateAnalysis().isPresent()) {
          PredicateAnalysisResult predicateResult = analyzed.getPredicateAnalysis().get();
          if (predicateResult.getResidualPredicates() != null
              && !predicateResult.getResidualPredicates().isEmpty()) {
            var residualExpr = predicateResult.getResidualPredicates().get(0);
            ExpressionEvaluator filterEval = new ExpressionEvaluator(residualExpr, inputColumnMap);
            OperatorContext filterCtx =
                new OperatorContext(
                    queryId, stageId, 0, operatorId++, "Filter", budget);
            pipeline = new FilterOperator(filterCtx, pipeline, filterEval);
          }
        }

        // Add projection if not SELECT *
        if (!analyzed.isSelectAll() && analyzed.getOutputExpressions() != null) {
          List<ExpressionEvaluator> projections = new ArrayList<>();
          for (var expr : analyzed.getOutputExpressions()) {
            projections.add(new ExpressionEvaluator(expr, inputColumnMap));
          }
          OperatorContext projectCtx =
              new OperatorContext(
                  queryId, stageId, 0, operatorId++, "Project", budget);
          pipeline = new ProjectOperator(projectCtx, pipeline, projections);
        }

        // Drive the pipeline, push pages to sink
        while (!pipeline.isFinished()) {
          Page page = pipeline.getOutput();
          if (page != null && page.getPositionCount() > 0) {
            sink.addPage(page);
          }
        }
        pipeline.close();
      }

      // Finish the sink (sends final isLast chunk)
      sink.finish();
      logger.debug("[{}] Stage {} completed, all shards processed", queryId, stageId);

    } catch (Exception e) {
      // Abort the sink on failure
      sink.abort();
      throw new DqeException(
          String.format("Stage %d execution failed for query [%s]: %s", stageId, queryId, e.getMessage()),
          DqeErrorCode.STAGE_EXECUTION_FAILED,
          e);
    } finally {
      // Release PITs
      for (PitHandle pit : pits) {
        try {
          pitManager.releasePit(pit);
        } catch (Exception e) {
          logger.warn("[{}] Failed to release PIT: {}", queryId, e.getMessage());
        }
      }
    }
  }
}
