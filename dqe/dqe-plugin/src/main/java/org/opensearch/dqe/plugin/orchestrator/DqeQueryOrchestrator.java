/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dqe.plugin.orchestrator;

import io.trino.spi.Page;
import io.trino.sql.tree.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.action.support.PlainActionFuture;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.dqe.analyzer.AnalyzedQuery;
import org.opensearch.dqe.analyzer.DqeAnalyzer;
import org.opensearch.dqe.analyzer.security.SecurityContext;
import org.opensearch.dqe.exchange.action.DqeStageExecuteRequest.ShardSplitInfo;
import org.opensearch.dqe.exchange.buffer.ExchangeBuffer;
import org.opensearch.dqe.exchange.gather.ExchangePushHandler;
import org.opensearch.dqe.exchange.gather.ExchangeSourceOperator;
import org.opensearch.dqe.exchange.gather.GatherExchangeSource;
import org.opensearch.dqe.exchange.stage.StageScheduleResult;
import org.opensearch.dqe.exchange.stage.StageScheduler;
import org.opensearch.dqe.execution.driver.Driver;
import org.opensearch.dqe.execution.driver.Pipeline;
import org.opensearch.dqe.execution.operator.Operator;
import org.opensearch.dqe.execution.operator.OperatorContext;
import org.opensearch.dqe.execution.pit.PitHandle;
import org.opensearch.dqe.execution.pit.PitManager;
import org.opensearch.dqe.execution.plan.PlanFragment;
import org.opensearch.dqe.memory.AdmissionController;
import org.opensearch.dqe.memory.DqeMemoryTracker;
import org.opensearch.dqe.memory.QueryCleanup;
import org.opensearch.dqe.memory.QueryMemoryBudget;
import org.opensearch.dqe.metadata.DqeMetadata;
import org.opensearch.dqe.metadata.DqeShardSplit;
import org.opensearch.dqe.parser.DqeErrorCode;
import org.opensearch.dqe.parser.DqeException;
import org.opensearch.dqe.parser.DqeSqlParser;
import org.opensearch.dqe.plugin.execution.CoordinatorPipelineBuilder;
import org.opensearch.dqe.plugin.logging.DqeAuditLogger;
import org.opensearch.dqe.plugin.logging.SlowQueryLogger;
import org.opensearch.dqe.plugin.metrics.DqeMetrics;
import org.opensearch.dqe.plugin.request.DqeQueryRequest;
import org.opensearch.dqe.plugin.response.DqeQueryResponse;
import org.opensearch.dqe.plugin.security.PermissiveSecurityContext;
import org.opensearch.dqe.plugin.settings.DqeSettings;
import org.opensearch.dqe.types.DqeType;
import org.opensearch.dqe.types.converter.PageToRowConverter;
import org.opensearch.transport.TransportService;

/**
 * Orchestrates end-to-end DQE query execution. This is the central coordinator that wires together
 * all DQE components for a single query lifecycle:
 *
 * <ol>
 *   <li>Admission control: reject if at capacity
 *   <li>Parse: SQL text to Trino AST via DqeSqlParser
 *   <li>Analyze: resolve tables/columns/types via DqeAnalyzer + DqeMetadata
 *   <li>Plan: build operator tree from AnalyzedQuery (Phase 2)
 *   <li>Schedule: dispatch shard stages (Phase 2)
 *   <li>Execute: drive operators and collect results (Phase 2)
 *   <li>Format: build DqeQueryResponse
 *   <li>Cleanup: release memory, close PIT, record metrics, log
 * </ol>
 *
 * <p>Phase 1 execution is fully wired: shard splits are scheduled to data nodes via
 * StageScheduler, data flows through gather exchange, and the coordinator pipeline applies
 * Sort/TopN/Limit per the PipelineDecision before converting pages to row data.
 */
public class DqeQueryOrchestrator {

  private static final Logger LOG = LogManager.getLogger(DqeQueryOrchestrator.class);

  private final DqeSqlParser parser;
  private final DqeAnalyzer analyzer;
  private final DqeMetadata metadata;
  private final DqeSettings settings;
  private final AdmissionController admissionController;
  private final DqeMemoryTracker memoryTracker;
  private final DqeMetrics metrics;
  private final SlowQueryLogger slowQueryLogger;
  private final DqeAuditLogger auditLogger;
  private final ClusterService clusterService;
  private final PitManager pitManager;
  private final StageScheduler stageScheduler;
  private final ExchangePushHandler exchangePushHandler;
  private final TransportService transportService;
  private final org.opensearch.transport.client.Client client;

  /** Active queries by queryId, for cancellation support. */
  private final ConcurrentMap<String, QueryState> activeQueries = new ConcurrentHashMap<>();

  /**
   * Creates the orchestrator with all required dependencies.
   *
   * @param parser the SQL parser
   * @param analyzer the semantic analyzer
   * @param metadata the metadata service
   * @param settings DQE settings
   * @param admissionController concurrency limiter
   * @param memoryTracker node-level memory tracker
   * @param metrics observability metrics
   * @param slowQueryLogger slow query logger
   * @param auditLogger audit logger
   * @param clusterService for obtaining ClusterState
   * @param pitManager PIT lifecycle manager
   * @param stageScheduler stage scheduler for dispatching to data nodes
   * @param exchangePushHandler exchange push handler for registering buffers
   * @param transportService transport service for node ID resolution
   */
  public DqeQueryOrchestrator(
      DqeSqlParser parser,
      DqeAnalyzer analyzer,
      DqeMetadata metadata,
      DqeSettings settings,
      AdmissionController admissionController,
      DqeMemoryTracker memoryTracker,
      DqeMetrics metrics,
      SlowQueryLogger slowQueryLogger,
      DqeAuditLogger auditLogger,
      ClusterService clusterService,
      PitManager pitManager,
      StageScheduler stageScheduler,
      ExchangePushHandler exchangePushHandler,
      TransportService transportService,
      org.opensearch.transport.client.Client client) {
    this.parser = Objects.requireNonNull(parser, "parser must not be null");
    this.analyzer = Objects.requireNonNull(analyzer, "analyzer must not be null");
    this.metadata = Objects.requireNonNull(metadata, "metadata must not be null");
    this.settings = Objects.requireNonNull(settings, "settings must not be null");
    this.admissionController =
        Objects.requireNonNull(admissionController, "admissionController must not be null");
    this.memoryTracker =
        Objects.requireNonNull(memoryTracker, "memoryTracker must not be null");
    this.metrics = Objects.requireNonNull(metrics, "metrics must not be null");
    this.slowQueryLogger =
        Objects.requireNonNull(slowQueryLogger, "slowQueryLogger must not be null");
    this.auditLogger = Objects.requireNonNull(auditLogger, "auditLogger must not be null");
    this.clusterService =
        Objects.requireNonNull(clusterService, "clusterService must not be null");
    this.pitManager = Objects.requireNonNull(pitManager, "pitManager must not be null");
    this.stageScheduler = stageScheduler; // nullable until TransportService is available
    this.exchangePushHandler =
        Objects.requireNonNull(exchangePushHandler, "exchangePushHandler must not be null");
    this.transportService = transportService; // nullable until TransportService is available
    this.client = Objects.requireNonNull(client, "client must not be null");
  }

  /**
   * Execute a DQE query request end-to-end.
   *
   * @param request the parsed query request
   * @return the query response
   * @throws DqeException on any execution error
   */
  public DqeQueryResponse execute(DqeQueryRequest request) throws DqeException {
    String queryId = request.getQueryId();
    long startTimeMs = System.currentTimeMillis();
    boolean admitted = false;

    // Resolve per-query memory budget
    long budgetBytes = resolveQueryMemoryBudget(request);

    // Create per-query memory budget and cleanup handler
    QueryMemoryBudget budget = new QueryMemoryBudget(queryId, budgetBytes, memoryTracker);
    QueryCleanup cleanup = new QueryCleanup(budget);

    // Security context (Phase 1: permissive)
    SecurityContext securityContext = new PermissiveSecurityContext("dqe_user");

    try {
      // Step 1: Admission control
      if (!admissionController.tryAcquire()) {
        throw new DqeException(
            "Too many concurrent DQE queries. Max: "
                + admissionController.getMaxConcurrentQueries(),
            DqeErrorCode.TOO_MANY_CONCURRENT_QUERIES);
      }
      admitted = true;
      cleanup.registerCleanupAction(admissionController::release);

      // Step 2: Metrics - record query submitted
      metrics.recordQuerySubmitted();
      metrics.incrementActiveQueries();
      cleanup.registerCleanupAction(metrics::decrementActiveQueries);

      // Track active query for cancellation
      QueryState state = new QueryState(queryId, cleanup);
      activeQueries.put(queryId, state);
      cleanup.registerCleanupAction(() -> activeQueries.remove(queryId));

      // Step 3: Audit log - query started
      auditLogger.logQueryStarted(
          queryId, securityContext.getUserName(), request.getQuery(), List.of());

      // Step 4: Parse SQL
      LOG.debug("Parsing query [{}]", queryId);
      Statement statement = parser.parse(request.getQuery());

      // Step 5: Analyze
      LOG.debug("Analyzing query [{}]", queryId);
      AnalyzedQuery analyzed =
          analyzer.analyze(statement, metadata, securityContext);

      // Update audit log with resolved indices
      List<String> indices = List.of(analyzed.getTable().getIndexName());
      auditLogger.logQueryStarted(
          queryId, securityContext.getUserName(), request.getQuery(), indices);

      // Step 6: Get shard splits
      String localNodeId = clusterService.state().nodes().getLocalNodeId();
      List<DqeShardSplit> splits =
          metadata.getSplits(clusterService.state(), analyzed.getTable(), localNodeId);
      LOG.debug("[{}] Got {} shard splits", queryId, splits.size());

      // Collect results via exchange pipeline
      List<List<Object>> data = new ArrayList<>();
      AtomicLong totalRows = new AtomicLong(0);

      if (!splits.isEmpty() && stageScheduler != null) {
        // Distributed execution path (via transport exchange)
        // Step 7: Create exchange infrastructure
        ExchangeBuffer exchangeBuffer =
            new ExchangeBuffer(
                settings.getExchangeBufferSize().getBytes(),
                settings.getExchangeBackpressureTimeout().millis(),
                memoryTracker,
                queryId);
        cleanup.registerCleanupAction(exchangeBuffer::close);

        GatherExchangeSource exchangeSource =
            new GatherExchangeSource(queryId, 0, splits.size(), exchangeBuffer);
        cleanup.registerCleanupAction(exchangeSource::close);

        exchangePushHandler.registerBuffer(queryId, 0, exchangeBuffer);
        cleanup.registerCleanupAction(() -> exchangePushHandler.deregisterBuffer(queryId, 0));

        // Step 8: Build and serialize plan fragment
        PlanFragment planFragment = new PlanFragment(request.getQuery(), queryId, 300L, 1000);
        byte[] serializedFragment = planFragment.serialize();

        // Step 9: Convert splits to ShardSplitInfo and schedule stage
        List<ShardSplitInfo> splitInfos =
            splits.stream()
                .map(
                    s ->
                        new ShardSplitInfo(
                            s.getShardId(), s.getNodeId(), s.getIndexName(), s.isPrimary()))
                .collect(Collectors.toList());

        PlainActionFuture<StageScheduleResult> scheduleFuture = new PlainActionFuture<>();
        stageScheduler.scheduleStage(
            queryId, 0, serializedFragment, splitInfos, localNodeId, budgetBytes, scheduleFuture);
        scheduleFuture.actionGet();
        cleanup.registerCleanupAction(() -> stageScheduler.deregisterQuery(queryId));

        // Step 10: Build coordinator pipeline
        OperatorContext exchangeOpCtx =
            new OperatorContext(queryId, 0, 0, 0, "ExchangeSource", budget);
        ExchangeSourceOperator exchangeOp =
            new ExchangeSourceOperator(exchangeOpCtx, exchangeSource);
        Pipeline coordinatorPipeline =
            CoordinatorPipelineBuilder.build(exchangeOp, analyzed, budget);
        cleanup.registerCleanupAction(coordinatorPipeline::close);

        // Step 11: Drive coordinator pipeline, collect results
        List<Page> resultPages = new ArrayList<>();
        Driver driver =
            new Driver(
                coordinatorPipeline,
                page -> {
                  resultPages.add(page);
                  totalRows.addAndGet(page.getPositionCount());
                });
        while (driver.process()) {
          // pull pages
        }

        // Step 12: Convert pages to row data
        for (Page page : resultPages) {
          data.addAll(PageToRowConverter.convert(page, analyzed.getOutputColumnTypes()));
        }
      } else if (!splits.isEmpty()) {
        // Local execution path (no TransportService / single-node fallback)
        // Build shard pipelines directly in-process and collect results
        LOG.debug("[{}] Using local execution path ({} splits)", queryId, splits.size());

        var predicateConverter = new org.opensearch.dqe.execution.predicate.PredicateToQueryDslConverter();
        org.opensearch.index.query.QueryBuilder pushdownQuery = null;
        if (analyzed.getPredicateAnalysis().isPresent()) {
          var pushdowns = analyzed.getPredicateAnalysis().get().getPushdownPredicates();
          if (!pushdowns.isEmpty()) {
            pushdownQuery = predicateConverter.convertAll(pushdowns);
          }
        }

        // Build column descriptors from required columns
        var requiredColumnHandles = analyzed.getRequiredColumns().isAllColumns()
            ? java.util.Set.copyOf(metadata.getColumnHandles(clusterService.state(), analyzed.getTable()))
            : analyzed.getRequiredColumns().getColumns();

        var orderedColumns = new ArrayList<>(requiredColumnHandles);
        var columnDescriptors = orderedColumns.stream()
            .map(col -> new org.opensearch.dqe.types.converter.ColumnDescriptor(
                col.getFieldPath(), col.getType()))
            .collect(Collectors.toList());
        var requiredColumnPaths = orderedColumns.stream()
            .map(org.opensearch.dqe.metadata.DqeColumnHandle::getFieldPath)
            .collect(Collectors.toList());

        // Column name -> channel index in scan output
        var inputColumnMap = new java.util.LinkedHashMap<String, Integer>();
        for (int i = 0; i < orderedColumns.size(); i++) {
          inputColumnMap.put(orderedColumns.get(i).getFieldName(), i);
          inputColumnMap.putIfAbsent(orderedColumns.get(i).getFieldPath(), i);
        }

        var converter = new org.opensearch.dqe.types.converter.SearchHitToPageConverter(columnDescriptors, 1000);

        // Process each shard locally
        for (var splitInfo : splits) {
          var pitHandle = pitManager.createPit(
              splitInfo.getIndexName(),
              org.opensearch.common.unit.TimeValue.timeValueMinutes(5));
          pitManager.registerPit(queryId, pitHandle);
          cleanup.registerCleanupAction(() -> {
            try { pitManager.releasePit(pitHandle); } catch (Exception e) { /* ignore */ }
          });

          var searchReqBuilder = new org.opensearch.dqe.execution.operator.scan.SearchRequestBuilder(
              splitInfo.getIndexName(), splitInfo.getShardId(),
              requiredColumnPaths, pushdownQuery, null, 1000);
          OperatorContext scanCtx = new OperatorContext(queryId, 0, 0, 0, "ShardScan", budget);
          Operator pipeline = new org.opensearch.dqe.execution.operator.scan.ShardScanOperator(
              scanCtx, searchReqBuilder, pitHandle, client, 1000, converter);

          // Add projection if not SELECT *
          if (!analyzed.isSelectAll() && analyzed.getOutputExpressions() != null
              && !analyzed.getOutputExpressions().isEmpty()) {
            var projections = new ArrayList<org.opensearch.dqe.execution.expression.ExpressionEvaluator>();
            for (var expr : analyzed.getOutputExpressions()) {
              projections.add(new org.opensearch.dqe.execution.expression.ExpressionEvaluator(expr, inputColumnMap));
            }
            OperatorContext projectCtx = new OperatorContext(queryId, 0, 0, 1, "Project", budget);
            pipeline = new org.opensearch.dqe.execution.operator.ProjectOperator(projectCtx, pipeline, projections);
          }

          // Drive pipeline, collect pages
          while (!pipeline.isFinished()) {
            Page page = pipeline.getOutput();
            if (page != null && page.getPositionCount() > 0) {
              totalRows.addAndGet(page.getPositionCount());
              data.addAll(PageToRowConverter.convert(page, analyzed.getOutputColumnTypes()));
            }
          }
          pipeline.close();
        }

        // Apply coordinator-side sort/limit if needed
        // (For local path, we skip sort/limit for now — data is unordered from shards)
      }

      long elapsedMs = System.currentTimeMillis() - startTimeMs;

      // Build schema from analyzed output
      List<DqeQueryResponse.ColumnSchema> schema = new ArrayList<>();
      for (int i = 0; i < analyzed.getOutputColumnNames().size(); i++) {
        String colName = analyzed.getOutputColumnNames().get(i);
        DqeType colType = analyzed.getOutputColumnTypes().get(i);
        schema.add(new DqeQueryResponse.ColumnSchema(colName, colType.getDisplayName()));
      }

      // Build stats
      DqeQueryResponse.QueryStats stats =
          DqeQueryResponse.QueryStats.builder()
              .state("COMPLETED")
              .queryId(queryId)
              .elapsedMs(elapsedMs)
              .rowsProcessed(totalRows.get())
              .bytesProcessed(budget.getUsedBytes())
              .stages(1)
              .shardsQueried(splits.size())
              .build();

      DqeQueryResponse response =
          DqeQueryResponse.builder()
              .engine("dqe")
              .schema(schema)
              .data(data)
              .stats(stats)
              .build();

      // Step 9: Record success metrics
      metrics.recordQuerySucceeded(elapsedMs, 0);

      // Step 10: Slow query log
      slowQueryLogger.maybeLog(request, stats, budget.getUsedBytes());

      // Step 11: Audit log - query succeeded
      auditLogger.logQuerySucceeded(queryId, securityContext.getUserName(), elapsedMs, 0);

      LOG.info("DQE query [{}] completed in {} ms", queryId, elapsedMs);
      return response;

    } catch (DqeException e) {
      long elapsedMs = System.currentTimeMillis() - startTimeMs;
      metrics.recordQueryFailed(e.getErrorCode().name());
      auditLogger.logQueryFailed(queryId, "dqe_user", e.getMessage());
      LOG.warn("DQE query [{}] failed after {} ms: {}", queryId, elapsedMs, e.getMessage());
      throw e;
    } catch (Exception e) {
      long elapsedMs = System.currentTimeMillis() - startTimeMs;
      metrics.recordQueryFailed("INTERNAL_ERROR");
      auditLogger.logQueryFailed(queryId, "dqe_user", e.getMessage());
      LOG.error("DQE query [{}] failed with unexpected error after {} ms", queryId, elapsedMs, e);
      throw new DqeException(
          "Internal error during query execution: " + e.getMessage(),
          DqeErrorCode.EXECUTION_ERROR);
    } finally {
      cleanup.cleanup();
    }
  }

  /**
   * Cancel a running query by its ID.
   *
   * @param queryId the query ID to cancel
   */
  public void cancel(String queryId) {
    QueryState state = activeQueries.get(queryId);
    if (state == null) {
      LOG.debug("Cannot cancel query [{}]: not found in active queries", queryId);
      return;
    }

    LOG.info("Cancelling DQE query [{}]", queryId);
    metrics.recordQueryCancelled();
    auditLogger.logQueryCancelled(queryId, "dqe_user", "user_cancelled");

    // Trigger cleanup which will release admission slot, memory, etc.
    state.cleanup().cleanup();
  }

  /**
   * Returns the number of currently active queries.
   *
   * @return active query count
   */
  public int getActiveQueryCount() {
    return activeQueries.size();
  }

  private long resolveQueryMemoryBudget(DqeQueryRequest request) {
    // Per-request override takes precedence
    if (request.getQueryMaxMemoryBytes().isPresent()) {
      return request.getQueryMaxMemoryBytes().get();
    }
    // Fall back to cluster setting
    return settings.getQueryMaxMemory().getBytes();
  }

  /** Tracks the state of an active query for cancellation support. */
  private record QueryState(String queryId, QueryCleanup cleanup) {}
}
