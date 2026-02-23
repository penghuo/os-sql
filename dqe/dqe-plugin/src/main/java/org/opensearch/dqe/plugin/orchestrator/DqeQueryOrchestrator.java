/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dqe.plugin.orchestrator;

import io.trino.sql.tree.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import javax.annotation.Nullable;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.dqe.analyzer.AnalyzedQuery;
import org.opensearch.dqe.analyzer.DqeAnalyzer;
import org.opensearch.dqe.analyzer.security.SecurityContext;
import org.opensearch.dqe.memory.AdmissionController;
import org.opensearch.dqe.memory.DqeMemoryTracker;
import org.opensearch.dqe.memory.QueryCleanup;
import org.opensearch.dqe.memory.QueryMemoryBudget;
import org.opensearch.dqe.metadata.DqeMetadata;
import org.opensearch.dqe.parser.DqeErrorCode;
import org.opensearch.dqe.parser.DqeException;
import org.opensearch.dqe.parser.DqeSqlParser;
import org.opensearch.dqe.plugin.logging.DqeAuditLogger;
import org.opensearch.dqe.plugin.logging.SlowQueryLogger;
import org.opensearch.dqe.plugin.metrics.DqeMetrics;
import org.opensearch.dqe.plugin.request.DqeQueryRequest;
import org.opensearch.dqe.plugin.response.DqeQueryResponse;
import org.opensearch.dqe.plugin.security.PermissiveSecurityContext;
import org.opensearch.dqe.plugin.settings.DqeSettings;
import org.opensearch.dqe.types.DqeType;

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
 * <p>Phase 1 implements steps 1-3 and 7-8 fully. Steps 4-6 (operator execution) are stubbed and
 * will be wired when dqe-execution and dqe-exchange modules are complete.
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
      ClusterService clusterService) {
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

      // Step 6: Build operator tree (Phase 2 - E-9)
      // TODO(PL-10/E-9): Build shard pipeline + coordinator pipeline from AnalyzedQuery.
      //   - PitManager.createPit(indexName, keepAlive)
      //   - Build scan operators from analyzed.getRequiredColumns()
      //   - Build filter operators from analyzed.getPredicateAnalysis()
      //   - Build sort/topN/limit from analyzed.getPipelineDecision()

      // Step 7: Schedule stages (Phase 2 - X-6)
      // TODO(PL-10/X-6): StageScheduler.scheduleStage(splits, fragment, ...)
      //   - Get shard splits from metadata.getShardSplits(table, localNodeId)
      //   - Create GatherExchangeSource
      //   - Dispatch stage execute requests to data nodes

      // Step 8: Execute and collect results (Phase 2 - E-9, X-6)
      // TODO(PL-10/E-9): Drive operators via Driver, read pages from GatherExchangeSource
      //   - Merge pages from exchange source
      //   - Apply coordinator-side operators (merge sort, final project)

      // Phase 1: return analyzed metadata as the response (no actual data)
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
              .rowsProcessed(0) // Phase 1: no actual execution
              .bytesProcessed(0)
              .stages(1)
              .shardsQueried(0)
              .build();

      DqeQueryResponse response =
          DqeQueryResponse.builder()
              .engine("dqe")
              .schema(schema)
              .data(List.of()) // Phase 1: no actual data
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
