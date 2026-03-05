/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dqe.plugin;

import java.util.Collections;
import java.util.List;
import java.util.Objects;
import javax.annotation.Nullable;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.action.ActionRequest;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.concurrent.OpenSearchExecutors;
import org.opensearch.core.action.ActionResponse;
import org.opensearch.dqe.analyzer.DqeAnalyzer;
import org.opensearch.dqe.exchange.action.DqeExchangePushAction;
import org.opensearch.dqe.exchange.action.DqeExchangePushRequest;
import org.opensearch.dqe.exchange.action.DqeStageCancelAction;
import org.opensearch.dqe.exchange.action.DqeStageCancelRequest;
import org.opensearch.dqe.exchange.action.DqeStageExecuteAction;
import org.opensearch.dqe.exchange.action.DqeStageExecuteRequest;
import org.opensearch.dqe.exchange.gather.ExchangePushHandler;
import org.opensearch.dqe.exchange.stage.StageCancelHandler;
import org.opensearch.dqe.exchange.stage.StageExecutionHandler;
import org.opensearch.dqe.exchange.stage.StageScheduler;
import org.opensearch.dqe.execution.pit.PitManager;
import org.opensearch.dqe.memory.AdmissionController;
import org.opensearch.dqe.metadata.DqeColumnHandle;
import org.opensearch.dqe.metadata.DqeTableHandle;
import org.opensearch.dqe.memory.DqeMemoryTracker;
import org.opensearch.dqe.metadata.DqeMetadata;
import org.opensearch.dqe.metadata.StatisticsCache;
import org.opensearch.dqe.parser.DqeSqlParser;
import org.opensearch.dqe.plugin.execution.ShardStageExecutor;
import org.opensearch.dqe.plugin.explain.DqeExplainHandler;
import org.opensearch.dqe.plugin.explain.PlanPrinter;
import org.opensearch.dqe.plugin.logging.DqeAuditLogger;
import org.opensearch.dqe.plugin.logging.SlowQueryLogger;
import org.opensearch.dqe.plugin.metrics.DqeMetrics;
import org.opensearch.dqe.plugin.orchestrator.DqeQueryOrchestrator;
import org.opensearch.dqe.plugin.routing.EngineRouter;
import org.opensearch.dqe.plugin.settings.DqeSettings;
import org.opensearch.transport.TransportService;
import org.opensearch.dqe.types.mapping.DateFormatResolver;
import org.opensearch.dqe.types.mapping.MultiFieldResolver;
import org.opensearch.dqe.types.mapping.OpenSearchTypeMappingResolver;
import org.opensearch.plugins.ActionPlugin;
import org.opensearch.rest.RestHandler;
import org.opensearch.threadpool.ExecutorBuilder;
import org.opensearch.threadpool.FixedExecutorBuilder;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.client.node.NodeClient;

/**
 * Entry point for the DQE engine within the opensearch-sql plugin. Handles registration of
 * transport actions, thread pools, settings, and provides the engine router for request dispatch.
 *
 * <p>This class is instantiated by {@code SQLPlugin} during plugin initialization.
 *
 * <p>Lifecycle:
 *
 * <ol>
 *   <li>Constructor: captures OpenSearch core services
 *   <li>{@link #getSettings()}: returns DQE cluster settings for registration
 *   <li>{@link #getExecutorBuilders(Settings)}: returns DQE thread pool builders
 *   <li>{@link #initialize()}: wires up internal components (called from
 *       SQLPlugin.createComponents)
 *   <li>{@link #getEngineRouter()}: provides the routing integration point
 *   <li>{@link #close()}: graceful shutdown
 * </ol>
 */
public class DqeEnginePlugin {

  private static final Logger LOG = LogManager.getLogger(DqeEnginePlugin.class);

  /** Thread pool name for operator pipeline execution. */
  public static final String DQE_WORKER_POOL = "dqe_worker";

  /** Thread pool name for exchange send/receive. */
  public static final String DQE_EXCHANGE_POOL = "dqe_exchange";

  /** Thread pool name for plan, schedule, and final merge. */
  public static final String DQE_COORDINATOR_POOL = "dqe_coordinator";

  private final Settings settings;
  private final ClusterService clusterService;
  private final ThreadPool threadPool;
  private final NodeClient client;

  private volatile DqeSettings dqeSettings;
  private volatile EngineRouter engineRouter;
  private volatile DqeQueryOrchestrator orchestrator;
  private volatile DqeMetrics metrics;
  private volatile boolean initialized = false;

  /**
   * Creates a new DqeEnginePlugin.
   *
   * @param settings node-level settings
   * @param clusterService the cluster service
   * @param threadPool the thread pool
   * @param client the node client
   */
  public DqeEnginePlugin(
      Settings settings, ClusterService clusterService, ThreadPool threadPool, NodeClient client) {
    this.settings = Objects.requireNonNull(settings, "settings must not be null");
    this.clusterService =
        Objects.requireNonNull(clusterService, "clusterService must not be null");
    this.threadPool = Objects.requireNonNull(threadPool, "threadPool must not be null");
    this.client = Objects.requireNonNull(client, "client must not be null");
  }

  /**
   * Returns DQE-specific transport actions to register. Currently empty — DQE stats endpoint is
   * registered as a REST handler, not a transport action. Transport actions for exchange are
   * registered by the exchange module.
   *
   * @return list of action handlers (empty in Phase 1)
   */
  public List<ActionPlugin.ActionHandler<? extends ActionRequest, ? extends ActionResponse>>
      getActions() {
    return Collections.emptyList();
  }

  /**
   * Returns DQE thread pool executor builders. Three fixed-size pools with AbortPolicy rejection:
   *
   * <ul>
   *   <li>{@code dqe_worker}: min(4, cores/2), queue 100
   *   <li>{@code dqe_exchange}: min(2, cores/4), queue 200
   *   <li>{@code dqe_coordinator}: min(2, cores/4), queue 50
   * </ul>
   *
   * @param settings node-level settings for pool size overrides
   * @return list of executor builders
   */
  public List<ExecutorBuilder<?>> getExecutorBuilders(Settings settings) {
    int cores = OpenSearchExecutors.allocatedProcessors(settings);

    int workerSize =
        DqeSettings.WORKER_POOL_SIZE_SETTING.exists(settings)
            ? DqeSettings.WORKER_POOL_SIZE_SETTING.get(settings)
            : Math.max(1, Math.min(4, cores / 2));

    int exchangeSize =
        DqeSettings.EXCHANGE_POOL_SIZE_SETTING.exists(settings)
            ? DqeSettings.EXCHANGE_POOL_SIZE_SETTING.get(settings)
            : Math.max(1, Math.min(2, cores / 4));

    int coordinatorSize =
        DqeSettings.COORDINATOR_POOL_SIZE_SETTING.exists(settings)
            ? DqeSettings.COORDINATOR_POOL_SIZE_SETTING.get(settings)
            : Math.max(1, Math.min(2, cores / 4));

    return List.of(
        new FixedExecutorBuilder(
            settings, DQE_WORKER_POOL, workerSize, 100, "thread_pool." + DQE_WORKER_POOL),
        new FixedExecutorBuilder(
            settings,
            DQE_EXCHANGE_POOL,
            exchangeSize,
            200,
            "thread_pool." + DQE_EXCHANGE_POOL),
        new FixedExecutorBuilder(
            settings,
            DQE_COORDINATOR_POOL,
            coordinatorSize,
            50,
            "thread_pool." + DQE_COORDINATOR_POOL));
  }

  /**
   * Returns all DQE cluster settings for registration with OpenSearch.
   *
   * @return list of all DQE settings
   */
  public List<org.opensearch.common.settings.Setting<?>> getSettings() {
    return DqeSettings.getAllSettings();
  }

  /**
   * Initialize DQE components. Called during SQLPlugin.createComponents after cluster settings are
   * available.
   *
   * <p>Wires up all DQE components: parser, analyzer, metadata, orchestrator, explain handler,
   * metrics, admission control, memory tracking, and audit/slow-query logging. The EngineRouter
   * receives the fully wired orchestrator and explain handler.
   */
  public void initialize() {
    initialize(null);
  }

  /**
   * Initialize DQE components with TransportService for inter-node exchange.
   *
   * @param transportService the transport service (null to skip exchange registration)
   */
  public void initialize(@Nullable TransportService transportService) {
    if (initialized) {
      LOG.warn("DqeEnginePlugin.initialize() called more than once; ignoring.");
      return;
    }
    this.dqeSettings = new DqeSettings(clusterService.getClusterSettings());

    // Parser
    DqeSqlParser parser = new DqeSqlParser();

    // Analyzer
    DqeAnalyzer analyzer = new DqeAnalyzer();

    // Metadata
    OpenSearchTypeMappingResolver typeMappingResolver =
        new OpenSearchTypeMappingResolver(new MultiFieldResolver(), new DateFormatResolver());
    StatisticsCache statisticsCache = new StatisticsCache(60_000L);
    // Create metadata with ClusterState-aware overrides for analyzer use
    DqeMetadata metadata =
        new DqeMetadata(typeMappingResolver, statisticsCache) {
          @Override
          public DqeTableHandle getTableHandle(String schema, String tableName) {
            return getTableHandle(clusterService.state(), schema, tableName);
          }

          @Override
          public java.util.List<DqeColumnHandle> getColumnHandles(DqeTableHandle table) {
            return getColumnHandles(clusterService.state(), table);
          }
        };

    // Admission control
    AdmissionController admissionController =
        new AdmissionController(dqeSettings.getMaxConcurrentQueries());

    // Memory tracking: use noop breakers since DQE circuit breaker registration
    // with OpenSearch's CircuitBreakerService happens at a higher level (SQLPlugin).
    // The orchestrator's QueryMemoryBudget handles per-query accounting.
    DqeMemoryTracker memoryTracker =
        new DqeMemoryTracker(
            new org.opensearch.core.common.breaker.NoopCircuitBreaker("dqe"),
            new org.opensearch.core.common.breaker.NoopCircuitBreaker("parent"));

    // Metrics
    this.metrics = new DqeMetrics();

    // Logging
    SlowQueryLogger slowQueryLogger = new SlowQueryLogger(dqeSettings);
    DqeAuditLogger auditLogger = new DqeAuditLogger();

    // PIT manager
    PitManager pitManager = new PitManager(client);

    // Exchange push handler (coordinator-side, works with or without transport)
    ExchangePushHandler exchangePushHandler = new ExchangePushHandler();

    // Stage scheduling and exchange (requires TransportService for inter-node communication)
    StageScheduler stageScheduler = null;
    if (transportService != null) {
      stageScheduler = new StageScheduler(transportService, clusterService);

      // Stage execution handler (data-node side)
      StageExecutionHandler stageExecutionHandler =
          new StageExecutionHandler(threadPool, transportService, memoryTracker);
      StageCancelHandler stageCancelHandler = new StageCancelHandler(stageExecutionHandler);

      // Shard stage executor (data-node side pipeline builder)
      ShardStageExecutor shardStageExecutor =
          new ShardStageExecutor(
              parser,
              analyzer,
              metadata,
              pitManager,
              transportService,
              clusterService,
              memoryTracker,
              threadPool,
              client);
      stageExecutionHandler.setExecutionCallback(shardStageExecutor);

      // Register transport actions for exchange and stage execution
      transportService.registerRequestHandler(
          DqeExchangePushAction.NAME,
          ThreadPool.Names.SAME,
          DqeExchangePushRequest::new,
          exchangePushHandler);
      transportService.registerRequestHandler(
          DqeStageExecuteAction.NAME,
          DQE_WORKER_POOL,
          DqeStageExecuteRequest::new,
          stageExecutionHandler);
      transportService.registerRequestHandler(
          DqeStageCancelAction.NAME,
          ThreadPool.Names.SAME,
          DqeStageCancelRequest::new,
          stageCancelHandler);
    } else {
      LOG.warn("TransportService not provided; DQE exchange transport handlers not registered");
    }

    // Orchestrator (PL-10) — now with full execution wiring
    this.orchestrator =
        new DqeQueryOrchestrator(
            parser,
            analyzer,
            metadata,
            dqeSettings,
            admissionController,
            memoryTracker,
            metrics,
            slowQueryLogger,
            auditLogger,
            clusterService,
            pitManager,
            stageScheduler,
            exchangePushHandler,
            transportService,
            client,
            threadPool);

    // Explain handler (PL-5)
    PlanPrinter planPrinter = new PlanPrinter();
    DqeExplainHandler explainHandler = new DqeExplainHandler(parser, planPrinter);

    // Engine router with fully wired orchestrator and explain handler
    this.engineRouter = new EngineRouter(dqeSettings, orchestrator, explainHandler);

    this.initialized = true;
    LOG.info(
        "DQE engine initialized. enabled={}, default_engine={}, max_concurrent_queries={}",
        dqeSettings.isDqeEnabled(),
        dqeSettings.getEngine(),
        dqeSettings.getMaxConcurrentQueries());
  }

  /**
   * Returns the engine router for use by SQLPlugin's REST handler.
   *
   * @return the engine router
   * @throws IllegalStateException if {@link #initialize()} has not been called
   */
  public EngineRouter getEngineRouter() {
    if (!initialized) {
      throw new IllegalStateException("DqeEnginePlugin has not been initialized");
    }
    return engineRouter;
  }

  /**
   * Returns the DQE stats REST handler. Placeholder for PL-8.
   *
   * @return null in Phase 1 pre-PL-8
   */
  public RestHandler getStatsHandler() {
    // Will be implemented in PL-8
    return null;
  }

  /** Graceful shutdown. Logs shutdown and marks engine as uninitialized. */
  public void close() {
    LOG.info(
        "DQE engine shutting down. Active queries: {}",
        orchestrator != null ? orchestrator.getActiveQueryCount() : 0);
    initialized = false;
  }

  /**
   * Whether the DQE engine is currently enabled.
   *
   * @return true if DQE is enabled and initialized
   */
  public boolean isEnabled() {
    return initialized && dqeSettings != null && dqeSettings.isDqeEnabled();
  }

  /**
   * Returns the DqeSettings instance. Available after {@link #initialize()}.
   *
   * @return the DQE settings
   */
  public DqeSettings getDqeSettings() {
    return dqeSettings;
  }

  /**
   * Returns the DQE metrics instance. Available after {@link #initialize()}.
   *
   * @return the DQE metrics
   */
  public DqeMetrics getMetrics() {
    return metrics;
  }

  /**
   * Returns the query orchestrator. Available after {@link #initialize()}.
   *
   * @return the orchestrator
   */
  public DqeQueryOrchestrator getOrchestrator() {
    return orchestrator;
  }
}
