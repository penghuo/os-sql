/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.distributed;

import com.google.common.util.concurrent.ListenableFuture;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.apache.calcite.rel.RelNode;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import org.opensearch.cluster.service.ClusterService;
import org.opensearch.sql.calcite.CalcitePlanContext;
import org.opensearch.sql.common.response.ResponseListener;
import org.opensearch.sql.distributed.context.DriverContext;
import org.opensearch.sql.distributed.context.OperatorContext;
import org.opensearch.sql.distributed.context.PipelineContext;
import org.opensearch.sql.distributed.context.QueryContext;
import org.opensearch.sql.distributed.context.TaskContext;
import org.opensearch.sql.distributed.data.Page;
import org.opensearch.sql.distributed.driver.Driver;
import org.opensearch.sql.distributed.driver.Pipeline;
import org.opensearch.sql.distributed.exchange.GatherExchange;
import org.opensearch.sql.distributed.memory.MemoryPool;
import org.opensearch.sql.distributed.operator.Operator;
import org.opensearch.sql.distributed.operator.OperatorFactory;
import org.opensearch.sql.distributed.operator.SinkOperator;
import org.opensearch.sql.distributed.planner.LuceneTableScanNode;
import org.opensearch.sql.distributed.planner.PlanNode;
import org.opensearch.sql.distributed.planner.PlanNodeVisitor;
import org.opensearch.sql.distributed.planner.RelNodeToPlanNodeConverter;
import org.opensearch.sql.distributed.planner.RemoteSourceNode;
import org.opensearch.sql.distributed.planner.bridge.PlanNodeToOperatorConverter;
import org.opensearch.sql.distributed.planner.plan.AddExchanges;
import org.opensearch.sql.distributed.planner.plan.PlanFragmenter;
import org.opensearch.sql.distributed.planner.plan.StageFragment;
import org.opensearch.sql.distributed.scheduler.NodeAssignment;
import org.opensearch.sql.distributed.scheduler.StageExecution;
import org.opensearch.sql.distributed.scheduler.StageScheduler;
import org.opensearch.sql.distributed.transport.FragmentRegistry;
import org.opensearch.sql.executor.DistributedQueryExecutor;
import org.opensearch.sql.executor.ExecutionEngine;
import org.opensearch.sql.executor.ExecutionEngine.DistributedExplainInfo;
import org.opensearch.transport.TransportService;

/**
 * Implementation of {@link DistributedQueryExecutor} that bridges the Calcite planner with the
 * distributed execution engine. Converts a Calcite RelNode into a PlanNode tree, fragments it into
 * stages, schedules stages to nodes, dispatches leaf fragments via TransportShardQueryAction, and
 * runs the root fragment locally on the coordinator.
 *
 * <p>For unsupported query patterns (joins, window functions, etc.), the converter throws {@link
 * org.opensearch.sql.distributed.planner.UnsupportedPatternException} which propagates to
 * QueryService for DSL fallback.
 */
public class DistributedQueryExecutorImpl implements DistributedQueryExecutor {

  private static final Logger log = LogManager.getLogger(DistributedQueryExecutorImpl.class);

  private static final long DEFAULT_MEMORY_POOL_BYTES = 64 * 1024 * 1024; // 64 MB
  private static final long DEFAULT_QUERY_TIMEOUT_MILLIS = 30_000;
  private static final int MAX_DRIVER_ITERATIONS = 1_000_000;

  private final PlanNodeToOperatorConverter.SourceOperatorFactoryProvider sourceProvider;
  private final long memoryPoolBytes;
  private final long queryTimeoutMillis;

  // Distributed execution deps (nullable for backward compat with existing tests)
  private final TransportService transportService;
  private final ClusterService clusterService;

  public DistributedQueryExecutorImpl(
      PlanNodeToOperatorConverter.SourceOperatorFactoryProvider sourceProvider) {
    this(sourceProvider, DEFAULT_MEMORY_POOL_BYTES, DEFAULT_QUERY_TIMEOUT_MILLIS);
  }

  public DistributedQueryExecutorImpl(
      PlanNodeToOperatorConverter.SourceOperatorFactoryProvider sourceProvider,
      long memoryPoolBytes,
      long queryTimeoutMillis) {
    this.sourceProvider = sourceProvider;
    this.memoryPoolBytes = memoryPoolBytes;
    this.queryTimeoutMillis = queryTimeoutMillis;
    this.transportService = null;
    this.clusterService = null;
  }

  /**
   * Full constructor for distributed execution with transport dispatch support.
   * ClusterService.localNode() is resolved lazily during execute() to avoid accessing
   * cluster state during Guice initialization when it may not be set yet.
   *
   * @param sourceProvider source operator factory provider for local scans
   * @param transportService transport service for dispatching shard queries
   * @param clusterService cluster service for routing, metadata, and local node resolution
   */
  public DistributedQueryExecutorImpl(
      PlanNodeToOperatorConverter.SourceOperatorFactoryProvider sourceProvider,
      TransportService transportService,
      ClusterService clusterService) {
    this(sourceProvider, transportService, clusterService,
        DEFAULT_MEMORY_POOL_BYTES, DEFAULT_QUERY_TIMEOUT_MILLIS);
  }

  public DistributedQueryExecutorImpl(
      PlanNodeToOperatorConverter.SourceOperatorFactoryProvider sourceProvider,
      TransportService transportService,
      ClusterService clusterService,
      long memoryPoolBytes,
      long queryTimeoutMillis) {
    this.sourceProvider = sourceProvider;
    this.transportService = transportService;
    this.clusterService = clusterService;
    this.memoryPoolBytes = memoryPoolBytes;
    this.queryTimeoutMillis = queryTimeoutMillis;
  }

  @Override
  public void execute(
      RelNode plan,
      CalcitePlanContext context,
      ResponseListener<ExecutionEngine.QueryResponse> listener) {
    try {
      // Step 1: Convert RelNode to PlanNode tree
      RelNodeToPlanNodeConverter relConverter = new RelNodeToPlanNodeConverter();
      PlanNode planNode = relConverter.convert(plan);

      // If distributed deps are available, use the full planning + fragmentation + dispatch path
      if (transportService != null && clusterService != null) {
        executeDistributed(planNode, plan, listener);
      } else {
        executeLocal(planNode, plan, listener);
      }
    } catch (UnsupportedOperationException e) {
      // Let UnsupportedOperationException (including UnsupportedPatternException) propagate
      // for DSL fallback in QueryService
      throw e;
    } catch (Exception e) {
      listener.onFailure(e);
    }
  }

  /**
   * Distributed execution path: AddExchanges -> PlanFragmenter -> StageScheduler -> dispatch leaf
   * via transport -> run root locally.
   */
  private void executeDistributed(
      PlanNode planNode,
      RelNode relPlan,
      ResponseListener<ExecutionEngine.QueryResponse> listener)
      throws Exception {

    // Step 2: Insert exchange nodes for distributed execution.
    // addExchanges splits aggregations into PARTIAL (leaf) + FINAL (root) phases,
    // and ensureGatherExchange adds a GatherExchange at the top if not already present.
    AddExchanges addExchanges = new AddExchanges();
    PlanNode withExchanges = addExchanges.addExchanges(planNode);
    withExchanges = addExchanges.ensureGatherExchange(withExchanges);

    // Step 3: Fragment into stages
    PlanFragmenter fragmenter = new PlanFragmenter();
    List<StageFragment> fragments = fragmenter.fragment(withExchanges);

    // Step 4: Verify the leaf stage has a LuceneTableScanNode before scheduling.
    // If not (e.g., Calcite optimized away the scan for empty index, or the plan doesn't
    // touch an index), fall back to local execution which handles all node types.
    StageFragment leafFragment =
        fragments.stream()
            .filter(StageFragment::isLeafStage)
            .findFirst()
            .orElse(null);
    if (leafFragment == null || tryExtractIndexName(leafFragment) == null) {
      log.debug("Leaf stage has no LuceneTableScanNode, falling back to local execution");
      executeLocal(planNode, relPlan, listener);
      return;
    }

    // Step 5: Schedule stages to nodes
    StageScheduler scheduler = new StageScheduler();
    StageExecution execution =
        scheduler.schedule(fragments, clusterService.state(), clusterService.localNode());

    // Step 6: Create query context
    QueryContext queryContext = createQueryContext();
    String queryId = queryContext.getQueryId();

    try {
      // Step 7: Register the leaf fragment in FragmentRegistry so that
      // TransportShardQueryAction can look it up when the request arrives.
      FragmentRegistry.put(queryId, leafFragment.getStageId(), leafFragment);

      // Step 8: Build root fragment pipeline with GatherExchange that dispatches
      // via TransportService to data nodes. GatherExchange sends ShardQueryRequests
      // and collects response Pages asynchronously.
      StageFragment rootFragment = execution.getRootStage();
      NodeAssignment leafAssignment = execution.getAssignment(leafFragment.getStageId());
      String indexName = extractIndexName(leafFragment);

      DistributedSourceProvider distributedProvider =
          new DistributedSourceProvider(
              sourceProvider,
              transportService,
              queryId,
              leafFragment.getStageId(),
              new byte[] {1}, // placeholder — fragment is looked up from FragmentRegistry by queryId:stageId
              indexName,
              leafAssignment);

      List<OperatorFactory> factories =
          PlanNodeToOperatorConverter.convert(rootFragment.getRoot(), distributedProvider);

      // Step 9: Append collecting sink
      List<OperatorFactory> withSink = new ArrayList<>(factories);
      withSink.add(new CollectingSinkFactory());

      // Step 10: Build pipeline and run
      TaskContext taskContext = queryContext.addTaskContext(rootFragment.getStageId());
      PipelineContext pipelineContext = taskContext.addPipelineContext(0);
      Pipeline pipeline = new Pipeline(pipelineContext, withSink);

      DriverContext driverContext = pipelineContext.addDriverContext();
      Driver driver = pipeline.createDriver(driverContext);
      try {
        List<Page> outputPages = runDriverToCompletion(driver);

        ExecutionEngine.QueryResponse response =
            PageToResponseConverter.convert(outputPages, relPlan.getRowType());
        listener.onResponse(response);
      } finally {
        driver.close();
      }
    } finally {
      // Clean up the registered fragment
      FragmentRegistry.remove(queryId, leafFragment.getStageId());
      queryContext.close();
    }
  }

  /**
   * Local execution path (backward compat): runs the PlanNode directly without fragmentation.
   * Used when distributed deps (TransportService, ClusterService) are not available, or when the
   * plan doesn't contain a LuceneTableScanNode (e.g., Calcite optimized away the scan).
   */
  private void executeLocal(
      PlanNode planNode,
      RelNode relPlan,
      ResponseListener<ExecutionEngine.QueryResponse> listener)
      throws Exception {

    List<OperatorFactory> factories =
        PlanNodeToOperatorConverter.convert(planNode, sourceProvider);

    List<OperatorFactory> withSink = new ArrayList<>(factories);
    withSink.add(new CollectingSinkFactory());

    QueryContext queryContext = createQueryContext();
    try {
      TaskContext taskContext = queryContext.addTaskContext(0);
      PipelineContext pipelineContext = taskContext.addPipelineContext(0);
      Pipeline pipeline = new Pipeline(pipelineContext, withSink);

      DriverContext driverContext = pipelineContext.addDriverContext();
      Driver driver = pipeline.createDriver(driverContext);
      try {
        List<Page> outputPages = runDriverToCompletion(driver);

        ExecutionEngine.QueryResponse response =
            PageToResponseConverter.convert(outputPages, relPlan.getRowType());
        // Local path: use calcite_local engine tag (not distributed)
        response.setEngine(ExecutionEngine.ENGINE_CALCITE_LOCAL);
        listener.onResponse(response);
      } finally {
        driver.close();
      }
    } finally {
      queryContext.close();
    }
  }

  @Override
  public DistributedExplainInfo explain(RelNode plan, CalcitePlanContext context) {
    // Step 1: Convert RelNode to PlanNode (throws UnsupportedPatternException if unsupported)
    RelNodeToPlanNodeConverter relConverter = new RelNodeToPlanNodeConverter();
    PlanNode planNode = relConverter.convert(plan);

    // Step 2: Insert exchange nodes to define distribution boundaries
    AddExchanges addExchanges = new AddExchanges();
    PlanNode withExchanges = addExchanges.addExchanges(planNode);
    withExchanges = addExchanges.ensureGatherExchange(withExchanges);

    // Step 3: Fragment into stages at exchange boundaries
    PlanFragmenter fragmenter = new PlanFragmenter();
    List<StageFragment> fragments = fragmenter.fragment(withExchanges);

    // Step 4: Build the plan tree description
    String planDescription = explainPlanTree(planNode, "");

    // Step 5: Build stage descriptions
    List<String> stageDescriptions = new ArrayList<>();
    for (StageFragment fragment : fragments) {
      String stageType = fragment.isLeafStage() ? "leaf" : "root";
      String stageTree = explainPlanTree(fragment.getRoot(), "  ").trim();
      stageDescriptions.add(
          String.format("Stage %d (%s): %s", fragment.getStageId(), stageType, stageTree));
    }

    return new DistributedExplainInfo(
        planDescription, stageDescriptions, ExecutionEngine.ENGINE_DISTRIBUTED);
  }

  /**
   * Recursively builds a human-readable description of a PlanNode tree with indentation.
   *
   * @param node the plan node to describe
   * @param indent the current indentation prefix
   * @return a multi-line string describing the plan tree
   */
  private String explainPlanTree(PlanNode node, String indent) {
    StringBuilder sb = new StringBuilder();
    sb.append(indent).append(node.toString()).append("\n");
    for (PlanNode child : node.getSources()) {
      sb.append(explainPlanTree(child, indent + "  "));
    }
    return sb.toString();
  }

  private QueryContext createQueryContext() {
    String queryId = "dist-" + System.nanoTime();
    MemoryPool pool = new MemoryPool(queryId, memoryPoolBytes);
    return new QueryContext(queryId, pool, queryTimeoutMillis);
  }

  /**
   * Runs the Driver in a cooperative loop until finished or the iteration limit is reached. Each
   * call to process() moves data between operators for one quantum. Supports async operators
   * (like GatherExchange) that return unresolved futures — waits on them with a timeout.
   */
  private List<Page> runDriverToCompletion(Driver driver) {
    int iterations = 0;
    while (!driver.isFinished() && iterations < MAX_DRIVER_ITERATIONS) {
      ListenableFuture<?> blocked = driver.process();
      if (!blocked.isDone()) {
        // Async operator (e.g., GatherExchange) — wait for it to resolve
        try {
          blocked.get(queryTimeoutMillis, TimeUnit.MILLISECONDS);
        } catch (TimeoutException e) {
          throw new IllegalStateException(
              "Driver blocked future timed out after " + queryTimeoutMillis + "ms");
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          throw new RuntimeException("Interrupted while waiting for blocked future", e);
        } catch (Exception e) {
          throw new RuntimeException("Driver blocked future failed", e);
        }
      }
      iterations++;
    }
    if (!driver.isFinished()) {
      throw new IllegalStateException(
          "Driver did not finish within " + MAX_DRIVER_ITERATIONS + " iterations");
    }

    // Extract collected pages from the sink operator (last in the chain)
    Operator sinkOp = driver.getOperators().get(driver.getOperators().size() - 1);
    if (sinkOp instanceof CollectingSink collectingSink) {
      return collectingSink.getCollectedPages();
    }
    throw new IllegalStateException("Last operator is not a CollectingSink");
  }

  /**
   * Source provider for the root stage that creates GatherExchange operators for RemoteSourceNodes.
   * GatherExchange dispatches ShardQueryRequests to data nodes via transport and collects response
   * Pages. For LuceneTableScanNode, delegates to the underlying local provider.
   */
  private static class DistributedSourceProvider
      implements PlanNodeToOperatorConverter.SourceOperatorFactoryProvider {

    private final PlanNodeToOperatorConverter.SourceOperatorFactoryProvider localProvider;
    private final TransportService transportService;
    private final String queryId;
    private final int leafStageId;
    private final byte[] fragmentKey;
    private final String indexName;
    private final NodeAssignment leafAssignment;

    DistributedSourceProvider(
        PlanNodeToOperatorConverter.SourceOperatorFactoryProvider localProvider,
        TransportService transportService,
        String queryId,
        int leafStageId,
        byte[] fragmentKey,
        String indexName,
        NodeAssignment leafAssignment) {
      this.localProvider = localProvider;
      this.transportService = transportService;
      this.queryId = queryId;
      this.leafStageId = leafStageId;
      this.fragmentKey = fragmentKey;
      this.indexName = indexName;
      this.leafAssignment = leafAssignment;
    }

    @Override
    public OperatorFactory createSourceFactory(LuceneTableScanNode node) {
      return localProvider.createSourceFactory(node);
    }

    @Override
    public OperatorFactory createRemoteSourceFactory(RemoteSourceNode node) {
      return new GatherExchangeFactory(
          transportService, queryId, leafStageId, fragmentKey, indexName, leafAssignment);
    }
  }

  /** OperatorFactory that creates a source operator emitting pre-collected pages. */
  private static class InMemoryPageSourceFactory implements OperatorFactory {
    private final List<Page> pages;
    private boolean closed;

    InMemoryPageSourceFactory(List<Page> pages) {
      this.pages = pages;
    }

    @Override
    public Operator createOperator(OperatorContext operatorContext) {
      if (closed) {
        throw new IllegalStateException("Factory is already closed");
      }
      return new InMemoryPageSource(operatorContext, pages);
    }

    @Override
    public void noMoreOperators() {
      closed = true;
    }
  }

  /** Source operator that emits pre-collected pages from an in-memory list. */
  private static class InMemoryPageSource implements Operator {
    private final OperatorContext operatorContext;
    private final List<Page> pages;
    private int currentIndex;
    private boolean finished;

    InMemoryPageSource(OperatorContext operatorContext, List<Page> pages) {
      this.operatorContext = operatorContext;
      this.pages = pages;
      this.currentIndex = 0;
    }

    @Override
    public ListenableFuture<?> isBlocked() {
      return NOT_BLOCKED;
    }

    @Override
    public boolean needsInput() {
      return false; // Source operator — produces output, doesn't consume input
    }

    @Override
    public void addInput(Page page) {
      throw new UnsupportedOperationException("Source operator does not accept input");
    }

    @Override
    public Page getOutput() {
      if (currentIndex < pages.size()) {
        return pages.get(currentIndex++);
      }
      finished = true;
      return null;
    }

    @Override
    public void finish() {
      finished = true;
    }

    @Override
    public boolean isFinished() {
      return finished && currentIndex >= pages.size();
    }

    @Override
    public OperatorContext getOperatorContext() {
      return operatorContext;
    }

    @Override
    public void close() {
      finished = true;
    }
  }

  /** Extracts the index name from a leaf stage's plan tree, or null if no scan node found. */
  private static String tryExtractIndexName(StageFragment fragment) {
    IndexNameExtractor extractor = new IndexNameExtractor();
    fragment.getRoot().accept(extractor, null);
    return extractor.indexName;
  }

  /** Extracts the index name from a leaf stage's plan tree. Throws if not found. */
  private static String extractIndexName(StageFragment fragment) {
    String indexName = tryExtractIndexName(fragment);
    if (indexName == null) {
      throw new IllegalStateException(
          "Leaf stage does not contain a LuceneTableScanNode: " + fragment.getRoot());
    }
    return indexName;
  }

  /** Visitor that finds the first LuceneTableScanNode and extracts its index name. */
  private static class IndexNameExtractor extends PlanNodeVisitor<Void, Void> {
    String indexName;

    @Override
    public Void visitLuceneTableScan(LuceneTableScanNode node, Void context) {
      if (indexName == null) {
        indexName = node.getIndexName();
      }
      return null;
    }

    @Override
    public Void visitPlan(PlanNode node, Void context) {
      for (PlanNode child : node.getSources()) {
        child.accept(this, context);
        if (indexName != null) {
          break;
        }
      }
      return null;
    }
  }

  /**
   * Source provider for the root stage that feeds pre-collected leaf Pages into the root pipeline
   * via an in-memory source operator. The leaf fragment is executed directly (same code path as
   * TransportShardQueryAction) and the results are passed to the root pipeline.
   */
  private static class InMemorySourceProvider
      implements PlanNodeToOperatorConverter.SourceOperatorFactoryProvider {

    private final PlanNodeToOperatorConverter.SourceOperatorFactoryProvider localProvider;
    private final List<Page> leafPages;

    InMemorySourceProvider(
        PlanNodeToOperatorConverter.SourceOperatorFactoryProvider localProvider,
        List<Page> leafPages) {
      this.localProvider = localProvider;
      this.leafPages = leafPages;
    }

    @Override
    public OperatorFactory createSourceFactory(LuceneTableScanNode node) {
      return localProvider.createSourceFactory(node);
    }

    @Override
    public OperatorFactory createRemoteSourceFactory(RemoteSourceNode node) {
      return new InMemoryPageSourceFactory(leafPages);
    }
  }

  /** OperatorFactory that creates GatherExchange operators for the root stage pipeline. */
  private static class GatherExchangeFactory implements OperatorFactory {
    private final TransportService transportService;
    private final String queryId;
    private final int stageId;
    private final byte[] serializedFragment;
    private final String indexName;
    private final NodeAssignment nodeAssignment;
    private boolean closed;

    GatherExchangeFactory(
        TransportService transportService,
        String queryId,
        int stageId,
        byte[] serializedFragment,
        String indexName,
        NodeAssignment nodeAssignment) {
      this.transportService = transportService;
      this.queryId = queryId;
      this.stageId = stageId;
      this.serializedFragment = serializedFragment;
      this.indexName = indexName;
      this.nodeAssignment = nodeAssignment;
    }

    @Override
    public Operator createOperator(OperatorContext operatorContext) {
      if (closed) {
        throw new IllegalStateException("Factory is already closed");
      }
      return new GatherExchange(
          operatorContext, transportService, queryId, stageId, serializedFragment, indexName,
          nodeAssignment);
    }

    @Override
    public void noMoreOperators() {
      closed = true;
    }
  }

  /** Factory that creates CollectingSink operators for collecting pipeline output. */
  private static class CollectingSinkFactory implements OperatorFactory {
    private boolean closed;

    @Override
    public Operator createOperator(OperatorContext operatorContext) {
      if (closed) {
        throw new IllegalStateException("Factory is already closed");
      }
      return new CollectingSink(operatorContext);
    }

    @Override
    public void noMoreOperators() {
      closed = true;
    }
  }

  /**
   * A sink operator that collects all input Pages into a list. Used as the terminal operator in the
   * pipeline to capture output for conversion to QueryResponse.
   */
  private static class CollectingSink implements SinkOperator {
    private final OperatorContext operatorContext;
    private final List<Page> collected = new ArrayList<>();
    private boolean finished;

    CollectingSink(OperatorContext operatorContext) {
      this.operatorContext = operatorContext;
    }

    @Override
    public ListenableFuture<?> isBlocked() {
      return NOT_BLOCKED;
    }

    @Override
    public boolean needsInput() {
      return !finished;
    }

    @Override
    public void addInput(Page page) {
      if (page != null && page.getPositionCount() > 0) {
        collected.add(page);
      }
    }

    @Override
    public void finish() {
      finished = true;
    }

    @Override
    public boolean isFinished() {
      return finished;
    }

    @Override
    public OperatorContext getOperatorContext() {
      return operatorContext;
    }

    @Override
    public void close() {
      finished = true;
    }

    List<Page> getCollectedPages() {
      return collected;
    }
  }
}
