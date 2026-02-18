/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.distributed;

import com.google.common.util.concurrent.ListenableFuture;
import java.util.ArrayList;
import java.util.List;
import org.apache.calcite.rel.RelNode;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
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
import org.opensearch.sql.distributed.memory.MemoryPool;
import org.opensearch.sql.distributed.operator.Operator;
import org.opensearch.sql.distributed.operator.OperatorFactory;
import org.opensearch.sql.distributed.operator.SinkOperator;
import org.opensearch.sql.distributed.planner.PlanNode;
import org.opensearch.sql.distributed.planner.RelNodeToPlanNodeConverter;
import org.opensearch.sql.distributed.planner.bridge.PlanNodeToOperatorConverter;
import org.opensearch.sql.distributed.planner.plan.AddExchanges;
import org.opensearch.sql.distributed.planner.plan.PlanFragmenter;
import org.opensearch.sql.distributed.planner.plan.StageFragment;
import org.opensearch.sql.executor.DistributedQueryExecutor;
import org.opensearch.sql.executor.ExecutionEngine;
import org.opensearch.sql.executor.ExecutionEngine.DistributedExplainInfo;

/**
 * Implementation of {@link DistributedQueryExecutor} that bridges the Calcite planner with the
 * distributed execution engine. Converts a Calcite RelNode into a PlanNode tree, builds an operator
 * pipeline, runs a cooperative Driver loop to completion, and converts the columnar Page output
 * into the standard QueryResponse format.
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

      // Step 2: Convert PlanNode to OperatorFactories
      List<OperatorFactory> factories =
          PlanNodeToOperatorConverter.convert(planNode, sourceProvider);

      // Step 3: Append a collecting sink to capture output pages
      List<OperatorFactory> withSink = new ArrayList<>(factories);
      withSink.add(new CollectingSinkFactory());

      // Step 4: Build context hierarchy and pipeline
      QueryContext queryContext = createQueryContext();
      try {
        TaskContext taskContext = queryContext.addTaskContext(0);
        PipelineContext pipelineContext = taskContext.addPipelineContext(0);
        Pipeline pipeline = new Pipeline(pipelineContext, withSink);

        // Step 5: Create and run the Driver
        DriverContext driverContext = pipelineContext.addDriverContext();
        Driver driver = pipeline.createDriver(driverContext);
        try {
          List<Page> outputPages = runDriverToCompletion(driver);

          // Step 6: Convert Pages to QueryResponse
          ExecutionEngine.QueryResponse response =
              PageToResponseConverter.convert(outputPages, plan.getRowType());
          listener.onResponse(response);
        } finally {
          driver.close();
        }
      } finally {
        queryContext.close();
      }
    } catch (UnsupportedOperationException e) {
      // Let UnsupportedOperationException (including UnsupportedPatternException) propagate
      // for DSL fallback in QueryService
      throw e;
    } catch (Exception e) {
      listener.onFailure(e);
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
   * call to process() moves data between operators for one quantum.
   */
  private List<Page> runDriverToCompletion(Driver driver) {
    int iterations = 0;
    while (!driver.isFinished() && iterations < MAX_DRIVER_ITERATIONS) {
      ListenableFuture<?> blocked = driver.process();
      if (!blocked.isDone()) {
        // In single-node local execution, blocking should not occur.
        // If it does, it likely indicates a memory pressure situation.
        throw new IllegalStateException("Driver blocked unexpectedly during distributed execution");
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
