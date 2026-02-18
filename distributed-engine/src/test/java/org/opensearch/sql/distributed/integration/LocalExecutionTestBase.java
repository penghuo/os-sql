/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.distributed.integration;

import com.google.common.util.concurrent.ListenableFuture;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import org.opensearch.sql.distributed.context.DriverContext;
import org.opensearch.sql.distributed.context.OperatorContext;
import org.opensearch.sql.distributed.context.PipelineContext;
import org.opensearch.sql.distributed.context.QueryContext;
import org.opensearch.sql.distributed.context.TaskContext;
import org.opensearch.sql.distributed.data.Block;
import org.opensearch.sql.distributed.data.LongArrayBlock;
import org.opensearch.sql.distributed.data.Page;
import org.opensearch.sql.distributed.data.VariableWidthBlock;
import org.opensearch.sql.distributed.driver.Driver;
import org.opensearch.sql.distributed.driver.Pipeline;
import org.opensearch.sql.distributed.memory.MemoryPool;
import org.opensearch.sql.distributed.operator.Operator;
import org.opensearch.sql.distributed.operator.OperatorFactory;
import org.opensearch.sql.distributed.operator.SinkOperator;
import org.opensearch.sql.distributed.operator.SourceOperator;
import org.opensearch.sql.distributed.planner.LuceneTableScanNode;
import org.opensearch.sql.distributed.planner.PlanNode;
import org.opensearch.sql.distributed.planner.RemoteSourceNode;
import org.opensearch.sql.distributed.planner.ValuesNode;
import org.opensearch.sql.distributed.planner.bridge.PlanNodeToOperatorConverter;

/**
 * Base class for IC-1 local execution integration tests. Provides infrastructure to build PlanNode
 * trees, convert them to operator pipelines, run them via Driver, and collect output.
 */
abstract class LocalExecutionTestBase {

  protected static final long MEMORY_POOL_SIZE = 64 * 1024 * 1024; // 64 MB

  /** Creates a full context hierarchy for a single-driver pipeline. */
  protected QueryContext createQueryContext() {
    return createQueryContext(MEMORY_POOL_SIZE);
  }

  protected QueryContext createQueryContext(long memoryBytes) {
    MemoryPool pool = new MemoryPool("test-query", memoryBytes);
    return new QueryContext("ic1-test", pool, 30000);
  }

  protected PipelineContext createPipelineContext(QueryContext queryContext) {
    TaskContext taskContext = queryContext.addTaskContext(0);
    return taskContext.addPipelineContext(0);
  }

  /**
   * Converts a PlanNode tree to OperatorFactories and builds a Pipeline. The source operator is
   * injected via TestSourceProvider which uses a pre-built list of Pages.
   */
  protected Pipeline buildPipeline(
      PlanNode planRoot, List<Page> sourcePages, PipelineContext pipelineContext) {
    TestSourceProvider sourceProvider = new TestSourceProvider(sourcePages);
    List<OperatorFactory> factories = PlanNodeToOperatorConverter.convert(planRoot, sourceProvider);

    // Add a collecting sink at the end
    List<OperatorFactory> withSink = new ArrayList<>(factories);
    withSink.add(new CollectingSinkFactory());

    return new Pipeline(pipelineContext, withSink);
  }

  /** Runs a Driver to completion and returns all collected output pages from the sink. */
  protected List<Page> runToCompletion(Driver driver) {
    int iterations = 0;
    int maxIterations = 10_000;
    while (!driver.isFinished() && iterations < maxIterations) {
      ListenableFuture<?> blocked = driver.process();
      if (!blocked.isDone()) {
        throw new IllegalStateException("Driver blocked unexpectedly in local execution test");
      }
      iterations++;
    }
    if (!driver.isFinished()) {
      throw new IllegalStateException(
          "Driver did not finish within " + maxIterations + " iterations");
    }

    // Extract collected pages from the sink operator (last in the operator list)
    Operator sinkOp = driver.getOperators().get(driver.getOperators().size() - 1);
    if (sinkOp instanceof CollectingSink collectingSink) {
      return collectingSink.getCollectedPages();
    }
    throw new IllegalStateException("Last operator is not a CollectingSink");
  }

  /** End-to-end helper: builds PlanNode → Pipeline → Driver → runs → returns output pages. */
  protected List<Page> execute(PlanNode planRoot, List<Page> sourcePages) {
    QueryContext queryContext = createQueryContext();
    PipelineContext pipelineContext = createPipelineContext(queryContext);
    Pipeline pipeline = buildPipeline(planRoot, sourcePages, pipelineContext);
    DriverContext driverContext = pipelineContext.addDriverContext();
    Driver driver = pipeline.createDriver(driverContext);
    return runToCompletion(driver);
  }

  // --- Test data builders ---

  /** Creates a Page with a single long column. */
  protected static Page longPage(long... values) {
    return new Page(new LongArrayBlock(values.length, Optional.empty(), values));
  }

  /** Creates a Page with two columns: a long column and a string column. */
  protected static Page longAndStringPage(long[] longs, String[] strings) {
    LongArrayBlock longBlock = new LongArrayBlock(longs.length, Optional.empty(), longs);
    VariableWidthBlock stringBlock = buildStringBlock(strings);
    return new Page(longs.length, longBlock, stringBlock);
  }

  /** Creates a Page with three columns: string (name), long (age), string (status). */
  protected static Page nameAgeStatusPage(String[] names, long[] ages, String[] statuses) {
    VariableWidthBlock nameBlock = buildStringBlock(names);
    LongArrayBlock ageBlock = new LongArrayBlock(ages.length, Optional.empty(), ages);
    VariableWidthBlock statusBlock = buildStringBlock(statuses);
    return new Page(ages.length, nameBlock, ageBlock, statusBlock);
  }

  protected static VariableWidthBlock buildStringBlock(String[] values) {
    byte[][] encoded = new byte[values.length][];
    int totalLen = 0;
    for (int i = 0; i < values.length; i++) {
      encoded[i] = values[i].getBytes(java.nio.charset.StandardCharsets.UTF_8);
      totalLen += encoded[i].length;
    }
    byte[] slice = new byte[totalLen];
    int[] offsets = new int[values.length + 1];
    int pos = 0;
    for (int i = 0; i < values.length; i++) {
      System.arraycopy(encoded[i], 0, slice, pos, encoded[i].length);
      pos += encoded[i].length;
      offsets[i + 1] = pos;
    }
    return new VariableWidthBlock(values.length, slice, offsets, Optional.empty());
  }

  /** Extracts all long values from a specific channel across multiple output pages. */
  protected static List<Long> collectLongs(List<Page> pages, int channel) {
    List<Long> result = new ArrayList<>();
    for (Page page : pages) {
      Block block = page.getBlock(channel);
      if (block instanceof LongArrayBlock longBlock) {
        for (int i = 0; i < longBlock.getPositionCount(); i++) {
          result.add(longBlock.getLong(i));
        }
      }
    }
    return result;
  }

  /** Extracts all string values from a specific channel across multiple output pages. */
  protected static List<String> collectStrings(List<Page> pages, int channel) {
    List<String> result = new ArrayList<>();
    for (Page page : pages) {
      Block block = page.getBlock(channel);
      if (block instanceof VariableWidthBlock varBlock) {
        for (int i = 0; i < varBlock.getPositionCount(); i++) {
          byte[] bytes = varBlock.getSlice(i);
          result.add(new String(bytes, java.nio.charset.StandardCharsets.UTF_8));
        }
      }
    }
    return result;
  }

  /** Returns total row count across all pages. */
  protected static int totalPositions(List<Page> pages) {
    int total = 0;
    for (Page page : pages) {
      total += page.getPositionCount();
    }
    return total;
  }

  // --- Source provider for tests ---

  /** Provides a fixed set of Pages as the source for a LuceneTableScanNode. */
  static class TestSourceProvider
      implements PlanNodeToOperatorConverter.SourceOperatorFactoryProvider {
    private final List<Page> pages;

    TestSourceProvider(List<Page> pages) {
      this.pages = pages;
    }

    @Override
    public OperatorFactory createSourceFactory(LuceneTableScanNode node) {
      return new TestSourceFactory(pages);
    }

    @Override
    public OperatorFactory createRemoteSourceFactory(RemoteSourceNode node) {
      throw new UnsupportedOperationException("RemoteSource not used in IC-1 local tests");
    }

    @Override
    public OperatorFactory createValuesFactory(ValuesNode node) {
      throw new UnsupportedOperationException("Values not used in IC-1 local tests");
    }
  }

  static class TestSourceFactory implements OperatorFactory {
    private final List<Page> pages;
    private boolean closed;

    TestSourceFactory(List<Page> pages) {
      this.pages = pages;
    }

    @Override
    public Operator createOperator(OperatorContext operatorContext) {
      if (closed) {
        throw new IllegalStateException("Factory closed");
      }
      return new TestSourceOperator(operatorContext, pages);
    }

    @Override
    public void noMoreOperators() {
      closed = true;
    }
  }

  static class TestSourceOperator implements SourceOperator {
    private final OperatorContext operatorContext;
    private final List<Page> pages;
    private int index;
    private boolean finished;

    TestSourceOperator(OperatorContext operatorContext, List<Page> pages) {
      this.operatorContext = operatorContext;
      this.pages = new ArrayList<>(pages);
    }

    @Override
    public ListenableFuture<?> isBlocked() {
      return NOT_BLOCKED;
    }

    @Override
    public Page getOutput() {
      if (index < pages.size()) {
        return pages.get(index++);
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
      return finished || index >= pages.size();
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

  // --- Collecting sink ---

  static class CollectingSinkFactory implements OperatorFactory {
    private boolean closed;

    @Override
    public Operator createOperator(OperatorContext operatorContext) {
      if (closed) {
        throw new IllegalStateException("Factory closed");
      }
      return new CollectingSink(operatorContext);
    }

    @Override
    public void noMoreOperators() {
      closed = true;
    }
  }

  static class CollectingSink implements SinkOperator {
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
      collected.add(page);
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
