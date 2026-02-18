/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.distributed.driver;

import static org.junit.jupiter.api.Assertions.*;

import com.google.common.util.concurrent.ListenableFuture;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.opensearch.sql.distributed.context.OperatorContext;
import org.opensearch.sql.distributed.context.PipelineContext;
import org.opensearch.sql.distributed.context.QueryContext;
import org.opensearch.sql.distributed.context.TaskContext;
import org.opensearch.sql.distributed.data.LongArrayBlock;
import org.opensearch.sql.distributed.data.Page;
import org.opensearch.sql.distributed.memory.MemoryPool;
import org.opensearch.sql.distributed.operator.Operator;
import org.opensearch.sql.distributed.operator.OperatorFactory;
import org.opensearch.sql.distributed.operator.SinkOperator;
import org.opensearch.sql.distributed.operator.SourceOperator;

class PipelineTest {

  private PipelineContext createPipelineContext() {
    MemoryPool pool = new MemoryPool("test", 1024 * 1024);
    QueryContext qc = new QueryContext("q1", pool, 30000);
    TaskContext tc = qc.addTaskContext(0);
    return tc.addPipelineContext(0);
  }

  @Test
  @DisplayName("Pipeline creates correct number of Drivers")
  void testCreateDrivers() {
    PipelineContext pipelineCtx = createPipelineContext();
    List<OperatorFactory> factories = List.of(new TestSourceFactory(), new TestSinkFactory());
    Pipeline pipeline = new Pipeline(pipelineCtx, factories);

    List<Driver> drivers = pipeline.createDrivers(3);

    assertEquals(3, drivers.size());
    assertEquals(3, pipelineCtx.getDriverContexts().size());
  }

  @Test
  @DisplayName("Pipeline lifecycle: create, process, finish")
  void testPipelineLifecycle() {
    PipelineContext pipelineCtx = createPipelineContext();
    Page page = new Page(new LongArrayBlock(2, Optional.empty(), new long[] {1, 2}));

    List<OperatorFactory> factories = List.of(new TestSourceFactory(page), new TestSinkFactory());
    Pipeline pipeline = new Pipeline(pipelineCtx, factories);

    List<Driver> drivers = pipeline.createDrivers(1);
    assertEquals(1, drivers.size());

    Driver driver = drivers.get(0);
    int processCount = 0;
    while (!driver.isFinished() && processCount < 100) {
      driver.process();
      processCount++;
    }
    assertTrue(driver.isFinished());
  }

  @Test
  @DisplayName("Pipeline with N Drivers (parallel shard processing)")
  void testParallelDrivers() {
    PipelineContext pipelineCtx = createPipelineContext();
    int driverCount = 5;

    List<OperatorFactory> factories = List.of(new TestSourceFactory(), new TestSinkFactory());
    Pipeline pipeline = new Pipeline(pipelineCtx, factories);
    List<Driver> drivers = pipeline.createDrivers(driverCount);

    assertEquals(driverCount, drivers.size());
    // All drivers should start not finished
    for (Driver driver : drivers) {
      assertFalse(driver.isFinished());
    }
  }

  @Test
  @DisplayName("Pipeline requires at least 2 operator factories")
  void testMinimumFactories() {
    PipelineContext pipelineCtx = createPipelineContext();
    assertThrows(
        IllegalArgumentException.class,
        () -> new Pipeline(pipelineCtx, List.of(new TestSourceFactory())));
  }

  // Factory helpers
  static class TestSourceFactory implements OperatorFactory {
    private final Page page;

    TestSourceFactory() {
      this(null);
    }

    TestSourceFactory(Page page) {
      this.page = page;
    }

    @Override
    public Operator createOperator(OperatorContext operatorContext) {
      return new SimpleSource(page);
    }

    @Override
    public void noMoreOperators() {}
  }

  static class TestSinkFactory implements OperatorFactory {
    @Override
    public Operator createOperator(OperatorContext operatorContext) {
      return new SimpleSink();
    }

    @Override
    public void noMoreOperators() {}
  }

  static class SimpleSource implements SourceOperator {
    private Page page;
    private boolean finished;
    private final OperatorContext ctx = new OperatorContext(0, "SimpleSource");

    SimpleSource(Page page) {
      this.page = page;
      if (page == null) finished = true;
    }

    @Override
    public ListenableFuture<?> isBlocked() {
      return NOT_BLOCKED;
    }

    @Override
    public Page getOutput() {
      Page out = page;
      page = null;
      return out;
    }

    @Override
    public void finish() {
      finished = true;
    }

    @Override
    public boolean isFinished() {
      return finished || page == null;
    }

    @Override
    public OperatorContext getOperatorContext() {
      return ctx;
    }

    @Override
    public void close() {}
  }

  static class SimpleSink implements SinkOperator {
    private final List<Page> received = new ArrayList<>();
    private boolean finished;
    private final OperatorContext ctx = new OperatorContext(1, "SimpleSink");

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
      received.add(page);
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
      return ctx;
    }

    @Override
    public void close() {}
  }
}
