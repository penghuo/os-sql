/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.distributed.driver;

import static org.junit.jupiter.api.Assertions.*;

import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.opensearch.sql.distributed.context.DriverContext;
import org.opensearch.sql.distributed.context.OperatorContext;
import org.opensearch.sql.distributed.context.PipelineContext;
import org.opensearch.sql.distributed.context.QueryContext;
import org.opensearch.sql.distributed.context.TaskContext;
import org.opensearch.sql.distributed.data.LongArrayBlock;
import org.opensearch.sql.distributed.data.Page;
import org.opensearch.sql.distributed.memory.MemoryPool;
import org.opensearch.sql.distributed.operator.Operator;
import org.opensearch.sql.distributed.operator.SinkOperator;
import org.opensearch.sql.distributed.operator.SourceOperator;

class DriverTest {

  private DriverContext createDriverContext() {
    MemoryPool pool = new MemoryPool("test", 1024 * 1024);
    QueryContext qc = new QueryContext("q1", pool, 30000);
    TaskContext tc = qc.addTaskContext(0);
    PipelineContext pc = tc.addPipelineContext(0);
    return pc.addDriverContext();
  }

  @Test
  @DisplayName("Driver processes Pages from source to sink")
  void testBasicPageFlow() {
    Page page1 = new Page(new LongArrayBlock(3, Optional.empty(), new long[] {1, 2, 3}));
    Page page2 = new Page(new LongArrayBlock(2, Optional.empty(), new long[] {4, 5}));

    TestSource source = new TestSource(List.of(page1, page2));
    TestSink sink = new TestSink();

    Driver driver =
        new Driver(createDriverContext(), List.of(source, sink), new DriverYieldSignal());

    // Process until finished
    while (!driver.isFinished()) {
      driver.process();
    }

    assertEquals(2, sink.getReceivedPages().size());
    assertEquals(3, sink.getReceivedPages().get(0).getPositionCount());
    assertEquals(2, sink.getReceivedPages().get(1).getPositionCount());
  }

  @Test
  @DisplayName("Driver yields after processing quantum")
  void testDriverYields() {
    List<Page> pages = new ArrayList<>();
    for (int i = 0; i < 100; i++) {
      pages.add(new Page(new LongArrayBlock(1, Optional.empty(), new long[] {i})));
    }

    TestSource source = new TestSource(pages);
    TestSink sink = new TestSink();
    DriverYieldSignal yieldSignal = new DriverYieldSignal();
    Driver driver = new Driver(createDriverContext(), List.of(source, sink), yieldSignal);

    // Process once and check it eventually finishes
    int processCount = 0;
    while (!driver.isFinished() && processCount < 1000) {
      driver.process();
      processCount++;
    }

    assertTrue(driver.isFinished());
    assertEquals(100, sink.getReceivedPages().size());
  }

  @Test
  @DisplayName("Driver with blocked operator yields")
  void testDriverWithBlockedOperator() {
    Page page = new Page(new LongArrayBlock(1, Optional.empty(), new long[] {1}));
    TestSource source = new TestSource(List.of(page));
    BlockableOperator blocked = new BlockableOperator();
    TestSink sink = new TestSink();

    Driver driver =
        new Driver(createDriverContext(), List.of(source, blocked, sink), new DriverYieldSignal());

    // First process - blocked operator will cause yield
    ListenableFuture<?> future = driver.process();
    assertFalse(future.isDone());

    // Unblock
    blocked.unblock();
    // Continue processing
    while (!driver.isFinished()) {
      driver.process();
    }

    assertTrue(driver.isFinished());
  }

  @Test
  @DisplayName("Driver finishes and calls finish on all operators")
  void testDriverFinishPropagation() {
    TestSource source = new TestSource(List.of()); // empty source
    TestSink sink = new TestSink();

    Driver driver =
        new Driver(createDriverContext(), List.of(source, sink), new DriverYieldSignal());
    source.markFinished();

    while (!driver.isFinished()) {
      driver.process();
    }

    assertTrue(driver.isFinished());
    assertTrue(source.isFinished());
    assertTrue(sink.isFinished());
  }

  @Test
  @DisplayName("Driver requires at least 2 operators")
  void testMinimumOperators() {
    TestSource source = new TestSource(List.of());
    assertThrows(
        IllegalArgumentException.class,
        () -> new Driver(createDriverContext(), List.of(source), new DriverYieldSignal()));
  }

  @Test
  @DisplayName("Driver close cleans up all operators")
  void testDriverClose() {
    TestSource source = new TestSource(List.of());
    TestSink sink = new TestSink();
    DriverContext driverCtx = createDriverContext();

    Driver driver = new Driver(driverCtx, List.of(source, sink), new DriverYieldSignal());
    driver.close();

    assertTrue(driver.isFinished());
    assertTrue(source.isClosed());
    assertTrue(sink.isClosed());
    assertTrue(driverCtx.isClosed());
  }

  // Test helpers
  static class TestSource implements SourceOperator {
    private final List<Page> pages;
    private int index;
    private boolean finished;
    private boolean closed;
    private final OperatorContext ctx = new OperatorContext(0, "TestSource");

    TestSource(List<Page> pages) {
      this.pages = new ArrayList<>(pages);
    }

    void markFinished() {
      finished = true;
    }

    @Override
    public ListenableFuture<?> isBlocked() {
      return NOT_BLOCKED;
    }

    @Override
    public Page getOutput() {
      if (index < pages.size()) return pages.get(index++);
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
      return ctx;
    }

    boolean isClosed() {
      return closed;
    }

    @Override
    public void close() {
      closed = true;
    }
  }

  static class TestSink implements SinkOperator {
    private final List<Page> received = new ArrayList<>();
    private boolean finished;
    private boolean closed;
    private final OperatorContext ctx = new OperatorContext(1, "TestSink");

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

    List<Page> getReceivedPages() {
      return received;
    }

    boolean isClosed() {
      return closed;
    }

    @Override
    public void close() {
      closed = true;
    }
  }

  static class BlockableOperator implements Operator {
    private SettableFuture<Void> blockedFuture = SettableFuture.create();
    private Page buffered;
    private boolean finished;
    private final OperatorContext ctx = new OperatorContext(1, "Blockable");

    void unblock() {
      blockedFuture.set(null);
    }

    @Override
    public ListenableFuture<?> isBlocked() {
      return blockedFuture;
    }

    @Override
    public boolean needsInput() {
      return !finished && buffered == null;
    }

    @Override
    public void addInput(Page page) {
      buffered = page;
    }

    @Override
    public Page getOutput() {
      Page out = buffered;
      buffered = null;
      return out;
    }

    @Override
    public void finish() {
      finished = true;
    }

    @Override
    public boolean isFinished() {
      return finished && buffered == null;
    }

    @Override
    public OperatorContext getOperatorContext() {
      return ctx;
    }

    @Override
    public void close() {}
  }
}
