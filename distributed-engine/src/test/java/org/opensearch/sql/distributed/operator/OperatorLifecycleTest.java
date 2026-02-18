/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.distributed.operator;

import static org.junit.jupiter.api.Assertions.*;

import com.google.common.util.concurrent.ListenableFuture;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.opensearch.sql.distributed.context.OperatorContext;
import org.opensearch.sql.distributed.data.LongArrayBlock;
import org.opensearch.sql.distributed.data.Page;

class OperatorLifecycleTest {

  @Test
  @DisplayName("Operator finish()/isFinished() contract")
  void testFinishContract() throws Exception {
    // A simple pass-through operator that buffers input and returns it on getOutput()
    TestPassThroughOperator op = new TestPassThroughOperator();

    assertFalse(op.isFinished());
    assertTrue(op.needsInput());

    // Add some input
    Page page = new Page(new LongArrayBlock(3, Optional.empty(), new long[] {1, 2, 3}));
    op.addInput(page);

    // Get output
    Page output = op.getOutput();
    assertNotNull(output);
    assertEquals(3, output.getPositionCount());

    // Finish
    op.finish();
    // After finish + last getOutput, isFinished should be true
    assertNull(op.getOutput()); // no more output
    assertTrue(op.isFinished());

    op.close();
  }

  @Test
  @DisplayName("Operator isBlocked() returns resolved future when not blocked")
  void testNotBlocked() {
    TestPassThroughOperator op = new TestPassThroughOperator();
    ListenableFuture<?> blocked = op.isBlocked();
    assertTrue(blocked.isDone());
  }

  @Test
  @DisplayName("SourceOperator rejects addInput")
  void testSourceOperatorRejectsInput() {
    TestSourceOperator source = new TestSourceOperator();
    assertFalse(source.needsInput());
    assertThrows(
        UnsupportedOperationException.class,
        () -> source.addInput(new Page(new LongArrayBlock(1, Optional.empty(), new long[] {1}))));
  }

  @Test
  @DisplayName("Operator needsInput contract: cannot add when needsInput is false")
  void testNeedsInputContract() {
    TestPassThroughOperator op = new TestPassThroughOperator();
    Page page = new Page(new LongArrayBlock(1, Optional.empty(), new long[] {1}));

    assertTrue(op.needsInput());
    op.addInput(page);
    // After adding input, needsInput should be false until getOutput is called
    assertFalse(op.needsInput());
    // Get output to make it accept input again
    assertNotNull(op.getOutput());
    assertTrue(op.needsInput());
  }

  // Test helper: a simple pass-through operator
  static class TestPassThroughOperator implements Operator {
    private Page buffered;
    private boolean finished;
    private final OperatorContext ctx = new OperatorContext(0, "TestPassThrough");

    @Override
    public ListenableFuture<?> isBlocked() {
      return NOT_BLOCKED;
    }

    @Override
    public boolean needsInput() {
      return !finished && buffered == null;
    }

    @Override
    public void addInput(Page page) {
      if (!needsInput()) throw new IllegalStateException("Cannot addInput when needsInput=false");
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

  // Test helper: a source operator that produces a fixed set of pages
  static class TestSourceOperator implements SourceOperator {
    private final List<Page> pages = new ArrayList<>();
    private int index;
    private boolean finished;
    private final OperatorContext ctx = new OperatorContext(0, "TestSource");

    TestSourceOperator(Page... initialPages) {
      pages.addAll(List.of(initialPages));
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
      return null;
    }

    @Override
    public void finish() {
      finished = true;
    }

    @Override
    public boolean isFinished() {
      return finished && index >= pages.size();
    }

    @Override
    public OperatorContext getOperatorContext() {
      return ctx;
    }

    @Override
    public void close() {}
  }
}
