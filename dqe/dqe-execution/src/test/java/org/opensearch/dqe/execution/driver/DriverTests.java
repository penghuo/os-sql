/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dqe.execution.driver;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.trino.spi.Page;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.type.BigintType;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.opensearch.dqe.execution.operator.Operator;
import org.opensearch.dqe.execution.operator.OperatorContext;
import org.opensearch.dqe.memory.QueryMemoryBudget;

class DriverTests {

  private Page createPage(int numRows) {
    BlockBuilder builder = BigintType.BIGINT.createBlockBuilder(null, numRows);
    for (int i = 0; i < numRows; i++) {
      BigintType.BIGINT.writeLong(builder, i);
    }
    return new Page(numRows, builder.build());
  }

  @Test
  @DisplayName("process pulls pages from output operator and delivers to consumer")
  void processDeliversPagesToConsumer() {
    Page page = createPage(3);
    Operator outputOp = mock(Operator.class);
    when(outputOp.getOutput()).thenReturn(page).thenReturn(null);
    when(outputOp.isFinished()).thenReturn(false).thenReturn(false).thenReturn(true);

    OperatorContext ctx = mock(OperatorContext.class);
    lenient().when(ctx.getQueryId()).thenReturn("q1");
    when(outputOp.getOperatorContext()).thenReturn(ctx);

    Pipeline pipeline = new Pipeline(List.of(outputOp));
    List<Page> collected = new ArrayList<>();
    Driver driver = new Driver(pipeline, collected::add);

    // First process: gets page, delivers to consumer, then gets null -> yields
    driver.process();
    assertEquals(1, collected.size());
    assertEquals(3, collected.get(0).getPositionCount());
  }

  @Test
  @DisplayName("process returns false when operator is finished")
  void processReturnsFalseWhenFinished() {
    Operator outputOp = mock(Operator.class);
    when(outputOp.isFinished()).thenReturn(true);

    OperatorContext ctx = mock(OperatorContext.class);
    lenient().when(ctx.getQueryId()).thenReturn("q1");
    when(outputOp.getOperatorContext()).thenReturn(ctx);

    Pipeline pipeline = new Pipeline(List.of(outputOp));
    Driver driver = new Driver(pipeline, p -> {});

    assertFalse(driver.process());
    assertTrue(driver.isFinished());
  }

  @Test
  @DisplayName("process produces multiple pages then finishes")
  void processProducesMultiplePages() {
    Operator outputOp = mock(Operator.class);
    Page page = createPage(1);
    when(outputOp.getOutput()).thenReturn(page).thenReturn(page).thenReturn(page).thenReturn(null);
    when(outputOp.isFinished())
        .thenReturn(false)
        .thenReturn(false)
        .thenReturn(false)
        .thenReturn(false)
        .thenReturn(true);

    OperatorContext ctx = mock(OperatorContext.class);
    lenient().when(ctx.getQueryId()).thenReturn("q1");
    when(outputOp.getOperatorContext()).thenReturn(ctx);

    Pipeline pipeline = new Pipeline(List.of(outputOp));
    List<Page> collected = new ArrayList<>();
    Driver driver = new Driver(pipeline, collected::add);

    // Drive until finished
    while (driver.process()) {
      // keep going
    }
    assertTrue(driver.isFinished());
    assertEquals(3, collected.size());
  }

  @Test
  @DisplayName("close closes pipeline and marks finished")
  void closeClosesPipeline() {
    Operator outputOp = mock(Operator.class);
    OperatorContext ctx = mock(OperatorContext.class);
    lenient().when(ctx.getQueryId()).thenReturn("q1");
    when(outputOp.getOperatorContext()).thenReturn(ctx);

    Pipeline pipeline = new Pipeline(List.of(outputOp));
    Driver driver = new Driver(pipeline, p -> {});

    driver.close();
    assertTrue(driver.isFinished());
    verify(outputOp).close();
  }

  @Test
  @DisplayName("getOperatorContexts returns contexts from pipeline")
  void getOperatorContexts() {
    QueryMemoryBudget budget = mock(QueryMemoryBudget.class);
    OperatorContext ctx = new OperatorContext("q1", 0, 0, 0, "Test", budget);
    Operator op = mock(Operator.class);
    when(op.getOperatorContext()).thenReturn(ctx);

    Pipeline pipeline = new Pipeline(List.of(op));
    Driver driver = new Driver(pipeline, p -> {});

    List<OperatorContext> contexts = driver.getOperatorContexts();
    assertEquals(1, contexts.size());
    assertEquals("q1", contexts.get(0).getQueryId());
  }

  @Test
  @DisplayName("getPipeline returns the pipeline")
  void getPipelineReturnsThePipeline() {
    Operator op = mock(Operator.class);
    OperatorContext ctx = mock(OperatorContext.class);
    lenient().when(ctx.getQueryId()).thenReturn("q1");
    when(op.getOperatorContext()).thenReturn(ctx);

    Pipeline pipeline = new Pipeline(List.of(op));
    Driver driver = new Driver(pipeline, p -> {});

    assertNotNull(driver.getPipeline());
  }

  @Test
  @DisplayName("already finished driver returns false on process")
  void alreadyFinishedReturnsFalse() {
    Operator op = mock(Operator.class);
    when(op.isFinished()).thenReturn(true);
    OperatorContext ctx = mock(OperatorContext.class);
    lenient().when(ctx.getQueryId()).thenReturn("q1");
    when(op.getOperatorContext()).thenReturn(ctx);

    Pipeline pipeline = new Pipeline(List.of(op));
    Driver driver = new Driver(pipeline, p -> {});

    driver.process(); // finishes
    assertFalse(driver.process()); // already finished
  }
}
