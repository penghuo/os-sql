/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dqe.exchange.gather;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.trino.spi.Page;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.type.BigintType;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.dqe.execution.operator.OperatorContext;
import org.opensearch.dqe.execution.task.DqeQueryCancelledException;
import org.opensearch.dqe.memory.QueryMemoryBudget;
import org.opensearch.dqe.parser.DqeException;

@ExtendWith(MockitoExtension.class)
class ExchangeSourceOperatorTests {

  @Mock private GatherExchangeSource source;
  @Mock private QueryMemoryBudget memoryBudget;
  private OperatorContext operatorContext;
  private ExchangeSourceOperator operator;

  @BeforeEach
  void setUp() {
    operatorContext = new OperatorContext("q1", 0, 0, 0, "ExchangeSource", memoryBudget);
    operator = new ExchangeSourceOperator(operatorContext, source);
  }

  private Page createPage(int rows) {
    BlockBuilder builder = BigintType.BIGINT.createBlockBuilder(null, rows);
    for (int i = 0; i < rows; i++) {
      BigintType.BIGINT.writeLong(builder, i);
    }
    return new Page(builder.build());
  }

  @Test
  @DisplayName("getOutput returns pages from exchange source")
  void getOutputReturnsPages() throws InterruptedException {
    Page expected = createPage(5);
    when(source.getNextPage()).thenReturn(expected);

    Page result = operator.getOutput();
    assertNotNull(result);
    assertEquals(5, result.getPositionCount());
  }

  @Test
  @DisplayName("getOutput returns null and finishes when source returns null")
  void getOutputFinishesOnNull() throws InterruptedException {
    when(source.getNextPage()).thenReturn(null);

    Page result = operator.getOutput();
    assertNull(result);
    assertTrue(operator.isFinished());
  }

  @Test
  @DisplayName("isFinished delegates to source when not locally finished")
  void isFinishedDelegatesToSource() {
    when(source.isFinished()).thenReturn(false);
    assertFalse(operator.isFinished());

    when(source.isFinished()).thenReturn(true);
    assertTrue(operator.isFinished());
  }

  @Test
  @DisplayName("getOutput wraps InterruptedException in DqeException")
  void getOutputWrapsInterruptedException() throws InterruptedException {
    when(source.getNextPage()).thenThrow(new InterruptedException("test interrupt"));

    DqeException ex = assertThrows(DqeException.class, () -> operator.getOutput());
    assertTrue(ex.getMessage().contains("interrupted"));
    assertTrue(Thread.currentThread().isInterrupted());
    // Clear interrupt flag
    Thread.interrupted();
  }

  @Test
  @DisplayName("close delegates to source")
  void closeDelegatesToSource() {
    operator.close();
    verify(source).close();
  }

  @Test
  @DisplayName("checkInterrupted throws on cancelled query")
  void checkInterruptedThrows() {
    operatorContext.setInterrupted(true);
    assertThrows(DqeQueryCancelledException.class, () -> operator.getOutput());
  }

  @Test
  @DisplayName("finish makes operator finished")
  void finishMakesFinished() {
    operator.finish();
    assertTrue(operator.isFinished());
    assertNull(operator.getOutput());
  }

  @Test
  @DisplayName("getOperatorContext returns context")
  void getOperatorContextReturnsContext() {
    assertEquals(operatorContext, operator.getOperatorContext());
  }

  @Test
  @DisplayName("tracks input and output positions")
  void tracksPositions() throws InterruptedException {
    Page page = createPage(10);
    when(source.getNextPage()).thenReturn(page);

    operator.getOutput();

    assertEquals(10, operatorContext.getInputPositions());
    assertEquals(10, operatorContext.getOutputPositions());
  }
}
