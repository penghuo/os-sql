/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dqe.execution.operator;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.mock;
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
import org.opensearch.dqe.execution.expression.ExpressionEvaluator;
import org.opensearch.dqe.memory.QueryMemoryBudget;

@ExtendWith(MockitoExtension.class)
class FilterOperatorTests {

  @Mock private QueryMemoryBudget memoryBudget;
  @Mock private ExpressionEvaluator filterPredicate;
  private OperatorContext operatorContext;

  @BeforeEach
  void setUp() {
    operatorContext = new OperatorContext("q1", 0, 0, 0, "FilterOperator", memoryBudget);
  }

  private Page createPage(long... values) {
    BlockBuilder builder = BigintType.BIGINT.createBlockBuilder(null, values.length);
    for (long v : values) {
      BigintType.BIGINT.writeLong(builder, v);
    }
    return new Page(values.length, builder.build());
  }

  @Test
  @DisplayName("filters rows where predicate returns false")
  void filtersRowsOnFalse() {
    Page input = createPage(10, 20, 30, 40, 50);
    Operator source = mock(Operator.class);
    when(source.getOutput()).thenReturn(input).thenReturn(null);
    lenient().when(source.isFinished()).thenReturn(false).thenReturn(true);

    // Only keep rows with value > 25 (30, 40, 50)
    when(filterPredicate.evaluate(any(Page.class), anyInt()))
        .thenAnswer(
            inv -> {
              Page page = inv.getArgument(0);
              int pos = inv.getArgument(1);
              long val = page.getBlock(0).getLong(pos, 0);
              return val > 25;
            });

    FilterOperator filter = new FilterOperator(operatorContext, source, filterPredicate);
    Page result = filter.getOutput();

    assertEquals(3, result.getPositionCount());
    assertEquals(30L, result.getBlock(0).getLong(0, 0));
    assertEquals(40L, result.getBlock(0).getLong(1, 0));
    assertEquals(50L, result.getBlock(0).getLong(2, 0));
  }

  @Test
  @DisplayName("returns original page when all rows pass")
  void returnsOriginalWhenAllPass() {
    Page input = createPage(10, 20, 30);
    Operator source = mock(Operator.class);
    when(source.getOutput()).thenReturn(input).thenReturn(null);
    lenient().when(source.isFinished()).thenReturn(false).thenReturn(true);

    when(filterPredicate.evaluate(any(Page.class), anyInt())).thenReturn(true);

    FilterOperator filter = new FilterOperator(operatorContext, source, filterPredicate);
    Page result = filter.getOutput();

    // Should return the same page reference when all pass
    assertEquals(3, result.getPositionCount());
  }

  @Test
  @DisplayName("returns null when no rows pass")
  void returnsNullWhenNonePass() {
    Page input = createPage(10, 20, 30);
    Operator source = mock(Operator.class);
    when(source.getOutput()).thenReturn(input).thenReturn(null);
    lenient().when(source.isFinished()).thenReturn(false).thenReturn(true);

    when(filterPredicate.evaluate(any(Page.class), anyInt())).thenReturn(false);

    FilterOperator filter = new FilterOperator(operatorContext, source, filterPredicate);
    Page result = filter.getOutput();
    assertNull(result); // no rows passed
  }

  @Test
  @DisplayName("finished when source is finished and no more output")
  void finishedWhenSourceFinished() {
    Operator source = mock(Operator.class);
    when(source.getOutput()).thenReturn(null);
    when(source.isFinished()).thenReturn(true);

    FilterOperator filter = new FilterOperator(operatorContext, source, filterPredicate);
    assertNull(filter.getOutput());
    assertTrue(filter.isFinished());
  }

  @Test
  @DisplayName("null predicate result treated as false")
  void nullPredicateResultTreatedAsFalse() {
    Page input = createPage(10, 20);
    Operator source = mock(Operator.class);
    when(source.getOutput()).thenReturn(input).thenReturn(null);
    lenient().when(source.isFinished()).thenReturn(false).thenReturn(true);

    when(filterPredicate.evaluate(any(Page.class), anyInt())).thenReturn(null);

    FilterOperator filter = new FilterOperator(operatorContext, source, filterPredicate);
    Page result = filter.getOutput();
    assertNull(result);
  }

  @Test
  @DisplayName("tracks input and output positions")
  void tracksPositions() {
    Page input = createPage(10, 20, 30, 40);
    Operator source = mock(Operator.class);
    when(source.getOutput()).thenReturn(input).thenReturn(null);
    lenient().when(source.isFinished()).thenReturn(false).thenReturn(true);

    // Only pass even-positioned rows (10, 30)
    when(filterPredicate.evaluate(any(Page.class), anyInt())).thenReturn(true, false, true, false);

    FilterOperator filter = new FilterOperator(operatorContext, source, filterPredicate);
    filter.getOutput();

    assertEquals(4, operatorContext.getInputPositions());
    assertEquals(2, operatorContext.getOutputPositions());
  }
}
