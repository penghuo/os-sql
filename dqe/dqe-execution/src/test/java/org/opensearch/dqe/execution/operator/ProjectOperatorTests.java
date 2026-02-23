/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dqe.execution.operator;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.trino.spi.Page;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.type.BigintType;
import io.trino.spi.type.VarcharType;
import java.util.List;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.dqe.execution.expression.ExpressionEvaluator;
import org.opensearch.dqe.memory.QueryMemoryBudget;

@ExtendWith(MockitoExtension.class)
class ProjectOperatorTests {

  @Mock private QueryMemoryBudget memoryBudget;
  private OperatorContext operatorContext;

  @BeforeEach
  void setUp() {
    operatorContext = new OperatorContext("q1", 0, 0, 0, "ProjectOperator", memoryBudget);
  }

  private Page createTwoColumnPage() {
    BlockBuilder col1 = BigintType.BIGINT.createBlockBuilder(null, 2);
    BigintType.BIGINT.writeLong(col1, 10L);
    BigintType.BIGINT.writeLong(col1, 20L);

    BlockBuilder col2 = VarcharType.VARCHAR.createBlockBuilder(null, 2);
    VarcharType.VARCHAR.writeSlice(col2, io.airlift.slice.Slices.utf8Slice("a"));
    VarcharType.VARCHAR.writeSlice(col2, io.airlift.slice.Slices.utf8Slice("b"));

    return new Page(2, col1.build(), col2.build());
  }

  @Test
  @DisplayName("projects to a single column")
  void projectsSingleColumn() {
    Page input = createTwoColumnPage();

    // Build a block that would be output of the first column
    BlockBuilder outBuilder = BigintType.BIGINT.createBlockBuilder(null, 2);
    BigintType.BIGINT.writeLong(outBuilder, 10L);
    BigintType.BIGINT.writeLong(outBuilder, 20L);
    Block outBlock = outBuilder.build();

    ExpressionEvaluator eval = mock(ExpressionEvaluator.class);
    when(eval.evaluateAll(any(Page.class))).thenReturn(outBlock);

    Operator source = mock(Operator.class);
    when(source.getOutput()).thenReturn(input).thenReturn(null);
    lenient().when(source.isFinished()).thenReturn(false).thenReturn(true);

    ProjectOperator project = new ProjectOperator(operatorContext, source, List.of(eval));
    Page result = project.getOutput();

    assertEquals(1, result.getChannelCount()); // projected to 1 column
    assertEquals(2, result.getPositionCount());
    assertEquals(10L, result.getBlock(0).getLong(0, 0));
  }

  @Test
  @DisplayName("finished when source finished")
  void finishedWhenSourceFinished() {
    Operator source = mock(Operator.class);
    when(source.getOutput()).thenReturn(null);
    when(source.isFinished()).thenReturn(true);

    ExpressionEvaluator eval = mock(ExpressionEvaluator.class);
    ProjectOperator project = new ProjectOperator(operatorContext, source, List.of(eval));

    assertNull(project.getOutput());
    assertTrue(project.isFinished());
  }

  @Test
  @DisplayName("returns null when source returns null but not finished")
  void returnsNullWhenSourceNotReady() {
    Operator source = mock(Operator.class);
    when(source.getOutput()).thenReturn(null);
    when(source.isFinished()).thenReturn(false);

    ExpressionEvaluator eval = mock(ExpressionEvaluator.class);
    ProjectOperator project = new ProjectOperator(operatorContext, source, List.of(eval));

    assertNull(project.getOutput());
    // Not finished yet since source isn't
    assertTrue(!project.isFinished());
  }

  @Test
  @DisplayName("tracks input and output positions")
  void tracksPositions() {
    Page input = createTwoColumnPage();

    BlockBuilder outBuilder = BigintType.BIGINT.createBlockBuilder(null, 2);
    BigintType.BIGINT.writeLong(outBuilder, 1L);
    BigintType.BIGINT.writeLong(outBuilder, 2L);
    Block outBlock = outBuilder.build();

    ExpressionEvaluator eval = mock(ExpressionEvaluator.class);
    when(eval.evaluateAll(any(Page.class))).thenReturn(outBlock);

    Operator source = mock(Operator.class);
    when(source.getOutput()).thenReturn(input).thenReturn(null);
    lenient().when(source.isFinished()).thenReturn(false).thenReturn(true);

    ProjectOperator project = new ProjectOperator(operatorContext, source, List.of(eval));
    project.getOutput();

    assertEquals(2, operatorContext.getInputPositions());
    assertEquals(2, operatorContext.getOutputPositions());
  }
}
