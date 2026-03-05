/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.dqe.operator;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import io.trino.spi.Page;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.type.BigintType;
import io.trino.sql.tree.ArithmeticBinaryExpression;
import java.util.List;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.opensearch.sql.dqe.function.expression.ArithmeticBlockExpression;
import org.opensearch.sql.dqe.function.expression.BlockExpression;
import org.opensearch.sql.dqe.function.expression.ColumnReference;
import org.opensearch.sql.dqe.function.expression.ConstantExpression;

@DisplayName("EvalOperator")
class EvalOperatorTest {

  @Test
  @DisplayName("Computes single expression: col0 * 2")
  void computesSingleExpression() {
    Page inputPage = buildBigintPage(10L, 20L, 30L);
    TestPageSource source = new TestPageSource(List.of(inputPage));

    BlockExpression col0 = new ColumnReference(0, BigintType.BIGINT);
    BlockExpression two = new ConstantExpression(2L, BigintType.BIGINT);
    BlockExpression expr =
        new ArithmeticBlockExpression(ArithmeticBinaryExpression.Operator.MULTIPLY, col0, two);

    EvalOperator eval = new EvalOperator(source, List.of(expr));
    Page result = eval.processNextBatch();

    assertEquals(3, result.getPositionCount());
    assertEquals(1, result.getChannelCount());
    assertEquals(20L, BigintType.BIGINT.getLong(result.getBlock(0), 0));
    assertEquals(40L, BigintType.BIGINT.getLong(result.getBlock(0), 1));
    assertEquals(60L, BigintType.BIGINT.getLong(result.getBlock(0), 2));
  }

  @Test
  @DisplayName("Multiple expressions: [col0, col0 + 1]")
  void multipleExpressions() {
    Page inputPage = buildBigintPage(10L, 20L);
    TestPageSource source = new TestPageSource(List.of(inputPage));

    BlockExpression col0 = new ColumnReference(0, BigintType.BIGINT);
    BlockExpression one = new ConstantExpression(1L, BigintType.BIGINT);
    BlockExpression col0Plus1 =
        new ArithmeticBlockExpression(ArithmeticBinaryExpression.Operator.ADD, col0, one);

    EvalOperator eval = new EvalOperator(source, List.of(col0, col0Plus1));
    Page result = eval.processNextBatch();

    assertEquals(2, result.getPositionCount());
    assertEquals(2, result.getChannelCount());
    assertEquals(10L, BigintType.BIGINT.getLong(result.getBlock(0), 0));
    assertEquals(20L, BigintType.BIGINT.getLong(result.getBlock(0), 1));
    assertEquals(11L, BigintType.BIGINT.getLong(result.getBlock(1), 0));
    assertEquals(21L, BigintType.BIGINT.getLong(result.getBlock(1), 1));
  }

  @Test
  @DisplayName("Returns null when source is exhausted")
  void returnsNullWhenExhausted() {
    TestPageSource source = new TestPageSource(List.of());
    BlockExpression col0 = new ColumnReference(0, BigintType.BIGINT);
    EvalOperator eval = new EvalOperator(source, List.of(col0));

    assertNull(eval.processNextBatch());
  }

  private static Page buildBigintPage(long... values) {
    BlockBuilder builder = BigintType.BIGINT.createBlockBuilder(null, values.length);
    for (long v : values) {
      BigintType.BIGINT.writeLong(builder, v);
    }
    return new Page(builder.build());
  }
}
