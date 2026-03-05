/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.dqe.function.expression;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.trino.spi.Page;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.type.BigintType;
import io.trino.spi.type.DoubleType;
import io.trino.sql.tree.ArithmeticBinaryExpression;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

@DisplayName("ArithmeticBlockExpression")
class ArithmeticBlockExpressionTest {

  @Test
  @DisplayName("BIGINT addition")
  void bigintAdd() {
    Page page = buildBigintPage(10L, 20L, 30L);
    BlockExpression left = new ColumnReference(0, BigintType.BIGINT);
    BlockExpression right = new ConstantExpression(5L, BigintType.BIGINT);

    Block result =
        new ArithmeticBlockExpression(ArithmeticBinaryExpression.Operator.ADD, left, right)
            .evaluate(page);

    assertEquals(15L, BigintType.BIGINT.getLong(result, 0));
    assertEquals(25L, BigintType.BIGINT.getLong(result, 1));
    assertEquals(35L, BigintType.BIGINT.getLong(result, 2));
  }

  @Test
  @DisplayName("BIGINT subtraction, multiply, divide, modulus")
  void bigintAllOperators() {
    Page page = buildBigintPage(10L, 20L, 30L);
    BlockExpression left = new ColumnReference(0, BigintType.BIGINT);
    BlockExpression right = new ConstantExpression(3L, BigintType.BIGINT);

    Block sub =
        new ArithmeticBlockExpression(ArithmeticBinaryExpression.Operator.SUBTRACT, left, right)
            .evaluate(page);
    assertEquals(7L, BigintType.BIGINT.getLong(sub, 0));

    Block mul =
        new ArithmeticBlockExpression(ArithmeticBinaryExpression.Operator.MULTIPLY, left, right)
            .evaluate(page);
    assertEquals(30L, BigintType.BIGINT.getLong(mul, 0));

    Block div =
        new ArithmeticBlockExpression(ArithmeticBinaryExpression.Operator.DIVIDE, left, right)
            .evaluate(page);
    assertEquals(3L, BigintType.BIGINT.getLong(div, 0));

    Block mod =
        new ArithmeticBlockExpression(ArithmeticBinaryExpression.Operator.MODULUS, left, right)
            .evaluate(page);
    assertEquals(1L, BigintType.BIGINT.getLong(mod, 0));
  }

  @Test
  @DisplayName("DOUBLE arithmetic")
  void doubleArithmetic() {
    Page page = buildDoublePage(3.0, 5.0);
    BlockExpression left = new ColumnReference(0, DoubleType.DOUBLE);
    BlockExpression right = new ConstantExpression(2.0, DoubleType.DOUBLE);

    Block result =
        new ArithmeticBlockExpression(ArithmeticBinaryExpression.Operator.MULTIPLY, left, right)
            .evaluate(page);

    assertEquals(6.0, DoubleType.DOUBLE.getDouble(result, 0), 0.001);
    assertEquals(10.0, DoubleType.DOUBLE.getDouble(result, 1), 0.001);
  }

  @Test
  @DisplayName("Mixed BIGINT + DOUBLE promotion")
  void mixedTypePromotion() {
    Page page = buildBigintPage(10L, 20L);
    BlockExpression left = new ColumnReference(0, BigintType.BIGINT);
    BlockExpression right = new ConstantExpression(2.5, DoubleType.DOUBLE);

    ArithmeticBlockExpression expr =
        new ArithmeticBlockExpression(ArithmeticBinaryExpression.Operator.MULTIPLY, left, right);

    assertEquals(DoubleType.DOUBLE, expr.getType());
    Block result = expr.evaluate(page);
    assertEquals(25.0, DoubleType.DOUBLE.getDouble(result, 0), 0.001);
    assertEquals(50.0, DoubleType.DOUBLE.getDouble(result, 1), 0.001);
  }

  @Test
  @DisplayName("NULL propagation")
  void nullPropagation() {
    BlockBuilder builder = BigintType.BIGINT.createBlockBuilder(null, 3);
    BigintType.BIGINT.writeLong(builder, 10L);
    builder.appendNull();
    BigintType.BIGINT.writeLong(builder, 30L);
    Page page = new Page(builder.build());

    BlockExpression left = new ColumnReference(0, BigintType.BIGINT);
    BlockExpression right = new ConstantExpression(5L, BigintType.BIGINT);

    Block result =
        new ArithmeticBlockExpression(ArithmeticBinaryExpression.Operator.ADD, left, right)
            .evaluate(page);

    assertEquals(15L, BigintType.BIGINT.getLong(result, 0));
    assertTrue(result.isNull(1));
    assertEquals(35L, BigintType.BIGINT.getLong(result, 2));
  }

  @Test
  @DisplayName("Output type is BIGINT for integer-only operations")
  void outputTypeBigint() {
    BlockExpression left = new ConstantExpression(1L, BigintType.BIGINT);
    BlockExpression right = new ConstantExpression(2L, BigintType.BIGINT);
    ArithmeticBlockExpression expr =
        new ArithmeticBlockExpression(ArithmeticBinaryExpression.Operator.ADD, left, right);
    assertEquals(BigintType.BIGINT, expr.getType());
  }

  private static Page buildBigintPage(long... values) {
    BlockBuilder builder = BigintType.BIGINT.createBlockBuilder(null, values.length);
    for (long v : values) {
      BigintType.BIGINT.writeLong(builder, v);
    }
    return new Page(builder.build());
  }

  private static Page buildDoublePage(double... values) {
    BlockBuilder builder = DoubleType.DOUBLE.createBlockBuilder(null, values.length);
    for (double v : values) {
      DoubleType.DOUBLE.writeDouble(builder, v);
    }
    return new Page(builder.build());
  }
}
