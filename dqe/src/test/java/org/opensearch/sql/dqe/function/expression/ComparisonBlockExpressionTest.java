/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.dqe.function.expression;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.airlift.slice.Slices;
import io.trino.spi.Page;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.type.BigintType;
import io.trino.spi.type.BooleanType;
import io.trino.spi.type.DoubleType;
import io.trino.spi.type.VarcharType;
import io.trino.sql.tree.ComparisonExpression;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

@DisplayName("ComparisonBlockExpression")
class ComparisonBlockExpressionTest {

  @Test
  @DisplayName("BIGINT equality")
  void bigintEquality() {
    // col0 = [10, 20, 30], compare with constant 20
    Page page = buildBigintPage(10L, 20L, 30L);
    BlockExpression left = new ColumnReference(0, BigintType.BIGINT);
    BlockExpression right = new ConstantExpression(20L, BigintType.BIGINT);

    Block result =
        new ComparisonBlockExpression(ComparisonExpression.Operator.EQUAL, left, right)
            .evaluate(page);

    assertFalse(BooleanType.BOOLEAN.getBoolean(result, 0));
    assertTrue(BooleanType.BOOLEAN.getBoolean(result, 1));
    assertFalse(BooleanType.BOOLEAN.getBoolean(result, 2));
  }

  @Test
  @DisplayName("BIGINT not equal, less than, greater than")
  void bigintAllOperators() {
    Page page = buildBigintPage(10L, 20L, 30L);
    BlockExpression left = new ColumnReference(0, BigintType.BIGINT);
    BlockExpression right = new ConstantExpression(20L, BigintType.BIGINT);

    Block neq =
        new ComparisonBlockExpression(ComparisonExpression.Operator.NOT_EQUAL, left, right)
            .evaluate(page);
    assertTrue(BooleanType.BOOLEAN.getBoolean(neq, 0));
    assertFalse(BooleanType.BOOLEAN.getBoolean(neq, 1));

    Block lt =
        new ComparisonBlockExpression(ComparisonExpression.Operator.LESS_THAN, left, right)
            .evaluate(page);
    assertTrue(BooleanType.BOOLEAN.getBoolean(lt, 0));
    assertFalse(BooleanType.BOOLEAN.getBoolean(lt, 1));

    Block lte =
        new ComparisonBlockExpression(ComparisonExpression.Operator.LESS_THAN_OR_EQUAL, left, right)
            .evaluate(page);
    assertTrue(BooleanType.BOOLEAN.getBoolean(lte, 0));
    assertTrue(BooleanType.BOOLEAN.getBoolean(lte, 1));
    assertFalse(BooleanType.BOOLEAN.getBoolean(lte, 2));

    Block gt =
        new ComparisonBlockExpression(ComparisonExpression.Operator.GREATER_THAN, left, right)
            .evaluate(page);
    assertFalse(BooleanType.BOOLEAN.getBoolean(gt, 0));
    assertFalse(BooleanType.BOOLEAN.getBoolean(gt, 1));
    assertTrue(BooleanType.BOOLEAN.getBoolean(gt, 2));

    Block gte =
        new ComparisonBlockExpression(
                ComparisonExpression.Operator.GREATER_THAN_OR_EQUAL, left, right)
            .evaluate(page);
    assertFalse(BooleanType.BOOLEAN.getBoolean(gte, 0));
    assertTrue(BooleanType.BOOLEAN.getBoolean(gte, 1));
    assertTrue(BooleanType.BOOLEAN.getBoolean(gte, 2));
  }

  @Test
  @DisplayName("VARCHAR comparisons")
  void varcharComparisons() {
    Page page = buildVarcharPage("apple", "banana", "cherry");
    BlockExpression left = new ColumnReference(0, VarcharType.VARCHAR);
    BlockExpression right = new ConstantExpression("banana", VarcharType.VARCHAR);

    Block eq =
        new ComparisonBlockExpression(ComparisonExpression.Operator.EQUAL, left, right)
            .evaluate(page);
    assertFalse(BooleanType.BOOLEAN.getBoolean(eq, 0));
    assertTrue(BooleanType.BOOLEAN.getBoolean(eq, 1));
    assertFalse(BooleanType.BOOLEAN.getBoolean(eq, 2));
  }

  @Test
  @DisplayName("DOUBLE comparisons")
  void doubleComparisons() {
    Page page = buildDoublePage(1.5, 3.0, 5.5);
    BlockExpression left = new ColumnReference(0, DoubleType.DOUBLE);
    BlockExpression right = new ConstantExpression(3.0, DoubleType.DOUBLE);

    Block gt =
        new ComparisonBlockExpression(ComparisonExpression.Operator.GREATER_THAN, left, right)
            .evaluate(page);
    assertFalse(BooleanType.BOOLEAN.getBoolean(gt, 0));
    assertFalse(BooleanType.BOOLEAN.getBoolean(gt, 1));
    assertTrue(BooleanType.BOOLEAN.getBoolean(gt, 2));
  }

  @Test
  @DisplayName("NULL propagation — either side null produces null output")
  void nullPropagation() {
    BlockBuilder builder = BigintType.BIGINT.createBlockBuilder(null, 3);
    BigintType.BIGINT.writeLong(builder, 10L);
    builder.appendNull();
    BigintType.BIGINT.writeLong(builder, 30L);
    Page page = new Page(builder.build());

    BlockExpression left = new ColumnReference(0, BigintType.BIGINT);
    BlockExpression right = new ConstantExpression(20L, BigintType.BIGINT);

    Block result =
        new ComparisonBlockExpression(ComparisonExpression.Operator.EQUAL, left, right)
            .evaluate(page);

    assertFalse(result.isNull(0));
    assertTrue(result.isNull(1)); // null compared -> null
    assertFalse(result.isNull(2));
  }

  @Test
  @DisplayName("Returns BOOLEAN type")
  void returnsBooleanType() {
    BlockExpression left = new ConstantExpression(1L, BigintType.BIGINT);
    BlockExpression right = new ConstantExpression(2L, BigintType.BIGINT);
    ComparisonBlockExpression expr =
        new ComparisonBlockExpression(ComparisonExpression.Operator.EQUAL, left, right);
    assertEquals(BooleanType.BOOLEAN, expr.getType());
  }

  private static Page buildBigintPage(long... values) {
    BlockBuilder builder = BigintType.BIGINT.createBlockBuilder(null, values.length);
    for (long v : values) {
      BigintType.BIGINT.writeLong(builder, v);
    }
    return new Page(builder.build());
  }

  private static Page buildVarcharPage(String... values) {
    BlockBuilder builder = VarcharType.VARCHAR.createBlockBuilder(null, values.length);
    for (String v : values) {
      VarcharType.VARCHAR.writeSlice(builder, Slices.utf8Slice(v));
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
