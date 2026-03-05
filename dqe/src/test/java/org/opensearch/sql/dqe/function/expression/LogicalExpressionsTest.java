/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.dqe.function.expression;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.trino.spi.Page;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.type.BigintType;
import io.trino.spi.type.BooleanType;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

@DisplayName("Logical expressions")
class LogicalExpressionsTest {

  // Helper: build a boolean page from Boolean values (null allowed)
  private static Page buildBooleanPage(Boolean... values) {
    BlockBuilder builder = BooleanType.BOOLEAN.createBlockBuilder(null, values.length);
    for (Boolean v : values) {
      if (v == null) {
        builder.appendNull();
      } else {
        BooleanType.BOOLEAN.writeBoolean(builder, v);
      }
    }
    return new Page(builder.build());
  }

  @Nested
  @DisplayName("AND")
  class AndTests {

    @Test
    @DisplayName("true AND true = true")
    void trueAndTrue() {
      Page page = buildBooleanPage(true);
      Block result =
          new LogicalAndExpression(
                  new ColumnReference(0, BooleanType.BOOLEAN),
                  new ConstantExpression(true, BooleanType.BOOLEAN))
              .evaluate(page);
      assertTrue(BooleanType.BOOLEAN.getBoolean(result, 0));
    }

    @Test
    @DisplayName("true AND false = false")
    void trueAndFalse() {
      Page page = buildBooleanPage(true);
      Block result =
          new LogicalAndExpression(
                  new ColumnReference(0, BooleanType.BOOLEAN),
                  new ConstantExpression(false, BooleanType.BOOLEAN))
              .evaluate(page);
      assertFalse(BooleanType.BOOLEAN.getBoolean(result, 0));
    }

    @Test
    @DisplayName("true AND null = null")
    void trueAndNull() {
      Page page = buildBooleanPage(true);
      Block result =
          new LogicalAndExpression(
                  new ColumnReference(0, BooleanType.BOOLEAN),
                  new ConstantExpression(null, BooleanType.BOOLEAN))
              .evaluate(page);
      assertTrue(result.isNull(0));
    }

    @Test
    @DisplayName("false AND null = false")
    void falseAndNull() {
      Page page = buildBooleanPage(false);
      Block result =
          new LogicalAndExpression(
                  new ColumnReference(0, BooleanType.BOOLEAN),
                  new ConstantExpression(null, BooleanType.BOOLEAN))
              .evaluate(page);
      assertFalse(result.isNull(0));
      assertFalse(BooleanType.BOOLEAN.getBoolean(result, 0));
    }
  }

  @Nested
  @DisplayName("OR")
  class OrTests {

    @Test
    @DisplayName("false OR false = false")
    void falseOrFalse() {
      Page page = buildBooleanPage(false);
      Block result =
          new LogicalOrExpression(
                  new ColumnReference(0, BooleanType.BOOLEAN),
                  new ConstantExpression(false, BooleanType.BOOLEAN))
              .evaluate(page);
      assertFalse(BooleanType.BOOLEAN.getBoolean(result, 0));
    }

    @Test
    @DisplayName("true OR false = true")
    void trueOrFalse() {
      Page page = buildBooleanPage(true);
      Block result =
          new LogicalOrExpression(
                  new ColumnReference(0, BooleanType.BOOLEAN),
                  new ConstantExpression(false, BooleanType.BOOLEAN))
              .evaluate(page);
      assertTrue(BooleanType.BOOLEAN.getBoolean(result, 0));
    }

    @Test
    @DisplayName("false OR null = null")
    void falseOrNull() {
      Page page = buildBooleanPage(false);
      Block result =
          new LogicalOrExpression(
                  new ColumnReference(0, BooleanType.BOOLEAN),
                  new ConstantExpression(null, BooleanType.BOOLEAN))
              .evaluate(page);
      assertTrue(result.isNull(0));
    }

    @Test
    @DisplayName("true OR null = true")
    void trueOrNull() {
      Page page = buildBooleanPage(true);
      Block result =
          new LogicalOrExpression(
                  new ColumnReference(0, BooleanType.BOOLEAN),
                  new ConstantExpression(null, BooleanType.BOOLEAN))
              .evaluate(page);
      assertFalse(result.isNull(0));
      assertTrue(BooleanType.BOOLEAN.getBoolean(result, 0));
    }
  }

  @Nested
  @DisplayName("NOT")
  class NotTests {

    @Test
    @DisplayName("NOT true = false")
    void notTrue() {
      Page page = buildBooleanPage(true);
      Block result =
          new NotBlockExpression(new ColumnReference(0, BooleanType.BOOLEAN)).evaluate(page);
      assertFalse(BooleanType.BOOLEAN.getBoolean(result, 0));
    }

    @Test
    @DisplayName("NOT false = true")
    void notFalse() {
      Page page = buildBooleanPage(false);
      Block result =
          new NotBlockExpression(new ColumnReference(0, BooleanType.BOOLEAN)).evaluate(page);
      assertTrue(BooleanType.BOOLEAN.getBoolean(result, 0));
    }

    @Test
    @DisplayName("NOT null = null")
    void notNull() {
      Page page = buildBooleanPage((Boolean) null);
      Block result =
          new NotBlockExpression(new ColumnReference(0, BooleanType.BOOLEAN)).evaluate(page);
      assertTrue(result.isNull(0));
    }
  }

  @Nested
  @DisplayName("IS NULL / IS NOT NULL")
  class IsNullTests {

    @Test
    @DisplayName("IS NULL: null -> true, non-null -> false")
    void isNull() {
      BlockBuilder builder = BigintType.BIGINT.createBlockBuilder(null, 2);
      builder.appendNull();
      BigintType.BIGINT.writeLong(builder, 42L);
      Page page = new Page(builder.build());

      Block result =
          new IsNullExpression(new ColumnReference(0, BigintType.BIGINT), false).evaluate(page);

      assertTrue(BooleanType.BOOLEAN.getBoolean(result, 0));
      assertFalse(BooleanType.BOOLEAN.getBoolean(result, 1));
    }

    @Test
    @DisplayName("IS NOT NULL: null -> false, non-null -> true")
    void isNotNull() {
      BlockBuilder builder = BigintType.BIGINT.createBlockBuilder(null, 2);
      builder.appendNull();
      BigintType.BIGINT.writeLong(builder, 42L);
      Page page = new Page(builder.build());

      Block result =
          new IsNullExpression(new ColumnReference(0, BigintType.BIGINT), true).evaluate(page);

      assertFalse(BooleanType.BOOLEAN.getBoolean(result, 0));
      assertTrue(BooleanType.BOOLEAN.getBoolean(result, 1));
    }
  }
}
