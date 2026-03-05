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
import io.trino.spi.type.VarcharType;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

@DisplayName("ConstantExpression")
class ConstantExpressionTest {

  @Test
  @DisplayName("Produce BIGINT constant Block across all positions")
  void bigintConstant() {
    ConstantExpression expr = new ConstantExpression(42L, BigintType.BIGINT);
    Page page = buildDummyPage(3);

    Block result = expr.evaluate(page);

    assertEquals(3, result.getPositionCount());
    assertEquals(42L, BigintType.BIGINT.getLong(result, 0));
    assertEquals(42L, BigintType.BIGINT.getLong(result, 1));
    assertEquals(42L, BigintType.BIGINT.getLong(result, 2));
  }

  @Test
  @DisplayName("Produce VARCHAR constant Block")
  void varcharConstant() {
    ConstantExpression expr = new ConstantExpression("hello", VarcharType.VARCHAR);
    Page page = buildDummyPage(2);

    Block result = expr.evaluate(page);

    assertEquals(2, result.getPositionCount());
    assertEquals("hello", VarcharType.VARCHAR.getSlice(result, 0).toStringUtf8());
    assertEquals("hello", VarcharType.VARCHAR.getSlice(result, 1).toStringUtf8());
  }

  @Test
  @DisplayName("Produce all-null Block for null constant")
  void nullConstant() {
    ConstantExpression expr = new ConstantExpression(null, BigintType.BIGINT);
    Page page = buildDummyPage(3);

    Block result = expr.evaluate(page);

    assertEquals(3, result.getPositionCount());
    assertTrue(result.isNull(0));
    assertTrue(result.isNull(1));
    assertTrue(result.isNull(2));
  }

  @Test
  @DisplayName("Returns correct type")
  void returnsCorrectType() {
    ConstantExpression expr = new ConstantExpression(42L, BigintType.BIGINT);
    assertEquals(BigintType.BIGINT, expr.getType());
  }

  /** Build a dummy Page with the given position count (single BIGINT column of zeros). */
  private static Page buildDummyPage(int positionCount) {
    BlockBuilder builder = BigintType.BIGINT.createBlockBuilder(null, positionCount);
    for (int i = 0; i < positionCount; i++) {
      BigintType.BIGINT.writeLong(builder, 0L);
    }
    return new Page(builder.build());
  }
}
