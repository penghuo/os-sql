/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.dqe.function.expression;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.airlift.slice.Slices;
import io.trino.spi.Page;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.type.BigintType;
import io.trino.spi.type.VarcharType;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

@DisplayName("ColumnReference expression")
class ColumnReferenceTest {

  @Test
  @DisplayName("Read BIGINT column from a Page")
  void readBigintColumn() {
    Page page = buildBigintPage(10L, 20L, 30L);
    ColumnReference ref = new ColumnReference(0, BigintType.BIGINT);

    Block result = ref.evaluate(page);

    assertEquals(3, result.getPositionCount());
    assertEquals(10L, BigintType.BIGINT.getLong(result, 0));
    assertEquals(20L, BigintType.BIGINT.getLong(result, 1));
    assertEquals(30L, BigintType.BIGINT.getLong(result, 2));
  }

  @Test
  @DisplayName("Read VARCHAR column from a Page")
  void readVarcharColumn() {
    Page page = buildVarcharPage("hello", "world");
    ColumnReference ref = new ColumnReference(0, VarcharType.VARCHAR);

    Block result = ref.evaluate(page);

    assertEquals(2, result.getPositionCount());
    assertEquals("hello", VarcharType.VARCHAR.getSlice(result, 0).toStringUtf8());
    assertEquals("world", VarcharType.VARCHAR.getSlice(result, 1).toStringUtf8());
  }

  @Test
  @DisplayName("Null positions return null in output Block")
  void nullPositions() {
    BlockBuilder builder = BigintType.BIGINT.createBlockBuilder(null, 3);
    BigintType.BIGINT.writeLong(builder, 10L);
    builder.appendNull();
    BigintType.BIGINT.writeLong(builder, 30L);
    Page page = new Page(builder.build());

    ColumnReference ref = new ColumnReference(0, BigintType.BIGINT);
    Block result = ref.evaluate(page);

    assertEquals(3, result.getPositionCount());
    assertEquals(10L, BigintType.BIGINT.getLong(result, 0));
    assertTrue(result.isNull(1));
    assertEquals(30L, BigintType.BIGINT.getLong(result, 2));
  }

  @Test
  @DisplayName("Returns correct type")
  void returnsCorrectType() {
    ColumnReference ref = new ColumnReference(0, BigintType.BIGINT);
    assertEquals(BigintType.BIGINT, ref.getType());
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
}
