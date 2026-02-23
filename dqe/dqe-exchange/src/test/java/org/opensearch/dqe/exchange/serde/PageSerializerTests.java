/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dqe.exchange.serde;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.trino.spi.Page;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.type.BigintType;
import io.trino.spi.type.BooleanType;
import io.trino.spi.type.DoubleType;
import io.trino.spi.type.IntegerType;
import io.trino.spi.type.VarcharType;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class PageSerializerTests {

  @Test
  @DisplayName("serialize and deserialize BIGINT page")
  void serializeDeserializeBigintPage() {
    BlockBuilder builder = BigintType.BIGINT.createBlockBuilder(null, 3);
    BigintType.BIGINT.writeLong(builder, 1L);
    BigintType.BIGINT.writeLong(builder, 2L);
    BigintType.BIGINT.writeLong(builder, 3L);
    Page page = new Page(builder.build());

    byte[] compressed = PageSerializer.serialize(page);
    assertNotNull(compressed);
    assertTrue(compressed.length > 0);

    Page result = PageDeserializer.deserialize(compressed, page.getSizeInBytes());
    assertEquals(3, result.getPositionCount());
    assertEquals(1, result.getChannelCount());
    assertEquals(1L, BigintType.BIGINT.getLong(result.getBlock(0), 0));
    assertEquals(2L, BigintType.BIGINT.getLong(result.getBlock(0), 1));
    assertEquals(3L, BigintType.BIGINT.getLong(result.getBlock(0), 2));
  }

  @Test
  @DisplayName("serialize and deserialize VARCHAR page")
  void serializeDeserializeVarcharPage() {
    BlockBuilder builder = VarcharType.VARCHAR.createBlockBuilder(null, 2);
    VarcharType.VARCHAR.writeString(builder, "foo");
    VarcharType.VARCHAR.writeString(builder, "bar");
    Page page = new Page(builder.build());

    byte[] compressed = PageSerializer.serialize(page);
    Page result = PageDeserializer.deserialize(compressed, page.getSizeInBytes());

    assertEquals(2, result.getPositionCount());
    assertEquals("foo", VarcharType.VARCHAR.getSlice(result.getBlock(0), 0).toStringUtf8());
    assertEquals("bar", VarcharType.VARCHAR.getSlice(result.getBlock(0), 1).toStringUtf8());
  }

  @Test
  @DisplayName("serialize and deserialize INTEGER page")
  void serializeDeserializeIntegerPage() {
    BlockBuilder builder = IntegerType.INTEGER.createBlockBuilder(null, 2);
    IntegerType.INTEGER.writeLong(builder, 42);
    IntegerType.INTEGER.writeLong(builder, -1);
    Page page = new Page(builder.build());

    byte[] compressed = PageSerializer.serialize(page);
    Page result = PageDeserializer.deserialize(compressed, page.getSizeInBytes());

    assertEquals(2, result.getPositionCount());
    assertEquals(42, IntegerType.INTEGER.getLong(result.getBlock(0), 0));
    assertEquals(-1, IntegerType.INTEGER.getLong(result.getBlock(0), 1));
  }

  @Test
  @DisplayName("serialize and deserialize DOUBLE page")
  void serializeDeserializeDoublePage() {
    BlockBuilder builder = DoubleType.DOUBLE.createBlockBuilder(null, 2);
    DoubleType.DOUBLE.writeDouble(builder, 3.14);
    DoubleType.DOUBLE.writeDouble(builder, -2.71);
    Page page = new Page(builder.build());

    byte[] compressed = PageSerializer.serialize(page);
    Page result = PageDeserializer.deserialize(compressed, page.getSizeInBytes());

    assertEquals(2, result.getPositionCount());
    assertEquals(3.14, DoubleType.DOUBLE.getDouble(result.getBlock(0), 0), 0.001);
    assertEquals(-2.71, DoubleType.DOUBLE.getDouble(result.getBlock(0), 1), 0.001);
  }

  @Test
  @DisplayName("serialize and deserialize BOOLEAN page")
  void serializeDeserializeBooleanPage() {
    BlockBuilder builder = BooleanType.BOOLEAN.createBlockBuilder(null, 3);
    BooleanType.BOOLEAN.writeBoolean(builder, true);
    BooleanType.BOOLEAN.writeBoolean(builder, false);
    BooleanType.BOOLEAN.writeBoolean(builder, true);
    Page page = new Page(builder.build());

    byte[] compressed = PageSerializer.serialize(page);
    Page result = PageDeserializer.deserialize(compressed, page.getSizeInBytes());

    assertEquals(3, result.getPositionCount());
    assertTrue(BooleanType.BOOLEAN.getBoolean(result.getBlock(0), 0));
    assertEquals(false, BooleanType.BOOLEAN.getBoolean(result.getBlock(0), 1));
    assertTrue(BooleanType.BOOLEAN.getBoolean(result.getBlock(0), 2));
  }

  @Test
  @DisplayName("serialize and deserialize multi-column page")
  void serializeDeserializeMultiColumnPage() {
    BlockBuilder bigint = BigintType.BIGINT.createBlockBuilder(null, 2);
    BigintType.BIGINT.writeLong(bigint, 10L);
    BigintType.BIGINT.writeLong(bigint, 20L);

    BlockBuilder varchar = VarcharType.VARCHAR.createBlockBuilder(null, 2);
    VarcharType.VARCHAR.writeString(varchar, "x");
    VarcharType.VARCHAR.writeString(varchar, "y");

    Page page = new Page(bigint.build(), varchar.build());

    byte[] compressed = PageSerializer.serialize(page);
    Page result = PageDeserializer.deserialize(compressed, page.getSizeInBytes());

    assertEquals(2, result.getPositionCount());
    assertEquals(2, result.getChannelCount());
    assertEquals(10L, BigintType.BIGINT.getLong(result.getBlock(0), 0));
    assertEquals("x", VarcharType.VARCHAR.getSlice(result.getBlock(1), 0).toStringUtf8());
  }

  @Test
  @DisplayName("serialize and deserialize page with nulls")
  void serializeDeserializeWithNulls() {
    BlockBuilder builder = BigintType.BIGINT.createBlockBuilder(null, 3);
    BigintType.BIGINT.writeLong(builder, 1L);
    builder.appendNull();
    BigintType.BIGINT.writeLong(builder, 3L);
    Page page = new Page(builder.build());

    byte[] compressed = PageSerializer.serialize(page);
    Page result = PageDeserializer.deserialize(compressed, page.getSizeInBytes());

    assertEquals(3, result.getPositionCount());
    assertEquals(1L, BigintType.BIGINT.getLong(result.getBlock(0), 0));
    assertTrue(result.getBlock(0).isNull(1));
    assertEquals(3L, BigintType.BIGINT.getLong(result.getBlock(0), 2));
  }

  @Test
  @DisplayName("estimateSerializedSize returns positive value")
  void estimateSerializedSizeReturnsPositive() {
    BlockBuilder builder = BigintType.BIGINT.createBlockBuilder(null, 10);
    for (int i = 0; i < 10; i++) {
      BigintType.BIGINT.writeLong(builder, i);
    }
    Page page = new Page(builder.build());

    long estimate = PageSerializer.estimateSerializedSize(page);
    assertTrue(estimate > 0);
  }

  @Test
  @DisplayName("empty page roundtrip")
  void emptyPageRoundtrip() {
    Page page = new Page(0);

    byte[] compressed = PageSerializer.serialize(page);
    Page result = PageDeserializer.deserialize(compressed, 0);

    assertEquals(0, result.getPositionCount());
    assertEquals(0, result.getChannelCount());
  }
}
