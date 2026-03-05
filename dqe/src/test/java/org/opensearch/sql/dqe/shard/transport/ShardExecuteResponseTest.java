/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.dqe.shard.transport;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.airlift.slice.Slices;
import io.trino.spi.Page;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.type.BigintType;
import io.trino.spi.type.Type;
import io.trino.spi.type.VarcharType;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.List;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.core.common.io.stream.InputStreamStreamInput;

@DisplayName("ShardExecuteResponse serialization round-trip")
class ShardExecuteResponseTest {

  @Test
  @DisplayName("Response preserves Pages with BIGINT column through serialization round-trip")
  void responseRoundTripBigint() throws IOException {
    BlockBuilder builder = BigintType.BIGINT.createBlockBuilder(null, 3);
    BigintType.BIGINT.writeLong(builder, 100L);
    BigintType.BIGINT.writeLong(builder, 200L);
    BigintType.BIGINT.writeLong(builder, 300L);

    Page page = new Page(builder.build());
    List<Type> columnTypes = List.of(BigintType.BIGINT);
    ShardExecuteResponse original = new ShardExecuteResponse(List.of(page), columnTypes);

    BytesStreamOutput out = new BytesStreamOutput();
    original.writeTo(out);

    ShardExecuteResponse deserialized =
        new ShardExecuteResponse(
            new InputStreamStreamInput(new ByteArrayInputStream(out.bytes().toBytesRef().bytes)));

    assertEquals(1, deserialized.getPages().size());
    assertEquals(1, deserialized.getColumnTypes().size());
    assertEquals(BigintType.BIGINT, deserialized.getColumnTypes().get(0));

    Page deserPage = deserialized.getPages().get(0);
    assertEquals(3, deserPage.getPositionCount());
    assertEquals(100L, BigintType.BIGINT.getLong(deserPage.getBlock(0), 0));
    assertEquals(200L, BigintType.BIGINT.getLong(deserPage.getBlock(0), 1));
    assertEquals(300L, BigintType.BIGINT.getLong(deserPage.getBlock(0), 2));
  }

  @Test
  @DisplayName("Response round-trips with empty page list")
  void responseRoundTripEmptyPages() throws IOException {
    List<Type> columnTypes = List.of(BigintType.BIGINT);
    ShardExecuteResponse original = new ShardExecuteResponse(List.of(), columnTypes);

    BytesStreamOutput out = new BytesStreamOutput();
    original.writeTo(out);

    ShardExecuteResponse deserialized =
        new ShardExecuteResponse(
            new InputStreamStreamInput(new ByteArrayInputStream(out.bytes().toBytesRef().bytes)));

    assertNotNull(deserialized.getPages());
    assertTrue(deserialized.getPages().isEmpty());
    assertEquals(1, deserialized.getColumnTypes().size());
  }

  @Test
  @DisplayName("Response round-trips with multi-column multi-row Pages")
  void responseRoundTripMultiColumnMultiRow() throws IOException {
    BlockBuilder bigintBuilder = BigintType.BIGINT.createBlockBuilder(null, 2);
    BigintType.BIGINT.writeLong(bigintBuilder, 1L);
    BigintType.BIGINT.writeLong(bigintBuilder, 2L);

    BlockBuilder varcharBuilder = VarcharType.VARCHAR.createBlockBuilder(null, 2);
    VarcharType.VARCHAR.writeSlice(varcharBuilder, Slices.utf8Slice("hello"));
    VarcharType.VARCHAR.writeSlice(varcharBuilder, Slices.utf8Slice("world"));

    Page page = new Page(bigintBuilder.build(), varcharBuilder.build());
    List<Type> columnTypes = List.of(BigintType.BIGINT, VarcharType.VARCHAR);
    ShardExecuteResponse original = new ShardExecuteResponse(List.of(page), columnTypes);

    BytesStreamOutput out = new BytesStreamOutput();
    original.writeTo(out);

    ShardExecuteResponse deserialized =
        new ShardExecuteResponse(
            new InputStreamStreamInput(new ByteArrayInputStream(out.bytes().toBytesRef().bytes)));

    assertEquals(1, deserialized.getPages().size());
    assertEquals(2, deserialized.getColumnTypes().size());

    Page deserPage = deserialized.getPages().get(0);
    assertEquals(2, deserPage.getChannelCount());
    assertEquals(2, deserPage.getPositionCount());

    assertEquals(1L, BigintType.BIGINT.getLong(deserPage.getBlock(0), 0));
    assertEquals(2L, BigintType.BIGINT.getLong(deserPage.getBlock(0), 1));
    assertEquals("hello", VarcharType.VARCHAR.getSlice(deserPage.getBlock(1), 0).toStringUtf8());
    assertEquals("world", VarcharType.VARCHAR.getSlice(deserPage.getBlock(1), 1).toStringUtf8());
  }

  @Test
  @DisplayName("Response round-trips with multiple pages")
  void responseRoundTripMultiplePages() throws IOException {
    BlockBuilder b1 = BigintType.BIGINT.createBlockBuilder(null, 2);
    BigintType.BIGINT.writeLong(b1, 10L);
    BigintType.BIGINT.writeLong(b1, 20L);

    BlockBuilder b2 = BigintType.BIGINT.createBlockBuilder(null, 2);
    BigintType.BIGINT.writeLong(b2, 30L);
    BigintType.BIGINT.writeLong(b2, 40L);

    List<Type> columnTypes = List.of(BigintType.BIGINT);
    ShardExecuteResponse original =
        new ShardExecuteResponse(List.of(new Page(b1.build()), new Page(b2.build())), columnTypes);

    BytesStreamOutput out = new BytesStreamOutput();
    original.writeTo(out);

    ShardExecuteResponse deserialized =
        new ShardExecuteResponse(
            new InputStreamStreamInput(new ByteArrayInputStream(out.bytes().toBytesRef().bytes)));

    assertEquals(2, deserialized.getPages().size());
    assertEquals(10L, BigintType.BIGINT.getLong(deserialized.getPages().get(0).getBlock(0), 0));
    assertEquals(20L, BigintType.BIGINT.getLong(deserialized.getPages().get(0).getBlock(0), 1));
    assertEquals(30L, BigintType.BIGINT.getLong(deserialized.getPages().get(1).getBlock(0), 0));
    assertEquals(40L, BigintType.BIGINT.getLong(deserialized.getPages().get(1).getBlock(0), 1));
  }

  @Test
  @DisplayName("Response round-trips with null values in Pages")
  void responseRoundTripWithNulls() throws IOException {
    BlockBuilder builder = BigintType.BIGINT.createBlockBuilder(null, 3);
    BigintType.BIGINT.writeLong(builder, 1L);
    builder.appendNull();
    BigintType.BIGINT.writeLong(builder, 3L);

    Page page = new Page(builder.build());
    List<Type> columnTypes = List.of(BigintType.BIGINT);
    ShardExecuteResponse original = new ShardExecuteResponse(List.of(page), columnTypes);

    BytesStreamOutput out = new BytesStreamOutput();
    original.writeTo(out);

    ShardExecuteResponse deserialized =
        new ShardExecuteResponse(
            new InputStreamStreamInput(new ByteArrayInputStream(out.bytes().toBytesRef().bytes)));

    Page deserPage = deserialized.getPages().get(0);
    assertFalse(deserPage.getBlock(0).isNull(0));
    assertTrue(deserPage.getBlock(0).isNull(1));
    assertFalse(deserPage.getBlock(0).isNull(2));
    assertEquals(1L, BigintType.BIGINT.getLong(deserPage.getBlock(0), 0));
    assertEquals(3L, BigintType.BIGINT.getLong(deserPage.getBlock(0), 2));
  }
}
