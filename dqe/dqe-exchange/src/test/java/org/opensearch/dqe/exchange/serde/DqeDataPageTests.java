/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dqe.exchange.serde;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.trino.spi.Page;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.type.BigintType;
import io.trino.spi.type.VarcharType;
import java.io.IOException;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.core.common.io.stream.StreamInput;

class DqeDataPageTests {

  @Test
  @DisplayName("DqeDataPage roundtrip with BIGINT block")
  void roundtripBigintBlock() throws IOException {
    BlockBuilder builder = BigintType.BIGINT.createBlockBuilder(null, 3);
    BigintType.BIGINT.writeLong(builder, 42L);
    BigintType.BIGINT.writeLong(builder, 100L);
    BigintType.BIGINT.writeLong(builder, -7L);
    Block block = builder.build();
    Page page = new Page(block);

    DqeDataPage dataPage = new DqeDataPage(page);

    BytesStreamOutput out = new BytesStreamOutput();
    dataPage.writeTo(out);
    StreamInput in = out.bytes().streamInput();
    DqeDataPage deserialized = new DqeDataPage(in);

    assertEquals(3, deserialized.getPositionCount());
    assertTrue(deserialized.getCompressedSizeInBytes() > 0);
    assertTrue(deserialized.getUncompressedSizeInBytes() > 0);

    Page result = deserialized.getPage();
    assertEquals(3, result.getPositionCount());
    assertEquals(1, result.getChannelCount());
    assertEquals(42L, BigintType.BIGINT.getLong(result.getBlock(0), 0));
    assertEquals(100L, BigintType.BIGINT.getLong(result.getBlock(0), 1));
    assertEquals(-7L, BigintType.BIGINT.getLong(result.getBlock(0), 2));
  }

  @Test
  @DisplayName("DqeDataPage roundtrip with VARCHAR block")
  void roundtripVarcharBlock() throws IOException {
    BlockBuilder builder = VarcharType.VARCHAR.createBlockBuilder(null, 2);
    VarcharType.VARCHAR.writeString(builder, "hello");
    VarcharType.VARCHAR.writeString(builder, "world");
    Block block = builder.build();
    Page page = new Page(block);

    DqeDataPage dataPage = new DqeDataPage(page);

    BytesStreamOutput out = new BytesStreamOutput();
    dataPage.writeTo(out);
    StreamInput in = out.bytes().streamInput();
    DqeDataPage deserialized = new DqeDataPage(in);

    Page result = deserialized.getPage();
    assertEquals(2, result.getPositionCount());
    assertEquals("hello", VarcharType.VARCHAR.getSlice(result.getBlock(0), 0).toStringUtf8());
    assertEquals("world", VarcharType.VARCHAR.getSlice(result.getBlock(0), 1).toStringUtf8());
  }

  @Test
  @DisplayName("DqeDataPage roundtrip with multiple columns")
  void roundtripMultipleColumns() throws IOException {
    BlockBuilder bigintBuilder = BigintType.BIGINT.createBlockBuilder(null, 2);
    BigintType.BIGINT.writeLong(bigintBuilder, 1L);
    BigintType.BIGINT.writeLong(bigintBuilder, 2L);

    BlockBuilder varcharBuilder = VarcharType.VARCHAR.createBlockBuilder(null, 2);
    VarcharType.VARCHAR.writeString(varcharBuilder, "a");
    VarcharType.VARCHAR.writeString(varcharBuilder, "b");

    Page page = new Page(bigintBuilder.build(), varcharBuilder.build());

    DqeDataPage dataPage = new DqeDataPage(page);
    BytesStreamOutput out = new BytesStreamOutput();
    dataPage.writeTo(out);
    StreamInput in = out.bytes().streamInput();
    DqeDataPage deserialized = new DqeDataPage(in);

    Page result = deserialized.getPage();
    assertEquals(2, result.getPositionCount());
    assertEquals(2, result.getChannelCount());
    assertEquals(1L, BigintType.BIGINT.getLong(result.getBlock(0), 0));
    assertEquals("a", VarcharType.VARCHAR.getSlice(result.getBlock(1), 0).toStringUtf8());
  }

  @Test
  @DisplayName("DqeDataPage handles empty page")
  void handlesEmptyPage() throws IOException {
    Page page = new Page(0);

    DqeDataPage dataPage = new DqeDataPage(page);
    BytesStreamOutput out = new BytesStreamOutput();
    dataPage.writeTo(out);
    StreamInput in = out.bytes().streamInput();
    DqeDataPage deserialized = new DqeDataPage(in);

    assertEquals(0, deserialized.getPositionCount());
    Page result = deserialized.getPage();
    assertEquals(0, result.getPositionCount());
  }

  @Test
  @DisplayName("DqeDataPage compressed size is less than uncompressed for large data")
  void compressedSizeSmallerForLargeData() {
    // Create a large page with repetitive data
    BlockBuilder builder = BigintType.BIGINT.createBlockBuilder(null, 1000);
    for (int i = 0; i < 1000; i++) {
      BigintType.BIGINT.writeLong(builder, 42L);
    }
    Page page = new Page(builder.build());

    DqeDataPage dataPage = new DqeDataPage(page);
    assertTrue(
        dataPage.getCompressedSizeInBytes() < dataPage.getUncompressedSizeInBytes(),
        "Compressed size should be less than uncompressed for repetitive data");
  }

  @Test
  @DisplayName("DqeDataPage constructor rejects null page")
  void constructorRejectsNullPage() {
    assertThrows(NullPointerException.class, () -> new DqeDataPage((Page) null));
  }

  @Test
  @DisplayName("DqeDataPage getPage returns same page on sender side")
  void getPageReturnsSamePageOnSenderSide() {
    BlockBuilder builder = BigintType.BIGINT.createBlockBuilder(null, 1);
    BigintType.BIGINT.writeLong(builder, 99L);
    Page page = new Page(builder.build());

    DqeDataPage dataPage = new DqeDataPage(page);
    assertNotNull(dataPage.getPage());
    assertEquals(1, dataPage.getPage().getPositionCount());
  }

  @Test
  @DisplayName("DqeDataPage with null values in block")
  void roundtripWithNullValues() throws IOException {
    BlockBuilder builder = BigintType.BIGINT.createBlockBuilder(null, 3);
    BigintType.BIGINT.writeLong(builder, 1L);
    builder.appendNull();
    BigintType.BIGINT.writeLong(builder, 3L);
    Block block = builder.build();
    Page page = new Page(block);

    DqeDataPage dataPage = new DqeDataPage(page);
    BytesStreamOutput out = new BytesStreamOutput();
    dataPage.writeTo(out);
    StreamInput in = out.bytes().streamInput();
    DqeDataPage deserialized = new DqeDataPage(in);

    Page result = deserialized.getPage();
    assertEquals(3, result.getPositionCount());
    assertEquals(1L, BigintType.BIGINT.getLong(result.getBlock(0), 0));
    assertTrue(result.getBlock(0).isNull(1));
    assertEquals(3L, BigintType.BIGINT.getLong(result.getBlock(0), 2));
  }
}
