/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.dqe.common.serde;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.airlift.slice.Slices;
import io.trino.spi.Page;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.type.BigintType;
import io.trino.spi.type.BooleanType;
import io.trino.spi.type.DoubleType;
import io.trino.spi.type.IntegerType;
import io.trino.spi.type.Type;
import io.trino.spi.type.VarcharType;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.List;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.core.common.io.stream.InputStreamStreamInput;

@DisplayName("PageSerializer round-trip serialization")
class PageSerializerTest {

  @Test
  @DisplayName("Round-trip a Page with BIGINT and VARCHAR columns")
  void roundTripBigintAndVarchar() throws IOException {
    // Build a Page with 2 columns: BIGINT and VARCHAR
    BlockBuilder bigintBuilder = BigintType.BIGINT.createBlockBuilder(null, 3);
    BigintType.BIGINT.writeLong(bigintBuilder, 100L);
    BigintType.BIGINT.writeLong(bigintBuilder, 200L);
    BigintType.BIGINT.writeLong(bigintBuilder, 300L);

    BlockBuilder varcharBuilder = VarcharType.VARCHAR.createBlockBuilder(null, 3);
    VarcharType.VARCHAR.writeSlice(varcharBuilder, Slices.utf8Slice("alpha"));
    VarcharType.VARCHAR.writeSlice(varcharBuilder, Slices.utf8Slice("beta"));
    VarcharType.VARCHAR.writeSlice(varcharBuilder, Slices.utf8Slice("gamma"));

    Page original = new Page(bigintBuilder.build(), varcharBuilder.build());
    List<Type> columnTypes = List.of(BigintType.BIGINT, VarcharType.VARCHAR);

    // Serialize
    BytesStreamOutput out = new BytesStreamOutput();
    PageSerializer.writePage(out, original, columnTypes);

    // Deserialize
    Page deserialized =
        PageSerializer.readPage(
            new InputStreamStreamInput(new ByteArrayInputStream(out.bytes().toBytesRef().bytes)));

    // Verify
    assertEquals(2, deserialized.getChannelCount());
    assertEquals(3, deserialized.getPositionCount());

    Block bigintBlock = deserialized.getBlock(0);
    assertEquals(100L, BigintType.BIGINT.getLong(bigintBlock, 0));
    assertEquals(200L, BigintType.BIGINT.getLong(bigintBlock, 1));
    assertEquals(300L, BigintType.BIGINT.getLong(bigintBlock, 2));

    Block varcharBlock = deserialized.getBlock(1);
    assertEquals("alpha", VarcharType.VARCHAR.getSlice(varcharBlock, 0).toStringUtf8());
    assertEquals("beta", VarcharType.VARCHAR.getSlice(varcharBlock, 1).toStringUtf8());
    assertEquals("gamma", VarcharType.VARCHAR.getSlice(varcharBlock, 2).toStringUtf8());
  }

  @Test
  @DisplayName("Round-trip a Page with DOUBLE and BOOLEAN columns")
  void roundTripDoubleAndBoolean() throws IOException {
    BlockBuilder doubleBuilder = DoubleType.DOUBLE.createBlockBuilder(null, 2);
    DoubleType.DOUBLE.writeDouble(doubleBuilder, 3.14);
    DoubleType.DOUBLE.writeDouble(doubleBuilder, 2.71);

    BlockBuilder boolBuilder = BooleanType.BOOLEAN.createBlockBuilder(null, 2);
    BooleanType.BOOLEAN.writeBoolean(boolBuilder, true);
    BooleanType.BOOLEAN.writeBoolean(boolBuilder, false);

    Page original = new Page(doubleBuilder.build(), boolBuilder.build());
    List<Type> columnTypes = List.of(DoubleType.DOUBLE, BooleanType.BOOLEAN);

    BytesStreamOutput out = new BytesStreamOutput();
    PageSerializer.writePage(out, original, columnTypes);

    Page deserialized =
        PageSerializer.readPage(
            new InputStreamStreamInput(new ByteArrayInputStream(out.bytes().toBytesRef().bytes)));

    assertEquals(2, deserialized.getChannelCount());
    assertEquals(2, deserialized.getPositionCount());

    assertEquals(3.14, DoubleType.DOUBLE.getDouble(deserialized.getBlock(0), 0), 0.001);
    assertEquals(2.71, DoubleType.DOUBLE.getDouble(deserialized.getBlock(0), 1), 0.001);

    assertTrue(BooleanType.BOOLEAN.getBoolean(deserialized.getBlock(1), 0));
    assertFalse(BooleanType.BOOLEAN.getBoolean(deserialized.getBlock(1), 1));
  }

  @Test
  @DisplayName("Round-trip a Page with INTEGER column")
  void roundTripInteger() throws IOException {
    BlockBuilder intBuilder = IntegerType.INTEGER.createBlockBuilder(null, 2);
    IntegerType.INTEGER.writeLong(intBuilder, 42);
    IntegerType.INTEGER.writeLong(intBuilder, -7);

    Page original = new Page(intBuilder.build());
    List<Type> columnTypes = List.of(IntegerType.INTEGER);

    BytesStreamOutput out = new BytesStreamOutput();
    PageSerializer.writePage(out, original, columnTypes);

    Page deserialized =
        PageSerializer.readPage(
            new InputStreamStreamInput(new ByteArrayInputStream(out.bytes().toBytesRef().bytes)));

    assertEquals(1, deserialized.getChannelCount());
    assertEquals(2, deserialized.getPositionCount());

    assertEquals(42, IntegerType.INTEGER.getLong(deserialized.getBlock(0), 0));
    assertEquals(-7, IntegerType.INTEGER.getLong(deserialized.getBlock(0), 1));
  }

  @Test
  @DisplayName("Round-trip a Page with null values")
  void roundTripWithNulls() throws IOException {
    BlockBuilder bigintBuilder = BigintType.BIGINT.createBlockBuilder(null, 3);
    BigintType.BIGINT.writeLong(bigintBuilder, 10L);
    bigintBuilder.appendNull();
    BigintType.BIGINT.writeLong(bigintBuilder, 30L);

    Page original = new Page(bigintBuilder.build());
    List<Type> columnTypes = List.of(BigintType.BIGINT);

    BytesStreamOutput out = new BytesStreamOutput();
    PageSerializer.writePage(out, original, columnTypes);

    Page deserialized =
        PageSerializer.readPage(
            new InputStreamStreamInput(new ByteArrayInputStream(out.bytes().toBytesRef().bytes)));

    assertEquals(1, deserialized.getChannelCount());
    assertEquals(3, deserialized.getPositionCount());

    assertFalse(deserialized.getBlock(0).isNull(0));
    assertEquals(10L, BigintType.BIGINT.getLong(deserialized.getBlock(0), 0));

    assertTrue(deserialized.getBlock(0).isNull(1));

    assertFalse(deserialized.getBlock(0).isNull(2));
    assertEquals(30L, BigintType.BIGINT.getLong(deserialized.getBlock(0), 2));
  }

  @Test
  @DisplayName("Round-trip an empty Page (zero rows)")
  void roundTripEmptyPage() throws IOException {
    BlockBuilder builder = BigintType.BIGINT.createBlockBuilder(null, 0);
    Page original = new Page(builder.build());
    List<Type> columnTypes = List.of(BigintType.BIGINT);

    BytesStreamOutput out = new BytesStreamOutput();
    PageSerializer.writePage(out, original, columnTypes);

    Page deserialized =
        PageSerializer.readPage(
            new InputStreamStreamInput(new ByteArrayInputStream(out.bytes().toBytesRef().bytes)));

    assertEquals(1, deserialized.getChannelCount());
    assertEquals(0, deserialized.getPositionCount());
  }

  @Test
  @DisplayName("Round-trip multiple Pages via writePages/readPages")
  void roundTripMultiplePages() throws IOException {
    List<Type> columnTypes = List.of(BigintType.BIGINT);

    BlockBuilder b1 = BigintType.BIGINT.createBlockBuilder(null, 2);
    BigintType.BIGINT.writeLong(b1, 1L);
    BigintType.BIGINT.writeLong(b1, 2L);

    BlockBuilder b2 = BigintType.BIGINT.createBlockBuilder(null, 2);
    BigintType.BIGINT.writeLong(b2, 3L);
    BigintType.BIGINT.writeLong(b2, 4L);

    List<Page> originals = List.of(new Page(b1.build()), new Page(b2.build()));

    BytesStreamOutput out = new BytesStreamOutput();
    PageSerializer.writePages(out, originals, columnTypes);

    List<Page> deserialized =
        PageSerializer.readPages(
            new InputStreamStreamInput(new ByteArrayInputStream(out.bytes().toBytesRef().bytes)));

    assertEquals(2, deserialized.size());

    assertEquals(2, deserialized.get(0).getPositionCount());
    assertEquals(1L, BigintType.BIGINT.getLong(deserialized.get(0).getBlock(0), 0));
    assertEquals(2L, BigintType.BIGINT.getLong(deserialized.get(0).getBlock(0), 1));

    assertEquals(2, deserialized.get(1).getPositionCount());
    assertEquals(3L, BigintType.BIGINT.getLong(deserialized.get(1).getBlock(0), 0));
    assertEquals(4L, BigintType.BIGINT.getLong(deserialized.get(1).getBlock(0), 1));
  }

  @Test
  @DisplayName("Round-trip empty page list via writePages/readPages")
  void roundTripEmptyPageList() throws IOException {
    List<Type> columnTypes = List.of(BigintType.BIGINT);
    List<Page> originals = List.of();

    BytesStreamOutput out = new BytesStreamOutput();
    PageSerializer.writePages(out, originals, columnTypes);

    List<Page> deserialized =
        PageSerializer.readPages(
            new InputStreamStreamInput(new ByteArrayInputStream(out.bytes().toBytesRef().bytes)));

    assertNotNull(deserialized);
    assertEquals(0, deserialized.size());
  }

  @Test
  @DisplayName("resolveType maps type signature strings back to Type singletons")
  void resolveTypeSignatures() {
    assertEquals(BigintType.BIGINT, PageSerializer.resolveType("bigint"));
    assertEquals(VarcharType.VARCHAR, PageSerializer.resolveType("varchar"));
    assertEquals(DoubleType.DOUBLE, PageSerializer.resolveType("double"));
    assertEquals(BooleanType.BOOLEAN, PageSerializer.resolveType("boolean"));
    assertEquals(IntegerType.INTEGER, PageSerializer.resolveType("integer"));
  }
}
