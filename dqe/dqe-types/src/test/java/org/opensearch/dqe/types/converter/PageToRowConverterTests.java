/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dqe.types.converter;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import io.airlift.slice.Slices;
import io.trino.spi.Page;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.type.BigintType;
import io.trino.spi.type.BooleanType;
import io.trino.spi.type.DoubleType;
import io.trino.spi.type.IntegerType;
import io.trino.spi.type.RealType;
import io.trino.spi.type.VarcharType;
import java.util.List;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.opensearch.dqe.types.DqeTypes;

class PageToRowConverterTests {

  @Test
  @DisplayName("converts varchar and bigint columns correctly")
  void convertsVarcharAndBigint() {
    BlockBuilder varcharBuilder = VarcharType.VARCHAR.createBlockBuilder(null, 3);
    VarcharType.VARCHAR.writeSlice(varcharBuilder, Slices.utf8Slice("Alice"));
    VarcharType.VARCHAR.writeSlice(varcharBuilder, Slices.utf8Slice("Bob"));
    VarcharType.VARCHAR.writeSlice(varcharBuilder, Slices.utf8Slice("Charlie"));

    BlockBuilder bigintBuilder = BigintType.BIGINT.createBlockBuilder(null, 3);
    BigintType.BIGINT.writeLong(bigintBuilder, 30L);
    BigintType.BIGINT.writeLong(bigintBuilder, 25L);
    BigintType.BIGINT.writeLong(bigintBuilder, 35L);

    Page page = new Page(3, varcharBuilder.build(), bigintBuilder.build());
    List<List<Object>> rows =
        PageToRowConverter.convert(page, List.of(DqeTypes.VARCHAR, DqeTypes.BIGINT));

    assertEquals(3, rows.size());
    assertEquals("Alice", rows.get(0).get(0));
    assertEquals(30L, rows.get(0).get(1));
    assertEquals("Bob", rows.get(1).get(0));
    assertEquals(25L, rows.get(1).get(1));
    assertEquals("Charlie", rows.get(2).get(0));
    assertEquals(35L, rows.get(2).get(1));
  }

  @Test
  @DisplayName("handles null values correctly")
  void handlesNullValues() {
    BlockBuilder builder = BigintType.BIGINT.createBlockBuilder(null, 3);
    BigintType.BIGINT.writeLong(builder, 10L);
    builder.appendNull();
    BigintType.BIGINT.writeLong(builder, 30L);

    Page page = new Page(3, builder.build());
    List<List<Object>> rows = PageToRowConverter.convert(page, List.of(DqeTypes.BIGINT));

    assertEquals(3, rows.size());
    assertEquals(10L, rows.get(0).get(0));
    assertNull(rows.get(1).get(0));
    assertEquals(30L, rows.get(2).get(0));
  }

  @Test
  @DisplayName("converts integer type correctly")
  void convertsInteger() {
    BlockBuilder builder = IntegerType.INTEGER.createBlockBuilder(null, 2);
    IntegerType.INTEGER.writeLong(builder, 42);
    IntegerType.INTEGER.writeLong(builder, -7);

    Page page = new Page(2, builder.build());
    List<List<Object>> rows = PageToRowConverter.convert(page, List.of(DqeTypes.INTEGER));

    assertEquals(2, rows.size());
    assertEquals(42, rows.get(0).get(0));
    assertEquals(-7, rows.get(1).get(0));
    // Verify it's an int, not a long
    assertEquals(Integer.class, rows.get(0).get(0).getClass());
  }

  @Test
  @DisplayName("converts double type correctly")
  void convertsDouble() {
    BlockBuilder builder = DoubleType.DOUBLE.createBlockBuilder(null, 2);
    DoubleType.DOUBLE.writeDouble(builder, 3.14);
    DoubleType.DOUBLE.writeDouble(builder, -2.718);

    Page page = new Page(2, builder.build());
    List<List<Object>> rows = PageToRowConverter.convert(page, List.of(DqeTypes.DOUBLE));

    assertEquals(3.14, (double) rows.get(0).get(0), 0.001);
    assertEquals(-2.718, (double) rows.get(1).get(0), 0.001);
  }

  @Test
  @DisplayName("converts boolean type correctly")
  void convertsBoolean() {
    BlockBuilder builder = BooleanType.BOOLEAN.createBlockBuilder(null, 2);
    BooleanType.BOOLEAN.writeBoolean(builder, true);
    BooleanType.BOOLEAN.writeBoolean(builder, false);

    Page page = new Page(2, builder.build());
    List<List<Object>> rows = PageToRowConverter.convert(page, List.of(DqeTypes.BOOLEAN));

    assertEquals(true, rows.get(0).get(0));
    assertEquals(false, rows.get(1).get(0));
  }

  @Test
  @DisplayName("converts real (float) type correctly")
  void convertsReal() {
    BlockBuilder builder = RealType.REAL.createBlockBuilder(null, 1);
    RealType.REAL.writeLong(builder, Float.floatToIntBits(1.5f));

    Page page = new Page(1, builder.build());
    List<List<Object>> rows = PageToRowConverter.convert(page, List.of(DqeTypes.REAL));

    assertEquals(1.5f, (float) rows.get(0).get(0), 0.001f);
  }

  @Test
  @DisplayName("handles empty page")
  void handlesEmptyPage() {
    BlockBuilder builder = BigintType.BIGINT.createBlockBuilder(null, 0);
    Page page = new Page(0, builder.build());
    List<List<Object>> rows = PageToRowConverter.convert(page, List.of(DqeTypes.BIGINT));

    assertEquals(0, rows.size());
  }

  @Test
  @DisplayName("converts multi-column page correctly")
  void convertsMultiColumn() {
    BlockBuilder nameBuilder = VarcharType.VARCHAR.createBlockBuilder(null, 2);
    VarcharType.VARCHAR.writeSlice(nameBuilder, Slices.utf8Slice("x"));
    VarcharType.VARCHAR.writeSlice(nameBuilder, Slices.utf8Slice("y"));

    BlockBuilder ageBuilder = IntegerType.INTEGER.createBlockBuilder(null, 2);
    IntegerType.INTEGER.writeLong(ageBuilder, 1);
    IntegerType.INTEGER.writeLong(ageBuilder, 2);

    BlockBuilder activeBuilder = BooleanType.BOOLEAN.createBlockBuilder(null, 2);
    BooleanType.BOOLEAN.writeBoolean(activeBuilder, true);
    BooleanType.BOOLEAN.writeBoolean(activeBuilder, false);

    Page page = new Page(2, nameBuilder.build(), ageBuilder.build(), activeBuilder.build());
    List<List<Object>> rows =
        PageToRowConverter.convert(
            page, List.of(DqeTypes.VARCHAR, DqeTypes.INTEGER, DqeTypes.BOOLEAN));

    assertEquals(2, rows.size());
    assertEquals(3, rows.get(0).size());
    assertEquals("x", rows.get(0).get(0));
    assertEquals(1, rows.get(0).get(1));
    assertEquals(true, rows.get(0).get(2));
  }
}
