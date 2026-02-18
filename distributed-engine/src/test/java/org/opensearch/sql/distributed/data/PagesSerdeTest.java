/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.distributed.data;

import static org.junit.jupiter.api.Assertions.*;

import java.nio.charset.StandardCharsets;
import java.util.Optional;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class PagesSerdeTest {

  private final PagesSerde serde = new PagesSerde();

  @Test
  @DisplayName("Round-trip: Page with LongArrayBlock")
  void testRoundTripLongBlock() {
    LongArrayBlock block = new LongArrayBlock(3, Optional.empty(), new long[] {10L, 20L, 30L});
    Page original = new Page(block);

    byte[] bytes = serde.serialize(original);
    Page deserialized = serde.deserialize(bytes);

    assertEquals(original.getPositionCount(), deserialized.getPositionCount());
    assertEquals(original.getChannelCount(), deserialized.getChannelCount());
    LongArrayBlock result = (LongArrayBlock) deserialized.getBlock(0);
    assertEquals(10L, result.getLong(0));
    assertEquals(20L, result.getLong(1));
    assertEquals(30L, result.getLong(2));
  }

  @Test
  @DisplayName("Round-trip: Null handling preserved")
  void testRoundTripNullHandling() {
    boolean[] nulls = {false, true, false};
    LongArrayBlock block = new LongArrayBlock(3, Optional.of(nulls), new long[] {10L, 0L, 30L});
    Page original = new Page(block);

    Page deserialized = serde.deserialize(serde.serialize(original));
    LongArrayBlock result = (LongArrayBlock) deserialized.getBlock(0);

    assertTrue(result.mayHaveNull());
    assertFalse(result.isNull(0));
    assertTrue(result.isNull(1));
    assertFalse(result.isNull(2));
  }

  @Test
  @DisplayName("Round-trip: Empty Page (0 positions)")
  void testRoundTripEmptyPage() {
    LongArrayBlock block = new LongArrayBlock(0, Optional.empty(), new long[0]);
    Page original = new Page(block);

    Page deserialized = serde.deserialize(serde.serialize(original));
    assertEquals(0, deserialized.getPositionCount());
    assertEquals(1, deserialized.getChannelCount());
    assertEquals(0, deserialized.getBlock(0).getPositionCount());
  }

  @Test
  @DisplayName("Round-trip: Page with all block types")
  void testRoundTripAllTypes() {
    LongArrayBlock longBlock = new LongArrayBlock(2, Optional.empty(), new long[] {1L, 2L});
    IntArrayBlock intBlock = new IntArrayBlock(2, Optional.empty(), new int[] {3, 4});
    DoubleArrayBlock doubleBlock =
        new DoubleArrayBlock(2, Optional.empty(), new double[] {5.5, 6.6});
    ByteArrayBlock byteBlock = new ByteArrayBlock(2, Optional.empty(), new byte[] {7, 8});
    ShortArrayBlock shortBlock = new ShortArrayBlock(2, Optional.empty(), new short[] {9, 10});
    BooleanArrayBlock boolBlock =
        new BooleanArrayBlock(2, Optional.empty(), new boolean[] {true, false});
    byte[] data = "hiworld".getBytes(StandardCharsets.UTF_8);
    VariableWidthBlock varBlock =
        new VariableWidthBlock(2, data, new int[] {0, 2, 7}, Optional.empty());

    Page original =
        new Page(longBlock, intBlock, doubleBlock, byteBlock, shortBlock, boolBlock, varBlock);
    Page deserialized = serde.deserialize(serde.serialize(original));

    assertEquals(7, deserialized.getChannelCount());
    assertEquals(2, deserialized.getPositionCount());

    LongArrayBlock rl = (LongArrayBlock) deserialized.getBlock(0);
    assertEquals(1L, rl.getLong(0));
    assertEquals(2L, rl.getLong(1));

    IntArrayBlock ri = (IntArrayBlock) deserialized.getBlock(1);
    assertEquals(3, ri.getInt(0));
    assertEquals(4, ri.getInt(1));

    DoubleArrayBlock rd = (DoubleArrayBlock) deserialized.getBlock(2);
    assertEquals(5.5, rd.getDouble(0), 1e-10);
    assertEquals(6.6, rd.getDouble(1), 1e-10);

    ByteArrayBlock rb = (ByteArrayBlock) deserialized.getBlock(3);
    assertEquals(7, rb.getByte(0));
    assertEquals(8, rb.getByte(1));

    ShortArrayBlock rs = (ShortArrayBlock) deserialized.getBlock(4);
    assertEquals(9, rs.getShort(0));
    assertEquals(10, rs.getShort(1));

    BooleanArrayBlock rbl = (BooleanArrayBlock) deserialized.getBlock(5);
    assertTrue(rbl.getBoolean(0));
    assertFalse(rbl.getBoolean(1));

    VariableWidthBlock rv = (VariableWidthBlock) deserialized.getBlock(6);
    assertEquals("hi", rv.getString(0));
    assertEquals("world", rv.getString(1));
  }

  @Test
  @DisplayName("Round-trip: DictionaryBlock")
  void testRoundTripDictionaryBlock() {
    LongArrayBlock dict = new LongArrayBlock(2, Optional.empty(), new long[] {100L, 200L});
    DictionaryBlock block = new DictionaryBlock(4, dict, new int[] {0, 1, 1, 0});
    Page original = new Page(block);

    Page deserialized = serde.deserialize(serde.serialize(original));
    DictionaryBlock result = (DictionaryBlock) deserialized.getBlock(0);

    assertEquals(4, result.getPositionCount());
    assertEquals(0, result.getId(0));
    assertEquals(1, result.getId(1));
    assertEquals(1, result.getId(2));
    assertEquals(0, result.getId(3));
  }

  @Test
  @DisplayName("Round-trip: RunLengthEncodedBlock")
  void testRoundTripRleBlock() {
    LongArrayBlock value = new LongArrayBlock(1, Optional.empty(), new long[] {42L});
    RunLengthEncodedBlock block = new RunLengthEncodedBlock(value, 100);
    Page original = new Page(block);

    Page deserialized = serde.deserialize(serde.serialize(original));
    RunLengthEncodedBlock result = (RunLengthEncodedBlock) deserialized.getBlock(0);

    assertEquals(100, result.getPositionCount());
    assertFalse(result.isNull(0));
  }

  @Test
  @DisplayName("Round-trip: Large Page with 100K+ positions")
  void testRoundTripLargePage() {
    int size = 100_000;
    long[] values = new long[size];
    for (int i = 0; i < size; i++) {
      values[i] = i;
    }
    LongArrayBlock block = new LongArrayBlock(size, Optional.empty(), values);
    Page original = new Page(block);

    Page deserialized = serde.deserialize(serde.serialize(original));
    assertEquals(size, deserialized.getPositionCount());
    LongArrayBlock result = (LongArrayBlock) deserialized.getBlock(0);
    assertEquals(0L, result.getLong(0));
    assertEquals(99999L, result.getLong(99999));
  }
}
