/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.distributed.data;

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class BlockBuilderTest {

  @Test
  @DisplayName("LongBlockBuilder produces correct LongArrayBlock")
  void testLongBlockBuilder() {
    LongBlockBuilder builder = new LongBlockBuilder();
    builder.appendLong(10L);
    builder.appendLong(20L);
    builder.appendNull();
    builder.appendLong(40L);

    assertEquals(4, builder.getPositionCount());
    LongArrayBlock block = builder.build();

    assertEquals(4, block.getPositionCount());
    assertEquals(10L, block.getLong(0));
    assertEquals(20L, block.getLong(1));
    assertTrue(block.isNull(2));
    assertEquals(40L, block.getLong(3));
  }

  @Test
  @DisplayName("IntBlockBuilder produces correct IntArrayBlock")
  void testIntBlockBuilder() {
    IntBlockBuilder builder = new IntBlockBuilder();
    builder.appendInt(1);
    builder.appendInt(2);
    builder.appendNull();

    IntArrayBlock block = builder.build();
    assertEquals(3, block.getPositionCount());
    assertEquals(1, block.getInt(0));
    assertEquals(2, block.getInt(1));
    assertTrue(block.isNull(2));
  }

  @Test
  @DisplayName("DoubleBlockBuilder produces correct DoubleArrayBlock")
  void testDoubleBlockBuilder() {
    DoubleBlockBuilder builder = new DoubleBlockBuilder();
    builder.appendDouble(1.5);
    builder.appendDouble(2.5);

    DoubleArrayBlock block = builder.build();
    assertEquals(2, block.getPositionCount());
    assertEquals(1.5, block.getDouble(0), 1e-10);
    assertEquals(2.5, block.getDouble(1), 1e-10);
    assertFalse(block.mayHaveNull());
  }

  @Test
  @DisplayName("BooleanBlockBuilder produces correct BooleanArrayBlock")
  void testBooleanBlockBuilder() {
    BooleanBlockBuilder builder = new BooleanBlockBuilder();
    builder.appendBoolean(true);
    builder.appendBoolean(false);
    builder.appendNull();

    BooleanArrayBlock block = builder.build();
    assertEquals(3, block.getPositionCount());
    assertTrue(block.getBoolean(0));
    assertFalse(block.getBoolean(1));
    assertTrue(block.isNull(2));
  }

  @Test
  @DisplayName("VariableWidthBlockBuilder produces correct VariableWidthBlock")
  void testVariableWidthBlockBuilder() {
    VariableWidthBlockBuilder builder = new VariableWidthBlockBuilder();
    builder.appendString("hello");
    builder.appendString("world");
    builder.appendNull();
    builder.appendString("");

    VariableWidthBlock block = builder.build();
    assertEquals(4, block.getPositionCount());
    assertEquals("hello", block.getString(0));
    assertEquals("world", block.getString(1));
    assertTrue(block.isNull(2));
    assertEquals("", block.getString(3));
    assertEquals(0, block.getSliceLength(3));
  }

  @Test
  @DisplayName("BlockBuilder with capacity growth")
  void testBlockBuilderGrowth() {
    LongBlockBuilder builder = new LongBlockBuilder(2);
    for (int i = 0; i < 1000; i++) {
      builder.appendLong(i);
    }

    LongArrayBlock block = builder.build();
    assertEquals(1000, block.getPositionCount());
    assertEquals(0L, block.getLong(0));
    assertEquals(999L, block.getLong(999));
  }

  @Test
  @DisplayName("Empty builder produces empty block")
  void testEmptyBuilder() {
    LongBlockBuilder builder = new LongBlockBuilder();
    LongArrayBlock block = builder.build();
    assertEquals(0, block.getPositionCount());
  }
}
