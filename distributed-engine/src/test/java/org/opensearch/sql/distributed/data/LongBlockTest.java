/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.distributed.data;

import static org.junit.jupiter.api.Assertions.*;

import java.util.Optional;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class LongBlockTest {

  @Test
  @DisplayName("Create LongBlock from array, verify get/positionCount")
  void testBasicLongBlock() {
    long[] values = {10L, 20L, 30L, 40L, 50L};
    LongArrayBlock block = new LongArrayBlock(5, Optional.empty(), values);

    assertEquals(5, block.getPositionCount());
    assertEquals(10L, block.getLong(0));
    assertEquals(30L, block.getLong(2));
    assertEquals(50L, block.getLong(4));
    assertFalse(block.mayHaveNull());
    assertFalse(block.isNull(0));
  }

  @Test
  @DisplayName("LongBlock with null positions")
  void testLongBlockWithNulls() {
    long[] values = {10L, 0L, 30L};
    boolean[] nulls = {false, true, false};
    LongArrayBlock block = new LongArrayBlock(3, Optional.of(nulls), values);

    assertEquals(3, block.getPositionCount());
    assertTrue(block.mayHaveNull());
    assertFalse(block.isNull(0));
    assertTrue(block.isNull(1));
    assertFalse(block.isNull(2));
    assertEquals(10L, block.getLong(0));
    assertEquals(30L, block.getLong(2));
  }

  @Test
  @DisplayName("LongBlock getRegion returns a view")
  void testGetRegion() {
    long[] values = {1L, 2L, 3L, 4L, 5L};
    LongArrayBlock block = new LongArrayBlock(5, Optional.empty(), values);
    LongArrayBlock region = block.getRegion(1, 3);

    assertEquals(3, region.getPositionCount());
    assertEquals(2L, region.getLong(0));
    assertEquals(3L, region.getLong(1));
    assertEquals(4L, region.getLong(2));
  }

  @Test
  @DisplayName("LongBlock getSingleValueBlock")
  void testGetSingleValueBlock() {
    long[] values = {100L, 200L, 300L};
    LongArrayBlock block = new LongArrayBlock(3, Optional.empty(), values);
    LongArrayBlock single = block.getSingleValueBlock(1);

    assertEquals(1, single.getPositionCount());
    assertEquals(200L, single.getLong(0));
  }

  @Test
  @DisplayName("LongBlock getSingleValueBlock with null")
  void testGetSingleValueBlockNull() {
    long[] values = {100L, 0L, 300L};
    boolean[] nulls = {false, true, false};
    LongArrayBlock block = new LongArrayBlock(3, Optional.of(nulls), values);
    LongArrayBlock single = block.getSingleValueBlock(1);

    assertEquals(1, single.getPositionCount());
    assertTrue(single.isNull(0));
  }

  @Test
  @DisplayName("LongBlock copyPositions")
  void testCopyPositions() {
    long[] values = {10L, 20L, 30L, 40L, 50L};
    LongArrayBlock block = new LongArrayBlock(5, Optional.empty(), values);
    LongArrayBlock copy = block.copyPositions(new int[] {0, 2, 4}, 0, 3);

    assertEquals(3, copy.getPositionCount());
    assertEquals(10L, copy.getLong(0));
    assertEquals(30L, copy.getLong(1));
    assertEquals(50L, copy.getLong(2));
  }

  @Test
  @DisplayName("LongBlock getSizeInBytes")
  void testGetSizeInBytes() {
    long[] values = {1L, 2L, 3L};
    LongArrayBlock block = new LongArrayBlock(3, Optional.empty(), values);
    assertEquals(3 * Long.BYTES, block.getSizeInBytes());

    boolean[] nulls = {false, true, false};
    LongArrayBlock blockWithNulls = new LongArrayBlock(3, Optional.of(nulls), values);
    assertEquals(3 * Long.BYTES + 3, blockWithNulls.getSizeInBytes());
  }

  @Test
  @DisplayName("LongBlock bounds check")
  void testBoundsCheck() {
    long[] values = {1L, 2L};
    LongArrayBlock block = new LongArrayBlock(2, Optional.empty(), values);
    assertThrows(IndexOutOfBoundsException.class, () -> block.getLong(2));
    assertThrows(IndexOutOfBoundsException.class, () -> block.getLong(-1));
  }

  @Test
  @DisplayName("Empty LongBlock")
  void testEmptyBlock() {
    LongArrayBlock block = new LongArrayBlock(0, Optional.empty(), new long[0]);
    assertEquals(0, block.getPositionCount());
    assertEquals(0, block.getSizeInBytes());
  }
}
