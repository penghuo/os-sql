/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.distributed.data;

import static org.junit.jupiter.api.Assertions.*;

import java.util.Optional;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class RunLengthEncodedBlockTest {

  @Test
  @DisplayName("Create RLE block for repeated values")
  void testAllPositionsReturnSameValue() {
    LongArrayBlock singleValue = new LongArrayBlock(1, Optional.empty(), new long[] {42L});
    RunLengthEncodedBlock block = new RunLengthEncodedBlock(singleValue, 1000);

    assertEquals(1000, block.getPositionCount());
    assertFalse(block.isNull(0));
    assertFalse(block.isNull(500));
    assertFalse(block.isNull(999));
  }

  @Test
  @DisplayName("RLE block with null value")
  void testRleWithNull() {
    LongArrayBlock nullValue =
        new LongArrayBlock(1, Optional.of(new boolean[] {true}), new long[] {0L});
    RunLengthEncodedBlock block = new RunLengthEncodedBlock(nullValue, 100);

    assertTrue(block.mayHaveNull());
    assertTrue(block.isNull(0));
    assertTrue(block.isNull(50));
    assertTrue(block.isNull(99));
  }

  @Test
  @DisplayName("RLE block getRegion returns RLE")
  void testGetRegion() {
    LongArrayBlock singleValue = new LongArrayBlock(1, Optional.empty(), new long[] {7L});
    RunLengthEncodedBlock block = new RunLengthEncodedBlock(singleValue, 100);
    RunLengthEncodedBlock region = block.getRegion(10, 50);

    assertEquals(50, region.getPositionCount());
    assertSame(singleValue, region.getValue());
  }

  @Test
  @DisplayName("RLE block getSingleValueBlock returns the value block")
  void testGetSingleValueBlock() {
    LongArrayBlock singleValue = new LongArrayBlock(1, Optional.empty(), new long[] {99L});
    RunLengthEncodedBlock block = new RunLengthEncodedBlock(singleValue, 10);
    Block single = block.getSingleValueBlock(5);

    assertSame(singleValue, single);
  }

  @Test
  @DisplayName("RLE block getSizeInBytes is compact")
  void testGetSizeInBytes() {
    LongArrayBlock singleValue = new LongArrayBlock(1, Optional.empty(), new long[] {42L});
    RunLengthEncodedBlock block = new RunLengthEncodedBlock(singleValue, 10000);

    // Size should be constant regardless of positionCount
    assertEquals(singleValue.getSizeInBytes(), block.getSizeInBytes());
  }

  @Test
  @DisplayName("RLE block requires exactly 1 position in value")
  void testInvalidValueBlock() {
    LongArrayBlock twoValues = new LongArrayBlock(2, Optional.empty(), new long[] {1L, 2L});
    assertThrows(IllegalArgumentException.class, () -> new RunLengthEncodedBlock(twoValues, 10));
  }

  @Test
  @DisplayName("RLE block getChildren returns value block")
  void testGetChildren() {
    LongArrayBlock singleValue = new LongArrayBlock(1, Optional.empty(), new long[] {1L});
    RunLengthEncodedBlock block = new RunLengthEncodedBlock(singleValue, 5);

    assertEquals(1, block.getChildren().size());
    assertSame(singleValue, block.getChildren().get(0));
  }
}
