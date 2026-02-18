/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.distributed.data;

import static org.junit.jupiter.api.Assertions.*;

import java.util.Optional;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class BooleanBlockTest {

  @Test
  @DisplayName("Create BooleanBlock with true/false values")
  void testBasicBooleanBlock() {
    boolean[] values = {true, false, true, false};
    BooleanArrayBlock block = new BooleanArrayBlock(4, Optional.empty(), values);

    assertEquals(4, block.getPositionCount());
    assertTrue(block.getBoolean(0));
    assertFalse(block.getBoolean(1));
    assertTrue(block.getBoolean(2));
    assertFalse(block.getBoolean(3));
  }

  @Test
  @DisplayName("BooleanBlock with true/false/null combinations")
  void testBooleanBlockWithNulls() {
    boolean[] values = {true, false, false};
    boolean[] nulls = {false, true, false};
    BooleanArrayBlock block = new BooleanArrayBlock(3, Optional.of(nulls), values);

    assertTrue(block.mayHaveNull());
    assertFalse(block.isNull(0));
    assertTrue(block.getBoolean(0));
    assertTrue(block.isNull(1));
    assertFalse(block.isNull(2));
    assertFalse(block.getBoolean(2));
  }

  @Test
  @DisplayName("BooleanBlock getRegion")
  void testGetRegion() {
    boolean[] values = {true, false, true, false, true};
    BooleanArrayBlock block = new BooleanArrayBlock(5, Optional.empty(), values);
    BooleanArrayBlock region = block.getRegion(2, 2);

    assertEquals(2, region.getPositionCount());
    assertTrue(region.getBoolean(0));
    assertFalse(region.getBoolean(1));
  }

  @Test
  @DisplayName("BooleanBlock copyPositions")
  void testCopyPositions() {
    boolean[] values = {true, false, true, false, true};
    BooleanArrayBlock block = new BooleanArrayBlock(5, Optional.empty(), values);
    BooleanArrayBlock copy = block.copyPositions(new int[] {1, 3}, 0, 2);

    assertEquals(2, copy.getPositionCount());
    assertFalse(copy.getBoolean(0));
    assertFalse(copy.getBoolean(1));
  }
}
