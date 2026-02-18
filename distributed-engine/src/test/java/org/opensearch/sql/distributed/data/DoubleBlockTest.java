/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.distributed.data;

import static org.junit.jupiter.api.Assertions.*;

import java.util.Optional;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class DoubleBlockTest {

  @Test
  @DisplayName("Create DoubleBlock, verify values")
  void testBasicDoubleBlock() {
    double[] values = {1.1, 2.2, 3.3};
    DoubleArrayBlock block = new DoubleArrayBlock(3, Optional.empty(), values);

    assertEquals(3, block.getPositionCount());
    assertEquals(1.1, block.getDouble(0), 1e-10);
    assertEquals(2.2, block.getDouble(1), 1e-10);
    assertEquals(3.3, block.getDouble(2), 1e-10);
  }

  @Test
  @DisplayName("DoubleBlock IEEE 754 edge cases: NaN, Inf, -0")
  void testIeee754EdgeCases() {
    double[] values = {Double.NaN, Double.POSITIVE_INFINITY, Double.NEGATIVE_INFINITY, -0.0};
    DoubleArrayBlock block = new DoubleArrayBlock(4, Optional.empty(), values);

    assertTrue(Double.isNaN(block.getDouble(0)));
    assertEquals(Double.POSITIVE_INFINITY, block.getDouble(1));
    assertEquals(Double.NEGATIVE_INFINITY, block.getDouble(2));
    assertEquals(-0.0, block.getDouble(3));
    // -0.0 and +0.0 are == but have different bit patterns
    assertEquals(Double.doubleToRawLongBits(-0.0), Double.doubleToRawLongBits(block.getDouble(3)));
  }

  @Test
  @DisplayName("DoubleBlock with nulls")
  void testDoubleBlockWithNulls() {
    double[] values = {1.0, 0.0, 3.0};
    boolean[] nulls = {false, true, false};
    DoubleArrayBlock block = new DoubleArrayBlock(3, Optional.of(nulls), values);

    assertTrue(block.mayHaveNull());
    assertFalse(block.isNull(0));
    assertTrue(block.isNull(1));
    assertFalse(block.isNull(2));
  }

  @Test
  @DisplayName("DoubleBlock getRegion")
  void testGetRegion() {
    double[] values = {1.0, 2.0, 3.0, 4.0};
    DoubleArrayBlock block = new DoubleArrayBlock(4, Optional.empty(), values);
    DoubleArrayBlock region = block.getRegion(1, 2);

    assertEquals(2, region.getPositionCount());
    assertEquals(2.0, region.getDouble(0), 1e-10);
    assertEquals(3.0, region.getDouble(1), 1e-10);
  }

  @Test
  @DisplayName("DoubleBlock getSizeInBytes")
  void testGetSizeInBytes() {
    double[] values = {1.0, 2.0};
    DoubleArrayBlock block = new DoubleArrayBlock(2, Optional.empty(), values);
    assertEquals(2 * Double.BYTES, block.getSizeInBytes());
  }
}
