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

class VariableWidthBlockTest {

  @Test
  @DisplayName("Create variable-width block, verify getString")
  void testBasicVariableWidthBlock() {
    byte[] data = "helloworld".getBytes(StandardCharsets.UTF_8);
    int[] offsets = {0, 5, 10};
    VariableWidthBlock block = new VariableWidthBlock(2, data, offsets, Optional.empty());

    assertEquals(2, block.getPositionCount());
    assertEquals("hello", block.getString(0));
    assertEquals("world", block.getString(1));
  }

  @Test
  @DisplayName("Variable-width block with empty strings")
  void testEmptyStrings() {
    byte[] data = "abc".getBytes(StandardCharsets.UTF_8);
    int[] offsets = {0, 0, 3, 3};
    VariableWidthBlock block = new VariableWidthBlock(3, data, offsets, Optional.empty());

    assertEquals(3, block.getPositionCount());
    assertEquals("", block.getString(0));
    assertEquals("abc", block.getString(1));
    assertEquals("", block.getString(2));
    assertEquals(0, block.getSliceLength(0));
    assertEquals(3, block.getSliceLength(1));
  }

  @Test
  @DisplayName("Variable-width block with nulls")
  void testVariableWidthWithNulls() {
    byte[] data = "test".getBytes(StandardCharsets.UTF_8);
    int[] offsets = {0, 4, 4};
    boolean[] nulls = {false, true};
    VariableWidthBlock block = new VariableWidthBlock(2, data, offsets, Optional.of(nulls));

    assertTrue(block.mayHaveNull());
    assertFalse(block.isNull(0));
    assertTrue(block.isNull(1));
    assertEquals("test", block.getString(0));
  }

  @Test
  @DisplayName("Variable-width block with UTF-8 multibyte chars")
  void testUtf8Multibyte() {
    String s = "\u00e9\u00e8\u00ea"; // é, è, ê
    byte[] data = s.getBytes(StandardCharsets.UTF_8);
    int[] offsets = {0, data.length};
    VariableWidthBlock block = new VariableWidthBlock(1, data, offsets, Optional.empty());

    assertEquals(s, block.getString(0));
  }

  @Test
  @DisplayName("Variable-width block getRegion")
  void testGetRegion() {
    byte[] data = "aaabbbccc".getBytes(StandardCharsets.UTF_8);
    int[] offsets = {0, 3, 6, 9};
    VariableWidthBlock block = new VariableWidthBlock(3, data, offsets, Optional.empty());
    VariableWidthBlock region = block.getRegion(1, 2);

    assertEquals(2, region.getPositionCount());
    assertEquals("bbb", region.getString(0));
    assertEquals("ccc", region.getString(1));
  }

  @Test
  @DisplayName("Variable-width block getSingleValueBlock")
  void testGetSingleValueBlock() {
    byte[] data = "foobar".getBytes(StandardCharsets.UTF_8);
    int[] offsets = {0, 3, 6};
    VariableWidthBlock block = new VariableWidthBlock(2, data, offsets, Optional.empty());
    VariableWidthBlock single = block.getSingleValueBlock(1);

    assertEquals(1, single.getPositionCount());
    assertEquals("bar", single.getString(0));
  }

  @Test
  @DisplayName("Variable-width block copyPositions")
  void testCopyPositions() {
    byte[] data = "aaabbbccc".getBytes(StandardCharsets.UTF_8);
    int[] offsets = {0, 3, 6, 9};
    VariableWidthBlock block = new VariableWidthBlock(3, data, offsets, Optional.empty());
    VariableWidthBlock copy = block.copyPositions(new int[] {2, 0}, 0, 2);

    assertEquals(2, copy.getPositionCount());
    assertEquals("ccc", copy.getString(0));
    assertEquals("aaa", copy.getString(1));
  }

  @Test
  @DisplayName("Variable-width block with large values")
  void testLargeValues() {
    String large = "x".repeat(10000);
    byte[] data = large.getBytes(StandardCharsets.UTF_8);
    int[] offsets = {0, data.length};
    VariableWidthBlock block = new VariableWidthBlock(1, data, offsets, Optional.empty());

    assertEquals(large, block.getString(0));
    assertEquals(10000, block.getSliceLength(0));
  }
}
