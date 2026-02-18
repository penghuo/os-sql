/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.distributed.data;

import static org.junit.jupiter.api.Assertions.*;

import java.util.Optional;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class DictionaryBlockTest {

  @Test
  @DisplayName("Create dictionary-encoded block, verify transparent access")
  void testDictionaryBlock() {
    long[] dictValues = {100L, 200L, 300L};
    LongArrayBlock dictionary = new LongArrayBlock(3, Optional.empty(), dictValues);
    int[] ids = {0, 1, 2, 1, 0};
    DictionaryBlock block = new DictionaryBlock(5, dictionary, ids);

    assertEquals(5, block.getPositionCount());
    assertFalse(block.isNull(0));
    // Access through dictionary
    assertEquals(0, block.getId(0));
    assertEquals(1, block.getId(1));
    assertEquals(2, block.getId(2));
    assertEquals(1, block.getId(3));
    assertEquals(0, block.getId(4));
  }

  @Test
  @DisplayName("DictionaryBlock memory savings: dictionary smaller than full expansion")
  void testMemorySavings() {
    long[] dictValues = {42L, 99L};
    LongArrayBlock dictionary = new LongArrayBlock(2, Optional.empty(), dictValues);
    int[] ids = {0, 0, 0, 1, 1, 1, 0, 0, 1, 1};
    DictionaryBlock block = new DictionaryBlock(10, dictionary, ids);

    // Dictionary has 2 longs = 16 bytes, ids = 10*4 = 40 bytes. Total = 56
    // Full expansion would be 10 longs = 80 bytes
    assertTrue(block.getSizeInBytes() < 10 * Long.BYTES);
  }

  @Test
  @DisplayName("DictionaryBlock with null values in dictionary")
  void testDictionaryWithNulls() {
    long[] dictValues = {100L, 0L};
    boolean[] dictNulls = {false, true};
    LongArrayBlock dictionary = new LongArrayBlock(2, Optional.of(dictNulls), dictValues);
    int[] ids = {0, 1, 0};
    DictionaryBlock block = new DictionaryBlock(3, dictionary, ids);

    assertTrue(block.mayHaveNull());
    assertFalse(block.isNull(0));
    assertTrue(block.isNull(1));
    assertFalse(block.isNull(2));
  }

  @Test
  @DisplayName("DictionaryBlock getRegion")
  void testGetRegion() {
    long[] dictValues = {10L, 20L, 30L};
    LongArrayBlock dictionary = new LongArrayBlock(3, Optional.empty(), dictValues);
    int[] ids = {0, 1, 2, 1, 0};
    DictionaryBlock block = new DictionaryBlock(5, dictionary, ids);
    DictionaryBlock region = block.getRegion(1, 3);

    assertEquals(3, region.getPositionCount());
    assertEquals(1, region.getId(0));
    assertEquals(2, region.getId(1));
    assertEquals(1, region.getId(2));
  }

  @Test
  @DisplayName("DictionaryBlock getSingleValueBlock unwraps to ValueBlock")
  void testGetSingleValueBlock() {
    long[] dictValues = {42L, 99L};
    LongArrayBlock dictionary = new LongArrayBlock(2, Optional.empty(), dictValues);
    int[] ids = {0, 1};
    DictionaryBlock block = new DictionaryBlock(2, dictionary, ids);
    Block single = block.getSingleValueBlock(1);

    assertInstanceOf(LongArrayBlock.class, single);
    assertEquals(1, single.getPositionCount());
  }

  @Test
  @DisplayName("DictionaryBlock getChildren returns dictionary")
  void testGetChildren() {
    long[] dictValues = {1L};
    LongArrayBlock dictionary = new LongArrayBlock(1, Optional.empty(), dictValues);
    DictionaryBlock block = new DictionaryBlock(3, dictionary, new int[] {0, 0, 0});

    assertEquals(1, block.getChildren().size());
    assertSame(dictionary, block.getChildren().get(0));
  }
}
