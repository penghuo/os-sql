/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.dqe.operator;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class SliceRangeHashSetTest {

  @Test
  @DisplayName("Empty set has size 0")
  void testEmptySet() {
    SliceRangeHashSet set = new SliceRangeHashSet(16);
    assertEquals(0, set.size());
  }

  @Test
  @DisplayName("Add distinct strings and get correct count")
  void testAddDistinctStrings() {
    SliceRangeHashSet set = new SliceRangeHashSet(16);
    Slice a = Slices.utf8Slice("hello");
    Slice b = Slices.utf8Slice("world");
    Slice c = Slices.utf8Slice("foo");

    assertTrue(set.add(a, 0, a.length()));
    assertTrue(set.add(b, 0, b.length()));
    assertTrue(set.add(c, 0, c.length()));
    assertEquals(3, set.size());
  }

  @Test
  @DisplayName("Duplicate adds return false and do not increase size")
  void testDuplicates() {
    SliceRangeHashSet set = new SliceRangeHashSet(16);
    Slice a = Slices.utf8Slice("hello");
    Slice aCopy = Slices.utf8Slice("hello"); // Same content, different object

    assertTrue(set.add(a, 0, a.length()));
    assertFalse(set.add(aCopy, 0, aCopy.length()));
    assertEquals(1, set.size());
  }

  @Test
  @DisplayName("Sub-slice ranges work correctly")
  void testSubSliceRanges() {
    SliceRangeHashSet set = new SliceRangeHashSet(16);
    // "helloworld" contains "hello" at [0,5) and "world" at [5,10)
    Slice combined = Slices.utf8Slice("helloworld");

    assertTrue(set.add(combined, 0, 5)); // "hello"
    assertTrue(set.add(combined, 5, 5)); // "world"
    assertEquals(2, set.size());

    // Adding "hello" again from a different slice should be duplicate
    Slice hello = Slices.utf8Slice("hello");
    assertFalse(set.add(hello, 0, hello.length()));
    assertEquals(2, set.size());
  }

  @Test
  @DisplayName("Empty string handling")
  void testEmptyString() {
    SliceRangeHashSet set = new SliceRangeHashSet(16);
    Slice empty = Slices.utf8Slice("");

    assertTrue(set.add(empty, 0, 0));
    assertFalse(set.add(empty, 0, 0));
    assertEquals(1, set.size());

    // Non-empty string should still work
    Slice a = Slices.utf8Slice("a");
    assertTrue(set.add(a, 0, a.length()));
    assertEquals(2, set.size());
  }

  @Test
  @DisplayName("Large number of entries triggers resize")
  void testResize() {
    SliceRangeHashSet set = new SliceRangeHashSet(16);
    int count = 10000;
    for (int i = 0; i < count; i++) {
      Slice s = Slices.utf8Slice("entry_" + i);
      assertTrue(set.add(s, 0, s.length()));
    }
    assertEquals(count, set.size());

    // Re-adding should all return false
    for (int i = 0; i < count; i++) {
      Slice s = Slices.utf8Slice("entry_" + i);
      assertFalse(set.add(s, 0, s.length()));
    }
    assertEquals(count, set.size());
  }

  @Test
  @DisplayName("Cross-slice dedup with shared backing array")
  void testCrossSliceDedup() {
    // Simulate two blocks from different shards with overlapping values
    SliceRangeHashSet set = new SliceRangeHashSet(100);
    Slice shard1 = Slices.utf8Slice("aaa bbb ccc");
    Slice shard2 = Slices.utf8Slice("bbb ddd eee");

    // Shard 1: add "aaa", "bbb", "ccc"
    assertTrue(set.add(shard1, 0, 3)); // "aaa"
    assertTrue(set.add(shard1, 4, 3)); // "bbb"
    assertTrue(set.add(shard1, 8, 3)); // "ccc"

    // Shard 2: "bbb" is duplicate, "ddd" and "eee" are new
    assertFalse(set.add(shard2, 0, 3)); // "bbb" — duplicate
    assertTrue(set.add(shard2, 4, 3)); // "ddd"
    assertTrue(set.add(shard2, 8, 3)); // "eee"

    assertEquals(5, set.size());
  }
}
