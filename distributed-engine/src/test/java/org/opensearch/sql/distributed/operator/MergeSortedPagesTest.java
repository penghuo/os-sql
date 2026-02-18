/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.distributed.operator;

import static org.junit.jupiter.api.Assertions.*;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.opensearch.sql.distributed.data.LongArrayBlock;
import org.opensearch.sql.distributed.data.Page;

class MergeSortedPagesTest {

  private static final PageWithPositionComparator ASC_LONG =
      new SimplePageWithPositionComparator(List.of(0), List.of(SortOrder.ASC_NULLS_LAST));

  @Test
  @DisplayName("Merge 2 sorted streams")
  void testMergeTwoStreams() {
    Page stream1 = createLongPage(1, 3, 5, 7);
    Page stream2 = createLongPage(2, 4, 6, 8);

    Iterator<Page> merged =
        MergeSortedPages.mergeSortedPages(
            List.of(List.of(stream1).iterator(), List.of(stream2).iterator()),
            ASC_LONG,
            new int[] {0});

    List<Long> values = collectLongValues(merged);
    assertEquals(List.of(1L, 2L, 3L, 4L, 5L, 6L, 7L, 8L), values);
  }

  @Test
  @DisplayName("Merge 10 sorted streams")
  void testMergeTenStreams() {
    List<Iterator<Page>> streams = new ArrayList<>();
    for (int i = 0; i < 10; i++) {
      streams.add(List.of(createLongPage(i, i + 10, i + 20)).iterator());
    }

    Iterator<Page> merged = MergeSortedPages.mergeSortedPages(streams, ASC_LONG, new int[] {0});

    List<Long> values = collectLongValues(merged);
    assertEquals(30, values.size());
    // Verify sorted
    for (int i = 1; i < values.size(); i++) {
      assertTrue(
          values.get(i) >= values.get(i - 1),
          "Not sorted at index " + i + ": " + values.get(i - 1) + " > " + values.get(i));
    }
  }

  @Test
  @DisplayName("Merge with empty streams")
  void testMergeWithEmptyStreams() {
    Page stream1 = createLongPage(1, 3, 5);
    Page emptyPage = new Page(0, new LongArrayBlock(0, Optional.empty(), new long[0]));

    Iterator<Page> merged =
        MergeSortedPages.mergeSortedPages(
            List.of(
                List.of(stream1).iterator(),
                List.of(emptyPage).iterator(),
                List.of(createLongPage(2, 4)).iterator()),
            ASC_LONG,
            new int[] {0});

    List<Long> values = collectLongValues(merged);
    assertEquals(List.of(1L, 2L, 3L, 4L, 5L), values);
  }

  @Test
  @DisplayName("Merge single-element streams")
  void testMergeSingleElementStreams() {
    Iterator<Page> merged =
        MergeSortedPages.mergeSortedPages(
            List.of(
                List.of(createLongPage(3)).iterator(),
                List.of(createLongPage(1)).iterator(),
                List.of(createLongPage(2)).iterator()),
            ASC_LONG,
            new int[] {0});

    List<Long> values = collectLongValues(merged);
    assertEquals(List.of(1L, 2L, 3L), values);
  }

  @Test
  @DisplayName("Merge all-duplicate streams")
  void testMergeAllDuplicates() {
    Iterator<Page> merged =
        MergeSortedPages.mergeSortedPages(
            List.of(
                List.of(createLongPage(5, 5, 5)).iterator(),
                List.of(createLongPage(5, 5)).iterator()),
            ASC_LONG,
            new int[] {0});

    List<Long> values = collectLongValues(merged);
    assertEquals(5, values.size());
    assertTrue(values.stream().allMatch(v -> v == 5));
  }

  @Test
  @DisplayName("Merge empty stream list")
  void testMergeEmptyStreamList() {
    Iterator<Page> merged = MergeSortedPages.mergeSortedPages(List.of(), ASC_LONG, new int[] {0});
    assertFalse(merged.hasNext());
  }

  @Test
  @DisplayName("Single stream optimization")
  void testSingleStream() {
    Page page = createLongPage(1, 2, 3);
    Iterator<Page> merged =
        MergeSortedPages.mergeSortedPages(
            List.of(List.of(page).iterator()), ASC_LONG, new int[] {0});

    List<Long> values = collectLongValues(merged);
    assertEquals(List.of(1L, 2L, 3L), values);
  }

  // --- Helper methods ---

  private static Page createLongPage(long... values) {
    LongArrayBlock block = new LongArrayBlock(values.length, Optional.empty(), values);
    return new Page(block);
  }

  private static List<Long> collectLongValues(Iterator<Page> pages) {
    List<Long> values = new ArrayList<>();
    while (pages.hasNext()) {
      Page page = pages.next();
      LongArrayBlock block = (LongArrayBlock) page.getBlock(0);
      for (int i = 0; i < page.getPositionCount(); i++) {
        values.add(block.getLong(i));
      }
    }
    return values;
  }
}
