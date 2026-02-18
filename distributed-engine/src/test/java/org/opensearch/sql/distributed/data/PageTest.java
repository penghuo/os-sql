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

class PageTest {

  @Test
  @DisplayName("Create Page from multiple Blocks")
  void testPageCreation() {
    LongArrayBlock ages = new LongArrayBlock(3, Optional.empty(), new long[] {25, 30, 35});
    DoubleArrayBlock scores =
        new DoubleArrayBlock(3, Optional.empty(), new double[] {1.1, 2.2, 3.3});

    Page page = new Page(ages, scores);

    assertEquals(3, page.getPositionCount());
    assertEquals(2, page.getChannelCount());
    assertSame(ages, page.getBlock(0));
    assertSame(scores, page.getBlock(1));
  }

  @Test
  @DisplayName("Page positionCount consistent across blocks")
  void testPositionCountConsistency() {
    LongArrayBlock block1 = new LongArrayBlock(3, Optional.empty(), new long[] {1, 2, 3});
    LongArrayBlock block2 = new LongArrayBlock(3, Optional.empty(), new long[] {4, 5, 6});
    Page page = new Page(block1, block2);

    assertEquals(3, page.getPositionCount());
    assertEquals(page.getBlock(0).getPositionCount(), page.getBlock(1).getPositionCount());
  }

  @Test
  @DisplayName("Page rejects blocks with different position counts")
  void testMismatchedPositionCounts() {
    LongArrayBlock block1 = new LongArrayBlock(2, Optional.empty(), new long[] {1, 2});
    LongArrayBlock block2 = new LongArrayBlock(3, Optional.empty(), new long[] {4, 5, 6});

    assertThrows(IllegalArgumentException.class, () -> new Page(block1, block2));
  }

  @Test
  @DisplayName("Page getRegion returns correct subset")
  void testGetRegion() {
    LongArrayBlock ids = new LongArrayBlock(5, Optional.empty(), new long[] {1, 2, 3, 4, 5});
    byte[] data = "aaabbbcccdddee".getBytes(StandardCharsets.UTF_8);
    int[] offsets = {0, 3, 6, 9, 12, 14};
    VariableWidthBlock names = new VariableWidthBlock(5, data, offsets, Optional.empty());

    Page page = new Page(ids, names);
    Page region = page.getRegion(1, 3);

    assertEquals(3, region.getPositionCount());
    assertEquals(2, region.getChannelCount());
    LongArrayBlock regionIds = (LongArrayBlock) region.getBlock(0);
    assertEquals(2L, regionIds.getLong(0));
    assertEquals(3L, regionIds.getLong(1));
    assertEquals(4L, regionIds.getLong(2));
  }

  @Test
  @DisplayName("Page getColumns returns correct columns")
  void testGetColumns() {
    LongArrayBlock col0 = new LongArrayBlock(2, Optional.empty(), new long[] {1, 2});
    DoubleArrayBlock col1 = new DoubleArrayBlock(2, Optional.empty(), new double[] {3.0, 4.0});
    LongArrayBlock col2 = new LongArrayBlock(2, Optional.empty(), new long[] {5, 6});

    Page page = new Page(col0, col1, col2);
    Page projected = page.getColumns(2, 0);

    assertEquals(2, projected.getChannelCount());
    assertSame(col2, projected.getBlock(0));
    assertSame(col0, projected.getBlock(1));
  }

  @Test
  @DisplayName("Page appendColumn adds a column")
  void testAppendColumn() {
    LongArrayBlock col0 = new LongArrayBlock(3, Optional.empty(), new long[] {1, 2, 3});
    Page page = new Page(col0);
    DoubleArrayBlock col1 = new DoubleArrayBlock(3, Optional.empty(), new double[] {4.0, 5.0, 6.0});

    Page withExtra = page.appendColumn(col1);

    assertEquals(2, withExtra.getChannelCount());
    assertSame(col0, withExtra.getBlock(0));
    assertSame(col1, withExtra.getBlock(1));
  }

  @Test
  @DisplayName("Page prependColumn adds a column at start")
  void testPrependColumn() {
    LongArrayBlock col0 = new LongArrayBlock(3, Optional.empty(), new long[] {1, 2, 3});
    Page page = new Page(col0);
    DoubleArrayBlock col1 = new DoubleArrayBlock(3, Optional.empty(), new double[] {4.0, 5.0, 6.0});

    Page withPrepend = page.prependColumn(col1);

    assertEquals(2, withPrepend.getChannelCount());
    assertSame(col1, withPrepend.getBlock(0));
    assertSame(col0, withPrepend.getBlock(1));
  }

  @Test
  @DisplayName("Empty Page (0 positions)")
  void testEmptyPage() {
    Page page = new Page();
    assertEquals(0, page.getPositionCount());
    assertEquals(0, page.getChannelCount());
    assertEquals(0, page.getSizeInBytes());
  }

  @Test
  @DisplayName("Page getSizeInBytes sums block sizes")
  void testGetSizeInBytes() {
    LongArrayBlock col0 = new LongArrayBlock(3, Optional.empty(), new long[] {1, 2, 3});
    DoubleArrayBlock col1 = new DoubleArrayBlock(3, Optional.empty(), new double[] {4.0, 5.0, 6.0});
    Page page = new Page(col0, col1);

    assertEquals(col0.getSizeInBytes() + col1.getSizeInBytes(), page.getSizeInBytes());
  }
}
