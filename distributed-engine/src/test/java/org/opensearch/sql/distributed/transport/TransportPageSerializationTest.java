/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.distributed.transport;

import static org.junit.jupiter.api.Assertions.*;

import java.util.List;
import java.util.Optional;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.opensearch.sql.distributed.data.DoubleArrayBlock;
import org.opensearch.sql.distributed.data.LongArrayBlock;
import org.opensearch.sql.distributed.data.Page;
import org.opensearch.sql.distributed.data.PagesSerde;
import org.opensearch.sql.distributed.data.VariableWidthBlock;

class TransportPageSerializationTest {

  private final PagesSerde serde = new PagesSerde();

  @Test
  @DisplayName("Page round-trip through ShardQueryResponse serialization")
  void testPageRoundTripViaResponse() {
    LongArrayBlock longBlock = new LongArrayBlock(3, Optional.empty(), new long[] {10, 20, 30});
    DoubleArrayBlock doubleBlock =
        new DoubleArrayBlock(3, Optional.empty(), new double[] {1.1, 2.2, 3.3});
    Page original = new Page(longBlock, doubleBlock);

    ShardQueryResponse response = new ShardQueryResponse(List.of(original), false, serde);
    List<Page> deserialized = response.getPages(serde);

    assertEquals(1, deserialized.size());
    Page result = deserialized.get(0);
    assertEquals(original.getPositionCount(), result.getPositionCount());
    assertEquals(original.getChannelCount(), result.getChannelCount());

    LongArrayBlock resultLong = (LongArrayBlock) result.getBlock(0);
    for (int i = 0; i < 3; i++) {
      assertEquals(longBlock.getLong(i), resultLong.getLong(i));
    }
  }

  @Test
  @DisplayName("Multiple pages serialize and deserialize correctly")
  void testMultiplePages() {
    Page page1 = new Page(new LongArrayBlock(2, Optional.empty(), new long[] {1, 2}));
    Page page2 = new Page(new LongArrayBlock(3, Optional.empty(), new long[] {3, 4, 5}));

    ShardQueryResponse response = new ShardQueryResponse(List.of(page1, page2), false, serde);
    assertEquals(2, response.getPageCount());

    List<Page> pages = response.getPages(serde);
    assertEquals(2, pages.size());
    assertEquals(2, pages.get(0).getPositionCount());
    assertEquals(3, pages.get(1).getPositionCount());
  }

  @Test
  @DisplayName("Empty page list serializes correctly")
  void testEmptyPageList() {
    ShardQueryResponse response = new ShardQueryResponse(List.of(), false, serde);
    assertEquals(0, response.getPageCount());
    assertTrue(response.getPages(serde).isEmpty());
  }

  @Test
  @DisplayName("Page with null values round-trips correctly")
  void testPageWithNulls() {
    boolean[] nulls = new boolean[] {false, true, false};
    LongArrayBlock block = new LongArrayBlock(3, Optional.of(nulls), new long[] {10, 0, 30});
    Page original = new Page(block);

    ShardQueryResponse response = new ShardQueryResponse(List.of(original), false, serde);
    Page result = response.getPages(serde).get(0);

    LongArrayBlock resultBlock = (LongArrayBlock) result.getBlock(0);
    assertFalse(resultBlock.isNull(0));
    assertTrue(resultBlock.isNull(1));
    assertFalse(resultBlock.isNull(2));
    assertEquals(10, resultBlock.getLong(0));
    assertEquals(30, resultBlock.getLong(2));
  }

  @Test
  @DisplayName("Variable-width block round-trips correctly")
  void testVariableWidthBlock() {
    byte[] data = "helloworld".getBytes();
    int[] offsets = new int[] {0, 5, 10};
    VariableWidthBlock block = new VariableWidthBlock(2, data, offsets, Optional.empty());
    Page original = new Page(block);

    ShardQueryResponse response = new ShardQueryResponse(List.of(original), false, serde);
    Page result = response.getPages(serde).get(0);

    VariableWidthBlock resultBlock = (VariableWidthBlock) result.getBlock(0);
    assertEquals(2, resultBlock.getPositionCount());
    assertArrayEquals("hello".getBytes(), resultBlock.getSlice(0));
    assertArrayEquals("world".getBytes(), resultBlock.getSlice(1));
  }

  @Test
  @DisplayName("hasMore flag preserved in response")
  void testHasMoreFlag() {
    ShardQueryResponse withMore = new ShardQueryResponse(List.of(), true, serde);
    assertTrue(withMore.hasMore());

    ShardQueryResponse withoutMore = new ShardQueryResponse(List.of(), false, serde);
    assertFalse(withoutMore.hasMore());
  }
}
