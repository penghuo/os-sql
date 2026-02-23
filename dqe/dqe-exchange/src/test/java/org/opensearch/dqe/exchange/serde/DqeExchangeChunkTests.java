/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dqe.exchange.serde;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.trino.spi.Page;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.type.BigintType;
import java.io.IOException;
import java.util.Collections;
import java.util.List;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.core.common.io.stream.StreamInput;

class DqeExchangeChunkTests {

  private DqeDataPage createTestDataPage(int rowCount) {
    BlockBuilder builder = BigintType.BIGINT.createBlockBuilder(null, rowCount);
    for (int i = 0; i < rowCount; i++) {
      BigintType.BIGINT.writeLong(builder, i);
    }
    return new DqeDataPage(new Page(builder.build()));
  }

  @Test
  @DisplayName("DqeExchangeChunk roundtrip serialization")
  void roundtripSerialization() throws IOException {
    DqeDataPage page1 = createTestDataPage(10);
    DqeDataPage page2 = createTestDataPage(20);

    DqeExchangeChunk chunk =
        new DqeExchangeChunk("query-1", 0, 1, 42L, List.of(page1, page2), false, 5000L);

    BytesStreamOutput out = new BytesStreamOutput();
    chunk.writeTo(out);
    StreamInput in = out.bytes().streamInput();
    DqeExchangeChunk deserialized = new DqeExchangeChunk(in);

    assertEquals("query-1", deserialized.getQueryId());
    assertEquals(0, deserialized.getStageId());
    assertEquals(1, deserialized.getPartitionId());
    assertEquals(42L, deserialized.getSequenceNumber());
    assertEquals(2, deserialized.getPages().size());
    assertFalse(deserialized.isLast());
    assertEquals(5000L, deserialized.getUncompressedBytes());

    // Verify page data
    Page result0 = deserialized.getPages().get(0).getPage();
    assertEquals(10, result0.getPositionCount());
    Page result1 = deserialized.getPages().get(1).getPage();
    assertEquals(20, result1.getPositionCount());
  }

  @Test
  @DisplayName("DqeExchangeChunk roundtrip with isLast=true")
  void roundtripWithIsLast() throws IOException {
    DqeDataPage page = createTestDataPage(5);

    DqeExchangeChunk chunk = new DqeExchangeChunk("query-2", 1, 0, 99L, List.of(page), true, 1000L);

    BytesStreamOutput out = new BytesStreamOutput();
    chunk.writeTo(out);
    StreamInput in = out.bytes().streamInput();
    DqeExchangeChunk deserialized = new DqeExchangeChunk(in);

    assertTrue(deserialized.isLast());
    assertEquals(1, deserialized.getPages().size());
  }

  @Test
  @DisplayName("DqeExchangeChunk roundtrip with empty page list")
  void roundtripWithEmptyPages() throws IOException {
    DqeExchangeChunk chunk =
        new DqeExchangeChunk("query-3", 0, 0, 0L, Collections.emptyList(), true, 0L);

    BytesStreamOutput out = new BytesStreamOutput();
    chunk.writeTo(out);
    StreamInput in = out.bytes().streamInput();
    DqeExchangeChunk deserialized = new DqeExchangeChunk(in);

    assertEquals(0, deserialized.getPages().size());
    assertTrue(deserialized.isLast());
    assertEquals(0, deserialized.getCompressedBytes());
  }

  @Test
  @DisplayName("getCompressedBytes returns sum of all page compressed sizes")
  void getCompressedBytesReturnsSumOfPages() {
    DqeDataPage page1 = createTestDataPage(100);
    DqeDataPage page2 = createTestDataPage(200);

    DqeExchangeChunk chunk =
        new DqeExchangeChunk("query-4", 0, 0, 0L, List.of(page1, page2), false, 5000L);

    long expectedCompressed = page1.getCompressedSizeInBytes() + page2.getCompressedSizeInBytes();
    assertEquals(expectedCompressed, chunk.getCompressedBytes());
  }

  @Test
  @DisplayName("pages list is unmodifiable")
  void pagesListIsUnmodifiable() {
    DqeDataPage page = createTestDataPage(5);
    DqeExchangeChunk chunk = new DqeExchangeChunk("query-5", 0, 0, 0L, List.of(page), false, 1000L);

    assertThrows(UnsupportedOperationException.class, () -> chunk.getPages().add(page));
  }

  @Test
  @DisplayName("constructor rejects null queryId")
  void constructorRejectsNullQueryId() {
    assertThrows(
        NullPointerException.class,
        () -> new DqeExchangeChunk(null, 0, 0, 0L, Collections.emptyList(), false, 0L));
  }

  @Test
  @DisplayName("constructor rejects null pages list")
  void constructorRejectsNullPages() {
    assertThrows(
        NullPointerException.class, () -> new DqeExchangeChunk("q", 0, 0, 0L, null, false, 0L));
  }

  @Test
  @DisplayName("DEFAULT_MAX_CHUNK_BYTES is 1MB")
  void defaultMaxChunkBytesIs1MB() {
    assertEquals(1024L * 1024L, DqeExchangeChunk.DEFAULT_MAX_CHUNK_BYTES);
  }
}
