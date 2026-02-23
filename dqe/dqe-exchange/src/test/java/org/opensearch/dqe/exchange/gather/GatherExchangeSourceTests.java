/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dqe.exchange.gather;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;

import io.trino.spi.Page;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.type.BigintType;
import java.util.List;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.opensearch.core.common.breaker.CircuitBreaker;
import org.opensearch.dqe.exchange.buffer.ExchangeBuffer;
import org.opensearch.dqe.exchange.serde.DqeDataPage;
import org.opensearch.dqe.exchange.serde.DqeExchangeChunk;
import org.opensearch.dqe.memory.DqeMemoryTracker;

class GatherExchangeSourceTests {

  private DqeMemoryTracker tracker;

  @BeforeEach
  void setUp() {
    tracker = new DqeMemoryTracker(mock(CircuitBreaker.class), mock(CircuitBreaker.class));
  }

  private DqeExchangeChunk createChunk(int rows, int partitionId, long seqNum, boolean isLast) {
    BlockBuilder builder = BigintType.BIGINT.createBlockBuilder(null, rows);
    for (int i = 0; i < rows; i++) {
      BigintType.BIGINT.writeLong(builder, i);
    }
    Page page = new Page(builder.build());
    DqeDataPage dataPage = new DqeDataPage(page);
    return new DqeExchangeChunk(
        "q1", 0, partitionId, seqNum, List.of(dataPage), isLast, page.getSizeInBytes());
  }

  @Test
  @DisplayName("getNextPage returns pages from single producer")
  void getNextPageFromSingleProducer() throws Exception {
    ExchangeBuffer buffer = new ExchangeBuffer(1024 * 1024, 5000, tracker, "q1");
    GatherExchangeSource source = new GatherExchangeSource("q1", 0, 1, buffer);

    buffer.addChunk(createChunk(10, 0, 0, false));
    buffer.addChunk(createChunk(20, 0, 1, true));

    Page page1 = source.getNextPage();
    assertNotNull(page1);
    assertEquals(10, page1.getPositionCount());

    Page page2 = source.getNextPage();
    assertNotNull(page2);
    assertEquals(20, page2.getPositionCount());

    // Should return null after all data consumed
    Page page3 = source.getNextPage();
    assertNull(page3);
    assertTrue(source.isFinished());
    source.close();
  }

  @Test
  @DisplayName("getNextPage returns pages from multiple producers")
  void getNextPageFromMultipleProducers() throws Exception {
    ExchangeBuffer buffer = new ExchangeBuffer(1024 * 1024, 5000, tracker, "q1");
    GatherExchangeSource source = new GatherExchangeSource("q1", 0, 2, buffer);

    buffer.addChunk(createChunk(5, 0, 0, true));
    buffer.addChunk(createChunk(7, 1, 0, true));

    Page page1 = source.getNextPage();
    assertNotNull(page1);
    Page page2 = source.getNextPage();
    assertNotNull(page2);
    Page page3 = source.getNextPage();
    assertNull(page3);
    assertTrue(source.isFinished());
    source.close();
  }

  @Test
  @DisplayName("getNextPage handles multi-page chunks")
  void getNextPageMultiPageChunks() throws Exception {
    ExchangeBuffer buffer = new ExchangeBuffer(1024 * 1024, 5000, tracker, "q1");
    GatherExchangeSource source = new GatherExchangeSource("q1", 0, 1, buffer);

    // Create a chunk with two pages
    BlockBuilder b1 = BigintType.BIGINT.createBlockBuilder(null, 3);
    BigintType.BIGINT.writeLong(b1, 1L);
    BigintType.BIGINT.writeLong(b1, 2L);
    BigintType.BIGINT.writeLong(b1, 3L);
    DqeDataPage dp1 = new DqeDataPage(new Page(b1.build()));

    BlockBuilder b2 = BigintType.BIGINT.createBlockBuilder(null, 2);
    BigintType.BIGINT.writeLong(b2, 4L);
    BigintType.BIGINT.writeLong(b2, 5L);
    DqeDataPage dp2 = new DqeDataPage(new Page(b2.build()));

    DqeExchangeChunk chunk = new DqeExchangeChunk("q1", 0, 0, 0, List.of(dp1, dp2), true, 100L);
    buffer.addChunk(chunk);

    Page p1 = source.getNextPage();
    assertNotNull(p1);
    assertEquals(3, p1.getPositionCount());

    Page p2 = source.getNextPage();
    assertNotNull(p2);
    assertEquals(2, p2.getPositionCount());

    assertNull(source.getNextPage());
    source.close();
  }

  @Test
  @DisplayName("abort stops the source")
  void abortStopsSource() throws Exception {
    ExchangeBuffer buffer = new ExchangeBuffer(1024 * 1024, 5000, tracker, "q1");
    GatherExchangeSource source = new GatherExchangeSource("q1", 0, 1, buffer);
    buffer.addChunk(createChunk(10, 0, 0, false));

    source.abort();
    assertTrue(buffer.isAborted());
    source.close();
  }

  @Test
  @DisplayName("close returns null on subsequent getNextPage")
  void closeReturnsNull() throws Exception {
    ExchangeBuffer buffer = new ExchangeBuffer(1024 * 1024, 5000, tracker, "q1");
    GatherExchangeSource source = new GatherExchangeSource("q1", 0, 1, buffer);
    source.close();
    assertNull(source.getNextPage());
    assertTrue(source.isFinished());
  }

  @Test
  @DisplayName("getQueryId and getStageId return correct values")
  void metadataAccessors() {
    ExchangeBuffer buffer = new ExchangeBuffer(1024 * 1024, 5000, tracker, "q42");
    GatherExchangeSource source = new GatherExchangeSource("q42", 3, 5, buffer);
    assertEquals("q42", source.getQueryId());
    assertEquals(3, source.getStageId());
    assertEquals(5, source.getExpectedProducerCount());
    source.close();
  }
}
