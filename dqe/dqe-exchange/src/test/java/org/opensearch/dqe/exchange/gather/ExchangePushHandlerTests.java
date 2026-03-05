/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dqe.exchange.gather;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;

import io.trino.spi.Page;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.type.BigintType;
import java.util.List;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.opensearch.dqe.exchange.action.DqeExchangePushRequest;
import org.opensearch.dqe.exchange.action.DqeExchangePushResponse;
import org.opensearch.dqe.exchange.buffer.ExchangeBuffer;
import org.opensearch.dqe.exchange.serde.DqeDataPage;
import org.opensearch.dqe.exchange.serde.DqeExchangeChunk;
import org.opensearch.dqe.memory.DqeMemoryTracker;
import org.opensearch.tasks.Task;
import org.opensearch.transport.TransportChannel;

class ExchangePushHandlerTests {

  private ExchangePushHandler handler;
  private DqeMemoryTracker memoryTracker;

  @BeforeEach
  void setUp() {
    handler = new ExchangePushHandler();
    memoryTracker = mock(DqeMemoryTracker.class);
  }

  private Page createPage(int rows) {
    BlockBuilder builder = BigintType.BIGINT.createBlockBuilder(null, rows);
    for (int i = 0; i < rows; i++) {
      BigintType.BIGINT.writeLong(builder, i);
    }
    return new Page(builder.build());
  }

  private DqeExchangePushRequest createPushRequest(String queryId, int stageId, boolean isLast) {
    Page page = createPage(5);
    DqeDataPage dataPage = new DqeDataPage(page);
    DqeExchangeChunk chunk =
        new DqeExchangeChunk(queryId, stageId, 0, 0, List.of(dataPage), isLast, 40L);
    byte[] serialized = TransportChunkSender.serializePages(chunk);
    return new DqeExchangePushRequest(queryId, stageId, 0, 0, serialized, isLast, 40L, 1);
  }

  @Test
  @DisplayName("push adds chunk to registered buffer")
  void pushAddsChunkToBuffer() throws Exception {
    ExchangeBuffer buffer = new ExchangeBuffer(1024 * 1024, 5000, memoryTracker, "q1");
    buffer.setExpectedProducerCount(1);
    handler.registerBuffer("q1", 0, buffer);

    DqeExchangePushRequest request = createPushRequest("q1", 0, true);
    StubTransportChannel channel = new StubTransportChannel();

    handler.messageReceived(request, channel, mock(Task.class));

    assertTrue(channel.responded);
    assertTrue(channel.lastResponse instanceof DqeExchangePushResponse);
    DqeExchangePushResponse response = (DqeExchangePushResponse) channel.lastResponse;
    assertTrue(response.isAccepted());

    // Buffer should have the chunk
    DqeExchangeChunk polled = buffer.pollChunk(1000);
    assertNotNull(polled);
    assertEquals(1, polled.getPages().size());
    assertTrue(polled.isLast());
  }

  @Test
  @DisplayName("push to unregistered buffer returns rejected response")
  void pushToUnregisteredBufferRejected() throws Exception {
    DqeExchangePushRequest request = createPushRequest("q-unknown", 0, false);
    StubTransportChannel channel = new StubTransportChannel();

    handler.messageReceived(request, channel, mock(Task.class));

    assertTrue(channel.responded);
    DqeExchangePushResponse response = (DqeExchangePushResponse) channel.lastResponse;
    assertFalse(response.isAccepted());
  }

  @Test
  @DisplayName("deregister removes buffer from registry")
  void deregisterRemovesBuffer() throws Exception {
    ExchangeBuffer buffer = new ExchangeBuffer(1024 * 1024, 5000, memoryTracker, "q1");
    handler.registerBuffer("q1", 0, buffer);
    handler.deregisterBuffer("q1", 0);

    DqeExchangePushRequest request = createPushRequest("q1", 0, false);
    StubTransportChannel channel = new StubTransportChannel();

    handler.messageReceived(request, channel, mock(Task.class));

    DqeExchangePushResponse response = (DqeExchangePushResponse) channel.lastResponse;
    assertFalse(response.isAccepted());
  }

  @Test
  @DisplayName("reconstructChunk preserves all fields")
  void reconstructChunkPreservesFields() {
    DqeExchangePushRequest request = createPushRequest("q42", 3, true);
    DqeExchangeChunk chunk = ExchangePushHandler.reconstructChunk(request);

    assertEquals("q42", chunk.getQueryId());
    assertEquals(3, chunk.getStageId());
    assertEquals(0, chunk.getPartitionId());
    assertEquals(0, chunk.getSequenceNumber());
    assertEquals(1, chunk.getPages().size());
    assertTrue(chunk.isLast());
  }

  @Test
  @DisplayName("round-trip serialize/deserialize preserves page data")
  void roundTripPreservesPageData() {
    Page original = createPage(10);
    DqeDataPage dataPage = new DqeDataPage(original);
    DqeExchangeChunk chunk = new DqeExchangeChunk("q1", 0, 0, 0, List.of(dataPage), false, 80L);

    byte[] serialized = TransportChunkSender.serializePages(chunk);
    List<DqeDataPage> deserialized = ExchangePushHandler.deserializePages(serialized, 1);

    assertEquals(1, deserialized.size());
    Page restored = deserialized.get(0).getPage();
    assertEquals(10, restored.getPositionCount());
    assertEquals(1, restored.getChannelCount());
    for (int i = 0; i < 10; i++) {
      assertEquals((long) i, restored.getBlock(0).getLong(i, 0));
    }
  }

  /** Simple TransportChannel stub for testing. */
  private static class StubTransportChannel implements TransportChannel {
    boolean responded = false;
    Object lastResponse = null;

    @Override
    public String getProfileName() {
      return "test";
    }

    @Override
    public String getChannelType() {
      return "test";
    }

    @Override
    public void sendResponse(org.opensearch.core.transport.TransportResponse response) {
      responded = true;
      lastResponse = response;
    }

    @Override
    public void sendResponse(Exception exception) {
      responded = true;
    }
  }
}
