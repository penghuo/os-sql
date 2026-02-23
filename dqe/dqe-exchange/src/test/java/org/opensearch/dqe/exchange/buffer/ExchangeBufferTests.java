/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dqe.exchange.buffer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;

import io.trino.spi.Page;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.type.BigintType;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.opensearch.core.common.breaker.CircuitBreaker;
import org.opensearch.dqe.exchange.serde.DqeDataPage;
import org.opensearch.dqe.exchange.serde.DqeExchangeChunk;
import org.opensearch.dqe.memory.DqeMemoryTracker;
import org.opensearch.dqe.parser.DqeErrorCode;
import org.opensearch.dqe.parser.DqeException;

class ExchangeBufferTests {

  private DqeMemoryTracker tracker;
  private CircuitBreaker dqeBreaker;
  private CircuitBreaker parentBreaker;

  @BeforeEach
  void setUp() {
    dqeBreaker = mock(CircuitBreaker.class);
    parentBreaker = mock(CircuitBreaker.class);
    tracker = new DqeMemoryTracker(dqeBreaker, parentBreaker);
  }

  private DqeExchangeChunk createChunk(
      String queryId, int partitionId, long seqNum, int rows, boolean isLast) {
    BlockBuilder builder = BigintType.BIGINT.createBlockBuilder(null, rows);
    for (int i = 0; i < rows; i++) {
      BigintType.BIGINT.writeLong(builder, i);
    }
    Page page = new Page(builder.build());
    DqeDataPage dataPage = new DqeDataPage(page);
    return new DqeExchangeChunk(
        queryId, 0, partitionId, seqNum, List.of(dataPage), isLast, page.getSizeInBytes());
  }

  @Test
  @DisplayName("addChunk and pollChunk roundtrip")
  void addAndPollRoundtrip() throws Exception {
    ExchangeBuffer buffer = new ExchangeBuffer(1024 * 1024, 5000, tracker, "q1");
    DqeExchangeChunk chunk = createChunk("q1", 0, 0, 10, false);
    buffer.addChunk(chunk);
    buffer.setNoMoreData();

    DqeExchangeChunk polled = buffer.pollChunk(1000);
    assertNotNull(polled);
    assertEquals(10, polled.getPages().get(0).getPositionCount());
    buffer.close();
  }

  @Test
  @DisplayName("pollChunk returns null when buffer is finished and empty")
  void pollReturnsNullWhenFinished() throws Exception {
    ExchangeBuffer buffer = new ExchangeBuffer(1024 * 1024, 5000, tracker, "q1");
    buffer.setNoMoreData();
    assertNull(buffer.pollChunk(100));
    assertTrue(buffer.isFinished());
    buffer.close();
  }

  @Test
  @DisplayName("duplicate sequence numbers are discarded")
  void duplicateSequenceDiscarded() throws Exception {
    ExchangeBuffer buffer = new ExchangeBuffer(1024 * 1024, 5000, tracker, "q1");
    DqeExchangeChunk chunk1 = createChunk("q1", 0, 0, 5, false);
    DqeExchangeChunk chunk1dup = createChunk("q1", 0, 0, 5, false);
    DqeExchangeChunk chunk2 = createChunk("q1", 0, 1, 10, true);

    buffer.addChunk(chunk1);
    buffer.addChunk(chunk1dup); // duplicate — should be silently discarded
    buffer.addChunk(chunk2);

    assertEquals(2, buffer.getBufferedChunkCount());
    buffer.close();
  }

  @Test
  @DisplayName("addChunk throws when buffer is aborted")
  void addChunkThrowsWhenAborted() {
    ExchangeBuffer buffer = new ExchangeBuffer(1024 * 1024, 5000, tracker, "q1");
    buffer.abort();
    DqeExchangeChunk chunk = createChunk("q1", 0, 0, 5, false);
    DqeException ex = assertThrows(DqeException.class, () -> buffer.addChunk(chunk));
    assertEquals(DqeErrorCode.EXCHANGE_BUFFER_TIMEOUT, ex.getErrorCode());
  }

  @Test
  @DisplayName("addChunk throws when buffer is closed")
  void addChunkThrowsWhenClosed() {
    ExchangeBuffer buffer = new ExchangeBuffer(1024 * 1024, 5000, tracker, "q1");
    buffer.close();
    DqeExchangeChunk chunk = createChunk("q1", 0, 0, 5, false);
    DqeException ex = assertThrows(DqeException.class, () -> buffer.addChunk(chunk));
    assertEquals(DqeErrorCode.EXCHANGE_BUFFER_TIMEOUT, ex.getErrorCode());
  }

  @Test
  @DisplayName("abort releases tracked memory")
  void abortReleasesTrackedMemory() throws Exception {
    ExchangeBuffer buffer = new ExchangeBuffer(1024 * 1024, 5000, tracker, "q1");
    buffer.addChunk(createChunk("q1", 0, 0, 100, false));
    assertTrue(buffer.getBufferedBytes() > 0);

    buffer.abort();
    assertTrue(buffer.isAborted());
    assertEquals(0, buffer.getBufferedChunkCount());
  }

  @Test
  @DisplayName("close releases tracked memory")
  void closeReleasesTrackedMemory() throws Exception {
    ExchangeBuffer buffer = new ExchangeBuffer(1024 * 1024, 5000, tracker, "q1");
    buffer.addChunk(createChunk("q1", 0, 0, 50, false));
    buffer.close();
    assertEquals(0, buffer.getBufferedChunkCount());
  }

  @Test
  @DisplayName("producer completion auto-finishes buffer")
  void producerCompletionAutoFinishes() throws Exception {
    ExchangeBuffer buffer = new ExchangeBuffer(1024 * 1024, 5000, tracker, "q1");
    buffer.setExpectedProducerCount(2);

    buffer.addChunk(createChunk("q1", 0, 0, 5, true)); // producer 0 done
    assertFalse(buffer.isFinished());

    buffer.addChunk(createChunk("q1", 1, 0, 5, true)); // producer 1 done
    // Not finished yet because queue still has items
    assertFalse(buffer.isFinished());

    // Drain
    buffer.pollChunk(100);
    buffer.pollChunk(100);
    assertTrue(buffer.isFinished());
    buffer.close();
  }

  @Test
  @DisplayName("backpressure timeout throws EXCHANGE_BUFFER_TIMEOUT")
  void backpressureTimeoutThrows() throws Exception {
    // Create a chunk and use its actual size to set buffer capacity
    DqeExchangeChunk bigChunk = createChunk("q1", 0, 0, 5, false);
    long chunkSize = bigChunk.getCompressedBytes();

    // Buffer can hold exactly one chunk; short timeout
    ExchangeBuffer buffer = new ExchangeBuffer(chunkSize, 200, tracker, "q1");
    buffer.addChunk(bigChunk);

    // Second chunk should timeout because buffer is full
    DqeExchangeChunk chunk2 = createChunk("q1", 0, 1, 5, false);
    DqeException ex = assertThrows(DqeException.class, () -> buffer.addChunk(chunk2));
    assertEquals(DqeErrorCode.EXCHANGE_BUFFER_TIMEOUT, ex.getErrorCode());
    buffer.close();
  }

  @Test
  @DisplayName("consumer unblocks producer via backpressure")
  void consumerUnblocksProducer() throws Exception {
    ExchangeBuffer buffer = new ExchangeBuffer(200, 5000, tracker, "q1");
    // Fill buffer
    buffer.addChunk(createChunk("q1", 0, 0, 100, false));

    ExecutorService exec = Executors.newSingleThreadExecutor();
    CountDownLatch producerStarted = new CountDownLatch(1);
    AtomicReference<Exception> producerError = new AtomicReference<>();

    // Producer will block
    Future<?> producerFuture =
        exec.submit(
            () -> {
              try {
                producerStarted.countDown();
                buffer.addChunk(createChunk("q1", 0, 1, 100, false));
              } catch (Exception e) {
                producerError.set(e);
              }
            });

    producerStarted.await();
    Thread.sleep(100);

    // Consumer drains, freeing capacity
    buffer.pollChunk(1000);

    producerFuture.get(5, TimeUnit.SECONDS);
    assertNull(producerError.get());
    exec.shutdown();
    buffer.close();
  }

  @Test
  @DisplayName("DEFAULT_MAX_BUFFER_BYTES is 32MB")
  void defaultMaxBufferBytes() {
    assertEquals(32L * 1024L * 1024L, ExchangeBuffer.DEFAULT_MAX_BUFFER_BYTES);
  }

  @Test
  @DisplayName("DEFAULT_PRODUCER_TIMEOUT_MS is 30s")
  void defaultProducerTimeout() {
    assertEquals(30_000L, ExchangeBuffer.DEFAULT_PRODUCER_TIMEOUT_MS);
  }

  @Test
  @DisplayName("empty chunk with isLast counts as producer finished")
  void emptyChunkWithIsLastFinishesProducer() throws Exception {
    ExchangeBuffer buffer = new ExchangeBuffer(1024 * 1024, 5000, tracker, "q1");
    buffer.setExpectedProducerCount(1);
    DqeExchangeChunk emptyLast =
        new DqeExchangeChunk("q1", 0, 0, 0, Collections.emptyList(), true, 0L);
    buffer.addChunk(emptyLast);

    // Drain the empty last chunk
    DqeExchangeChunk polled = buffer.pollChunk(100);
    assertNotNull(polled);
    assertTrue(polled.isLast());
    assertTrue(buffer.isFinished());
    buffer.close();
  }
}
