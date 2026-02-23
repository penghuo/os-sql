/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dqe.exchange.buffer;

import java.io.Closeable;
import java.util.Objects;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import org.opensearch.dqe.exchange.serde.DqeExchangeChunk;
import org.opensearch.dqe.memory.DqeMemoryTracker;
import org.opensearch.dqe.parser.DqeErrorCode;
import org.opensearch.dqe.parser.DqeException;

/**
 * Bounded exchange buffer with backpressure that sits between producers and consumers.
 *
 * <p>Producers add chunks via {@link #addChunk(DqeExchangeChunk)}. When the buffer is full, the
 * producer blocks until capacity is available (up to the configured timeout). Consumers poll chunks
 * via {@link #pollChunk()} or {@link #pollChunk(long)}.
 *
 * <p>Memory is tracked under the DQE circuit breaker via {@link DqeMemoryTracker}. When the buffer
 * is closed or aborted, all tracked memory is released.
 *
 * <p>Thread-safe. Multiple producers and consumers are supported.
 */
public class ExchangeBuffer implements Closeable {

  /** Default maximum buffer size: 32MB. */
  public static final long DEFAULT_MAX_BUFFER_BYTES = 32L * 1024L * 1024L;

  /** Default producer timeout: 30 seconds. */
  public static final long DEFAULT_PRODUCER_TIMEOUT_MS = 30_000L;

  private final BackpressureController backpressure;
  private final SequenceTracker sequenceTracker;
  private final DqeMemoryTracker memoryTracker;
  private final String queryId;
  private final LinkedBlockingQueue<DqeExchangeChunk> queue;
  private final AtomicBoolean noMoreData;
  private final AtomicBoolean aborted;
  private final AtomicBoolean closed;
  private final AtomicLong trackedMemoryBytes;
  private final AtomicInteger finishedProducerCount;
  private final AtomicInteger expectedProducerCount;

  /**
   * Create an exchange buffer.
   *
   * @param maxBufferBytes maximum buffer size in bytes
   * @param producerTimeoutMs producer backpressure timeout in milliseconds
   * @param memoryTracker the node-level memory tracker for circuit breaker accounting
   * @param queryId query identifier for labeling memory reservations
   */
  public ExchangeBuffer(
      long maxBufferBytes, long producerTimeoutMs, DqeMemoryTracker memoryTracker, String queryId) {
    this.backpressure = new BackpressureController(maxBufferBytes, producerTimeoutMs);
    this.sequenceTracker = new SequenceTracker();
    this.memoryTracker = Objects.requireNonNull(memoryTracker, "memoryTracker must not be null");
    this.queryId = Objects.requireNonNull(queryId, "queryId must not be null");
    this.queue = new LinkedBlockingQueue<>();
    this.noMoreData = new AtomicBoolean(false);
    this.aborted = new AtomicBoolean(false);
    this.closed = new AtomicBoolean(false);
    this.trackedMemoryBytes = new AtomicLong(0);
    this.finishedProducerCount = new AtomicInteger(0);
    this.expectedProducerCount = new AtomicInteger(0);
  }

  /**
   * Set the expected number of producers. When all producers have finished (sent isLast chunks),
   * the buffer automatically transitions to finished.
   *
   * @param count expected number of producers
   */
  public void setExpectedProducerCount(int count) {
    expectedProducerCount.set(count);
  }

  /**
   * Add a chunk to the buffer. Blocks if the buffer is full until capacity is available or the
   * producer timeout expires.
   *
   * @param chunk the exchange chunk to add
   * @throws DqeException with {@link DqeErrorCode#EXCHANGE_BUFFER_TIMEOUT} if the producer timeout
   *     expires
   * @throws DqeException if the buffer has been aborted
   */
  public void addChunk(DqeExchangeChunk chunk) throws DqeException {
    Objects.requireNonNull(chunk, "chunk must not be null");

    if (aborted.get()) {
      throw new DqeException(
          "Exchange buffer aborted for query [" + queryId + "]",
          DqeErrorCode.EXCHANGE_BUFFER_TIMEOUT);
    }
    if (closed.get()) {
      throw new DqeException(
          "Exchange buffer closed for query [" + queryId + "]",
          DqeErrorCode.EXCHANGE_BUFFER_TIMEOUT);
    }

    // Sequence number dedup: discard chunks already seen (transport retries)
    if (!sequenceTracker.acceptIfNew(chunk.getPartitionId(), chunk.getSequenceNumber())) {
      return; // duplicate — silently discard
    }

    long chunkBytes = chunk.getCompressedBytes();

    // Wait for backpressure capacity
    try {
      boolean acquired = backpressure.awaitCapacity(chunkBytes);
      if (!acquired) {
        if (aborted.get()) {
          throw new DqeException(
              "Exchange buffer aborted for query [" + queryId + "]",
              DqeErrorCode.EXCHANGE_BUFFER_TIMEOUT);
        }
        throw new DqeException(
            String.format(
                "Exchange buffer timeout for query [%s]: producer waited too long to add chunk"
                    + " (buffered=%d bytes)",
                queryId, backpressure.getBufferedBytes()),
            DqeErrorCode.EXCHANGE_BUFFER_TIMEOUT);
      }
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new DqeException(
          "Exchange buffer interrupted for query [" + queryId + "]",
          DqeErrorCode.EXCHANGE_BUFFER_TIMEOUT,
          e);
    }

    // Track memory under circuit breaker
    if (chunkBytes > 0) {
      memoryTracker.reserve(chunkBytes, queryId + ":exchange-buffer");
      trackedMemoryBytes.addAndGet(chunkBytes);
    }

    queue.add(chunk);

    // Track producer completion
    if (chunk.isLast()) {
      int finished = finishedProducerCount.incrementAndGet();
      int expected = expectedProducerCount.get();
      if (expected > 0 && finished >= expected) {
        noMoreData.set(true);
      }
    }
  }

  /**
   * Poll for the next chunk, blocking until one is available.
   *
   * @return the next chunk, or null if the buffer is finished and empty
   * @throws InterruptedException if the waiting thread is interrupted
   */
  public DqeExchangeChunk pollChunk() throws InterruptedException {
    return pollChunk(0);
  }

  /**
   * Poll for the next chunk with a timeout.
   *
   * @param timeoutMs timeout in milliseconds (0 means block indefinitely until data or finished)
   * @return the next chunk, or null if the buffer is finished/empty/timed out
   * @throws InterruptedException if the waiting thread is interrupted
   */
  public DqeExchangeChunk pollChunk(long timeoutMs) throws InterruptedException {
    while (!aborted.get()) {
      DqeExchangeChunk chunk;
      if (timeoutMs > 0) {
        chunk = queue.poll(timeoutMs, TimeUnit.MILLISECONDS);
      } else {
        // Poll with a short timeout to recheck finish condition
        chunk = queue.poll(100, TimeUnit.MILLISECONDS);
      }

      if (chunk != null) {
        long chunkBytes = chunk.getCompressedBytes();
        backpressure.notifyConsumed(chunkBytes);
        if (chunkBytes > 0) {
          memoryTracker.release(chunkBytes, queryId + ":exchange-buffer");
          trackedMemoryBytes.addAndGet(-chunkBytes);
        }
        return chunk;
      }

      // Check if we should stop waiting
      if (isFinished()) {
        return null;
      }

      // If a specific timeout was given, we already waited
      if (timeoutMs > 0) {
        return null;
      }
    }
    return null;
  }

  /** Signal that no more data will be added to this buffer. */
  public void setNoMoreData() {
    noMoreData.set(true);
  }

  /** Abort the buffer. All pending producers are unblocked, and the buffer is drained. */
  public void abort() {
    if (aborted.compareAndSet(false, true)) {
      backpressure.abort();
      releaseTrackedMemory();
      queue.clear();
    }
  }

  /**
   * Whether the buffer is finished: no more data will arrive and the queue is empty, or the buffer
   * has been aborted.
   */
  public boolean isFinished() {
    return aborted.get() || (noMoreData.get() && queue.isEmpty());
  }

  /** Current buffered bytes (compressed). */
  public long getBufferedBytes() {
    return backpressure.getBufferedBytes();
  }

  /** Number of chunks currently in the buffer. */
  public int getBufferedChunkCount() {
    return queue.size();
  }

  @Override
  public void close() {
    if (closed.compareAndSet(false, true)) {
      backpressure.abort();
      releaseTrackedMemory();
      queue.clear();
    }
  }

  /** Whether the buffer has been aborted. */
  public boolean isAborted() {
    return aborted.get();
  }

  private void releaseTrackedMemory() {
    long remaining = trackedMemoryBytes.getAndSet(0);
    if (remaining > 0) {
      memoryTracker.release(remaining, queryId + ":exchange-buffer");
    }
  }
}
