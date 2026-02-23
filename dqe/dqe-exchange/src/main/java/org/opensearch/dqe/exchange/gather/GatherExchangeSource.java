/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dqe.exchange.gather;

import io.trino.spi.Page;
import java.io.Closeable;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;
import org.opensearch.dqe.exchange.buffer.ExchangeBuffer;
import org.opensearch.dqe.exchange.serde.DqeDataPage;
import org.opensearch.dqe.exchange.serde.DqeExchangeChunk;

/**
 * Coordinator-side exchange source that receives pages from all shard producers via an {@link
 * ExchangeBuffer}.
 *
 * <p>The coordinator creates one GatherExchangeSource per query. After dispatching stages to data
 * nodes, it reads pages from this source to feed into final-stage operators (e.g., merge sort, top
 * N).
 *
 * <p>Pages are returned in the order they arrive (FIFO within the buffer). For ordered results, the
 * coordinator wraps this source with a merge operator.
 */
public class GatherExchangeSource implements Closeable {

  private final String queryId;
  private final int stageId;
  private final int expectedProducerCount;
  private final ExchangeBuffer buffer;
  private final AtomicBoolean closed;

  // Current chunk being consumed; pages are yielded one at a time
  private DqeExchangeChunk currentChunk;
  private int currentPageIndex;

  /**
   * Create a gather exchange source.
   *
   * @param queryId query identifier
   * @param stageId stage identifier
   * @param expectedProducerCount number of shard producers expected to contribute data
   * @param buffer the exchange buffer to read from
   */
  public GatherExchangeSource(
      String queryId, int stageId, int expectedProducerCount, ExchangeBuffer buffer) {
    this.queryId = Objects.requireNonNull(queryId, "queryId must not be null");
    this.stageId = stageId;
    this.expectedProducerCount = expectedProducerCount;
    this.buffer = Objects.requireNonNull(buffer, "buffer must not be null");
    this.closed = new AtomicBoolean(false);
    buffer.setExpectedProducerCount(expectedProducerCount);
  }

  /**
   * Get the next page from the exchange.
   *
   * <p>Blocks until a page is available or the exchange is finished.
   *
   * @return the next page, or null if the exchange is finished
   * @throws InterruptedException if the waiting thread is interrupted
   */
  public Page getNextPage() throws InterruptedException {
    if (closed.get()) {
      return null;
    }

    while (true) {
      // Try to return a page from the current chunk
      if (currentChunk != null && currentPageIndex < currentChunk.getPages().size()) {
        DqeDataPage dataPage = currentChunk.getPages().get(currentPageIndex);
        currentPageIndex++;
        return dataPage.getPage();
      }

      // Need a new chunk
      currentChunk = buffer.pollChunk();
      currentPageIndex = 0;

      if (currentChunk == null) {
        // Buffer is finished
        return null;
      }
    }
  }

  /** Whether the exchange is finished (no more pages will arrive). */
  public boolean isFinished() {
    if (closed.get()) {
      return true;
    }
    boolean bufferDone = buffer.isFinished();
    boolean currentChunkDone =
        currentChunk == null || currentPageIndex >= currentChunk.getPages().size();
    return bufferDone && currentChunkDone;
  }

  /** Abort the exchange. Stops all producers and drains the buffer. */
  public void abort() {
    buffer.abort();
    currentChunk = null;
    currentPageIndex = 0;
  }

  @Override
  public void close() {
    if (closed.compareAndSet(false, true)) {
      currentChunk = null;
      currentPageIndex = 0;
      buffer.close();
    }
  }

  /** The query identifier. */
  public String getQueryId() {
    return queryId;
  }

  /** The stage identifier. */
  public int getStageId() {
    return stageId;
  }

  /** The expected number of shard producers. */
  public int getExpectedProducerCount() {
    return expectedProducerCount;
  }
}
