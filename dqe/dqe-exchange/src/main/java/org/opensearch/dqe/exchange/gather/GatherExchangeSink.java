/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dqe.exchange.gather;

import io.trino.spi.Page;
import java.io.Closeable;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import org.opensearch.dqe.exchange.serde.DqeDataPage;
import org.opensearch.dqe.exchange.serde.DqeExchangeChunk;
import org.opensearch.dqe.parser.DqeErrorCode;
import org.opensearch.dqe.parser.DqeException;

/**
 * Shard-side exchange sink that accumulates pages and pushes them as chunks to the coordinator.
 *
 * <p>Pages are accumulated until the chunk reaches the max chunk size, then flushed. On {@link
 * #finish()}, the remaining pages are sent as the final chunk (with {@code isLast=true}).
 *
 * <p>The actual transport of chunks is delegated to a {@link ChunkSender} callback, which allows
 * testing without a real transport layer.
 */
public class GatherExchangeSink implements Closeable {

  /** Callback interface for sending chunks to the coordinator. */
  @FunctionalInterface
  public interface ChunkSender {
    /**
     * Send a chunk to the coordinator.
     *
     * @param chunk the chunk to send
     * @throws DqeException if sending fails
     */
    void send(DqeExchangeChunk chunk) throws DqeException;
  }

  private final String queryId;
  private final int stageId;
  private final int partitionId;
  private final long maxChunkBytes;
  private final ChunkSender chunkSender;
  private final AtomicLong sequenceNumber;
  private final AtomicBoolean finished;
  private final AtomicBoolean aborted;

  // Pending pages accumulator (guarded by synchronized)
  private final List<DqeDataPage> pendingPages;
  private long pendingCompressedBytes;
  private long pendingUncompressedBytes;

  /**
   * Create a gather exchange sink.
   *
   * @param queryId query identifier
   * @param stageId stage identifier
   * @param partitionId partition identifier (typically 0 for gather exchange)
   * @param maxChunkBytes maximum chunk size in bytes before auto-flushing
   * @param chunkSender callback to send chunks to the coordinator
   */
  public GatherExchangeSink(
      String queryId, int stageId, int partitionId, long maxChunkBytes, ChunkSender chunkSender) {
    this.queryId = Objects.requireNonNull(queryId, "queryId must not be null");
    this.stageId = stageId;
    this.partitionId = partitionId;
    this.maxChunkBytes = maxChunkBytes;
    this.chunkSender = Objects.requireNonNull(chunkSender, "chunkSender must not be null");
    this.sequenceNumber = new AtomicLong(0);
    this.finished = new AtomicBoolean(false);
    this.aborted = new AtomicBoolean(false);
    this.pendingPages = new ArrayList<>();
    this.pendingCompressedBytes = 0;
    this.pendingUncompressedBytes = 0;
  }

  /**
   * Add a page to the sink. If the accumulated chunk size exceeds the max, the chunk is flushed.
   *
   * @param page the page to add
   * @throws DqeException if the sink has been finished or aborted, or if sending fails
   */
  public void addPage(Page page) throws DqeException {
    Objects.requireNonNull(page, "page must not be null");
    if (finished.get()) {
      throw new DqeException(
          "Cannot add page to finished sink for query [" + queryId + "]",
          DqeErrorCode.STAGE_EXECUTION_FAILED);
    }
    if (aborted.get()) {
      throw new DqeException(
          "Cannot add page to aborted sink for query [" + queryId + "]",
          DqeErrorCode.STAGE_EXECUTION_FAILED);
    }

    DqeDataPage dataPage = new DqeDataPage(page);

    synchronized (pendingPages) {
      pendingPages.add(dataPage);
      pendingCompressedBytes += dataPage.getCompressedSizeInBytes();
      pendingUncompressedBytes += dataPage.getUncompressedSizeInBytes();

      if (pendingCompressedBytes >= maxChunkBytes) {
        flushPending(false);
      }
    }
  }

  /**
   * Finish the sink. Flushes any remaining pages as the final chunk.
   *
   * @throws DqeException if sending fails
   */
  public void finish() throws DqeException {
    if (finished.compareAndSet(false, true)) {
      synchronized (pendingPages) {
        flushPending(true);
      }
    }
  }

  /** Abort the sink. No more pages will be sent. */
  public void abort() {
    if (aborted.compareAndSet(false, true)) {
      synchronized (pendingPages) {
        pendingPages.clear();
        pendingCompressedBytes = 0;
        pendingUncompressedBytes = 0;
      }
    }
  }

  /** Current sequence number (next chunk will have this number). */
  public long getSequenceNumber() {
    return sequenceNumber.get();
  }

  /** Whether the sink has been finished. */
  public boolean isFinished() {
    return finished.get();
  }

  @Override
  public void close() {
    if (!finished.get() && !aborted.get()) {
      abort();
    }
  }

  /**
   * Flush pending pages as a chunk.
   *
   * @param isLast whether this is the final chunk
   */
  private void flushPending(boolean isLast) throws DqeException {
    if (pendingPages.isEmpty() && !isLast) {
      return;
    }

    List<DqeDataPage> pagesToSend = new ArrayList<>(pendingPages);
    long uncompressed = pendingUncompressedBytes;

    pendingPages.clear();
    pendingCompressedBytes = 0;
    pendingUncompressedBytes = 0;

    DqeExchangeChunk chunk =
        new DqeExchangeChunk(
            queryId,
            stageId,
            partitionId,
            sequenceNumber.getAndIncrement(),
            pagesToSend,
            isLast,
            uncompressed);

    chunkSender.send(chunk);
  }
}
