/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dqe.exchange.gather;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.core.common.bytes.BytesArray;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.dqe.exchange.action.DqeExchangePushRequest;
import org.opensearch.dqe.exchange.action.DqeExchangePushResponse;
import org.opensearch.dqe.exchange.buffer.ExchangeBuffer;
import org.opensearch.dqe.exchange.serde.DqeDataPage;
import org.opensearch.dqe.exchange.serde.DqeExchangeChunk;
import org.opensearch.dqe.parser.DqeErrorCode;
import org.opensearch.dqe.parser.DqeException;
import org.opensearch.tasks.Task;
import org.opensearch.transport.TransportChannel;
import org.opensearch.transport.TransportRequestHandler;

/**
 * Coordinator-side handler that receives {@link DqeExchangePushRequest} from data nodes and adds
 * the contained chunks to the appropriate {@link ExchangeBuffer}.
 *
 * <p>Manages a registry of buffers keyed by {@code queryId:stageId}. Buffers must be registered
 * before data arrives and deregistered after query completion.
 */
public class ExchangePushHandler implements TransportRequestHandler<DqeExchangePushRequest> {

  private static final Logger logger = LogManager.getLogger(ExchangePushHandler.class);

  private final ConcurrentHashMap<String, ExchangeBuffer> bufferRegistry;

  public ExchangePushHandler() {
    this.bufferRegistry = new ConcurrentHashMap<>();
  }

  /**
   * Register an exchange buffer for a query stage.
   *
   * @param queryId query identifier
   * @param stageId stage identifier
   * @param buffer the exchange buffer to receive chunks
   */
  public void registerBuffer(String queryId, int stageId, ExchangeBuffer buffer) {
    String key = bufferKey(queryId, stageId);
    bufferRegistry.put(key, buffer);
    logger.debug("[{}] Registered exchange buffer for stage {}", queryId, stageId);
  }

  /**
   * Deregister the exchange buffer for a query stage.
   *
   * @param queryId query identifier
   * @param stageId stage identifier
   */
  public void deregisterBuffer(String queryId, int stageId) {
    String key = bufferKey(queryId, stageId);
    bufferRegistry.remove(key);
    logger.debug("[{}] Deregistered exchange buffer for stage {}", queryId, stageId);
  }

  @Override
  public void messageReceived(DqeExchangePushRequest request, TransportChannel channel, Task task)
      throws Exception {

    String queryId = request.getQueryId();
    int stageId = request.getStageId();
    String key = bufferKey(queryId, stageId);

    ExchangeBuffer buffer = bufferRegistry.get(key);
    if (buffer == null) {
      logger.warn(
          "[{}] No exchange buffer registered for stage {} (seq={})",
          queryId,
          stageId,
          request.getSequenceNumber());
      channel.sendResponse(new DqeExchangePushResponse(false, 0));
      return;
    }

    DqeExchangeChunk chunk = reconstructChunk(request);

    try {
      buffer.addChunk(chunk);
      long remaining = Math.max(0, buffer.getBufferedBytes());
      channel.sendResponse(new DqeExchangePushResponse(true, remaining));
      logger.trace(
          "[{}] Accepted push chunk stage={} seq={} pages={} isLast={}",
          queryId,
          stageId,
          request.getSequenceNumber(),
          request.getPageCount(),
          request.isLast());
    } catch (DqeException e) {
      logger.warn("[{}] Failed to add chunk to buffer for stage {}: {}", queryId, stageId, e.getMessage());
      channel.sendResponse(new DqeExchangePushResponse(false, 0));
    }
  }

  /** Reconstruct a DqeExchangeChunk from the push request fields. */
  static DqeExchangeChunk reconstructChunk(DqeExchangePushRequest request) {
    List<DqeDataPage> pages = deserializePages(request.getSerializedPages(), request.getPageCount());
    return new DqeExchangeChunk(
        request.getQueryId(),
        request.getStageId(),
        request.getPartitionId(),
        request.getSequenceNumber(),
        pages,
        request.isLast(),
        request.getUncompressedBytes());
  }

  /** Deserialize the DqeDataPage list from bytes using OpenSearch stream format. */
  static List<DqeDataPage> deserializePages(byte[] serializedPages, int expectedPageCount) {
    try (StreamInput in = new BytesArray(serializedPages).streamInput()) {
      int pageCount = in.readVInt();
      if (pageCount != expectedPageCount) {
        throw new DqeException(
            String.format(
                "Page count mismatch: header says %d but request says %d",
                pageCount, expectedPageCount),
            DqeErrorCode.EXECUTION_ERROR);
      }
      List<DqeDataPage> pages = new ArrayList<>(pageCount);
      for (int i = 0; i < pageCount; i++) {
        pages.add(new DqeDataPage(in));
      }
      return pages;
    } catch (IOException e) {
      throw new DqeException(
          "Failed to deserialize exchange pages: " + e.getMessage(),
          DqeErrorCode.EXECUTION_ERROR,
          e);
    }
  }

  private static String bufferKey(String queryId, int stageId) {
    return queryId + ":" + stageId;
  }
}
