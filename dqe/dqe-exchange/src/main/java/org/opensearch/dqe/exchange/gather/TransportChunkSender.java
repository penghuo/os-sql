/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dqe.exchange.gather;

import java.io.IOException;
import java.util.Objects;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.action.ActionListenerResponseHandler;
import org.opensearch.action.support.PlainActionFuture;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.dqe.exchange.action.DqeExchangePushAction;
import org.opensearch.dqe.exchange.action.DqeExchangePushRequest;
import org.opensearch.dqe.exchange.action.DqeExchangePushResponse;
import org.opensearch.dqe.exchange.serde.DqeDataPage;
import org.opensearch.dqe.exchange.serde.DqeExchangeChunk;
import org.opensearch.dqe.parser.DqeErrorCode;
import org.opensearch.dqe.parser.DqeException;
import org.opensearch.transport.TransportService;

/**
 * {@link GatherExchangeSink.ChunkSender} implementation that sends exchange chunks to the
 * coordinator via the {@link DqeExchangePushAction} transport action.
 *
 * <p>Sends synchronously by blocking on a {@link PlainActionFuture} until the coordinator
 * acknowledges the chunk.
 */
public class TransportChunkSender implements GatherExchangeSink.ChunkSender {

  private static final Logger logger = LogManager.getLogger(TransportChunkSender.class);

  private final TransportService transportService;
  private final ClusterService clusterService;
  private final String coordinatorNodeId;
  private final String queryId;
  private final int stageId;

  /**
   * Create a transport chunk sender.
   *
   * @param transportService the transport service for sending requests
   * @param clusterService the cluster service for resolving node IDs
   * @param coordinatorNodeId the node ID of the coordinator to send chunks to
   * @param queryId query identifier
   * @param stageId stage identifier
   */
  public TransportChunkSender(
      TransportService transportService,
      ClusterService clusterService,
      String coordinatorNodeId,
      String queryId,
      int stageId) {
    this.transportService =
        Objects.requireNonNull(transportService, "transportService must not be null");
    this.clusterService = Objects.requireNonNull(clusterService, "clusterService must not be null");
    this.coordinatorNodeId =
        Objects.requireNonNull(coordinatorNodeId, "coordinatorNodeId must not be null");
    this.queryId = Objects.requireNonNull(queryId, "queryId must not be null");
    this.stageId = stageId;
  }

  @Override
  public void send(DqeExchangeChunk chunk) throws DqeException {
    Objects.requireNonNull(chunk, "chunk must not be null");

    DiscoveryNode coordinatorNode = clusterService.state().nodes().get(coordinatorNodeId);
    if (coordinatorNode == null) {
      throw new DqeException(
          String.format(
              "Coordinator node [%s] not found in cluster state for query [%s]",
              coordinatorNodeId, queryId),
          DqeErrorCode.EXECUTION_ERROR);
    }

    byte[] serializedPages = serializePages(chunk);

    DqeExchangePushRequest request =
        new DqeExchangePushRequest(
            chunk.getQueryId(),
            chunk.getStageId(),
            chunk.getPartitionId(),
            chunk.getSequenceNumber(),
            serializedPages,
            chunk.isLast(),
            chunk.getUncompressedBytes(),
            chunk.getPages().size());

    PlainActionFuture<DqeExchangePushResponse> future = new PlainActionFuture<>();

    transportService.sendRequest(
        coordinatorNode,
        DqeExchangePushAction.NAME,
        request,
        new ActionListenerResponseHandler<>(future, DqeExchangePushResponse::new));

    try {
      DqeExchangePushResponse response = future.actionGet();
      if (!response.isAccepted()) {
        throw new DqeException(
            String.format(
                "Coordinator rejected exchange chunk for query [%s] stage %d seq %d",
                queryId, stageId, chunk.getSequenceNumber()),
            DqeErrorCode.EXECUTION_ERROR);
      }
      logger.trace(
          "[{}] Sent chunk seq={} pages={} isLast={} remaining={}",
          queryId,
          chunk.getSequenceNumber(),
          chunk.getPages().size(),
          chunk.isLast(),
          response.getBufferRemainingBytes());
    } catch (DqeException e) {
      throw e;
    } catch (Exception e) {
      throw new DqeException(
          String.format(
              "Failed to send exchange chunk for query [%s] stage %d: %s",
              queryId, stageId, e.getMessage()),
          DqeErrorCode.EXECUTION_ERROR,
          e);
    }
  }

  /** Serialize the DqeDataPage list to a byte array using OpenSearch stream format. */
  static byte[] serializePages(DqeExchangeChunk chunk) {
    try (BytesStreamOutput out = new BytesStreamOutput()) {
      out.writeVInt(chunk.getPages().size());
      for (DqeDataPage page : chunk.getPages()) {
        page.writeTo(out);
      }
      return BytesReference.toBytes(out.bytes());
    } catch (IOException e) {
      throw new DqeException(
          "Failed to serialize exchange pages: " + e.getMessage(), DqeErrorCode.EXECUTION_ERROR, e);
    }
  }
}
