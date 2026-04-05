/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.trino.transport;

import java.util.concurrent.atomic.AtomicLong;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.core.action.ActionListener;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.TransportService;

/**
 * Fetches pages from a single upstream node's OutputBuffer via transport. One instance per (upstream
 * task, buffer ID) pair.
 *
 * <p>Implements token-based exactly-once delivery:
 *
 * <ol>
 *   <li>Request with token N
 *   <li>Receive pages [N, N+K)
 *   <li>Ack token N+K
 *   <li>Next request with token N+K
 * </ol>
 */
public class TransportPageBufferClient {

  private static final Logger LOG = LogManager.getLogger(TransportPageBufferClient.class);

  private final TransportService transportService;
  private final DiscoveryNode sourceNode;
  private final String taskId;
  private final int bufferId;
  private final AtomicLong currentToken = new AtomicLong(0);
  private volatile boolean closed = false;

  public TransportPageBufferClient(
      TransportService transportService, DiscoveryNode sourceNode, String taskId, int bufferId) {
    this.transportService = transportService;
    this.sourceNode = sourceNode;
    this.taskId = taskId;
    this.bufferId = bufferId;
  }

  /**
   * Fetch next batch of pages. Calls listener with the response containing TRINO_PAGES bytes.
   *
   * @param maxBytes maximum bytes to fetch
   * @param listener callback for the response
   */
  public void getPages(long maxBytes, ActionListener<TrinoTaskResultsResponse> listener) {
    if (closed) {
      listener.onFailure(new IllegalStateException("PageBufferClient is closed"));
      return;
    }

    TrinoTaskResultsRequest request =
        new TrinoTaskResultsRequest(taskId, bufferId, currentToken.get(), maxBytes);

    transportService.sendRequest(
        sourceNode,
        TrinoTaskResultsAction.NAME,
        request,
        new org.opensearch.transport.TransportResponseHandler<TrinoTaskResultsResponse>() {
          @Override
          public TrinoTaskResultsResponse read(
              org.opensearch.core.common.io.stream.StreamInput in) throws java.io.IOException {
            return new TrinoTaskResultsResponse(in);
          }

          @Override
          public void handleResponse(TrinoTaskResultsResponse response) {
            currentToken.set(response.getNextToken());
            // Send ack for flow control
            sendAck(response.getToken());
            listener.onResponse(response);
          }

          @Override
          public void handleException(org.opensearch.transport.TransportException exp) {
            listener.onFailure(exp);
          }

          @Override
          public String executor() {
            return ThreadPool.Names.SAME;
          }
        });
  }

  private void sendAck(long token) {
    if (closed) {
      return;
    }
    TrinoTaskResultsAckRequest ackRequest = new TrinoTaskResultsAckRequest(taskId, bufferId, token);
    transportService.sendRequest(
        sourceNode,
        TrinoTaskResultsAckAction.NAME,
        ackRequest,
        new org.opensearch.transport.TransportResponseHandler<TrinoTaskResultsAckResponse>() {
          @Override
          public TrinoTaskResultsAckResponse read(
              org.opensearch.core.common.io.stream.StreamInput in) throws java.io.IOException {
            return new TrinoTaskResultsAckResponse(in);
          }

          @Override
          public void handleResponse(TrinoTaskResultsAckResponse response) {
            // Ack confirmed
          }

          @Override
          public void handleException(org.opensearch.transport.TransportException exp) {
            LOG.warn("Failed to ack results for task={}, buffer={}: {}", taskId, bufferId,
                exp.getMessage());
          }

          @Override
          public String executor() {
            return ThreadPool.Names.SAME;
          }
        });
  }

  public void close() {
    closed = true;
  }

  public String getTaskId() {
    return taskId;
  }

  public int getBufferId() {
    return bufferId;
  }

  public DiscoveryNode getSourceNode() {
    return sourceNode;
  }
}
