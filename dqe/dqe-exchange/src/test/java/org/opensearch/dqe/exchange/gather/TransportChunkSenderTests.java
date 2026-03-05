/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dqe.exchange.gather;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.trino.spi.Page;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.type.BigintType;
import java.util.List;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.node.DiscoveryNodes;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.dqe.exchange.action.DqeExchangePushAction;
import org.opensearch.dqe.exchange.action.DqeExchangePushRequest;
import org.opensearch.dqe.exchange.action.DqeExchangePushResponse;
import org.opensearch.dqe.exchange.serde.DqeDataPage;
import org.opensearch.dqe.exchange.serde.DqeExchangeChunk;
import org.opensearch.dqe.parser.DqeException;
import org.opensearch.transport.TransportResponseHandler;
import org.opensearch.transport.TransportService;

class TransportChunkSenderTests {

  private TransportService transportService;
  private ClusterService clusterService;
  private ClusterState clusterState;
  private DiscoveryNodes discoveryNodes;
  private DiscoveryNode coordinatorNode;
  private TransportChunkSender sender;

  @BeforeEach
  void setUp() {
    transportService = mock(TransportService.class);
    clusterService = mock(ClusterService.class);
    clusterState = mock(ClusterState.class);
    discoveryNodes = mock(DiscoveryNodes.class);
    coordinatorNode = mock(DiscoveryNode.class);

    when(clusterService.state()).thenReturn(clusterState);
    when(clusterState.nodes()).thenReturn(discoveryNodes);
    when(discoveryNodes.get("coord-1")).thenReturn(coordinatorNode);

    sender = new TransportChunkSender(transportService, clusterService, "coord-1", "q1", 0);
  }

  private DqeExchangeChunk createChunk(int rows, boolean isLast) {
    BlockBuilder builder = BigintType.BIGINT.createBlockBuilder(null, rows);
    for (int i = 0; i < rows; i++) {
      BigintType.BIGINT.writeLong(builder, i);
    }
    Page page = new Page(builder.build());
    DqeDataPage dataPage = new DqeDataPage(page);
    return new DqeExchangeChunk("q1", 0, 0, 0, List.of(dataPage), isLast, rows * 8L);
  }

  @SuppressWarnings("unchecked")
  @Test
  @DisplayName("send builds correct request and sends to coordinator")
  void sendBuildsCorrectRequest() {
    DqeExchangeChunk chunk = createChunk(10, false);

    // Mock transport to capture the request and respond with success
    doAnswer(
            inv -> {
              TransportResponseHandler<DqeExchangePushResponse> handler = inv.getArgument(3);
              handler.handleResponse(new DqeExchangePushResponse(true, 1024));
              return null;
            })
        .when(transportService)
        .sendRequest(any(DiscoveryNode.class), eq(DqeExchangePushAction.NAME), any(), any());

    sender.send(chunk);

    ArgumentCaptor<DqeExchangePushRequest> captor =
        ArgumentCaptor.forClass(DqeExchangePushRequest.class);
    verify(transportService)
        .sendRequest(eq(coordinatorNode), eq(DqeExchangePushAction.NAME), captor.capture(), any());

    DqeExchangePushRequest request = captor.getValue();
    assertEquals("q1", request.getQueryId());
    assertEquals(0, request.getStageId());
    assertEquals(0, request.getPartitionId());
    assertEquals(0, request.getSequenceNumber());
    assertNotNull(request.getSerializedPages());
    assertTrue(request.getSerializedPages().length > 0);
    assertEquals(false, request.isLast());
    assertEquals(1, request.getPageCount());
  }

  @SuppressWarnings("unchecked")
  @Test
  @DisplayName("send throws when coordinator node not found")
  void sendThrowsWhenCoordinatorNotFound() {
    when(discoveryNodes.get("coord-1")).thenReturn(null);

    DqeExchangeChunk chunk = createChunk(5, true);
    DqeException ex = assertThrows(DqeException.class, () -> sender.send(chunk));
    assertTrue(ex.getMessage().contains("not found"));
  }

  @SuppressWarnings("unchecked")
  @Test
  @DisplayName("send throws when coordinator rejects chunk")
  void sendThrowsWhenRejected() {
    DqeExchangeChunk chunk = createChunk(5, false);

    doAnswer(
            inv -> {
              TransportResponseHandler<DqeExchangePushResponse> handler = inv.getArgument(3);
              handler.handleResponse(new DqeExchangePushResponse(false, 0));
              return null;
            })
        .when(transportService)
        .sendRequest(any(DiscoveryNode.class), eq(DqeExchangePushAction.NAME), any(), any());

    DqeException ex = assertThrows(DqeException.class, () -> sender.send(chunk));
    assertTrue(ex.getMessage().contains("rejected"));
  }

  @Test
  @DisplayName("serializePages produces non-empty output for non-empty chunk")
  void serializePagesProducesOutput() {
    DqeExchangeChunk chunk = createChunk(5, false);
    byte[] serialized = TransportChunkSender.serializePages(chunk);
    assertNotNull(serialized);
    assertTrue(serialized.length > 0);
  }

  @Test
  @DisplayName("send rejects null chunk")
  void sendRejectsNull() {
    assertThrows(NullPointerException.class, () -> sender.send(null));
  }
}
