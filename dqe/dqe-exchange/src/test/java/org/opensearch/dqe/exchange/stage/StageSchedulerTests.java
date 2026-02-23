/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dqe.exchange.stage;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.opensearch.action.ActionListenerResponseHandler;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.node.DiscoveryNodes;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.core.action.ActionListener;
import org.opensearch.dqe.exchange.action.DqeStageExecuteAction;
import org.opensearch.dqe.exchange.action.DqeStageExecuteRequest;
import org.opensearch.dqe.exchange.action.DqeStageExecuteRequest.ShardSplitInfo;
import org.opensearch.dqe.exchange.action.DqeStageExecuteResponse;
import org.opensearch.transport.TransportResponseHandler;
import org.opensearch.transport.TransportService;

class StageSchedulerTests {

  private TransportService transportService;
  private ClusterService clusterService;
  private ClusterState clusterState;
  private DiscoveryNodes discoveryNodes;
  private StageScheduler scheduler;

  @BeforeEach
  void setUp() {
    transportService = mock(TransportService.class);
    clusterService = mock(ClusterService.class);
    clusterState = mock(ClusterState.class);
    discoveryNodes = mock(DiscoveryNodes.class);
    when(clusterService.state()).thenReturn(clusterState);
    when(clusterState.nodes()).thenReturn(discoveryNodes);
    scheduler = new StageScheduler(transportService, clusterService);
  }

  @Test
  @DisplayName("constructor rejects null transportService")
  void constructorRejectsNullTransportService() {
    assertThrows(NullPointerException.class, () -> new StageScheduler(null, clusterService));
  }

  @Test
  @DisplayName("constructor rejects null clusterService")
  void constructorRejectsNullClusterService() {
    assertThrows(NullPointerException.class, () -> new StageScheduler(transportService, null));
  }

  @Test
  @DisplayName("scheduleStage with empty splits completes immediately")
  void scheduleStageEmptySplits() {
    AtomicReference<StageScheduleResult> result = new AtomicReference<>();
    scheduler.scheduleStage(
        "q1",
        0,
        new byte[0],
        Collections.emptyList(),
        "coord",
        1024,
        ActionListener.wrap(
            result::set,
            e -> {
              throw new AssertionError("unexpected failure", e);
            }));

    assertEquals("q1", result.get().getQueryId());
    assertEquals(0, result.get().getStageId());
    assertEquals(0, result.get().getDispatchedTaskCount());
  }

  @Test
  @DisplayName("scheduleStage rejects null queryId")
  void scheduleStageRejectsNullQueryId() {
    assertThrows(
        NullPointerException.class,
        () ->
            scheduler.scheduleStage(
                null,
                0,
                new byte[0],
                Collections.emptyList(),
                "coord",
                1024,
                ActionListener.wrap(r -> {}, e -> {})));
  }

  @Test
  @DisplayName("scheduleStage dispatches to nodes and collects accepted count")
  @SuppressWarnings("unchecked")
  void scheduleStageDispatchesToNodes() {
    DiscoveryNode node1 = mock(DiscoveryNode.class);
    DiscoveryNode node2 = mock(DiscoveryNode.class);
    when(discoveryNodes.get("node1")).thenReturn(node1);
    when(discoveryNodes.get("node2")).thenReturn(node2);

    ShardSplitInfo split1 = mock(ShardSplitInfo.class);
    when(split1.getNodeId()).thenReturn("node1");
    ShardSplitInfo split2 = mock(ShardSplitInfo.class);
    when(split2.getNodeId()).thenReturn("node2");

    List<TransportResponseHandler<DqeStageExecuteResponse>> capturedHandlers = new ArrayList<>();

    doAnswer(
            inv -> {
              TransportResponseHandler<DqeStageExecuteResponse> handler = inv.getArgument(3);
              capturedHandlers.add(handler);
              return null;
            })
        .when(transportService)
        .sendRequest(
            any(DiscoveryNode.class),
            eq(DqeStageExecuteAction.NAME),
            any(DqeStageExecuteRequest.class),
            any(ActionListenerResponseHandler.class));

    AtomicReference<StageScheduleResult> result = new AtomicReference<>();
    scheduler.scheduleStage(
        "q1",
        0,
        new byte[0],
        List.of(split1, split2),
        "coord",
        1024,
        ActionListener.wrap(
            result::set,
            e -> {
              throw new AssertionError("unexpected failure", e);
            }));

    assertEquals(2, capturedHandlers.size());

    // Simulate both nodes accepting
    capturedHandlers.get(0).handleResponse(new DqeStageExecuteResponse("q1", 0, true));
    // First response shouldn't complete yet
    if (result.get() == null) {
      capturedHandlers.get(1).handleResponse(new DqeStageExecuteResponse("q1", 0, true));
    }

    assertEquals(2, result.get().getDispatchedTaskCount());
  }

  @Test
  @DisplayName("scheduleStage skips nodes not in cluster state")
  @SuppressWarnings("unchecked")
  void scheduleStageSkipsMissingNodes() {
    when(discoveryNodes.get("node1")).thenReturn(null);

    ShardSplitInfo split1 = mock(ShardSplitInfo.class);
    when(split1.getNodeId()).thenReturn("node1");

    AtomicReference<StageScheduleResult> result = new AtomicReference<>();
    scheduler.scheduleStage(
        "q1",
        0,
        new byte[0],
        List.of(split1),
        "coord",
        1024,
        ActionListener.wrap(
            result::set,
            e -> {
              throw new AssertionError("unexpected failure", e);
            }));

    // Should complete with 0 accepted
    assertEquals(0, result.get().getDispatchedTaskCount());
  }

  @Test
  @DisplayName("scheduleStage handles node rejection")
  @SuppressWarnings("unchecked")
  void scheduleStageHandlesRejection() {
    DiscoveryNode node1 = mock(DiscoveryNode.class);
    when(discoveryNodes.get("node1")).thenReturn(node1);

    ShardSplitInfo split1 = mock(ShardSplitInfo.class);
    when(split1.getNodeId()).thenReturn("node1");

    List<TransportResponseHandler<DqeStageExecuteResponse>> capturedHandlers = new ArrayList<>();

    doAnswer(
            inv -> {
              TransportResponseHandler<DqeStageExecuteResponse> handler = inv.getArgument(3);
              capturedHandlers.add(handler);
              return null;
            })
        .when(transportService)
        .sendRequest(
            any(DiscoveryNode.class),
            eq(DqeStageExecuteAction.NAME),
            any(DqeStageExecuteRequest.class),
            any(ActionListenerResponseHandler.class));

    AtomicReference<StageScheduleResult> result = new AtomicReference<>();
    scheduler.scheduleStage(
        "q1",
        0,
        new byte[0],
        List.of(split1),
        "coord",
        1024,
        ActionListener.wrap(
            result::set,
            e -> {
              throw new AssertionError("unexpected failure", e);
            }));

    // Node rejects
    capturedHandlers.get(0).handleResponse(new DqeStageExecuteResponse("q1", 0, false));

    assertEquals(0, result.get().getDispatchedTaskCount());
  }

  @Test
  @DisplayName("scheduleStage with all nodes failing reports error")
  @SuppressWarnings("unchecked")
  void scheduleStageAllNodesFailReportsError() {
    DiscoveryNode node1 = mock(DiscoveryNode.class);
    when(discoveryNodes.get("node1")).thenReturn(node1);

    ShardSplitInfo split1 = mock(ShardSplitInfo.class);
    when(split1.getNodeId()).thenReturn("node1");

    List<TransportResponseHandler<DqeStageExecuteResponse>> capturedHandlers = new ArrayList<>();

    doAnswer(
            inv -> {
              TransportResponseHandler<DqeStageExecuteResponse> handler = inv.getArgument(3);
              capturedHandlers.add(handler);
              return null;
            })
        .when(transportService)
        .sendRequest(
            any(DiscoveryNode.class),
            eq(DqeStageExecuteAction.NAME),
            any(DqeStageExecuteRequest.class),
            any(ActionListenerResponseHandler.class));

    AtomicReference<Exception> failure = new AtomicReference<>();
    scheduler.scheduleStage(
        "q1",
        0,
        new byte[0],
        List.of(split1),
        "coord",
        1024,
        ActionListener.wrap(
            r -> {
              throw new AssertionError("expected failure");
            },
            failure::set));

    capturedHandlers
        .get(0)
        .handleException(
            new org.opensearch.transport.RemoteTransportException(
                "connection refused", new RuntimeException("connection refused")));

    assertTrue(failure.get().getMessage().contains("no data nodes accepted"));
  }

  @Test
  @DisplayName("getActiveNodes returns node IDs for active queries")
  @SuppressWarnings("unchecked")
  void getActiveNodesReturnsTracked() {
    DiscoveryNode node1 = mock(DiscoveryNode.class);
    when(discoveryNodes.get("node1")).thenReturn(node1);

    ShardSplitInfo split1 = mock(ShardSplitInfo.class);
    when(split1.getNodeId()).thenReturn("node1");

    // Don't complete the response so the node stays active
    doAnswer(inv -> null)
        .when(transportService)
        .sendRequest(
            any(DiscoveryNode.class),
            eq(DqeStageExecuteAction.NAME),
            any(DqeStageExecuteRequest.class),
            any(ActionListenerResponseHandler.class));

    scheduler.scheduleStage(
        "q1",
        0,
        new byte[0],
        List.of(split1),
        "coord",
        1024,
        ActionListener.wrap(r -> {}, e -> {}));

    Set<String> activeNodes = scheduler.getActiveNodes("q1");
    assertTrue(activeNodes.contains("node1"));
  }

  @Test
  @DisplayName("getActiveNodes returns empty set for unknown query")
  void getActiveNodesEmptyForUnknown() {
    assertTrue(scheduler.getActiveNodes("unknown").isEmpty());
  }

  @Test
  @DisplayName("deregisterQuery removes tracking")
  @SuppressWarnings("unchecked")
  void deregisterQueryRemovesTracking() {
    DiscoveryNode node1 = mock(DiscoveryNode.class);
    when(discoveryNodes.get("node1")).thenReturn(node1);

    ShardSplitInfo split1 = mock(ShardSplitInfo.class);
    when(split1.getNodeId()).thenReturn("node1");

    doAnswer(inv -> null)
        .when(transportService)
        .sendRequest(
            any(DiscoveryNode.class),
            eq(DqeStageExecuteAction.NAME),
            any(DqeStageExecuteRequest.class),
            any(ActionListenerResponseHandler.class));

    scheduler.scheduleStage(
        "q1",
        0,
        new byte[0],
        List.of(split1),
        "coord",
        1024,
        ActionListener.wrap(r -> {}, e -> {}));

    assertTrue(scheduler.getActiveNodes("q1").contains("node1"));

    scheduler.deregisterQuery("q1");
    assertTrue(scheduler.getActiveNodes("q1").isEmpty());
  }

  @Test
  @DisplayName("cancelQuery with no active nodes completes immediately")
  void cancelQueryNoActiveNodes() {
    AtomicReference<Boolean> completed = new AtomicReference<>(false);
    scheduler.cancelQuery(
        "unknown",
        ActionListener.wrap(
            v -> completed.set(true),
            e -> {
              throw new AssertionError("unexpected", e);
            }));
    assertTrue(completed.get());
  }

  @Test
  @DisplayName("cancelStage with no active nodes completes immediately")
  void cancelStageNoActiveNodes() {
    AtomicReference<Boolean> completed = new AtomicReference<>(false);
    scheduler.cancelStage(
        "unknown",
        0,
        ActionListener.wrap(
            v -> completed.set(true),
            e -> {
              throw new AssertionError("unexpected", e);
            }));
    assertTrue(completed.get());
  }

  @Test
  @DisplayName("scheduleStage with partial failure still reports accepted count")
  @SuppressWarnings("unchecked")
  void scheduleStagePartialFailure() {
    DiscoveryNode node1 = mock(DiscoveryNode.class);
    DiscoveryNode node2 = mock(DiscoveryNode.class);
    when(discoveryNodes.get("node1")).thenReturn(node1);
    when(discoveryNodes.get("node2")).thenReturn(node2);

    ShardSplitInfo split1 = mock(ShardSplitInfo.class);
    when(split1.getNodeId()).thenReturn("node1");
    ShardSplitInfo split2 = mock(ShardSplitInfo.class);
    when(split2.getNodeId()).thenReturn("node2");

    List<TransportResponseHandler<DqeStageExecuteResponse>> capturedHandlers = new ArrayList<>();

    doAnswer(
            inv -> {
              TransportResponseHandler<DqeStageExecuteResponse> handler = inv.getArgument(3);
              capturedHandlers.add(handler);
              return null;
            })
        .when(transportService)
        .sendRequest(
            any(DiscoveryNode.class),
            eq(DqeStageExecuteAction.NAME),
            any(DqeStageExecuteRequest.class),
            any(ActionListenerResponseHandler.class));

    AtomicReference<StageScheduleResult> result = new AtomicReference<>();
    scheduler.scheduleStage(
        "q1",
        0,
        new byte[0],
        List.of(split1, split2),
        "coord",
        1024,
        ActionListener.wrap(
            result::set,
            e -> {
              throw new AssertionError("unexpected failure", e);
            }));

    // First node accepts
    capturedHandlers.get(0).handleResponse(new DqeStageExecuteResponse("q1", 0, true));
    // Second node fails
    capturedHandlers
        .get(1)
        .handleException(
            new org.opensearch.transport.RemoteTransportException(
                "node2 down", new RuntimeException("node2 down")));

    // Should still complete successfully since at least one node accepted
    assertEquals(1, result.get().getDispatchedTaskCount());
  }
}
