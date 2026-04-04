/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.trino.node;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.trino.metadata.AllNodes;
import io.trino.metadata.InternalNode;
import io.trino.metadata.NodeState;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Iterator;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.stream.Stream;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.opensearch.Version;
import org.opensearch.cluster.ClusterChangedEvent;
import org.opensearch.cluster.ClusterName;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.node.DiscoveryNodes;
import org.opensearch.core.common.transport.TransportAddress;

class OpenSearchNodeManagerTest {

  private DiscoveryNode localDiscoveryNode;
  private OpenSearchNodeManager nodeManager;

  @BeforeEach
  void setUp() throws UnknownHostException {
    localDiscoveryNode = createDiscoveryNode("node-1", "10.0.0.1", 9300);
    nodeManager = new OpenSearchNodeManager(localDiscoveryNode);
  }

  @Test
  void testCurrentNodeReturnedOnInit() {
    InternalNode current = nodeManager.getCurrentNode();
    assertNotNull(current);
    assertEquals("node-1", current.getNodeIdentifier());
    assertTrue(current.isCoordinator());
  }

  @Test
  void testLocalNodeIsActiveCoordinatorAndWorker() {
    Set<InternalNode> active = nodeManager.getNodes(NodeState.ACTIVE);
    assertEquals(1, active.size());

    Set<InternalNode> coordinators = nodeManager.getCoordinators();
    assertEquals(1, coordinators.size());
    assertTrue(coordinators.iterator().next().isCoordinator());

    AllNodes allNodes = nodeManager.getAllNodes();
    assertEquals(1, allNodes.getActiveNodes().size());
    assertEquals(1, allNodes.getActiveCoordinators().size());
    assertTrue(allNodes.getInactiveNodes().isEmpty());
    assertTrue(allNodes.getShuttingDownNodes().isEmpty());
  }

  @Test
  void testClusterStateChangeUpdatesNodeSet() throws UnknownHostException {
    DiscoveryNode node2 = createDiscoveryNode("node-2", "10.0.0.2", 9300);
    DiscoveryNode node3 = createDiscoveryNode("node-3", "10.0.0.3", 9300);

    ClusterChangedEvent event = createClusterChangedEvent(
        localDiscoveryNode, true, localDiscoveryNode, node2, node3);

    nodeManager.clusterChanged(event);

    Set<InternalNode> active = nodeManager.getNodes(NodeState.ACTIVE);
    assertEquals(3, active.size());

    Set<String> nodeIds = active.stream()
        .map(InternalNode::getNodeIdentifier)
        .collect(java.util.stream.Collectors.toSet());
    assertTrue(nodeIds.contains("node-1"));
    assertTrue(nodeIds.contains("node-2"));
    assertTrue(nodeIds.contains("node-3"));

    AllNodes allNodes = nodeManager.getAllNodes();
    assertEquals(3, allNodes.getActiveNodes().size());
    assertEquals(3, allNodes.getActiveCoordinators().size());
  }

  @Test
  void testListenerFiresOnTopologyChange() throws UnknownHostException {
    AtomicReference<AllNodes> received = new AtomicReference<>();
    Consumer<AllNodes> listener = received::set;
    nodeManager.addNodeChangeListener(listener);

    DiscoveryNode node2 = createDiscoveryNode("node-2", "10.0.0.2", 9300);
    ClusterChangedEvent event = createClusterChangedEvent(
        localDiscoveryNode, true, localDiscoveryNode, node2);

    nodeManager.clusterChanged(event);

    assertNotNull(received.get());
    assertEquals(2, received.get().getActiveNodes().size());
  }

  @Test
  void testRemovedListenerDoesNotFire() throws UnknownHostException {
    AtomicReference<AllNodes> received = new AtomicReference<>();
    Consumer<AllNodes> listener = received::set;
    nodeManager.addNodeChangeListener(listener);
    nodeManager.removeNodeChangeListener(listener);

    DiscoveryNode node2 = createDiscoveryNode("node-2", "10.0.0.2", 9300);
    ClusterChangedEvent event = createClusterChangedEvent(
        localDiscoveryNode, true, localDiscoveryNode, node2);
    nodeManager.clusterChanged(event);

    // Listener was removed, so it should not have been called
    assertTrue(received.get() == null, "Listener should not have been invoked after removal");
  }

  @Test
  void testRemovedNodeIsGone() throws UnknownHostException {
    // First add two nodes
    DiscoveryNode node2 = createDiscoveryNode("node-2", "10.0.0.2", 9300);
    ClusterChangedEvent event1 = createClusterChangedEvent(
        localDiscoveryNode, true, localDiscoveryNode, node2);
    nodeManager.clusterChanged(event1);
    assertEquals(2, nodeManager.getNodes(NodeState.ACTIVE).size());

    // Then remove node-2
    ClusterChangedEvent event2 = createClusterChangedEvent(
        localDiscoveryNode, true, localDiscoveryNode);
    nodeManager.clusterChanged(event2);

    Set<InternalNode> active = nodeManager.getNodes(NodeState.ACTIVE);
    assertEquals(1, active.size());
    assertEquals("node-1", active.iterator().next().getNodeIdentifier());
  }

  @Test
  void testNoChangeEventDoesNotUpdateNodes() throws UnknownHostException {
    // Event where nodesChanged() returns false
    DiscoveryNode node2 = createDiscoveryNode("node-2", "10.0.0.2", 9300);
    ClusterChangedEvent event = createClusterChangedEvent(
        localDiscoveryNode, false, localDiscoveryNode, node2);

    nodeManager.clusterChanged(event);

    // Should still have only the initial local node
    assertEquals(1, nodeManager.getNodes(NodeState.ACTIVE).size());
  }

  @Test
  void testInactiveAndShuttingDownStatesReturnEmpty() {
    assertTrue(nodeManager.getNodes(NodeState.INACTIVE).isEmpty());
    assertTrue(nodeManager.getNodes(NodeState.SHUTTING_DOWN).isEmpty());
  }

  @Test
  void testGetActiveCatalogNodesReturnsAllActive() throws UnknownHostException {
    DiscoveryNode node2 = createDiscoveryNode("node-2", "10.0.0.2", 9300);
    ClusterChangedEvent event = createClusterChangedEvent(
        localDiscoveryNode, true, localDiscoveryNode, node2);
    nodeManager.clusterChanged(event);

    // Should return all active nodes regardless of catalog
    assertEquals(2, nodeManager.getActiveCatalogNodes(null).size());
  }

  @Test
  void testGetActiveNodesSnapshot() throws UnknownHostException {
    DiscoveryNode node2 = createDiscoveryNode("node-2", "10.0.0.2", 9300);
    ClusterChangedEvent event = createClusterChangedEvent(
        localDiscoveryNode, true, localDiscoveryNode, node2);
    nodeManager.clusterChanged(event);

    var snapshot = nodeManager.getActiveNodesSnapshot();
    assertNotNull(snapshot);
    assertEquals(2, snapshot.getAllNodes().size());
  }

  // --- Helpers ---

  private static DiscoveryNode createDiscoveryNode(String id, String host, int port)
      throws UnknownHostException {
    TransportAddress address = new TransportAddress(InetAddress.getByName(host), port);
    return new DiscoveryNode(
        id,       // node name
        id,       // node id
        address,
        java.util.Collections.emptyMap(),
        java.util.Collections.emptySet(),
        Version.CURRENT);
  }

  @SuppressWarnings("unchecked")
  private static ClusterChangedEvent createClusterChangedEvent(
      DiscoveryNode localNode, boolean nodesChanged, DiscoveryNode... nodes) {

    DiscoveryNodes.Builder builder = DiscoveryNodes.builder();
    for (DiscoveryNode node : nodes) {
      builder.add(node);
    }
    builder.localNodeId(localNode.getId());
    builder.clusterManagerNodeId(localNode.getId());
    DiscoveryNodes discoveryNodes = builder.build();

    ClusterState state = ClusterState.builder(new ClusterName("test-cluster"))
        .nodes(discoveryNodes)
        .build();

    ClusterChangedEvent event = mock(ClusterChangedEvent.class);
    when(event.state()).thenReturn(state);
    when(event.nodesChanged()).thenReturn(nodesChanged);
    return event;
  }
}
