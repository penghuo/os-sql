/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.trino.node;

import io.trino.client.NodeVersion;
import io.trino.metadata.AllNodes;
import io.trino.metadata.InternalNode;
import io.trino.metadata.InternalNodeManager;
import io.trino.metadata.NodeState;
import io.trino.spi.connector.CatalogHandle;
import java.net.URI;
import java.util.Collections;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.Consumer;
import org.opensearch.cluster.ClusterChangedEvent;
import org.opensearch.cluster.ClusterStateListener;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.node.DiscoveryNodes;

/**
 * Maps OpenSearch cluster topology to Trino's {@link InternalNodeManager} interface.
 *
 * <p>Listens for OpenSearch cluster state changes via {@link ClusterStateListener} and maintains a
 * set of Trino {@link InternalNode} instances representing the current cluster topology. Every data
 * node is treated as both a coordinator and a worker (symmetric design).
 */
public class OpenSearchNodeManager implements InternalNodeManager, ClusterStateListener {

  private final CopyOnWriteArrayList<Consumer<AllNodes>> listeners = new CopyOnWriteArrayList<>();

  private volatile InternalNode currentNode;
  private volatile Set<InternalNode> activeNodes;
  private volatile AllNodes allNodes;

  /**
   * Creates an OpenSearchNodeManager seeded with the local node.
   *
   * @param localNode the local OpenSearch discovery node
   */
  public OpenSearchNodeManager(DiscoveryNode localNode) {
    this.currentNode = toInternalNode(localNode);
    this.activeNodes = Set.of(currentNode);
    this.allNodes = buildAllNodes(activeNodes);
  }

  @Override
  public void clusterChanged(ClusterChangedEvent event) {
    if (!event.nodesChanged()) {
      return;
    }
    DiscoveryNodes discoveryNodes = event.state().nodes();
    Set<InternalNode> newActiveNodes = new HashSet<>();
    for (DiscoveryNode dn : discoveryNodes) {
      newActiveNodes.add(toInternalNode(dn));
    }
    newActiveNodes = Collections.unmodifiableSet(newActiveNodes);

    // Update local node reference from cluster state
    DiscoveryNode localDiscoveryNode = discoveryNodes.getLocalNode();
    if (localDiscoveryNode != null) {
      this.currentNode = toInternalNode(localDiscoveryNode);
    }

    this.activeNodes = newActiveNodes;
    AllNodes newAllNodes = buildAllNodes(newActiveNodes);
    this.allNodes = newAllNodes;

    // Notify listeners
    for (Consumer<AllNodes> listener : listeners) {
      listener.accept(newAllNodes);
    }
  }

  @Override
  public Set<InternalNode> getNodes(NodeState state) {
    return switch (state) {
      case ACTIVE -> activeNodes;
      case INACTIVE, SHUTTING_DOWN -> Collections.emptySet();
    };
  }

  @Override
  public Set<InternalNode> getActiveCatalogNodes(CatalogHandle catalogHandle) {
    // All active nodes serve all catalogs in symmetric design
    return activeNodes;
  }

  @Override
  public NodesSnapshot getActiveNodesSnapshot() {
    return new NodesSnapshot(activeNodes, Optional.empty());
  }

  @Override
  public InternalNode getCurrentNode() {
    return currentNode;
  }

  @Override
  public Set<InternalNode> getCoordinators() {
    // In symmetric design every node is a coordinator
    return activeNodes;
  }

  @Override
  public AllNodes getAllNodes() {
    return allNodes;
  }

  @Override
  public void refreshNodes() {
    // No-op: node set is updated via clusterChanged()
  }

  @Override
  public void addNodeChangeListener(Consumer<AllNodes> listener) {
    listeners.add(listener);
  }

  @Override
  public void removeNodeChangeListener(Consumer<AllNodes> listener) {
    listeners.remove(listener);
  }

  /**
   * Convert an OpenSearch {@link DiscoveryNode} to a Trino {@link InternalNode}.
   *
   * <p>Uses the node's transport address to construct a URI and maps the OpenSearch version string
   * to a Trino {@link NodeVersion}. All nodes are marked as coordinators (symmetric design).
   */
  static InternalNode toInternalNode(DiscoveryNode discoveryNode) {
    String host = discoveryNode.getHostAddress();
    int port = discoveryNode.getAddress().address().getPort();
    URI uri = URI.create("http://" + host + ":" + port);
    NodeVersion version = new NodeVersion(discoveryNode.getVersion().toString());
    return new InternalNode(discoveryNode.getId(), uri, version, /* isCoordinator= */ true);
  }

  private static AllNodes buildAllNodes(Set<InternalNode> active) {
    // AllNodes(activeNodes, inactiveNodes, shuttingDownNodes, activeCoordinators)
    return new AllNodes(active, Collections.emptySet(), Collections.emptySet(), active);
  }
}
