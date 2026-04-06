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
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.Consumer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
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

  private static final Logger LOG = LogManager.getLogger(OpenSearchNodeManager.class);

  private final CopyOnWriteArrayList<Consumer<AllNodes>> listeners = new CopyOnWriteArrayList<>();

  // Reverse mapping: Trino node ID → OpenSearch DiscoveryNode
  private final Map<String, DiscoveryNode> nodeIdToDiscoveryNode = new ConcurrentHashMap<>();

  // Trino HTTP URL mapping: OpenSearch node ID → Trino HTTP server URI
  // Used to construct InternalNode with correct HTTP URIs for exchange
  private final Map<String, URI> trinoHttpUrls = new ConcurrentHashMap<>();

  private volatile InternalNode currentNode;
  private volatile Set<InternalNode> activeNodes;
  private volatile AllNodes allNodes;

  /**
   * Creates an OpenSearchNodeManager seeded with the local node.
   *
   * @param localNode the local OpenSearch discovery node
   */
  public OpenSearchNodeManager(DiscoveryNode localNode) {
    this.currentNode = toInternalNode(localNode, null);
    this.activeNodes = Set.of(currentNode);
    this.allNodes = buildAllNodes(activeNodes);
    nodeIdToDiscoveryNode.put(localNode.getId(), localNode);
  }

  @Override
  public void clusterChanged(ClusterChangedEvent event) {
    // Always rebuild on explicit calls (e.g., trino-init) even if nodes didn't change,
    // to pick up registered Trino HTTP URLs.
    DiscoveryNodes discoveryNodes = event.state().nodes();
    Set<InternalNode> newActiveNodes = new HashSet<>();
    nodeIdToDiscoveryNode.clear();
    for (DiscoveryNode dn : discoveryNodes) {
      newActiveNodes.add(toInternalNode(dn, trinoHttpUrls.get(dn.getId())));
      nodeIdToDiscoveryNode.put(dn.getId(), dn);
    }
    newActiveNodes = Collections.unmodifiableSet(newActiveNodes);

    // Update local node reference from cluster state
    DiscoveryNode localDiscoveryNode = discoveryNodes.getLocalNode();
    if (localDiscoveryNode != null) {
      this.currentNode = toInternalNode(
          localDiscoveryNode, trinoHttpUrls.get(localDiscoveryNode.getId()));
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
   * Map a Trino {@link InternalNode} back to its OpenSearch {@link DiscoveryNode}. Used by {@code
   * TransportRemoteTaskFactory} to send transport actions to the correct node.
   *
   * @param internalNode the Trino node
   * @return the corresponding OpenSearch discovery node
   * @throws IllegalArgumentException if the node is not known
   */
  public DiscoveryNode toDiscoveryNode(InternalNode internalNode) {
    DiscoveryNode dn = nodeIdToDiscoveryNode.get(internalNode.getNodeIdentifier());
    if (dn == null) {
      throw new IllegalArgumentException(
          "Unknown Trino node: " + internalNode.getNodeIdentifier()
              + ". Known nodes: " + nodeIdToDiscoveryNode.keySet());
    }
    return dn;
  }

  /**
   * Register the Trino HTTP server URL for an OpenSearch node. This URL is used to construct
   * InternalNodes with correct HTTP URIs for Trino's exchange mechanism.
   *
   * @param nodeId OpenSearch node ID
   * @param httpUrl Trino HTTP server URI (e.g., http://host:port)
   */
  public void registerTrinoHttpUrl(String nodeId, URI httpUrl) {
    trinoHttpUrls.put(nodeId, httpUrl);
    LOG.info("Registered Trino HTTP URL for node {}: {}", nodeId, httpUrl);

    // Rebuild activeNodes with the new URL so the coordinator sees updated URIs
    rebuildActiveNodes();
  }

  /** Rebuild activeNodes from the current nodeIdToDiscoveryNode map with registered HTTP URLs. */
  private void rebuildActiveNodes() {
    if (nodeIdToDiscoveryNode.isEmpty()) {
      return;
    }
    Set<InternalNode> newActiveNodes = new HashSet<>();
    for (Map.Entry<String, DiscoveryNode> entry : nodeIdToDiscoveryNode.entrySet()) {
      newActiveNodes.add(toInternalNode(entry.getValue(), trinoHttpUrls.get(entry.getKey())));
    }
    newActiveNodes = Collections.unmodifiableSet(newActiveNodes);
    this.activeNodes = newActiveNodes;
    this.allNodes = buildAllNodes(newActiveNodes);
    // Update currentNode if its URL changed
    for (InternalNode node : newActiveNodes) {
      if (currentNode != null && node.getNodeIdentifier().equals(
          currentNode.getNodeIdentifier())) {
        this.currentNode = node;
        break;
      }
    }
  }

  /**
   * Get the registered Trino HTTP URL for a node. Returns null if not registered.
   */
  public URI getTrinoHttpUrl(String nodeId) {
    return trinoHttpUrls.get(nodeId);
  }

  /**
   * Convert an OpenSearch {@link DiscoveryNode} to a Trino {@link InternalNode}.
   *
   * <p>If a Trino HTTP URL is registered for this node, uses that URL. Otherwise falls back to
   * constructing a URI from the transport address.
   */
  static InternalNode toInternalNode(DiscoveryNode discoveryNode, URI trinoHttpUrl) {
    URI uri;
    if (trinoHttpUrl != null) {
      uri = trinoHttpUrl;
    } else {
      String host = discoveryNode.getHostAddress();
      int port = discoveryNode.getAddress().address().getPort();
      uri = URI.create("http://" + host + ":" + port);
    }
    NodeVersion version = new NodeVersion(discoveryNode.getVersion().toString());
    return new InternalNode(discoveryNode.getId(), uri, version, /* isCoordinator= */ true);
  }

  private static AllNodes buildAllNodes(Set<InternalNode> active) {
    // AllNodes(activeNodes, inactiveNodes, shuttingDownNodes, activeCoordinators)
    return new AllNodes(active, Collections.emptySet(), Collections.emptySet(), active);
  }
}
