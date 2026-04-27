/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.plugin.omni.cluster;

import com.google.common.collect.ImmutableSet;
import io.trino.client.NodeVersion;
import io.trino.metadata.AllNodes;
import io.trino.metadata.InternalNode;
import io.trino.metadata.InternalNodeManager;
import io.trino.metadata.NodeState;
import io.trino.spi.connector.CatalogHandle;
import org.opensearch.cluster.ClusterChangedEvent;
import org.opensearch.cluster.ClusterStateListener;
import org.opensearch.cluster.node.DiscoveryNode;

import java.net.URI;
import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.Consumer;

/**
 * Implements Trino's {@link InternalNodeManager} by listening to OpenSearch cluster state changes.
 * Maps OpenSearch {@link DiscoveryNode}s to Trino {@link InternalNode}s.
 */
public class ClusterStateNodeManager implements InternalNodeManager, ClusterStateListener {

    private static final NodeVersion NODE_VERSION = new NodeVersion("omni-1.0");

    private static final int DEFAULT_EXCHANGE_PORT = 9500;

    private final CopyOnWriteArrayList<Consumer<AllNodes>> listeners = new CopyOnWriteArrayList<>();
    private volatile InternalNode currentNode;
    private volatile Set<InternalNode> activeNodes = ImmutableSet.of();
    private volatile Set<InternalNode> coordinators = ImmutableSet.of();
    private final String localNodeId;
    // Maps node ID → exchange port, populated from DiscoveryNode attributes
    private final Map<String, Integer> nodeExchangePorts = new ConcurrentHashMap<>();

    public ClusterStateNodeManager(String localNodeId) {
        this.localNodeId = localNodeId;
        // currentNode is set on first clusterChanged() when local node info is available
        this.currentNode = new InternalNode(localNodeId, URI.create("opensearch://localhost:9300"), NODE_VERSION, true);
    }

    @Override
    public void clusterChanged(ClusterChangedEvent event) {
        ImmutableSet.Builder<InternalNode> allBuilder = ImmutableSet.builder();
        ImmutableSet.Builder<InternalNode> coordBuilder = ImmutableSet.builder();

        for (DiscoveryNode dn : event.state().nodes()) {
            InternalNode node = toInternalNode(dn);
            allBuilder.add(node);
            if (dn.isClusterManagerNode()) {
                coordBuilder.add(node);
            }
            if (dn.getId().equals(localNodeId)) {
                this.currentNode = node;
            }
            // Read exchange port from node attribute (published by OmniPlugin.additionalSettings)
            String portAttr = dn.getAttributes().get("omni_exchange_port");
            if (portAttr != null) {
                nodeExchangePorts.put(dn.getId(), Integer.parseInt(portAttr));
            }
        }

        this.activeNodes = allBuilder.build();
        this.coordinators = coordBuilder.build();

        AllNodes allNodes = getAllNodes();
        for (Consumer<AllNodes> listener : listeners) {
            listener.accept(allNodes);
        }
    }

    @Override
    public Set<InternalNode> getNodes(NodeState state) {
        if (state == NodeState.ACTIVE) {
            return activeNodes;
        }
        return ImmutableSet.of();
    }

    @Override
    public Set<InternalNode> getActiveCatalogNodes(CatalogHandle catalogHandle) {
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
        return coordinators;
    }

    @Override
    public AllNodes getAllNodes() {
        return new AllNodes(activeNodes, Collections.emptySet(), Collections.emptySet(), coordinators);
    }

    @Override
    public void refreshNodes() {
        // No-op: OpenSearch cluster state is push-based
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
     * Returns the exchange port for a given node, read from the node's
     * {@code omni_exchange_port} attribute published via cluster state.
     */
    public int getExchangePort(InternalNode node) {
        return nodeExchangePorts.getOrDefault(node.getNodeIdentifier(), DEFAULT_EXCHANGE_PORT);
    }

    private static InternalNode toInternalNode(DiscoveryNode dn) {
        URI uri = URI.create("opensearch://" + dn.getHostAddress() + ":" + dn.getAddress().getPort());
        return new InternalNode(dn.getId(), uri, NODE_VERSION, dn.isClusterManagerNode());
    }
}
