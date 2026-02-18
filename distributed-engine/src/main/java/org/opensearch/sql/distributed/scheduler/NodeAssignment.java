/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.distributed.scheduler;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import org.opensearch.cluster.node.DiscoveryNode;

/**
 * Describes the assignment of shards to nodes for a stage fragment. Each node receives a list of
 * shard IDs it is responsible for executing.
 */
public class NodeAssignment {

  private final Map<DiscoveryNode, List<Integer>> nodeToShards;

  public NodeAssignment(Map<DiscoveryNode, List<Integer>> nodeToShards) {
    this.nodeToShards = Map.copyOf(Objects.requireNonNull(nodeToShards, "nodeToShards is null"));
  }

  /** Returns the mapping of nodes to their assigned shard IDs. */
  public Map<DiscoveryNode, List<Integer>> getNodeToShards() {
    return nodeToShards;
  }

  /** Returns all nodes that have shard assignments. */
  public java.util.Set<DiscoveryNode> getNodes() {
    return nodeToShards.keySet();
  }

  /** Returns the shard IDs assigned to the given node, or empty list if none. */
  public List<Integer> getShardsForNode(DiscoveryNode node) {
    return nodeToShards.getOrDefault(node, List.of());
  }

  /** Returns the total number of shards across all nodes. */
  public int totalShards() {
    return nodeToShards.values().stream().mapToInt(List::size).sum();
  }

  @Override
  public String toString() {
    return "NodeAssignment{nodes=" + nodeToShards.size() + ", totalShards=" + totalShards() + "}";
  }
}
