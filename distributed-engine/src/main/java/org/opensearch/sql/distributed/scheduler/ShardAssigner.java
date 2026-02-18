/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.distributed.scheduler;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.routing.IndexRoutingTable;
import org.opensearch.cluster.routing.IndexShardRoutingTable;
import org.opensearch.cluster.routing.RoutingTable;
import org.opensearch.cluster.routing.ShardRouting;

/**
 * Assigns shards to their primary (preferred) data nodes using the cluster routing table. For each
 * shard, the primary replica's node is preferred for data locality. If the primary is unavailable,
 * a started replica is used as fallback.
 */
public class ShardAssigner {

  /**
   * Computes the shard-to-node assignment for a given index.
   *
   * @param routingTable the cluster routing table
   * @param nodes mapping of node IDs to DiscoveryNodes for resolving shard locations
   * @param indexName the index to compute assignments for
   * @return a NodeAssignment mapping each node to its assigned shard IDs
   * @throws IllegalStateException if no active shard is found for a shard ID
   */
  public NodeAssignment assign(
      RoutingTable routingTable, Map<String, DiscoveryNode> nodes, String indexName) {

    IndexRoutingTable indexRouting = routingTable.index(indexName);
    if (indexRouting == null) {
      throw new IllegalArgumentException("Index not found in routing table: " + indexName);
    }

    Map<DiscoveryNode, List<Integer>> nodeToShards = new HashMap<>();

    for (Map.Entry<Integer, IndexShardRoutingTable> entry : indexRouting.getShards().entrySet()) {
      int shardId = entry.getKey();
      IndexShardRoutingTable shardRoutingTable = entry.getValue();
      DiscoveryNode targetNode = selectNodeForShard(shardRoutingTable, nodes);

      nodeToShards.computeIfAbsent(targetNode, k -> new ArrayList<>()).add(shardId);
    }

    return new NodeAssignment(nodeToShards);
  }

  /**
   * Selects the best node for a shard. Prefers the primary shard's node for data locality. Falls
   * back to any started replica if primary is unavailable.
   */
  private DiscoveryNode selectNodeForShard(
      IndexShardRoutingTable shardRoutingTable, Map<String, DiscoveryNode> nodes) {

    // Prefer the primary shard's node
    ShardRouting primaryShard = shardRoutingTable.primaryShard();
    if (primaryShard.started() || primaryShard.relocating()) {
      String nodeId =
          primaryShard.relocating()
              ? primaryShard.relocatingNodeId()
              : primaryShard.currentNodeId();
      DiscoveryNode node = nodes.get(nodeId);
      if (node != null) {
        return node;
      }
    }

    // Fallback: find any started replica
    for (ShardRouting shard : shardRoutingTable.getShards()) {
      if (shard.started() || shard.relocating()) {
        String nodeId = shard.relocating() ? shard.relocatingNodeId() : shard.currentNodeId();
        DiscoveryNode node = nodes.get(nodeId);
        if (node != null) {
          return node;
        }
      }
    }

    throw new IllegalStateException(
        "No active shard found for shard " + shardRoutingTable.shardId());
  }
}
