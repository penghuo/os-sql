/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dqe.metadata;

import java.util.Objects;
import org.opensearch.cluster.routing.IndexShardRoutingTable;
import org.opensearch.cluster.routing.ShardRouting;

/**
 * Selects a single shard copy (primary or replica) from a shard routing table for query execution.
 *
 * <p>Selection strategy for Phase 1: prefer the primary shard. If the primary is not started
 * (relocating, unassigned), select the first started replica. Prefer local shards (same node as
 * coordinator) when available.
 */
public class ShardSelector {

  private final String localNodeId;

  public ShardSelector(String localNodeId) {
    this.localNodeId = Objects.requireNonNull(localNodeId, "localNodeId must not be null");
  }

  /**
   * Selects a shard copy from the routing table.
   *
   * @param shardRoutingTable the shard routing table containing primary and replicas
   * @return the selected shard routing, or null if no copy is available
   */
  public ShardRouting select(IndexShardRoutingTable shardRoutingTable) {
    ShardRouting primary = shardRoutingTable.primaryShard();

    // Prefer started primary
    if (primary != null && primary.started()) {
      return primary;
    }

    // Primary not started — find first started replica, prefer local
    ShardRouting localReplica = null;
    ShardRouting anyReplica = null;
    for (ShardRouting replica : shardRoutingTable) {
      if (replica.primary()) {
        continue;
      }
      if (!replica.started()) {
        continue;
      }
      if (replica.currentNodeId().equals(localNodeId)) {
        localReplica = replica;
        break; // Local replica is best
      }
      if (anyReplica == null) {
        anyReplica = replica;
      }
    }

    if (localReplica != null) {
      return localReplica;
    }
    if (anyReplica != null) {
      return anyReplica;
    }

    // No started copy available
    return null;
  }
}
