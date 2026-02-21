/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.dqe;

import java.util.List;
import java.util.stream.Collectors;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.routing.IndexRoutingTable;
import org.opensearch.cluster.routing.ShardRouting;
import org.opensearch.cluster.routing.ShardRoutingState;
import org.opensearch.cluster.service.ClusterService;

/**
 * Resolves the active primary shard IDs for a given OpenSearch index. Used by {@link
 * DistributedExecutor} to determine which shards to dispatch plan fragments to.
 */
public class ShardRoutingResolver {

  private final ClusterService clusterService;

  public ShardRoutingResolver(ClusterService clusterService) {
    this.clusterService = clusterService;
  }

  /**
   * Resolves active primary shard IDs for the given index.
   *
   * @param indexName the OpenSearch index name
   * @return list of active primary shard IDs
   * @throws IllegalArgumentException if the index does not exist or has no active shards
   */
  public List<Integer> resolve(String indexName) {
    ClusterState state = clusterService.state();
    IndexRoutingTable indexRouting = state.routingTable().index(indexName);
    if (indexRouting == null) {
      throw new IllegalArgumentException(
          "Index [" + indexName + "] not found in cluster routing table");
    }

    List<Integer> activeShardIds =
        indexRouting.shardsWithState(ShardRoutingState.STARTED).stream()
            .filter(ShardRouting::primary)
            .map(shard -> shard.shardId().id())
            .collect(Collectors.toList());

    if (activeShardIds.isEmpty()) {
      throw new IllegalArgumentException(
          "Index [" + indexName + "] has no active primary shards");
    }

    return activeShardIds;
  }
}
