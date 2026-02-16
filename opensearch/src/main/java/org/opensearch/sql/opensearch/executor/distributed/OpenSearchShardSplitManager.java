/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.executor.distributed;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import lombok.RequiredArgsConstructor;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.routing.IndexRoutingTable;
import org.opensearch.cluster.routing.IndexShardRoutingTable;
import org.opensearch.cluster.routing.ShardRouting;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.sql.planner.distributed.ShardSplit;
import org.opensearch.sql.planner.distributed.ShardSplitManager;

/**
 * OpenSearch implementation of {@link ShardSplitManager} that queries the cluster state to discover
 * shard topology for an index.
 */
@RequiredArgsConstructor
public class OpenSearchShardSplitManager implements ShardSplitManager {

    private final ClusterService clusterService;

    @Override
    public List<ShardSplit> getSplits(String indexName, String localNodeId) {
        ClusterState clusterState = clusterService.state();
        IndexRoutingTable indexRoutingTable = clusterState.routingTable().index(indexName);
        if (indexRoutingTable == null) {
            return List.of();
        }

        List<ShardSplit> splits = new ArrayList<>();
        for (Map.Entry<Integer, IndexShardRoutingTable> entry :
                indexRoutingTable.getShards().entrySet()) {
            int shardId = entry.getKey();
            IndexShardRoutingTable shardRoutingTable = entry.getValue();
            ShardRouting primaryShard = shardRoutingTable.primaryShard();
            if (primaryShard == null || !primaryShard.assignedToNode()) {
                continue;
            }

            String preferredNodeId = primaryShard.currentNodeId();
            List<String> replicaNodeIds =
                    shardRoutingTable.replicaShards().stream()
                            .filter(ShardRouting::assignedToNode)
                            .map(ShardRouting::currentNodeId)
                            .collect(Collectors.toList());
            boolean isLocal = preferredNodeId.equals(localNodeId);

            splits.add(
                    new ShardSplit(indexName, shardId, preferredNodeId, replicaNodeIds, isLocal));
        }
        return splits;
    }

    @Override
    public long getDocCount(String indexName) {
        ClusterState clusterState = clusterService.state();
        IndexMetadata indexMetadata = clusterState.metadata().index(indexName);
        if (indexMetadata == null) {
            return 0L;
        }
        // Without an indices stats request, return -1 to signal "unknown". The QueryRouter
        // interprets negative values as "default to distributed if enabled".
        return -1L;
    }
}
