/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.executor;

import java.util.ArrayList;
import java.util.List;
import org.apache.calcite.rel.RelNode;
import org.opensearch.cluster.routing.IndexRoutingTable;
import org.opensearch.cluster.routing.IndexShardRoutingTable;
import org.opensearch.cluster.routing.ShardRouting;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.sql.opensearch.planner.merge.Exchange;
import org.opensearch.sql.opensearch.storage.scan.CalciteEnumerableIndexScan;

/**
 * Resolves active shard routing information from the cluster state for distributed query dispatch.
 *
 * <p>Provides utility methods to look up active primary shards for a given index and to extract
 * index names from plan subtrees below Exchange nodes.
 */
public class ShardRoutingResolver {

    private ShardRoutingResolver() {
        // Utility class
    }

    /**
     * Resolves active primary shards for the given index.
     *
     * @param clusterService the cluster service for accessing cluster state
     * @param indexName the target index name
     * @return list of active primary shard routings
     * @throws IllegalStateException if the index is not found in the routing table
     */
    public static List<ShardRouting> resolveActiveShards(
            ClusterService clusterService, String indexName) {
        IndexRoutingTable indexRouting =
                clusterService.state().routingTable().index(indexName);
        if (indexRouting == null) {
            throw new IllegalStateException(
                    "Index not found in routing table: " + indexName);
        }
        List<ShardRouting> activeShards = new ArrayList<>();
        for (IndexShardRoutingTable shardRouting : indexRouting) {
            ShardRouting primary = shardRouting.primaryShard();
            if (primary != null && primary.active()) {
                activeShards.add(primary);
            }
        }
        return activeShards;
    }

    /**
     * Extracts the index name from a plan subtree by finding the first
     * {@link CalciteEnumerableIndexScan} node.
     *
     * @param plan the plan subtree to search
     * @return the index name, or null if no scan node is found
     */
    public static String extractIndexName(RelNode plan) {
        if (plan instanceof CalciteEnumerableIndexScan scan) {
            // The table's qualified name contains the index name as its last element
            List<String> qualifiedName = scan.getTable().getQualifiedName();
            return qualifiedName.get(qualifiedName.size() - 1);
        }
        // Skip Exchange nodes — they are coordinator-level boundaries
        if (plan instanceof Exchange) {
            return extractIndexName(((Exchange) plan).getInput());
        }
        // Recurse into children
        for (RelNode child : plan.getInputs()) {
            String name = extractIndexName(child);
            if (name != null) {
                return name;
            }
        }
        return null;
    }
}
