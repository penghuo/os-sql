/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.omni.connector.opensearch;

import io.trino.spi.HostAddress;
import io.trino.spi.connector.*;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.routing.IndexRoutingTable;
import org.opensearch.cluster.routing.IndexShardRoutingTable;
import org.opensearch.cluster.routing.ShardRouting;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.index.IndexService;
import org.opensearch.index.engine.Engine;
import org.opensearch.index.engine.Segment;
import org.opensearch.index.shard.IndexShard;
import org.opensearch.indices.IndicesService;

import java.util.*;
import java.util.function.Supplier;

import static java.util.Objects.requireNonNull;

public class OpenSearchSplitManager
        implements ConnectorSplitManager
{
    private static final Logger logger = LogManager.getLogger(OpenSearchSplitManager.class);
    private static final int TARGET_DOCS_PER_SPLIT = 100_000;
    private final ClusterService clusterService;
    private final Supplier<IndicesService> indicesServiceSupplier;

    public OpenSearchSplitManager(ClusterService clusterService, Supplier<IndicesService> indicesServiceSupplier)
    {
        this.clusterService = requireNonNull(clusterService);
        this.indicesServiceSupplier = requireNonNull(indicesServiceSupplier);
    }

    @Override
    public ConnectorSplitSource getSplits(
            ConnectorTransactionHandle transaction,
            ConnectorSession session,
            ConnectorTableHandle table,
            DynamicFilter dynamicFilter,
            Constraint constraint)
    {
        OpenSearchTableHandle osTable = (OpenSearchTableHandle) table;
        String indexName = osTable.getIndex();
        ClusterState state = clusterService.state();

        // 1. Get shard routing
        IndexRoutingTable routingTable = state.routingTable().index(indexName);
        if (routingTable == null) {
            return new FixedSplitSource(List.of());
        }

        // 2. Get segment metadata and build segment-level splits
        Map<Integer, List<Segment>> shardSegmentsMap = new HashMap<>();
        try {
            IndicesService indicesService = indicesServiceSupplier.get();
            if (indicesService != null) {
                IndexService indexService = indicesService.indexService(routingTable.getIndex());
                if (indexService != null) {
                    shardSegmentsMap = buildShardSegmentsMap(indexService);
                }
            }
        } catch (Exception e) {
            logger.warn("Failed to get segment metadata for index [{}], falling back to shard-level splits", indexName, e);
        }

        // 3. Build splits — segment-level when metadata available, shard-level otherwise
        List<ConnectorSplit> splits = new ArrayList<>();

        for (IndexShardRoutingTable shardRoutingTable : routingTable) {
            int shardId = shardRoutingTable.shardId().id();
            List<HostAddress> addresses = getShardAddresses(shardRoutingTable, state);
            if (addresses.isEmpty()) {
                continue;
            }

            List<Segment> segments = shardSegmentsMap.getOrDefault(shardId, List.of());
            if (segments.isEmpty()) {
                splits.add(OpenSearchSplit.shardSplit(indexName, shardId, TARGET_DOCS_PER_SPLIT, addresses));
                continue;
            }

            logger.debug("Shard {} has {} segments", shardId, segments.size());

            for (Segment segment : segments) {
                String segName = segment.getName();
                int docCount = segment.getNumDocs();

                // One split per segment (no doc-range sub-splitting for now).
                // Doc-range splits cause ClosedChannelException because multiple splits
                // sharing the same segment close each other's file handles via ref-counting.
                splits.add(new OpenSearchSplit(indexName, shardId, segName,
                        -1, -1, docCount, addresses));
            }
        }

        if (splits.isEmpty()) {
            splits.addAll(createShardLevelSplits(routingTable, state));
        }

        logger.debug("Total splits created: {} for index [{}]", splits.size(), indexName);
        return new FixedSplitSource(splits);
    }

    private Map<Integer, List<Segment>> buildShardSegmentsMap(IndexService indexService)
    {
        Map<Integer, List<Segment>> result = new HashMap<>();
        for (IndexShard shard : indexService) {
            try {
                // Get segments from the shard's engine
                List<Segment> segments = shard.segments(false);
                result.put(shard.shardId().id(), segments);
            } catch (Exception e) {
                logger.warn("Failed to get segment info for shard [{}], skipping", shard.shardId(), e);
            }
        }
        return result;
    }

    private List<ConnectorSplit> createShardLevelSplits(IndexRoutingTable routingTable, ClusterState state)
    {
        List<ConnectorSplit> splits = new ArrayList<>();
        for (IndexShardRoutingTable shardRoutingTable : routingTable) {
            int shardId = shardRoutingTable.shardId().id();
            List<HostAddress> addresses = getShardAddresses(shardRoutingTable, state);
            if (!addresses.isEmpty()) {
                splits.add(OpenSearchSplit.shardSplit(routingTable.getIndex().getName(),
                        shardId, TARGET_DOCS_PER_SPLIT, addresses));
            }
        }
        return splits;
    }

    private List<HostAddress> getShardAddresses(IndexShardRoutingTable shardRouting, ClusterState state)
    {
        List<HostAddress> addresses = new ArrayList<>();
        for (ShardRouting routing : shardRouting) {
            if (routing.active()) { // Only STARTED shards
                DiscoveryNode node = state.nodes().get(routing.currentNodeId());
                if (node != null) {
                    // Use the transport port (not exchange port) for split-to-node matching.
                    // InternalNode.getHostAndPort() uses the transport port, so split addresses
                    // must match for the scheduler's selectExactNodes() to find candidate nodes.
                    // The actual exchange communication port is resolved separately by LocationFactory.
                    addresses.add(HostAddress.fromParts(node.getHostAddress(), node.getAddress().getPort()));
                }
            }
        }
        return addresses;
    }
}
