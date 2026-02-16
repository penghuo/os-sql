/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.executor.distributed;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.metadata.Metadata;
import org.opensearch.cluster.routing.IndexRoutingTable;
import org.opensearch.cluster.routing.IndexShardRoutingTable;
import org.opensearch.cluster.routing.RoutingTable;
import org.opensearch.cluster.routing.ShardRouting;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.sql.planner.distributed.ShardSplit;

class OpenSearchShardSplitManagerTest {

    private ClusterService clusterService;
    private ClusterState clusterState;
    private RoutingTable routingTable;
    private Metadata metadata;
    private OpenSearchShardSplitManager manager;

    @BeforeEach
    void setUp() {
        clusterService = mock(ClusterService.class);
        clusterState = mock(ClusterState.class);
        routingTable = mock(RoutingTable.class);
        metadata = mock(Metadata.class);

        when(clusterService.state()).thenReturn(clusterState);
        when(clusterState.routingTable()).thenReturn(routingTable);
        when(clusterState.metadata()).thenReturn(metadata);

        manager = new OpenSearchShardSplitManager(clusterService);
    }

    @Test
    @DisplayName("getSplits returns empty list for unknown index")
    void getSplitsReturnsEmptyForUnknownIndex() {
        when(routingTable.index("unknown")).thenReturn(null);

        List<ShardSplit> splits = manager.getSplits("unknown", "node-1");
        assertTrue(splits.isEmpty());
    }

    @Test
    @DisplayName("getSplits returns shard splits for a valid index")
    void getSplitsReturnsShardSplits() {
        IndexRoutingTable indexRoutingTable = mock(IndexRoutingTable.class);
        when(routingTable.index("test-index")).thenReturn(indexRoutingTable);

        // Set up shard 0 with primary on node-1, replica on node-2
        IndexShardRoutingTable shard0 = mock(IndexShardRoutingTable.class);
        ShardRouting primary0 = mock(ShardRouting.class);
        when(primary0.assignedToNode()).thenReturn(true);
        when(primary0.currentNodeId()).thenReturn("node-1");
        when(shard0.primaryShard()).thenReturn(primary0);

        ShardRouting replica0 = mock(ShardRouting.class);
        when(replica0.assignedToNode()).thenReturn(true);
        when(replica0.currentNodeId()).thenReturn("node-2");
        when(shard0.replicaShards()).thenReturn(List.of(replica0));

        // Set up shard 1 with primary on node-2, replica on node-1
        IndexShardRoutingTable shard1 = mock(IndexShardRoutingTable.class);
        ShardRouting primary1 = mock(ShardRouting.class);
        when(primary1.assignedToNode()).thenReturn(true);
        when(primary1.currentNodeId()).thenReturn("node-2");
        when(shard1.primaryShard()).thenReturn(primary1);

        ShardRouting replica1 = mock(ShardRouting.class);
        when(replica1.assignedToNode()).thenReturn(true);
        when(replica1.currentNodeId()).thenReturn("node-1");
        when(shard1.replicaShards()).thenReturn(List.of(replica1));

        when(indexRoutingTable.getShards()).thenReturn(Map.of(0, shard0, 1, shard1));

        List<ShardSplit> splits = manager.getSplits("test-index", "node-1");

        assertEquals(2, splits.size());

        // Find the split for shard 0 (local to node-1)
        ShardSplit split0 =
                splits.stream().filter(s -> s.getShardId() == 0).findFirst().orElseThrow();
        assertEquals("test-index", split0.getIndexName());
        assertEquals("node-1", split0.getPreferredNodeId());
        assertEquals(List.of("node-2"), split0.getReplicaNodeIds());
        assertTrue(split0.isLocal());

        // Find the split for shard 1 (remote from node-1)
        ShardSplit split1 =
                splits.stream().filter(s -> s.getShardId() == 1).findFirst().orElseThrow();
        assertEquals("node-2", split1.getPreferredNodeId());
        assertFalse(split1.isLocal());
    }

    @Test
    @DisplayName("getSplits skips unassigned shards")
    void getSplitsSkipsUnassignedShards() {
        IndexRoutingTable indexRoutingTable = mock(IndexRoutingTable.class);
        when(routingTable.index("test-index")).thenReturn(indexRoutingTable);

        IndexShardRoutingTable shard0 = mock(IndexShardRoutingTable.class);
        ShardRouting primary0 = mock(ShardRouting.class);
        when(primary0.assignedToNode()).thenReturn(false);
        when(shard0.primaryShard()).thenReturn(primary0);

        when(indexRoutingTable.getShards()).thenReturn(Map.of(0, shard0));

        List<ShardSplit> splits = manager.getSplits("test-index", "node-1");
        assertTrue(splits.isEmpty());
    }

    @Test
    @DisplayName("getDocCount returns 0 for unknown index")
    void getDocCountReturnsZeroForUnknownIndex() {
        when(metadata.index("unknown")).thenReturn(null);

        assertEquals(0L, manager.getDocCount("unknown"));
    }

    @Test
    @DisplayName("getDocCount returns -1 for known index (unknown count)")
    void getDocCountReturnsNegativeOneForKnownIndex() {
        IndexMetadata indexMetadata = mock(IndexMetadata.class);
        when(metadata.index("test-index")).thenReturn(indexMetadata);

        assertEquals(-1L, manager.getDocCount("test-index"));
    }
}
