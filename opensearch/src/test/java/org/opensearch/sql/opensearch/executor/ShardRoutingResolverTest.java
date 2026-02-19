/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.executor;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Iterator;
import java.util.List;
import org.apache.calcite.adapter.enumerable.EnumerableConvention;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.volcano.VolcanoPlanner;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexBuilder;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.routing.IndexRoutingTable;
import org.opensearch.cluster.routing.IndexShardRoutingTable;
import org.opensearch.cluster.routing.RoutingTable;
import org.opensearch.cluster.routing.ShardRouting;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.sql.calcite.utils.OpenSearchTypeFactory;
import org.opensearch.sql.opensearch.planner.merge.ConcatExchange;
import org.opensearch.sql.opensearch.storage.scan.CalciteEnumerableIndexScan;

@ExtendWith(MockitoExtension.class)
class ShardRoutingResolverTest {

    @Mock private ClusterService clusterService;
    @Mock private ClusterState clusterState;
    @Mock private RoutingTable routingTable;

    @BeforeEach
    void setUp() {
        lenient().when(clusterService.state()).thenReturn(clusterState);
        lenient().when(clusterState.routingTable()).thenReturn(routingTable);
    }

    @Nested
    @DisplayName("resolveActiveShards tests")
    class ResolveActiveShardsTests {

        @Test
        @DisplayName("returns active primary shards for existing index")
        void testReturnsActivePrimaryShards() {
            IndexRoutingTable indexRoutingTable = mock(IndexRoutingTable.class);
            when(routingTable.index("test-index")).thenReturn(indexRoutingTable);

            ShardRouting activeShard1 = mock(ShardRouting.class);
            when(activeShard1.active()).thenReturn(true);

            ShardRouting activeShard2 = mock(ShardRouting.class);
            when(activeShard2.active()).thenReturn(true);

            ShardRouting inactiveShard = mock(ShardRouting.class);
            when(inactiveShard.active()).thenReturn(false);

            IndexShardRoutingTable shardTable1 = mock(IndexShardRoutingTable.class);
            when(shardTable1.primaryShard()).thenReturn(activeShard1);

            IndexShardRoutingTable shardTable2 = mock(IndexShardRoutingTable.class);
            when(shardTable2.primaryShard()).thenReturn(activeShard2);

            IndexShardRoutingTable shardTable3 = mock(IndexShardRoutingTable.class);
            when(shardTable3.primaryShard()).thenReturn(inactiveShard);

            @SuppressWarnings("unchecked")
            Iterator<IndexShardRoutingTable> iterator =
                    List.of(shardTable1, shardTable2, shardTable3).iterator();
            when(indexRoutingTable.iterator()).thenReturn(iterator);

            List<ShardRouting> result =
                    ShardRoutingResolver.resolveActiveShards(clusterService, "test-index");

            assertEquals(2, result.size());
            assertTrue(result.contains(activeShard1));
            assertTrue(result.contains(activeShard2));
        }

        @Test
        @DisplayName("returns empty list when no active primary shards exist")
        void testReturnsEmptyWhenNoActiveShards() {
            IndexRoutingTable indexRoutingTable = mock(IndexRoutingTable.class);
            when(routingTable.index("test-index")).thenReturn(indexRoutingTable);

            ShardRouting inactiveShard = mock(ShardRouting.class);
            when(inactiveShard.active()).thenReturn(false);

            IndexShardRoutingTable shardTable = mock(IndexShardRoutingTable.class);
            when(shardTable.primaryShard()).thenReturn(inactiveShard);

            @SuppressWarnings("unchecked")
            Iterator<IndexShardRoutingTable> iterator = List.of(shardTable).iterator();
            when(indexRoutingTable.iterator()).thenReturn(iterator);

            List<ShardRouting> result =
                    ShardRoutingResolver.resolveActiveShards(clusterService, "test-index");

            assertTrue(result.isEmpty());
        }

        @Test
        @DisplayName("returns empty list when shard has null primary")
        void testReturnsEmptyWhenNullPrimary() {
            IndexRoutingTable indexRoutingTable = mock(IndexRoutingTable.class);
            when(routingTable.index("test-index")).thenReturn(indexRoutingTable);

            IndexShardRoutingTable shardTable = mock(IndexShardRoutingTable.class);
            when(shardTable.primaryShard()).thenReturn(null);

            @SuppressWarnings("unchecked")
            Iterator<IndexShardRoutingTable> iterator = List.of(shardTable).iterator();
            when(indexRoutingTable.iterator()).thenReturn(iterator);

            List<ShardRouting> result =
                    ShardRoutingResolver.resolveActiveShards(clusterService, "test-index");

            assertTrue(result.isEmpty());
        }

        @Test
        @DisplayName("throws when index not found in routing table")
        void testThrowsWhenIndexNotFound() {
            when(routingTable.index("missing-index")).thenReturn(null);

            IllegalStateException exception =
                    assertThrows(
                            IllegalStateException.class,
                            () ->
                                    ShardRoutingResolver.resolveActiveShards(
                                            clusterService, "missing-index"));

            assertTrue(exception.getMessage().contains("Index not found in routing table"));
            assertTrue(exception.getMessage().contains("missing-index"));
        }
    }

    @Nested
    @DisplayName("extractIndexName tests")
    class ExtractIndexNameTests {

        private RelOptCluster cluster;

        @BeforeEach
        void setUp() {
            RexBuilder rexBuilder =
                    new RexBuilder(OpenSearchTypeFactory.TYPE_FACTORY);
            cluster = RelOptCluster.create(new VolcanoPlanner(), rexBuilder);
        }

        @Test
        @DisplayName("extracts index name from CalciteEnumerableIndexScan")
        void testExtractsFromScanNode() {
            RelOptTable table = mock(RelOptTable.class);
            when(table.getQualifiedName()).thenReturn(List.of("schema", "my_index"));

            CalciteEnumerableIndexScan scanNode = mock(CalciteEnumerableIndexScan.class);
            when(scanNode.getTable()).thenReturn(table);

            String indexName = ShardRoutingResolver.extractIndexName(scanNode);

            assertEquals("my_index", indexName);
        }

        @Test
        @DisplayName("extracts index name through Exchange node")
        void testExtractsFromExchangeChild() {
            RelOptTable table = mock(RelOptTable.class);
            when(table.getQualifiedName()).thenReturn(List.of("schema", "nested_index"));

            CalciteEnumerableIndexScan scanNode = mock(CalciteEnumerableIndexScan.class);
            when(scanNode.getTable()).thenReturn(table);
            when(scanNode.getCluster()).thenReturn(cluster);

            ConcatExchange exchange = ConcatExchange.create(scanNode);

            String indexName = ShardRoutingResolver.extractIndexName(exchange);

            assertEquals("nested_index", indexName);
        }

        @Test
        @DisplayName("extracts index name from nested child")
        void testExtractsFromNestedChild() {
            RelOptTable table = mock(RelOptTable.class);
            when(table.getQualifiedName()).thenReturn(List.of("catalog", "schema", "deep_index"));

            CalciteEnumerableIndexScan scanNode = mock(CalciteEnumerableIndexScan.class);
            when(scanNode.getTable()).thenReturn(table);

            // Wrap in a generic RelNode with a child
            RelNode wrapperNode = mock(RelNode.class);
            when(wrapperNode.getInputs()).thenReturn(List.of(scanNode));

            String indexName = ShardRoutingResolver.extractIndexName(wrapperNode);

            assertEquals("deep_index", indexName);
        }

        @Test
        @DisplayName("returns null when no scan node found")
        void testReturnsNullWhenNoScanNode() {
            RelNode node = mock(RelNode.class);
            when(node.getInputs()).thenReturn(List.of());

            String indexName = ShardRoutingResolver.extractIndexName(node);

            assertNull(indexName);
        }

        @Test
        @DisplayName("returns null for tree without CalciteEnumerableIndexScan")
        void testReturnsNullForTreeWithoutScan() {
            RelNode child1 = mock(RelNode.class);
            when(child1.getInputs()).thenReturn(List.of());

            RelNode child2 = mock(RelNode.class);
            when(child2.getInputs()).thenReturn(List.of());

            RelNode parent = mock(RelNode.class);
            when(parent.getInputs()).thenReturn(List.of(child1, child2));

            String indexName = ShardRoutingResolver.extractIndexName(parent);

            assertNull(indexName);
        }
    }
}
