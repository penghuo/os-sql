/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.plugin.transport;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import org.apache.calcite.adapter.enumerable.EnumerableConvention;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.volcano.VolcanoPlanner;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rex.RexBuilder;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.routing.IndexRoutingTable;
import org.opensearch.cluster.routing.IndexShardRoutingTable;
import org.opensearch.cluster.routing.RoutingTable;
import org.opensearch.cluster.routing.ShardRouting;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.core.action.ActionListener;
import org.opensearch.sql.calcite.utils.OpenSearchTypeFactory;
import org.opensearch.sql.opensearch.planner.merge.ConcatExchange;
import org.opensearch.transport.client.node.NodeClient;

@Ignore("We ignore it because it conflicts with shadow Jar solution of calcite.")
@RunWith(MockitoJUnitRunner.class)
public class DistributedExecutorTest {

    @Mock private NodeClient client;
    @Mock private ClusterService clusterService;
    @Mock private ClusterState clusterState;
    @Mock private RoutingTable routingTable;

    private RelOptCluster cluster;

    @Before
    public void setUp() {
        RexBuilder rexBuilder = new RexBuilder(OpenSearchTypeFactory.TYPE_FACTORY);
        cluster = RelOptCluster.create(new VolcanoPlanner(), rexBuilder);
        lenient().when(clusterService.state()).thenReturn(clusterState);
        lenient().when(clusterState.routingTable()).thenReturn(routingTable);
    }

    @Test
    public void testThrowsWhenNoExchangeNode() {
        DistributedExecutor executor = new DistributedExecutor(client, clusterService);
        RelNode plainNode = mock(RelNode.class);
        when(plainNode.getInputs()).thenReturn(List.of());

        try {
            executor.execute(plainNode, "test-index");
            fail("Expected IllegalStateException");
        } catch (IllegalStateException e) {
            assertTrue(e.getMessage().contains("No Exchange node found"));
        }
    }

    @Test
    public void testThrowsWhenIndexNotFound() {
        DistributedExecutor executor = new DistributedExecutor(client, clusterService);
        when(routingTable.index("missing-index")).thenReturn(null);

        RelNode mockInput = mock(RelNode.class);
        lenient().when(mockInput.getCluster()).thenReturn(cluster);
        lenient().when(mockInput.getInputs()).thenReturn(List.of());
        ConcatExchange exchange = ConcatExchange.create(mockInput);

        try {
            executor.execute(exchange, "missing-index");
            fail("Expected IllegalStateException");
        } catch (IllegalStateException e) {
            assertTrue(e.getMessage().contains("Index not found"));
        }
    }

    @Test
    public void testThrowsWhenNoActiveShards() {
        DistributedExecutor executor = new DistributedExecutor(client, clusterService);

        IndexRoutingTable indexRoutingTable = mock(IndexRoutingTable.class);
        when(routingTable.index("empty-index")).thenReturn(indexRoutingTable);

        @SuppressWarnings("unchecked")
        Iterator<IndexShardRoutingTable> emptyIterator = Collections.emptyIterator();
        when(indexRoutingTable.iterator()).thenReturn(emptyIterator);

        RelNode mockInput = mock(RelNode.class);
        lenient().when(mockInput.getCluster()).thenReturn(cluster);
        lenient().when(mockInput.getInputs()).thenReturn(List.of());
        ConcatExchange exchange = ConcatExchange.create(mockInput);

        try {
            executor.execute(exchange, "empty-index");
            fail("Expected IllegalStateException");
        } catch (IllegalStateException e) {
            assertTrue(e.getMessage().contains("No active shards"));
        }
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testExecuteWithConcatExchangeCollectsResults() {
        DistributedExecutor executor = new DistributedExecutor(client, clusterService);

        setupActiveShards("test-index", 2);

        doAnswer(invocation -> {
            CalciteShardRequest request = invocation.getArgument(1);
            ActionListener<CalciteShardResponse> listener = invocation.getArgument(2);

            int shardId = request.getShardId();
            CalciteShardResponse response;
            if (shardId == 0) {
                response = new CalciteShardResponse(
                        Arrays.<Object[]>asList(new Object[] {"row1", 10}),
                        List.of("name", "value"),
                        0);
            } else {
                response = new CalciteShardResponse(
                        Arrays.<Object[]>asList(new Object[] {"row2", 20}),
                        List.of("name", "value"),
                        1);
            }
            listener.onResponse(response);
            return null;
        }).when(client).execute(eq(CalciteShardAction.INSTANCE), any(), any());

        RelNode mockInput = mock(RelNode.class);
        lenient().when(mockInput.getCluster()).thenReturn(cluster);
        lenient().when(mockInput.getInputs()).thenReturn(List.of());
        lenient().when(mockInput.getRowType()).thenReturn(
                OpenSearchTypeFactory.TYPE_FACTORY.createStructType(
                        List.of(
                                OpenSearchTypeFactory.TYPE_FACTORY.createJavaType(String.class),
                                OpenSearchTypeFactory.TYPE_FACTORY.createJavaType(Integer.class)),
                        List.of("name", "value")));

        ConcatExchange exchange = ConcatExchange.create(mockInput);

        List<Object[]> results = executor.execute(exchange, "test-index");

        assertEquals(2, results.size());
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testThrowsWhenShardReturnsError() {
        DistributedExecutor executor = new DistributedExecutor(client, clusterService);

        setupActiveShards("test-index", 1);

        doAnswer(invocation -> {
            ActionListener<CalciteShardResponse> listener = invocation.getArgument(2);
            CalciteShardResponse errorResponse =
                    new CalciteShardResponse(0, "Shard execution failed: out of memory");
            listener.onResponse(errorResponse);
            return null;
        }).when(client).execute(eq(CalciteShardAction.INSTANCE), any(), any());

        RelNode mockInput = mock(RelNode.class);
        lenient().when(mockInput.getCluster()).thenReturn(cluster);
        lenient().when(mockInput.getInputs()).thenReturn(List.of());
        lenient().when(mockInput.getRowType()).thenReturn(
                OpenSearchTypeFactory.TYPE_FACTORY.createStructType(
                        List.of(OpenSearchTypeFactory.TYPE_FACTORY.createJavaType(String.class)),
                        List.of("col1")));

        ConcatExchange exchange = ConcatExchange.create(mockInput);

        try {
            executor.execute(exchange, "test-index");
            fail("Expected RuntimeException");
        } catch (RuntimeException e) {
            assertTrue(e.getMessage().contains("Shard"));
            assertTrue(e.getMessage().contains("failed"));
        }
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testThrowsWhenTransportFails() {
        DistributedExecutor executor = new DistributedExecutor(client, clusterService);

        setupActiveShards("test-index", 1);

        doAnswer(invocation -> {
            ActionListener<CalciteShardResponse> listener = invocation.getArgument(2);
            listener.onFailure(new RuntimeException("Connection refused"));
            return null;
        }).when(client).execute(eq(CalciteShardAction.INSTANCE), any(), any());

        RelNode mockInput = mock(RelNode.class);
        lenient().when(mockInput.getCluster()).thenReturn(cluster);
        lenient().when(mockInput.getInputs()).thenReturn(List.of());
        lenient().when(mockInput.getRowType()).thenReturn(
                OpenSearchTypeFactory.TYPE_FACTORY.createStructType(
                        List.of(OpenSearchTypeFactory.TYPE_FACTORY.createJavaType(String.class)),
                        List.of("col1")));

        ConcatExchange exchange = ConcatExchange.create(mockInput);

        try {
            executor.execute(exchange, "test-index");
            fail("Expected RuntimeException");
        } catch (RuntimeException e) {
            assertTrue(e.getMessage().contains("Shard dispatch failed"));
        }
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testShardResultsInjectedIntoExchange() {
        DistributedExecutor executor = new DistributedExecutor(client, clusterService);

        setupActiveShards("test-index", 2);

        doAnswer(invocation -> {
            CalciteShardRequest request = invocation.getArgument(1);
            ActionListener<CalciteShardResponse> listener = invocation.getArgument(2);

            int shardId = request.getShardId();
            CalciteShardResponse response = new CalciteShardResponse(
                    Arrays.asList(
                            new Object[] {"shard" + shardId + "_row1"},
                            new Object[] {"shard" + shardId + "_row2"}),
                    List.of("data"),
                    shardId,
                    Map.of());
            listener.onResponse(response);
            return null;
        }).when(client).execute(eq(CalciteShardAction.INSTANCE), any(), any());

        RelNode mockInput = mock(RelNode.class);
        lenient().when(mockInput.getCluster()).thenReturn(cluster);
        lenient().when(mockInput.getInputs()).thenReturn(List.of());
        lenient().when(mockInput.getRowType()).thenReturn(
                OpenSearchTypeFactory.TYPE_FACTORY.createStructType(
                        List.of(OpenSearchTypeFactory.TYPE_FACTORY.createJavaType(String.class)),
                        List.of("data")));

        ConcatExchange exchange = ConcatExchange.create(mockInput);

        List<Object[]> results = executor.execute(exchange, "test-index");

        // 2 shards * 2 rows each = 4 total rows
        assertEquals(4, results.size());
    }

    /**
     * Sets up mock routing infrastructure with the specified number of active primary shards
     * for the given index.
     */
    private void setupActiveShards(String indexName, int numShards) {
        IndexRoutingTable indexRoutingTable = mock(IndexRoutingTable.class);
        when(routingTable.index(indexName)).thenReturn(indexRoutingTable);

        java.util.List<IndexShardRoutingTable> shardTables = new java.util.ArrayList<>();
        for (int i = 0; i < numShards; i++) {
            ShardRouting shardRouting = mock(ShardRouting.class);
            when(shardRouting.active()).thenReturn(true);
            when(shardRouting.getId()).thenReturn(i);

            IndexShardRoutingTable shardTable = mock(IndexShardRoutingTable.class);
            when(shardTable.primaryShard()).thenReturn(shardRouting);
            shardTables.add(shardTable);
        }

        when(indexRoutingTable.iterator()).thenReturn(shardTables.iterator());
    }
}
