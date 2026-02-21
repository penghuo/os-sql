/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.dqe;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Collections;
import java.util.List;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.routing.IndexRoutingTable;
import org.opensearch.cluster.routing.RoutingTable;
import org.opensearch.cluster.routing.ShardRouting;
import org.opensearch.cluster.routing.ShardRoutingState;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.core.index.shard.ShardId;

@ExtendWith(MockitoExtension.class)
class ShardRoutingResolverTest {

  @Mock private ClusterService clusterService;

  @Mock private ClusterState clusterState;

  @Mock private RoutingTable routingTable;

  private ShardRoutingResolver resolver;

  @BeforeEach
  void setUp() {
    when(clusterService.state()).thenReturn(clusterState);
    when(clusterState.routingTable()).thenReturn(routingTable);
    resolver = new ShardRoutingResolver(clusterService);
  }

  @Test
  @DisplayName("Resolve returns correct shard IDs for index with multiple shards")
  void resolveMultipleShards() {
    String indexName = "test_index";
    IndexRoutingTable indexRoutingTable = mock(IndexRoutingTable.class);
    when(routingTable.index(indexName)).thenReturn(indexRoutingTable);

    ShardRouting shard0 = mockPrimaryShard(indexName, 0);
    ShardRouting shard1 = mockPrimaryShard(indexName, 1);
    ShardRouting shard2 = mockPrimaryShard(indexName, 2);
    when(indexRoutingTable.shardsWithState(ShardRoutingState.STARTED))
        .thenReturn(List.of(shard0, shard1, shard2));

    List<Integer> result = resolver.resolve(indexName);

    assertEquals(3, result.size());
    assertEquals(List.of(0, 1, 2), result);
  }

  @Test
  @DisplayName("Resolve returns single shard ID for index with 1 shard")
  void resolveSingleShard() {
    String indexName = "single_shard_index";
    IndexRoutingTable indexRoutingTable = mock(IndexRoutingTable.class);
    when(routingTable.index(indexName)).thenReturn(indexRoutingTable);

    ShardRouting shard0 = mockPrimaryShard(indexName, 0);
    when(indexRoutingTable.shardsWithState(ShardRoutingState.STARTED))
        .thenReturn(List.of(shard0));

    List<Integer> result = resolver.resolve(indexName);

    assertEquals(1, result.size());
    assertEquals(0, result.get(0));
  }

  @Test
  @DisplayName("Resolve throws for non-existent index")
  void resolveNonExistentIndex() {
    when(routingTable.index("missing_index")).thenReturn(null);

    IllegalArgumentException ex =
        assertThrows(IllegalArgumentException.class, () -> resolver.resolve("missing_index"));

    assertTrue(ex.getMessage().contains("missing_index"));
    assertTrue(ex.getMessage().contains("not found"));
  }

  @Test
  @DisplayName("Resolve filters out replica shards and returns only primary shard IDs")
  void resolveFiltersPrimaryOnly() {
    String indexName = "test_index";
    IndexRoutingTable indexRoutingTable = mock(IndexRoutingTable.class);
    when(routingTable.index(indexName)).thenReturn(indexRoutingTable);

    ShardRouting primaryShard = mockPrimaryShard(indexName, 0);
    ShardRouting replicaShard = mockReplicaShard(indexName, 0);
    when(indexRoutingTable.shardsWithState(ShardRoutingState.STARTED))
        .thenReturn(List.of(primaryShard, replicaShard));

    List<Integer> result = resolver.resolve(indexName);

    assertEquals(1, result.size());
    assertEquals(0, result.get(0));
  }

  @Test
  @DisplayName("Resolve throws when index has no active primary shards")
  void resolveThrowsWhenNoActiveShards() {
    String indexName = "empty_index";
    IndexRoutingTable indexRoutingTable = mock(IndexRoutingTable.class);
    when(routingTable.index(indexName)).thenReturn(indexRoutingTable);
    when(indexRoutingTable.shardsWithState(ShardRoutingState.STARTED))
        .thenReturn(Collections.emptyList());

    IllegalArgumentException ex =
        assertThrows(IllegalArgumentException.class, () -> resolver.resolve(indexName));

    assertTrue(ex.getMessage().contains("no active primary shards"));
  }

  /**
   * Creates a mock primary ShardRouting. We use RETURNS_DEEP_STUBS since ShardRouting and ShardId
   * have final methods that cannot be stubbed individually.
   */
  private ShardRouting mockPrimaryShard(String indexName, int shardIdNum) {
    ShardRouting shard = mock(ShardRouting.class, org.mockito.Mockito.RETURNS_DEEP_STUBS);
    ShardId shardId = new ShardId(indexName, "uuid", shardIdNum);
    when(shard.shardId()).thenReturn(shardId);
    when(shard.primary()).thenReturn(true);
    return shard;
  }

  private ShardRouting mockReplicaShard(String indexName, int shardIdNum) {
    ShardRouting shard = mock(ShardRouting.class, org.mockito.Mockito.RETURNS_DEEP_STUBS);
    ShardId shardId = new ShardId(indexName, "uuid", shardIdNum);
    when(shard.shardId()).thenReturn(shardId);
    when(shard.primary()).thenReturn(false);
    return shard;
  }
}
