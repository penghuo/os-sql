/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.dqe.coordinator.fragment;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.routing.IndexRoutingTable;
import org.opensearch.cluster.routing.IndexShardRoutingTable;
import org.opensearch.cluster.routing.RoutingTable;
import org.opensearch.cluster.routing.ShardRouting;
import org.opensearch.sql.dqe.planner.plan.AggregationNode;
import org.opensearch.sql.dqe.planner.plan.ProjectNode;
import org.opensearch.sql.dqe.planner.plan.TableScanNode;

@DisplayName("PlanFragmenter: split plan into per-shard fragments")
class PlanFragmenterTest {

  @Test
  @DisplayName("Produces one fragment per shard for a simple scan")
  void oneFragmentPerShard() {
    TableScanNode scan = new TableScanNode("logs", List.of("a"));
    ProjectNode project = new ProjectNode(scan, List.of("a"));

    ClusterState clusterState = mockClusterStateWithShards("logs", 3);

    PlanFragmenter fragmenter = new PlanFragmenter();
    PlanFragmenter.FragmentResult result = fragmenter.fragment(project, clusterState);

    assertEquals(3, result.shardFragments().size());
    for (PlanFragment frag : result.shardFragments()) {
      assertEquals("logs", frag.indexName());
    }
    // No aggregation -> coordinator plan is null
    assertNull(result.coordinatorPlan());
  }

  @Test
  @DisplayName("Splits aggregation into PARTIAL (shard) and FINAL (coordinator)")
  void splitsAggregation() {
    TableScanNode scan = new TableScanNode("logs", List.of("category"));
    AggregationNode agg =
        new AggregationNode(
            scan, List.of("category"), List.of("COUNT(*)"), AggregationNode.Step.PARTIAL);

    ClusterState clusterState = mockClusterStateWithShards("logs", 2);

    PlanFragmenter fragmenter = new PlanFragmenter();
    PlanFragmenter.FragmentResult result = fragmenter.fragment(agg, clusterState);

    assertEquals(2, result.shardFragments().size());
    assertNotNull(result.coordinatorPlan());
    // Coordinator plan should be a FINAL aggregation
    assertInstanceOf(AggregationNode.class, result.coordinatorPlan());
    assertEquals(
        AggregationNode.Step.FINAL, ((AggregationNode) result.coordinatorPlan()).getStep());
  }

  @Test
  @DisplayName("Fragment shard IDs are unique and sequential")
  void fragmentShardIdsUnique() {
    TableScanNode scan = new TableScanNode("logs", List.of("a"));
    ClusterState clusterState = mockClusterStateWithShards("logs", 3);

    PlanFragmenter fragmenter = new PlanFragmenter();
    PlanFragmenter.FragmentResult result = fragmenter.fragment(scan, clusterState);

    Set<Integer> shardIds =
        result.shardFragments().stream().map(PlanFragment::shardId).collect(Collectors.toSet());
    assertEquals(Set.of(0, 1, 2), shardIds);
  }

  @Test
  @DisplayName("Each fragment carries the correct node ID from shard routing")
  void fragmentNodeIds() {
    TableScanNode scan = new TableScanNode("logs", List.of("a"));
    ClusterState clusterState = mockClusterStateWithShards("logs", 2);

    PlanFragmenter fragmenter = new PlanFragmenter();
    PlanFragmenter.FragmentResult result = fragmenter.fragment(scan, clusterState);

    Set<String> nodeIds =
        result.shardFragments().stream().map(PlanFragment::nodeId).collect(Collectors.toSet());
    assertEquals(Set.of("node-0", "node-1"), nodeIds);
  }

  @Test
  @DisplayName("Shard fragments carry the original plan tree")
  void shardFragmentsCarryPlan() {
    TableScanNode scan = new TableScanNode("logs", List.of("a"));
    ProjectNode project = new ProjectNode(scan, List.of("a"));

    ClusterState clusterState = mockClusterStateWithShards("logs", 1);

    PlanFragmenter fragmenter = new PlanFragmenter();
    PlanFragmenter.FragmentResult result = fragmenter.fragment(project, clusterState);

    assertEquals(1, result.shardFragments().size());
    PlanFragment frag = result.shardFragments().get(0);
    assertInstanceOf(ProjectNode.class, frag.shardPlan());
  }

  private ClusterState mockClusterStateWithShards(String indexName, int numShards) {
    ClusterState state = mock(ClusterState.class);
    RoutingTable routingTable = mock(RoutingTable.class);
    IndexRoutingTable indexRoutingTable = mock(IndexRoutingTable.class);

    when(state.routingTable()).thenReturn(routingTable);
    when(routingTable.index(indexName)).thenReturn(indexRoutingTable);

    Map<Integer, IndexShardRoutingTable> shardMap = new HashMap<>();
    for (int i = 0; i < numShards; i++) {
      IndexShardRoutingTable shardRoutingTable = mock(IndexShardRoutingTable.class);
      ShardRouting primaryShard = mock(ShardRouting.class);
      when(primaryShard.currentNodeId()).thenReturn("node-" + i);
      when(shardRoutingTable.primaryShard()).thenReturn(primaryShard);
      shardMap.put(i, shardRoutingTable);
    }
    when(indexRoutingTable.shards()).thenReturn(shardMap);

    return state;
  }
}
