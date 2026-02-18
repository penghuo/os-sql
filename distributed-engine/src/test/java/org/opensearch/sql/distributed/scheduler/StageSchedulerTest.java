/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.distributed.scheduler;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.node.DiscoveryNodes;
import org.opensearch.cluster.routing.RoutingTable;
import org.opensearch.sql.distributed.planner.ExchangeNode;
import org.opensearch.sql.distributed.planner.LuceneTableScanNode;
import org.opensearch.sql.distributed.planner.PlanNodeId;
import org.opensearch.sql.distributed.planner.RemoteSourceNode;
import org.opensearch.sql.distributed.planner.plan.PartitioningScheme;
import org.opensearch.sql.distributed.planner.plan.StageFragment;

@ExtendWith(MockitoExtension.class)
class StageSchedulerTest {

  @Test
  @DisplayName("Root stage is assigned to coordinator node")
  void testRootStageAssignedToCoordinator() {
    DiscoveryNode coordinator = mock(DiscoveryNode.class);

    ClusterState clusterState = mock(ClusterState.class);
    DiscoveryNodes nodes = mock(DiscoveryNodes.class);
    when(clusterState.nodes()).thenReturn(nodes);
    when(nodes.getNodes()).thenReturn(Map.of());
    when(clusterState.routingTable()).thenReturn(mock(RoutingTable.class));

    // Create a root stage with a RemoteSourceNode
    RemoteSourceNode remoteSource =
        new RemoteSourceNode(
            PlanNodeId.next("RemoteSource"), List.of(0), ExchangeNode.ExchangeType.GATHER);
    StageFragment rootStage =
        new StageFragment(
            1, remoteSource, PartitioningScheme.singlePartitioning(), List.of(remoteSource));

    ShardAssigner mockAssigner = mock(ShardAssigner.class);
    StageScheduler scheduler = new StageScheduler(mockAssigner);

    StageExecution execution = scheduler.schedule(List.of(rootStage), clusterState, coordinator);

    NodeAssignment rootAssignment = execution.getAssignment(1);
    assertTrue(rootAssignment.getNodes().contains(coordinator));
    assertEquals(coordinator, execution.getCoordinatorNode());
  }

  @Test
  @DisplayName("Leaf stage uses ShardAssigner for node mapping")
  void testLeafStageUsesShardAssigner() {
    DiscoveryNode coordinator = mock(DiscoveryNode.class);
    DiscoveryNode dataNode1 = mock(DiscoveryNode.class);

    ClusterState clusterState = mock(ClusterState.class);
    DiscoveryNodes discoveryNodes = mock(DiscoveryNodes.class);
    RoutingTable routingTable = mock(RoutingTable.class);
    when(clusterState.nodes()).thenReturn(discoveryNodes);
    when(discoveryNodes.getNodes()).thenReturn(Map.of("node1", dataNode1));
    when(clusterState.routingTable()).thenReturn(routingTable);

    // Create leaf stage with LuceneTableScanNode
    LuceneTableScanNode scanNode =
        new LuceneTableScanNode(PlanNodeId.next("Scan"), "test-index", null, List.of("col1"));
    StageFragment leafStage =
        new StageFragment(0, scanNode, PartitioningScheme.sourceDistributed(), List.of());

    NodeAssignment expectedAssignment = new NodeAssignment(Map.of(dataNode1, List.of(0, 1)));

    ShardAssigner mockAssigner = mock(ShardAssigner.class);
    when(mockAssigner.assign(routingTable, Map.of("node1", dataNode1), "test-index"))
        .thenReturn(expectedAssignment);

    StageScheduler scheduler = new StageScheduler(mockAssigner);
    StageExecution execution = scheduler.schedule(List.of(leafStage), clusterState, coordinator);

    NodeAssignment leafAssignment = execution.getAssignment(0);
    assertEquals(expectedAssignment, leafAssignment);
    assertEquals(List.of(0, 1), leafAssignment.getShardsForNode(dataNode1));
  }

  @Test
  @DisplayName("StageExecution provides leaf and root stage accessors")
  void testStageExecutionAccessors() {
    DiscoveryNode coordinator = mock(DiscoveryNode.class);

    LuceneTableScanNode scanNode =
        new LuceneTableScanNode(PlanNodeId.next("Scan"), "idx", null, List.of());
    StageFragment leafStage =
        new StageFragment(0, scanNode, PartitioningScheme.sourceDistributed(), List.of());

    RemoteSourceNode remoteSource =
        new RemoteSourceNode(
            PlanNodeId.next("Remote"), List.of(0), ExchangeNode.ExchangeType.GATHER);
    StageFragment rootStage =
        new StageFragment(
            1, remoteSource, PartitioningScheme.singlePartitioning(), List.of(remoteSource));

    NodeAssignment leafAssignment = new NodeAssignment(Map.of(coordinator, List.of(0)));
    NodeAssignment rootAssignment = new NodeAssignment(Map.of(coordinator, List.of()));

    StageExecution execution =
        new StageExecution(
            List.of(leafStage, rootStage),
            Map.of(0, leafAssignment, 1, rootAssignment),
            coordinator);

    assertEquals(leafStage, execution.getLeafStage());
    assertEquals(rootStage, execution.getRootStage());
    assertEquals(2, execution.getFragments().size());
  }
}
