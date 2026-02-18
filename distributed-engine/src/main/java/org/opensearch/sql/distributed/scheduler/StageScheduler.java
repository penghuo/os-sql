/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.distributed.scheduler;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.routing.RoutingTable;
import org.opensearch.sql.distributed.planner.LuceneTableScanNode;
import org.opensearch.sql.distributed.planner.PlanNode;
import org.opensearch.sql.distributed.planner.PlanNodeVisitor;
import org.opensearch.sql.distributed.planner.plan.StageFragment;

/**
 * Maps StageFragments to OpenSearch nodes using ClusterState. Leaf stages are assigned to
 * shard-holding data nodes for data locality. The root stage runs on the coordinator node.
 *
 * <p>Uses {@link ShardAssigner} to map individual shards to their preferred nodes via the cluster's
 * {@link RoutingTable} and {@link org.opensearch.cluster.routing.IndexShardRoutingTable}.
 */
public class StageScheduler {

  private final ShardAssigner shardAssigner;

  public StageScheduler() {
    this.shardAssigner = new ShardAssigner();
  }

  public StageScheduler(ShardAssigner shardAssigner) {
    this.shardAssigner = shardAssigner;
  }

  /**
   * Schedules the given fragments by assigning each to appropriate nodes.
   *
   * @param fragments the fragmented plan (leaf stages first, root last)
   * @param clusterState the current cluster state for routing information
   * @param coordinatorNode the local node acting as coordinator
   * @return a StageExecution describing the full scheduled plan
   */
  public StageExecution schedule(
      List<StageFragment> fragments, ClusterState clusterState, DiscoveryNode coordinatorNode) {

    RoutingTable routingTable = clusterState.routingTable();
    Map<String, DiscoveryNode> nodeMap =
        clusterState.nodes().getNodes().entrySet().stream()
            .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

    Map<Integer, NodeAssignment> stageAssignments = new HashMap<>();

    for (StageFragment fragment : fragments) {
      NodeAssignment assignment;
      if (fragment.isLeafStage()) {
        assignment = scheduleLeafStage(fragment, routingTable, nodeMap);
      } else {
        assignment = scheduleRootStage(coordinatorNode);
      }
      stageAssignments.put(fragment.getStageId(), assignment);
    }

    return new StageExecution(fragments, stageAssignments, coordinatorNode);
  }

  /**
   * Schedules a leaf stage by finding the index name from the LuceneTableScanNode and assigning
   * shards to their preferred data nodes.
   */
  private NodeAssignment scheduleLeafStage(
      StageFragment fragment, RoutingTable routingTable, Map<String, DiscoveryNode> nodeMap) {

    String indexName = extractIndexName(fragment.getRoot());
    return shardAssigner.assign(routingTable, nodeMap, indexName);
  }

  /** Schedules the root stage to run on the coordinator node with no shard assignments. */
  private NodeAssignment scheduleRootStage(DiscoveryNode coordinatorNode) {
    return new NodeAssignment(Map.of(coordinatorNode, List.of()));
  }

  /**
   * Extracts the index name from a leaf stage's plan tree by finding the LuceneTableScanNode. In
   * Phase 1, each leaf stage scans exactly one index.
   */
  private String extractIndexName(PlanNode root) {
    IndexNameExtractor extractor = new IndexNameExtractor();
    root.accept(extractor, null);
    if (extractor.indexName == null) {
      throw new IllegalStateException("Leaf stage does not contain a LuceneTableScanNode: " + root);
    }
    return extractor.indexName;
  }

  /** Visitor that finds the first LuceneTableScanNode and extracts its index name. */
  private static class IndexNameExtractor extends PlanNodeVisitor<Void, Void> {
    String indexName;

    @Override
    public Void visitLuceneTableScan(LuceneTableScanNode node, Void context) {
      if (indexName == null) {
        indexName = node.getIndexName();
      }
      return null;
    }

    @Override
    public Void visitPlan(PlanNode node, Void context) {
      for (PlanNode child : node.getSources()) {
        child.accept(this, context);
        if (indexName != null) {
          break;
        }
      }
      return null;
    }
  }
}
