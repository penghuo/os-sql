/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.dqe.coordinator.fragment;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.routing.IndexRoutingTable;
import org.opensearch.cluster.routing.IndexShardRoutingTable;
import org.opensearch.cluster.routing.ShardRouting;
import org.opensearch.sql.dqe.planner.plan.AggregationNode;
import org.opensearch.sql.dqe.planner.plan.DqePlanNode;
import org.opensearch.sql.dqe.planner.plan.DqePlanVisitor;
import org.opensearch.sql.dqe.planner.plan.TableScanNode;

/**
 * Splits a logical plan into per-shard fragments using cluster routing state. For aggregation
 * queries, produces PARTIAL shard fragments and a FINAL coordinator plan.
 */
public class PlanFragmenter {

  /** Result of fragmenting a plan: per-shard fragments and an optional coordinator plan. */
  public record FragmentResult(List<PlanFragment> shardFragments, DqePlanNode coordinatorPlan) {}

  /**
   * Fragment the given plan across shards of the target index.
   *
   * @param plan the logical plan to fragment
   * @param clusterState the current cluster state for shard routing
   * @return fragment result with per-shard plans and optional coordinator plan
   */
  public FragmentResult fragment(DqePlanNode plan, ClusterState clusterState) {
    // 1. Walk plan to find TableScanNode -> get index name
    String indexName = findIndexName(plan);

    // 2. Look up routing table to get shard information
    IndexRoutingTable indexRoutingTable = clusterState.routingTable().index(indexName);
    Map<Integer, IndexShardRoutingTable> shards = indexRoutingTable.shards();

    // 3. Create one PlanFragment per shard (same plan for all shards in MVP)
    List<PlanFragment> shardFragments = new ArrayList<>();
    for (Map.Entry<Integer, IndexShardRoutingTable> entry : shards.entrySet()) {
      int shardId = entry.getKey();
      ShardRouting primaryShard = entry.getValue().primaryShard();
      String nodeId = primaryShard.currentNodeId();
      shardFragments.add(new PlanFragment(plan, indexName, shardId, nodeId));
    }

    // 4. If AggregationNode present, create a FINAL coordinator plan
    DqePlanNode coordinatorPlan = buildCoordinatorPlan(plan);

    return new FragmentResult(shardFragments, coordinatorPlan);
  }

  /**
   * Walk the plan tree to find the TableScanNode and extract the index name.
   *
   * @param plan the root plan node
   * @return the index name from the TableScanNode
   * @throws IllegalArgumentException if no TableScanNode is found
   */
  private String findIndexName(DqePlanNode plan) {
    String indexName =
        plan.accept(
            new DqePlanVisitor<String, Void>() {
              @Override
              public String visitTableScan(TableScanNode node, Void context) {
                return node.getIndexName();
              }

              @Override
              public String visitPlan(DqePlanNode node, Void context) {
                for (DqePlanNode child : node.getChildren()) {
                  String result = child.accept(this, context);
                  if (result != null) {
                    return result;
                  }
                }
                return null;
              }
            },
            null);

    if (indexName == null) {
      throw new IllegalArgumentException("Plan does not contain a TableScanNode");
    }
    return indexName;
  }

  /**
   * If the plan contains an AggregationNode with PARTIAL step, build a FINAL AggregationNode for
   * the coordinator to merge partial results.
   *
   * @param plan the root plan node
   * @return a FINAL AggregationNode if aggregation is present, null otherwise
   */
  private DqePlanNode buildCoordinatorPlan(DqePlanNode plan) {
    AggregationNode aggNode =
        plan.accept(
            new DqePlanVisitor<AggregationNode, Void>() {
              @Override
              public AggregationNode visitAggregation(AggregationNode node, Void context) {
                return node;
              }

              @Override
              public AggregationNode visitPlan(DqePlanNode node, Void context) {
                for (DqePlanNode child : node.getChildren()) {
                  AggregationNode result = child.accept(this, context);
                  if (result != null) {
                    return result;
                  }
                }
                return null;
              }
            },
            null);

    if (aggNode != null && aggNode.getStep() == AggregationNode.Step.PARTIAL) {
      // Create a FINAL aggregation node for the coordinator.
      // The child is null because the coordinator will receive merged partial results.
      return new AggregationNode(
          null,
          aggNode.getGroupByKeys(),
          aggNode.getAggregateFunctions(),
          AggregationNode.Step.FINAL);
    }
    return null;
  }
}
