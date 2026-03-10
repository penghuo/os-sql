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

    // 3. Build shard plan — strip non-PARTIAL aggregation (coordinator handles it)
    DqePlanNode shardPlan = buildShardPlan(plan);

    // 4. Create shard fragments
    List<PlanFragment> shardFragments = new ArrayList<>();
    for (Map.Entry<Integer, IndexShardRoutingTable> entry : shards.entrySet()) {
      int shardId = entry.getKey();
      ShardRouting primaryShard = entry.getValue().primaryShard();
      String nodeId = primaryShard.currentNodeId();
      shardFragments.add(new PlanFragment(shardPlan, indexName, shardId, nodeId));
    }

    // 5. If AggregationNode present, create coordinator plan for merging
    DqePlanNode coordinatorPlan = buildCoordinatorPlan(plan);

    return new FragmentResult(shardFragments, coordinatorPlan);
  }

  /**
   * Build the shard plan by stripping non-PARTIAL aggregation nodes (and any Sort/Limit above them)
   * from the plan. When aggregation is SINGLE (e.g., COUNT(DISTINCT)), the coordinator will run the
   * aggregation over concatenated shard scan results.
   */
  private DqePlanNode buildShardPlan(DqePlanNode plan) {
    AggregationNode aggNode = findAggregationNode(plan);
    // If no aggregation or aggregation is PARTIAL, use the full plan as-is
    if (aggNode == null || aggNode.getStep() == AggregationNode.Step.PARTIAL) {
      return plan;
    }
    // For non-PARTIAL aggregation (e.g., SINGLE with COUNT(DISTINCT)):
    // Strip the aggregation and everything above it. Shards only scan + filter.
    // The shard plan is just the child of the AggregationNode.
    return aggNode.getChild();
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
   * Build the coordinator plan for aggregation queries. For PARTIAL aggregation, creates a FINAL
   * merge node. For non-PARTIAL (SINGLE) aggregation, creates a SINGLE node so the coordinator runs
   * the full aggregation over concatenated shard scan results.
   */
  private DqePlanNode buildCoordinatorPlan(DqePlanNode plan) {
    AggregationNode aggNode = findAggregationNode(plan);

    if (aggNode != null && aggNode.getStep() == AggregationNode.Step.PARTIAL) {
      return new AggregationNode(
          null,
          aggNode.getGroupByKeys(),
          aggNode.getAggregateFunctions(),
          AggregationNode.Step.FINAL);
    }
    if (aggNode != null) {
      // Non-PARTIAL (SINGLE): coordinator runs full aggregation over raw shard data
      return new AggregationNode(
          null,
          aggNode.getGroupByKeys(),
          aggNode.getAggregateFunctions(),
          AggregationNode.Step.SINGLE);
    }
    return null;
  }

  /** Find the AggregationNode in the plan tree. */
  private AggregationNode findAggregationNode(DqePlanNode plan) {
    return plan.accept(
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
  }
}
