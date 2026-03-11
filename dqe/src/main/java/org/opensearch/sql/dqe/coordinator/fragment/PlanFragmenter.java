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
import org.opensearch.sql.dqe.planner.plan.EvalNode;
import org.opensearch.sql.dqe.planner.plan.FilterNode;
import org.opensearch.sql.dqe.planner.plan.LimitNode;
import org.opensearch.sql.dqe.planner.plan.ProjectNode;
import org.opensearch.sql.dqe.planner.plan.SortNode;
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

    // 3. Build shard plan — only strip Sort/Limit/HAVING for multi-shard aggregation
    boolean multiShard = shards.size() > 1;
    DqePlanNode shardPlan = multiShard ? buildShardPlan(plan) : plan;

    // 4. Create shard fragments
    List<PlanFragment> shardFragments = new ArrayList<>();
    for (Map.Entry<Integer, IndexShardRoutingTable> entry : shards.entrySet()) {
      int shardId = entry.getKey();
      ShardRouting primaryShard = entry.getValue().primaryShard();
      String nodeId = primaryShard.currentNodeId();
      shardFragments.add(new PlanFragment(shardPlan, indexName, shardId, nodeId));
    }

    // 5. If multi-shard and AggregationNode present, create coordinator plan for merging
    DqePlanNode coordinatorPlan = multiShard ? buildCoordinatorPlan(plan) : null;

    return new FragmentResult(shardFragments, coordinatorPlan);
  }

  /**
   * Build the shard plan. For aggregation queries:
   *
   * <ul>
   *   <li>PARTIAL: Strip Sort/Limit above aggregation (coordinator applies those after merge).
   *       Shard runs: [EvalNode →] AggregationNode(PARTIAL) → Filter → Scan.
   *   <li>SINGLE (COUNT DISTINCT): Strip aggregation and above. Shard runs: Filter → Scan. The
   *       shard may detect scalar COUNT(DISTINCT) on numeric columns and pre-dedup values.
   * </ul>
   *
   * For non-aggregation queries: use the full plan.
   */
  private DqePlanNode buildShardPlan(DqePlanNode plan) {
    AggregationNode aggNode = findAggregationNode(plan);
    if (aggNode == null) {
      return plan;
    }
    if (aggNode.getStep() == AggregationNode.Step.PARTIAL) {
      // Strip Sort, Limit, and Project above the aggregation.
      // Walk up from the aggregation to find it in the tree and return it as the root.
      return stripAboveAggregation(plan);
    }
    // SINGLE: strip aggregation and above, shards only scan+filter
    return aggNode.getChild();
  }

  /**
   * Return the subtree rooted at the AggregationNode, stripping Sort/Limit/Project nodes above it.
   * If the plan has structure like Limit(Sort(Project(Agg(...)))) or Limit(Sort(Agg(...))), returns
   * the Agg subtree. Also handles EvalNode above aggregation.
   */
  private DqePlanNode stripAboveAggregation(DqePlanNode node) {
    if (node instanceof AggregationNode) {
      return node;
    }
    if (node instanceof LimitNode) {
      return stripAboveAggregation(((LimitNode) node).getChild());
    }
    if (node instanceof SortNode) {
      return stripAboveAggregation(((SortNode) node).getChild());
    }
    if (node instanceof ProjectNode) {
      return stripAboveAggregation(((ProjectNode) node).getChild());
    }
    // FilterNode above aggregation = HAVING clause. Strip it — it must be
    // applied at the coordinator after merging partial results from all shards.
    if (node instanceof FilterNode) {
      return stripAboveAggregation(((FilterNode) node).getChild());
    }
    if (node instanceof EvalNode) {
      // EvalNode above aggregation may compute expressions on agg output.
      // Keep it — it can run on the shard with partial results.
      DqePlanNode child = stripAboveAggregation(((EvalNode) node).getChild());
      if (child instanceof AggregationNode) {
        return new EvalNode(
            child, ((EvalNode) node).getExpressions(), ((EvalNode) node).getOutputColumnNames());
      }
      return child;
    }
    // For any other node type, return as-is
    return node;
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
