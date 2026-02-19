/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.distributed.planner.plan;

import java.util.List;
import org.opensearch.sql.distributed.planner.AggregationNode;
import org.opensearch.sql.distributed.planner.AggregationNode.AggregationMode;
import org.opensearch.sql.distributed.planner.ExchangeNode;
import org.opensearch.sql.distributed.planner.ExchangeNode.ExchangeType;
import org.opensearch.sql.distributed.planner.FilterNode;
import org.opensearch.sql.distributed.planner.JoinNode;
import org.opensearch.sql.distributed.planner.LimitNode;
import org.opensearch.sql.distributed.planner.LuceneTableScanNode;
import org.opensearch.sql.distributed.planner.PlanNode;
import org.opensearch.sql.distributed.planner.PlanNodeId;
import org.opensearch.sql.distributed.planner.PlanNodeVisitor;
import org.opensearch.sql.distributed.planner.ProjectNode;
import org.opensearch.sql.distributed.planner.SortNode;
import org.opensearch.sql.distributed.planner.TopNNode;
import org.opensearch.sql.distributed.planner.WindowNode;

/**
 * Inserts ExchangeNodes into the PlanNode tree to define distribution boundaries between stages.
 *
 * <p>Adapted from Trino's {@code AddExchanges} (~1,700 LOC). Supports both Phase 1 (scatter-gather)
 * and Phase 2 (hash/broadcast exchange) patterns.
 *
 * <p>Phase 1 strategy:
 *
 * <ul>
 *   <li>For aggregation: split into partial (leaf) + GatherExchange + final (root)
 *   <li>For sort/topN: keep local sort on leaf + GatherExchange + merge on root
 *   <li>For filter/project only: wrap leaf in GatherExchange
 * </ul>
 *
 * <p>Phase 2 strategy additions:
 *
 * <ul>
 *   <li>For joins: BroadcastExchange on build side if small, else HashExchange on both sides
 *   <li>For window functions: HashExchange on partition-by columns
 *   <li>For high-cardinality aggregation: HashExchange on group keys between partial and final
 * </ul>
 */
public class AddExchanges {

  /**
   * Default threshold (in bytes) below which the build side of a join uses BroadcastExchange
   * instead of HashExchange. 10 MB default.
   */
  static final long DEFAULT_BROADCAST_THRESHOLD = 10L * 1024 * 1024;

  /**
   * Default threshold for group-key cardinality above which we add a HashExchange between partial
   * and final aggregation stages. If the group set has more than this many columns, we assume high
   * cardinality and add the intermediate hash exchange.
   */
  static final int HIGH_CARDINALITY_GROUP_KEY_THRESHOLD = 2;

  private long broadcastThreshold = DEFAULT_BROADCAST_THRESHOLD;

  /** Sets the broadcast threshold for join strategy decisions. */
  public void setBroadcastThreshold(long broadcastThreshold) {
    this.broadcastThreshold = broadcastThreshold;
  }

  /**
   * Adds exchange nodes to the plan tree.
   *
   * @param root the PlanNode tree (output of RelNodeToPlanNodeConverter)
   * @return the PlanNode tree with ExchangeNodes inserted
   */
  public PlanNode addExchanges(PlanNode root) {
    return root.accept(new ExchangeInsertionVisitor(), null);
  }

  /**
   * Visitor that traverses the PlanNode tree and inserts exchange nodes at the appropriate
   * boundaries. The visitor processes bottom-up: first recurse into children, then decide where to
   * place the exchange.
   */
  private class ExchangeInsertionVisitor extends PlanNodeVisitor<PlanNode, Void> {

    @Override
    public PlanNode visitPlan(PlanNode node, Void context) {
      // Default: recurse into children and reconstruct
      List<PlanNode> newChildren =
          node.getSources().stream().map(child -> child.accept(this, context)).toList();
      if (newChildren.equals(node.getSources())) {
        return node;
      }
      return node.replaceChildren(newChildren);
    }

    @Override
    public PlanNode visitAggregation(AggregationNode node, Void context) {
      // Process child first
      PlanNode source = node.getSource().accept(this, context);

      if (node.getMode() == AggregationMode.SINGLE) {
        int groupKeyCount = node.getGroupSet().cardinality();

        if (groupKeyCount > HIGH_CARDINALITY_GROUP_KEY_THRESHOLD) {
          // High-cardinality aggregation: use intermediate hash exchange
          // PARTIAL -> HashExchange(group_key) -> FINAL -> GatherExchange -> coordinator
          return buildHashDistributedAggregation(node, source);
        }

        // Standard scatter-gather: PARTIAL + GatherExchange + FINAL
        AggregationNode partialAgg =
            new AggregationNode(
                PlanNodeId.next("PartialAgg"),
                source,
                node.getGroupSet(),
                node.getAggregateCalls(),
                AggregationMode.PARTIAL);

        ExchangeNode exchange =
            new ExchangeNode(
                PlanNodeId.next("GatherExchange"),
                partialAgg,
                ExchangeType.GATHER,
                PartitioningScheme.gatherPartitioning());

        return new AggregationNode(
            node.getId(),
            exchange,
            node.getGroupSet(),
            node.getAggregateCalls(),
            AggregationMode.FINAL);
      }
      // Already split, just replace source
      return node.replaceChildren(List.of(source));
    }

    /**
     * Builds a hash-distributed aggregation plan for high-cardinality group keys:
     *
     * <pre>
     *   source -> PartialAgg -> HashExchange(group_keys) -> FinalAgg -> GatherExchange
     * </pre>
     *
     * <p>The intermediate HashExchange ensures rows with the same group key land on the same node,
     * enabling a correct distributed final aggregation.
     */
    private PlanNode buildHashDistributedAggregation(AggregationNode node, PlanNode source) {
      List<Integer> groupKeyColumns = node.getGroupSet().toList();

      // Step 1: Partial aggregation on leaf nodes
      AggregationNode partialAgg =
          new AggregationNode(
              PlanNodeId.next("PartialAgg"),
              source,
              node.getGroupSet(),
              node.getAggregateCalls(),
              AggregationMode.PARTIAL);

      // Step 2: Hash exchange on group keys
      ExchangeNode hashExchange =
          new ExchangeNode(
              PlanNodeId.next("HashExchange"),
              partialAgg,
              ExchangeType.HASH,
              PartitioningScheme.hashPartitioning(groupKeyColumns));

      // Step 3: Final aggregation on the hash-partitioned data
      AggregationNode finalAgg =
          new AggregationNode(
              node.getId(),
              hashExchange,
              node.getGroupSet(),
              node.getAggregateCalls(),
              AggregationMode.FINAL);

      // Step 4: Gather to coordinator
      return new ExchangeNode(
          PlanNodeId.next("GatherExchange"),
          finalAgg,
          ExchangeType.GATHER,
          PartitioningScheme.gatherPartitioning());
    }

    @Override
    public PlanNode visitJoin(JoinNode node, Void context) {
      PlanNode left = node.getLeft().accept(this, context);
      PlanNode right = node.getRight().accept(this, context);

      // Decision: broadcast vs hash-partition based on estimated build side size.
      // Since we don't have cost estimation yet, use a heuristic:
      // - If the right (build) side is a single LuceneTableScan (leaf), assume it may be
      //   small enough to broadcast.
      // - Otherwise, use hash-partitioned join.
      boolean useBroadcast = isBroadcastCandidate(right);

      if (useBroadcast) {
        return buildBroadcastJoin(node, left, right);
      } else {
        return buildHashPartitionedJoin(node, left, right);
      }
    }

    /**
     * Builds a broadcast join plan. The small (build) side is gathered to coordinator and then
     * broadcast to all nodes. The large (probe) side stays distributed.
     *
     * <pre>
     *   Probe: source -> local scan (distributed)
     *   Build: source -> GatherExchange -> BroadcastExchange (to all nodes)
     *   Join: probe + broadcasted build -> JoinNode -> GatherExchange -> coordinator
     * </pre>
     */
    private PlanNode buildBroadcastJoin(JoinNode node, PlanNode left, PlanNode right) {
      // Broadcast the build (right) side to all nodes
      ExchangeNode broadcastExchange =
          new ExchangeNode(
              PlanNodeId.next("BroadcastExchange"),
              right,
              ExchangeType.BROADCAST,
              PartitioningScheme.broadcastPartitioning());

      // Join on each node
      JoinNode joinNode =
          new JoinNode(
              node.getId(),
              left,
              broadcastExchange,
              node.getCondition(),
              node.getJoinType(),
              node.getLeftJoinKeys(),
              node.getRightJoinKeys());

      // Gather results to coordinator
      return new ExchangeNode(
          PlanNodeId.next("GatherExchange"),
          joinNode,
          ExchangeType.GATHER,
          PartitioningScheme.gatherPartitioning());
    }

    /**
     * Builds a hash-partitioned join plan. Both sides are hash-partitioned on the join key columns
     * so that matching rows from both sides end up on the same node.
     *
     * <pre>
     *   Probe: source -> HashExchange(join_key)
     *   Build: source -> HashExchange(join_key)
     *   Join: hash-partitioned probe + hash-partitioned build -> JoinNode -> GatherExchange
     * </pre>
     */
    private PlanNode buildHashPartitionedJoin(JoinNode node, PlanNode left, PlanNode right) {
      List<Integer> leftKeys = node.getLeftJoinKeys();
      List<Integer> rightKeys = node.getRightJoinKeys();

      // Hash-partition probe (left) side on join keys
      ExchangeNode leftExchange =
          new ExchangeNode(
              PlanNodeId.next("HashExchange"),
              left,
              ExchangeType.HASH,
              PartitioningScheme.hashPartitioning(leftKeys));

      // Hash-partition build (right) side on join keys
      ExchangeNode rightExchange =
          new ExchangeNode(
              PlanNodeId.next("HashExchange"),
              right,
              ExchangeType.HASH,
              PartitioningScheme.hashPartitioning(rightKeys));

      // Join on each node (rows with same key are co-located)
      JoinNode joinNode =
          new JoinNode(
              node.getId(),
              leftExchange,
              rightExchange,
              node.getCondition(),
              node.getJoinType(),
              node.getLeftJoinKeys(),
              node.getRightJoinKeys());

      // Gather results to coordinator
      return new ExchangeNode(
          PlanNodeId.next("GatherExchange"),
          joinNode,
          ExchangeType.GATHER,
          PartitioningScheme.gatherPartitioning());
    }

    @Override
    public PlanNode visitWindow(WindowNode node, Void context) {
      PlanNode source = node.getSource().accept(this, context);

      List<Integer> partitionByColumns = node.getPartitionByColumns();

      if (partitionByColumns.isEmpty()) {
        // No partition-by columns: gather all data to coordinator and compute window there
        ExchangeNode gatherExchange =
            new ExchangeNode(
                PlanNodeId.next("GatherExchange"),
                source,
                ExchangeType.GATHER,
                PartitioningScheme.gatherPartitioning());

        return new WindowNode(
            node.getId(),
            gatherExchange,
            node.getPartitionByColumns(),
            node.getOrderByCollation(),
            node.getConstants(),
            node.getOutputType());
      }

      // Hash-partition by partition-by columns, then apply window function
      ExchangeNode hashExchange =
          new ExchangeNode(
              PlanNodeId.next("HashExchange"),
              source,
              ExchangeType.HASH,
              PartitioningScheme.hashPartitioning(partitionByColumns));

      WindowNode windowNode =
          new WindowNode(
              node.getId(),
              hashExchange,
              node.getPartitionByColumns(),
              node.getOrderByCollation(),
              node.getConstants(),
              node.getOutputType());

      // Gather results to coordinator
      return new ExchangeNode(
          PlanNodeId.next("GatherExchange"),
          windowNode,
          ExchangeType.GATHER,
          PartitioningScheme.gatherPartitioning());
    }

    @Override
    public PlanNode visitSort(SortNode node, Void context) {
      PlanNode source = node.getSource().accept(this, context);

      // Insert GatherExchange below the sort.
      // Leaf nodes do local sort, coordinator does final merge-sort.
      ExchangeNode exchange =
          new ExchangeNode(
              PlanNodeId.next("GatherExchange"),
              new SortNode(PlanNodeId.next("LocalSort"), source, node.getCollation()),
              ExchangeType.GATHER,
              PartitioningScheme.gatherPartitioning());

      return new SortNode(node.getId(), exchange, node.getCollation());
    }

    @Override
    public PlanNode visitTopN(TopNNode node, Void context) {
      PlanNode source = node.getSource().accept(this, context);

      // Insert GatherExchange below the TopN.
      // Leaf nodes compute local top-N, coordinator does final top-N merge.
      ExchangeNode exchange =
          new ExchangeNode(
              PlanNodeId.next("GatherExchange"),
              new TopNNode(
                  PlanNodeId.next("LocalTopN"), source, node.getCollation(), node.getLimit()),
              ExchangeType.GATHER,
              PartitioningScheme.gatherPartitioning());

      return new TopNNode(node.getId(), exchange, node.getCollation(), node.getLimit());
    }

    @Override
    public PlanNode visitFilter(FilterNode node, Void context) {
      PlanNode source = node.getSource().accept(this, context);
      return new FilterNode(node.getId(), source, node.getPredicate());
    }

    @Override
    public PlanNode visitProject(ProjectNode node, Void context) {
      PlanNode source = node.getSource().accept(this, context);
      return new ProjectNode(node.getId(), source, node.getProjections(), node.getOutputType());
    }

    @Override
    public PlanNode visitLimit(LimitNode node, Void context) {
      PlanNode source = node.getSource().accept(this, context);
      return new LimitNode(node.getId(), source, node.getLimit(), node.getOffset());
    }

    @Override
    public PlanNode visitLuceneTableScan(LuceneTableScanNode node, Void context) {
      // Leaf node - no exchange needed below this
      return node;
    }

    /**
     * Heuristic to determine if a plan subtree is a broadcast candidate (small enough to
     * broadcast). Currently checks if it's a simple leaf scan without aggregation or join, which
     * typically represents small dimension tables.
     *
     * <p>Future improvement: integrate with Calcite's cost model for actual size estimation.
     */
    private boolean isBroadcastCandidate(PlanNode node) {
      // If it's a simple table scan (potentially with filter/project), it might be small
      if (node instanceof LuceneTableScanNode) {
        return true;
      }
      if (node instanceof FilterNode filterNode) {
        return isBroadcastCandidate(filterNode.getSource());
      }
      if (node instanceof ProjectNode projectNode) {
        return isBroadcastCandidate(projectNode.getSource());
      }
      // Complex subtrees (joins, aggregations, etc.) default to hash-partition
      return false;
    }
  }

  /**
   * Ensures the plan has at least one GatherExchange for distributed execution. If the plan has no
   * exchange (e.g., simple filter+project without aggregation), wraps the entire leaf in a
   * GatherExchange.
   *
   * @param root the plan tree (possibly already containing exchanges)
   * @return the plan tree guaranteed to have a GatherExchange
   */
  public PlanNode ensureGatherExchange(PlanNode root) {
    if (containsExchange(root)) {
      return root;
    }
    // No exchange found - wrap entire plan in a GatherExchange
    return new ExchangeNode(
        PlanNodeId.next("GatherExchange"),
        root,
        ExchangeType.GATHER,
        PartitioningScheme.gatherPartitioning());
  }

  private boolean containsExchange(PlanNode node) {
    if (node instanceof ExchangeNode) {
      return true;
    }
    return node.getSources().stream().anyMatch(this::containsExchange);
  }
}
