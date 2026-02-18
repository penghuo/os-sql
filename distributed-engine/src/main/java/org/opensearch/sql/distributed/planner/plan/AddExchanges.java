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
import org.opensearch.sql.distributed.planner.LimitNode;
import org.opensearch.sql.distributed.planner.LuceneTableScanNode;
import org.opensearch.sql.distributed.planner.PlanNode;
import org.opensearch.sql.distributed.planner.PlanNodeId;
import org.opensearch.sql.distributed.planner.PlanNodeVisitor;
import org.opensearch.sql.distributed.planner.ProjectNode;
import org.opensearch.sql.distributed.planner.SortNode;
import org.opensearch.sql.distributed.planner.TopNNode;

/**
 * Inserts ExchangeNodes into the PlanNode tree to define distribution boundaries between stages.
 *
 * <p>Adapted from Trino's {@code AddExchanges} (~1,700 LOC). Phase 1 scope: only GatherExchange for
 * scatter-gather pattern (single-index, no-join queries).
 *
 * <p>The strategy is:
 *
 * <ul>
 *   <li>For aggregation: split into partial (leaf) + GatherExchange + final (root)
 *   <li>For sort/topN: keep local sort on leaf + GatherExchange + merge on root
 *   <li>For filter/project only: wrap leaf in GatherExchange
 * </ul>
 */
public class AddExchanges {

  /**
   * Adds exchange nodes to the plan tree. In Phase 1, this always produces a scatter-gather plan
   * with a single GatherExchange separating the leaf stage from the root stage.
   *
   * @param root the PlanNode tree (output of RelNodeToPlanNodeConverter)
   * @return the PlanNode tree with ExchangeNodes inserted
   */
  public PlanNode addExchanges(PlanNode root) {
    return root.accept(new ExchangeInsertionVisitor(), null);
  }

  /**
   * Visitor that traverses the PlanNode tree and inserts GatherExchange at the appropriate
   * boundary. The visitor processes bottom-up: first recurse into children, then decide where to
   * place the exchange.
   */
  private static class ExchangeInsertionVisitor extends PlanNodeVisitor<PlanNode, Void> {

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
        // Split SINGLE aggregation into PARTIAL + exchange + FINAL
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
