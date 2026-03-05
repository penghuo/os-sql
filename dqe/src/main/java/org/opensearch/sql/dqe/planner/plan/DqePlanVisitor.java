/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.dqe.planner.plan;

/**
 * Visitor pattern for DQE plan nodes. Each concrete node type has a corresponding visit method that
 * defaults to {@link #visitPlan(DqePlanNode, Object)}.
 */
public abstract class DqePlanVisitor<R, C> {

  public R visitPlan(DqePlanNode node, C context) {
    return null;
  }

  public R visitTableScan(TableScanNode node, C context) {
    return visitPlan(node, context);
  }

  public R visitFilter(FilterNode node, C context) {
    return visitPlan(node, context);
  }

  public R visitProject(ProjectNode node, C context) {
    return visitPlan(node, context);
  }

  public R visitAggregation(AggregationNode node, C context) {
    return visitPlan(node, context);
  }

  public R visitSort(SortNode node, C context) {
    return visitPlan(node, context);
  }

  public R visitLimit(LimitNode node, C context) {
    return visitPlan(node, context);
  }

  public R visitEval(EvalNode node, C context) {
    return visitPlan(node, context);
  }
}
