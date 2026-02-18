/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.distributed.planner;

/**
 * Visitor pattern for traversing PlanNode trees. All visit methods default to {@link
 * #visitPlan(PlanNode, Object)}.
 */
public abstract class PlanNodeVisitor<R, C> {

  public R visitPlan(PlanNode node, C context) {
    return null;
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

  public R visitTopN(TopNNode node, C context) {
    return visitPlan(node, context);
  }

  public R visitLuceneTableScan(LuceneTableScanNode node, C context) {
    return visitPlan(node, context);
  }

  public R visitExchange(ExchangeNode node, C context) {
    return visitPlan(node, context);
  }

  public R visitRemoteSource(RemoteSourceNode node, C context) {
    return visitPlan(node, context);
  }

  public R visitDedup(DedupNode node, C context) {
    return visitPlan(node, context);
  }

  public R visitLimit(LimitNode node, C context) {
    return visitPlan(node, context);
  }

  public R visitValues(ValuesNode node, C context) {
    return visitPlan(node, context);
  }
}
