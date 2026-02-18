/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.distributed.planner;

import java.util.List;
import org.apache.calcite.rex.RexNode;

/**
 * Plan node representing a filter operation. Corresponds to Calcite's LogicalFilter. Evaluates a
 * predicate on each row and passes through only matching rows.
 */
public class FilterNode extends PlanNode {

  private final PlanNode source;
  private final RexNode predicate;

  public FilterNode(PlanNodeId id, PlanNode source, RexNode predicate) {
    super(id);
    this.source = source;
    this.predicate = predicate;
  }

  public PlanNode getSource() {
    return source;
  }

  public RexNode getPredicate() {
    return predicate;
  }

  @Override
  public List<PlanNode> getSources() {
    return List.of(source);
  }

  @Override
  public <R, C> R accept(PlanNodeVisitor<R, C> visitor, C context) {
    return visitor.visitFilter(this, context);
  }

  @Override
  public PlanNode replaceChildren(List<PlanNode> newChildren) {
    if (newChildren.size() != 1) {
      throw new IllegalArgumentException("FilterNode expects exactly 1 child");
    }
    return new FilterNode(getId(), newChildren.get(0), predicate);
  }

  @Override
  public String toString() {
    return "FilterNode{id=" + getId() + ", predicate=" + predicate + "}";
  }
}
