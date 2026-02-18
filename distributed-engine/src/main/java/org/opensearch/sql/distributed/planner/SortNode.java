/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.distributed.planner;

import java.util.List;
import org.apache.calcite.rel.RelCollation;

/**
 * Plan node representing a sort operation without a limit. Corresponds to Calcite's LogicalSort
 * when only collation is specified (no fetch/offset).
 */
public class SortNode extends PlanNode {

  private final PlanNode source;
  private final RelCollation collation;

  public SortNode(PlanNodeId id, PlanNode source, RelCollation collation) {
    super(id);
    this.source = source;
    this.collation = collation;
  }

  public PlanNode getSource() {
    return source;
  }

  public RelCollation getCollation() {
    return collation;
  }

  @Override
  public List<PlanNode> getSources() {
    return List.of(source);
  }

  @Override
  public <R, C> R accept(PlanNodeVisitor<R, C> visitor, C context) {
    return visitor.visitSort(this, context);
  }

  @Override
  public PlanNode replaceChildren(List<PlanNode> newChildren) {
    if (newChildren.size() != 1) {
      throw new IllegalArgumentException("SortNode expects exactly 1 child");
    }
    return new SortNode(getId(), newChildren.get(0), collation);
  }

  @Override
  public String toString() {
    return "SortNode{id=" + getId() + ", collation=" + collation + "}";
  }
}
