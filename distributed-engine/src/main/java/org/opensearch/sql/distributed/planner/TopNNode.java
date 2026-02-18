/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.distributed.planner;

import java.util.List;
import org.apache.calcite.rel.RelCollation;

/**
 * Plan node representing a sort with a limit (top-N). Corresponds to Calcite's LogicalSort when
 * both collation and fetch are specified. More efficient than separate sort + limit as it uses a
 * heap-based top-K algorithm.
 */
public class TopNNode extends PlanNode {

  private final PlanNode source;
  private final RelCollation collation;
  private final long limit;

  public TopNNode(PlanNodeId id, PlanNode source, RelCollation collation, long limit) {
    super(id);
    this.source = source;
    this.collation = collation;
    this.limit = limit;
  }

  public PlanNode getSource() {
    return source;
  }

  public RelCollation getCollation() {
    return collation;
  }

  public long getLimit() {
    return limit;
  }

  @Override
  public List<PlanNode> getSources() {
    return List.of(source);
  }

  @Override
  public <R, C> R accept(PlanNodeVisitor<R, C> visitor, C context) {
    return visitor.visitTopN(this, context);
  }

  @Override
  public PlanNode replaceChildren(List<PlanNode> newChildren) {
    if (newChildren.size() != 1) {
      throw new IllegalArgumentException("TopNNode expects exactly 1 child");
    }
    return new TopNNode(getId(), newChildren.get(0), collation, limit);
  }

  @Override
  public String toString() {
    return "TopNNode{id=" + getId() + ", collation=" + collation + ", limit=" + limit + "}";
  }
}
