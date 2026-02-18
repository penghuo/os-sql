/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.distributed.planner;

import java.util.List;

/**
 * Plan node representing a limit (and optional offset) operation without sorting. Corresponds to
 * Calcite's LogicalSort when only fetch/offset are specified (no collation).
 */
public class LimitNode extends PlanNode {

  private final PlanNode source;
  private final long limit;
  private final long offset;

  public LimitNode(PlanNodeId id, PlanNode source, long limit, long offset) {
    super(id);
    this.source = source;
    this.limit = limit;
    this.offset = offset;
  }

  public PlanNode getSource() {
    return source;
  }

  public long getLimit() {
    return limit;
  }

  public long getOffset() {
    return offset;
  }

  @Override
  public List<PlanNode> getSources() {
    return List.of(source);
  }

  @Override
  public <R, C> R accept(PlanNodeVisitor<R, C> visitor, C context) {
    return visitor.visitLimit(this, context);
  }

  @Override
  public PlanNode replaceChildren(List<PlanNode> newChildren) {
    if (newChildren.size() != 1) {
      throw new IllegalArgumentException("LimitNode expects exactly 1 child");
    }
    return new LimitNode(getId(), newChildren.get(0), limit, offset);
  }

  @Override
  public String toString() {
    return "LimitNode{id=" + getId() + ", limit=" + limit + ", offset=" + offset + "}";
  }
}
