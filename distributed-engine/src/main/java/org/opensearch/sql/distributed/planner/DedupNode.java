/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.distributed.planner;

import java.util.List;
import org.apache.calcite.rex.RexNode;

/**
 * Plan node representing a PPL dedup operation. This is a PPL-specific node that removes duplicate
 * rows based on specified fields. Maps from Calcite's LogicalDedup custom RelNode.
 */
public class DedupNode extends PlanNode {

  private final PlanNode source;
  private final List<RexNode> dedupeFields;
  private final int allowedDuplication;
  private final boolean keepEmpty;
  private final boolean consecutive;

  public DedupNode(
      PlanNodeId id,
      PlanNode source,
      List<RexNode> dedupeFields,
      int allowedDuplication,
      boolean keepEmpty,
      boolean consecutive) {
    super(id);
    this.source = source;
    this.dedupeFields = List.copyOf(dedupeFields);
    this.allowedDuplication = allowedDuplication;
    this.keepEmpty = keepEmpty;
    this.consecutive = consecutive;
  }

  public PlanNode getSource() {
    return source;
  }

  public List<RexNode> getDedupeFields() {
    return dedupeFields;
  }

  public int getAllowedDuplication() {
    return allowedDuplication;
  }

  public boolean isKeepEmpty() {
    return keepEmpty;
  }

  public boolean isConsecutive() {
    return consecutive;
  }

  @Override
  public List<PlanNode> getSources() {
    return List.of(source);
  }

  @Override
  public <R, C> R accept(PlanNodeVisitor<R, C> visitor, C context) {
    return visitor.visitDedup(this, context);
  }

  @Override
  public PlanNode replaceChildren(List<PlanNode> newChildren) {
    if (newChildren.size() != 1) {
      throw new IllegalArgumentException("DedupNode expects exactly 1 child");
    }
    return new DedupNode(
        getId(), newChildren.get(0), dedupeFields, allowedDuplication, keepEmpty, consecutive);
  }

  @Override
  public String toString() {
    return "DedupNode{id="
        + getId()
        + ", fields="
        + dedupeFields
        + ", allowedDup="
        + allowedDuplication
        + "}";
  }
}
