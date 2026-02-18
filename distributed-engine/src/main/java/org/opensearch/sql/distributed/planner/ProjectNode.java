/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.distributed.planner;

import java.util.List;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexNode;

/**
 * Plan node representing a projection operation. Corresponds to Calcite's LogicalProject. Projects
 * a set of expressions (column references or computed expressions) from the input.
 */
public class ProjectNode extends PlanNode {

  private final PlanNode source;
  private final List<RexNode> projections;
  private final RelDataType outputType;

  public ProjectNode(
      PlanNodeId id, PlanNode source, List<RexNode> projections, RelDataType outputType) {
    super(id);
    this.source = source;
    this.projections = List.copyOf(projections);
    this.outputType = outputType;
  }

  public PlanNode getSource() {
    return source;
  }

  public List<RexNode> getProjections() {
    return projections;
  }

  public RelDataType getOutputType() {
    return outputType;
  }

  @Override
  public List<PlanNode> getSources() {
    return List.of(source);
  }

  @Override
  public <R, C> R accept(PlanNodeVisitor<R, C> visitor, C context) {
    return visitor.visitProject(this, context);
  }

  @Override
  public PlanNode replaceChildren(List<PlanNode> newChildren) {
    if (newChildren.size() != 1) {
      throw new IllegalArgumentException("ProjectNode expects exactly 1 child");
    }
    return new ProjectNode(getId(), newChildren.get(0), projections, outputType);
  }

  @Override
  public String toString() {
    return "ProjectNode{id=" + getId() + ", projections=" + projections + "}";
  }
}
