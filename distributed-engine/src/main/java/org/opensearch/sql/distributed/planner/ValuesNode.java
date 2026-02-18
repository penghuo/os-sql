/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.distributed.planner;

import java.util.List;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexLiteral;

/**
 * Plan node representing inline literal values. Corresponds to Calcite's LogicalValues. Produces a
 * fixed set of rows with literal values.
 */
public class ValuesNode extends PlanNode {

  private final List<List<RexLiteral>> tuples;
  private final RelDataType outputType;

  public ValuesNode(PlanNodeId id, List<List<RexLiteral>> tuples, RelDataType outputType) {
    super(id);
    this.tuples = tuples;
    this.outputType = outputType;
  }

  public List<List<RexLiteral>> getTuples() {
    return tuples;
  }

  public RelDataType getOutputType() {
    return outputType;
  }

  @Override
  public List<PlanNode> getSources() {
    return List.of();
  }

  @Override
  public <R, C> R accept(PlanNodeVisitor<R, C> visitor, C context) {
    return visitor.visitValues(this, context);
  }

  @Override
  public PlanNode replaceChildren(List<PlanNode> newChildren) {
    if (!newChildren.isEmpty()) {
      throw new IllegalArgumentException("ValuesNode is a leaf node with no children");
    }
    return this;
  }

  @Override
  public String toString() {
    return "ValuesNode{id=" + getId() + ", rows=" + tuples.size() + "}";
  }
}
