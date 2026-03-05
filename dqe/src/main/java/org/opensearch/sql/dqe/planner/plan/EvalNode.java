/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.dqe.planner.plan;

import java.io.IOException;
import java.util.List;
import lombok.Getter;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;

/**
 * Plan node representing computed columns (expressions in SELECT list). Contains serialized
 * expression strings and output column names.
 */
@Getter
public class EvalNode extends DqePlanNode {

  private final DqePlanNode child;
  private final List<String> expressions;
  private final List<String> outputColumnNames;

  public EvalNode(DqePlanNode child, List<String> expressions, List<String> outputColumnNames) {
    this.child = child;
    this.expressions = expressions;
    this.outputColumnNames = outputColumnNames;
  }

  /** Deserialize from a stream. */
  public EvalNode(StreamInput in) throws IOException {
    this.child = DqePlanNode.readPlanNode(in);
    this.expressions = in.readStringList();
    this.outputColumnNames = in.readStringList();
  }

  @Override
  public void writeTo(StreamOutput out) throws IOException {
    DqePlanNode.writePlanNode(out, child);
    out.writeStringCollection(expressions);
    out.writeStringCollection(outputColumnNames);
  }

  @Override
  public List<DqePlanNode> getChildren() {
    return List.of(child);
  }

  @Override
  public <R, C> R accept(DqePlanVisitor<R, C> visitor, C context) {
    return visitor.visitEval(this, context);
  }
}
