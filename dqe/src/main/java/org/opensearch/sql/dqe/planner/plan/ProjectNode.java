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

/** Plan node representing a projection (SELECT columns) applied to its child. */
@Getter
public class ProjectNode extends DqePlanNode {

  private final DqePlanNode child;
  private final List<String> outputColumns;

  public ProjectNode(DqePlanNode child, List<String> outputColumns) {
    this.child = child;
    this.outputColumns = outputColumns;
  }

  /** Deserialize from a stream. */
  public ProjectNode(StreamInput in) throws IOException {
    this.child = DqePlanNode.readPlanNode(in);
    this.outputColumns = in.readStringList();
  }

  @Override
  public void writeTo(StreamOutput out) throws IOException {
    DqePlanNode.writePlanNode(out, child);
    out.writeStringCollection(outputColumns);
  }

  @Override
  public List<DqePlanNode> getChildren() {
    return List.of(child);
  }

  @Override
  public <R, C> R accept(DqePlanVisitor<R, C> visitor, C context) {
    return visitor.visitProject(this, context);
  }
}
