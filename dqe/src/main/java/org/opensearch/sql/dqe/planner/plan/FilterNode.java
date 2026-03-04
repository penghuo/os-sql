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

/** Plan node representing a filter (WHERE clause) applied to its child. */
@Getter
public class FilterNode extends DqePlanNode {

  private final DqePlanNode child;
  private final String predicateString;

  public FilterNode(DqePlanNode child, String predicateString) {
    this.child = child;
    this.predicateString = predicateString;
  }

  /** Deserialize from a stream. */
  public FilterNode(StreamInput in) throws IOException {
    this.child = DqePlanNode.readPlanNode(in);
    this.predicateString = in.readString();
  }

  @Override
  public void writeTo(StreamOutput out) throws IOException {
    DqePlanNode.writePlanNode(out, child);
    out.writeString(predicateString);
  }

  @Override
  public List<DqePlanNode> getChildren() {
    return List.of(child);
  }

  @Override
  public <R, C> R accept(DqePlanVisitor<R, C> visitor, C context) {
    return visitor.visitFilter(this, context);
  }
}
