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

/** Plan node representing a LIMIT/OFFSET clause that caps the number of output rows. */
@Getter
public class LimitNode extends DqePlanNode {

  private final DqePlanNode child;
  private final long count;
  private final long offset;

  public LimitNode(DqePlanNode child, long count) {
    this(child, count, 0);
  }

  public LimitNode(DqePlanNode child, long count, long offset) {
    this.child = child;
    this.count = count;
    this.offset = offset;
  }

  /** Deserialize from a stream. */
  public LimitNode(StreamInput in) throws IOException {
    this.child = DqePlanNode.readPlanNode(in);
    this.count = in.readLong();
    this.offset = in.readLong();
  }

  @Override
  public void writeTo(StreamOutput out) throws IOException {
    DqePlanNode.writePlanNode(out, child);
    out.writeLong(count);
    out.writeLong(offset);
  }

  @Override
  public List<DqePlanNode> getChildren() {
    return List.of(child);
  }

  @Override
  public <R, C> R accept(DqePlanVisitor<R, C> visitor, C context) {
    return visitor.visitLimit(this, context);
  }
}
