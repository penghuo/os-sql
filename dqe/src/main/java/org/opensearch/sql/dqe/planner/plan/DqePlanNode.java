/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.dqe.planner.plan;

import java.io.IOException;
import java.util.List;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.common.io.stream.Writeable;

/** Abstract base class for all DQE logical plan nodes. Each node is serializable via Writeable. */
public abstract class DqePlanNode implements Writeable {

  /** Returns the child nodes of this plan node. Leaf nodes return an empty list. */
  public abstract List<DqePlanNode> getChildren();

  /** Accept a visitor with the given context. */
  public abstract <R, C> R accept(DqePlanVisitor<R, C> visitor, C context);

  /**
   * Serialize a plan node to the stream. Writes the fully qualified class name first, then
   * delegates to the node's {@link #writeTo(StreamOutput)} method.
   */
  public static void writePlanNode(StreamOutput out, DqePlanNode node) throws IOException {
    out.writeString(node.getClass().getName());
    node.writeTo(out);
  }

  /**
   * Deserialize a plan node from the stream. Reads the class name and dispatches to the
   * corresponding constructor that takes a {@link StreamInput}.
   */
  public static DqePlanNode readPlanNode(StreamInput in) throws IOException {
    String className = in.readString();
    if (className.equals(TableScanNode.class.getName())) {
      return new TableScanNode(in);
    } else if (className.equals(FilterNode.class.getName())) {
      return new FilterNode(in);
    } else if (className.equals(ProjectNode.class.getName())) {
      return new ProjectNode(in);
    } else if (className.equals(AggregationNode.class.getName())) {
      return new AggregationNode(in);
    } else if (className.equals(SortNode.class.getName())) {
      return new SortNode(in);
    } else if (className.equals(LimitNode.class.getName())) {
      return new LimitNode(in);
    }
    throw new IOException("Unknown plan node type: " + className);
  }
}
