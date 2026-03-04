/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.dqe.planner.plan;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import lombok.Getter;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;

/** Plan node representing an ORDER BY clause with sort keys and directions. */
@Getter
public class SortNode extends DqePlanNode {

  private final DqePlanNode child;
  private final List<String> sortKeys;
  private final List<Boolean> ascending;

  public SortNode(DqePlanNode child, List<String> sortKeys, List<Boolean> ascending) {
    if (sortKeys.size() != ascending.size()) {
      throw new IllegalArgumentException("sortKeys and ascending must have the same size");
    }
    this.child = child;
    this.sortKeys = sortKeys;
    this.ascending = ascending;
  }

  /** Deserialize from a stream. */
  public SortNode(StreamInput in) throws IOException {
    this.child = DqePlanNode.readPlanNode(in);
    this.sortKeys = in.readStringList();
    int size = in.readVInt();
    List<Boolean> ascList = new ArrayList<>(size);
    for (int i = 0; i < size; i++) {
      ascList.add(in.readBoolean());
    }
    this.ascending = ascList;
  }

  @Override
  public void writeTo(StreamOutput out) throws IOException {
    DqePlanNode.writePlanNode(out, child);
    out.writeStringCollection(sortKeys);
    out.writeVInt(ascending.size());
    for (Boolean asc : ascending) {
      out.writeBoolean(asc);
    }
  }

  @Override
  public List<DqePlanNode> getChildren() {
    return List.of(child);
  }

  @Override
  public <R, C> R accept(DqePlanVisitor<R, C> visitor, C context) {
    return visitor.visitSort(this, context);
  }
}
