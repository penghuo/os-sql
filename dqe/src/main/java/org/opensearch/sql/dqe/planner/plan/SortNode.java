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

/**
 * Plan node representing an ORDER BY clause with sort keys, directions, and null ordering.
 *
 * <p>Each sort key has an associated ascending flag and a nullsFirst flag:
 *
 * <ul>
 *   <li>{@code ascending.get(i) == true} means ASC, {@code false} means DESC.
 *   <li>{@code nullsFirst.get(i) == true} means NULLS FIRST, {@code false} means NULLS LAST.
 * </ul>
 */
@Getter
public class SortNode extends DqePlanNode {

  private final DqePlanNode child;
  private final List<String> sortKeys;
  private final List<Boolean> ascending;
  private final List<Boolean> nullsFirst;

  /**
   * Convenience constructor that uses Trino-compatible null ordering defaults: NULLS LAST for both
   * ASC and DESC.
   */
  public SortNode(DqePlanNode child, List<String> sortKeys, List<Boolean> ascending) {
    this(child, sortKeys, ascending, defaultNullsFirst(ascending));
  }

  public SortNode(
      DqePlanNode child, List<String> sortKeys, List<Boolean> ascending, List<Boolean> nullsFirst) {
    if (sortKeys.size() != ascending.size()) {
      throw new IllegalArgumentException("sortKeys and ascending must have the same size");
    }
    if (sortKeys.size() != nullsFirst.size()) {
      throw new IllegalArgumentException("sortKeys and nullsFirst must have the same size");
    }
    this.child = child;
    this.sortKeys = sortKeys;
    this.ascending = ascending;
    this.nullsFirst = nullsFirst;
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
    int nfSize = in.readVInt();
    List<Boolean> nfList = new ArrayList<>(nfSize);
    for (int i = 0; i < nfSize; i++) {
      nfList.add(in.readBoolean());
    }
    this.nullsFirst = nfList;
  }

  @Override
  public void writeTo(StreamOutput out) throws IOException {
    DqePlanNode.writePlanNode(out, child);
    out.writeStringCollection(sortKeys);
    out.writeVInt(ascending.size());
    for (Boolean asc : ascending) {
      out.writeBoolean(asc);
    }
    out.writeVInt(nullsFirst.size());
    for (Boolean nf : nullsFirst) {
      out.writeBoolean(nf);
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

  /** Derive Trino-compatible null ordering: NULLS LAST for both ASC and DESC (Trino's default). */
  private static List<Boolean> defaultNullsFirst(List<Boolean> ascending) {
    List<Boolean> defaults = new ArrayList<>(ascending.size());
    for (int i = 0; i < ascending.size(); i++) {
      defaults.add(false); // Trino defaults to NULLS LAST for both ASC and DESC
    }
    return defaults;
  }
}
