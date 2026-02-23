/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dqe.analyzer.sort;

import java.util.Objects;
import org.opensearch.dqe.metadata.DqeColumnHandle;
import org.opensearch.dqe.types.DqeType;

/** A resolved sort column with direction and null ordering. */
public class SortSpecification {

  /** Sort direction for ORDER BY. */
  public enum SortDirection {
    ASC,
    DESC
  }

  /** Null ordering for ORDER BY. */
  public enum NullOrdering {
    NULLS_FIRST,
    NULLS_LAST
  }

  private final DqeColumnHandle column;
  private final DqeType type;
  private final SortDirection direction;
  private final NullOrdering nullOrdering;

  public SortSpecification(
      DqeColumnHandle column, DqeType type, SortDirection direction, NullOrdering nullOrdering) {
    this.column = Objects.requireNonNull(column, "column must not be null");
    this.type = Objects.requireNonNull(type, "type must not be null");
    this.direction = Objects.requireNonNull(direction, "direction must not be null");
    this.nullOrdering = Objects.requireNonNull(nullOrdering, "nullOrdering must not be null");
  }

  public DqeColumnHandle getColumn() {
    return column;
  }

  public DqeType getType() {
    return type;
  }

  public SortDirection getDirection() {
    return direction;
  }

  public NullOrdering getNullOrdering() {
    return nullOrdering;
  }

  @Override
  public String toString() {
    return column.getFieldName() + " " + direction + " " + nullOrdering;
  }
}
