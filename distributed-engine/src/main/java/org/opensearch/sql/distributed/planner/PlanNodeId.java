/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.distributed.planner;

import java.util.Objects;
import java.util.concurrent.atomic.AtomicInteger;

/** Unique identifier for a PlanNode within a query plan. */
public final class PlanNodeId {

  private static final AtomicInteger NEXT_ID = new AtomicInteger(0);

  private final int id;
  private final String name;

  public PlanNodeId(int id, String name) {
    this.id = id;
    this.name = name;
  }

  public static PlanNodeId next(String name) {
    return new PlanNodeId(NEXT_ID.getAndIncrement(), name);
  }

  public int getId() {
    return id;
  }

  public String getName() {
    return name;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (!(o instanceof PlanNodeId)) return false;
    PlanNodeId that = (PlanNodeId) o;
    return id == that.id;
  }

  @Override
  public int hashCode() {
    return Objects.hash(id);
  }

  @Override
  public String toString() {
    return name + "#" + id;
  }
}
