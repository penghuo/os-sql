/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.distributed.planner;

import java.util.List;

/**
 * Abstract base class for all plan nodes in the distributed execution plan. Modeled after Trino's
 * PlanNode but simplified for Phase 1 scatter-gather.
 *
 * <p>PlanNodes form an immutable tree that represents a physical execution plan. Each node has a
 * unique {@link PlanNodeId} and zero or more child sources.
 */
public abstract class PlanNode {

  private final PlanNodeId id;

  protected PlanNode(PlanNodeId id) {
    this.id = id;
  }

  /** Returns the unique identifier for this plan node. */
  public PlanNodeId getId() {
    return id;
  }

  /** Returns the child plan nodes (sources) of this node. */
  public abstract List<PlanNode> getSources();

  /** Accepts a visitor for tree traversal. */
  public abstract <R, C> R accept(PlanNodeVisitor<R, C> visitor, C context);

  /**
   * Returns a copy of this node with the given sources replaced. The new sources list must have the
   * same size as the original.
   */
  public abstract PlanNode replaceChildren(List<PlanNode> newChildren);

  @Override
  public String toString() {
    return getClass().getSimpleName() + "{id=" + id + "}";
  }
}
