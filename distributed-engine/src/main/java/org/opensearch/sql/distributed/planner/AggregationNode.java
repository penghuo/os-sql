/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.distributed.planner;

import java.util.List;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.util.ImmutableBitSet;

/**
 * Plan node representing an aggregation operation. Corresponds to Calcite's LogicalAggregate.
 * Supports partial and final aggregation modes for distributed execution.
 *
 * <p>In scatter-gather mode:
 *
 * <ul>
 *   <li>PARTIAL: runs on leaf nodes, produces intermediate aggregation state
 *   <li>FINAL: runs on coordinator, merges partial results
 *   <li>SINGLE: runs entirely on one node (no distribution)
 * </ul>
 */
public class AggregationNode extends PlanNode {

  /** Aggregation execution mode for distributed processing. */
  public enum AggregationMode {
    PARTIAL,
    FINAL,
    SINGLE
  }

  private final PlanNode source;
  private final ImmutableBitSet groupSet;
  private final List<AggregateCall> aggregateCalls;
  private final AggregationMode mode;

  public AggregationNode(
      PlanNodeId id,
      PlanNode source,
      ImmutableBitSet groupSet,
      List<AggregateCall> aggregateCalls,
      AggregationMode mode) {
    super(id);
    this.source = source;
    this.groupSet = groupSet;
    this.aggregateCalls = List.copyOf(aggregateCalls);
    this.mode = mode;
  }

  public PlanNode getSource() {
    return source;
  }

  public ImmutableBitSet getGroupSet() {
    return groupSet;
  }

  public List<AggregateCall> getAggregateCalls() {
    return aggregateCalls;
  }

  public AggregationMode getMode() {
    return mode;
  }

  @Override
  public List<PlanNode> getSources() {
    return List.of(source);
  }

  @Override
  public <R, C> R accept(PlanNodeVisitor<R, C> visitor, C context) {
    return visitor.visitAggregation(this, context);
  }

  @Override
  public PlanNode replaceChildren(List<PlanNode> newChildren) {
    if (newChildren.size() != 1) {
      throw new IllegalArgumentException("AggregationNode expects exactly 1 child");
    }
    return new AggregationNode(getId(), newChildren.get(0), groupSet, aggregateCalls, mode);
  }

  @Override
  public String toString() {
    return "AggregationNode{id="
        + getId()
        + ", mode="
        + mode
        + ", groupSet="
        + groupSet
        + ", aggCalls="
        + aggregateCalls
        + "}";
  }
}
