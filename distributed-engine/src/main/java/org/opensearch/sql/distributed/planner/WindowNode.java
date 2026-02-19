/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.distributed.planner;

import java.util.List;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexLiteral;

/**
 * Plan node representing a window function operation. Corresponds to Calcite's LogicalWindow.
 * Supports ROW_NUMBER, RANK, DENSE_RANK, LAG, LEAD, and aggregate-over-window functions.
 *
 * <p>In distributed execution, data is hash-partitioned by the partition-by columns, then sorted by
 * the order-by columns within each partition before the window function is applied.
 */
public class WindowNode extends PlanNode {

  private final PlanNode source;
  private final List<Integer> partitionByColumns;
  private final RelCollation orderByCollation;
  private final List<RexLiteral> constants;
  private final RelDataType outputType;

  /**
   * Creates a WindowNode.
   *
   * @param id the plan node identifier
   * @param source the input plan node
   * @param partitionByColumns column indices for PARTITION BY
   * @param orderByCollation collation for ORDER BY within each partition
   * @param constants constants referenced by window function expressions
   * @param outputType the output row type including window function result columns
   */
  public WindowNode(
      PlanNodeId id,
      PlanNode source,
      List<Integer> partitionByColumns,
      RelCollation orderByCollation,
      List<RexLiteral> constants,
      RelDataType outputType) {
    super(id);
    this.source = source;
    this.partitionByColumns = List.copyOf(partitionByColumns);
    this.orderByCollation = orderByCollation;
    this.constants = constants != null ? List.copyOf(constants) : List.of();
    this.outputType = outputType;
  }

  /** Returns the input plan node. */
  public PlanNode getSource() {
    return source;
  }

  /** Returns the partition-by column indices. */
  public List<Integer> getPartitionByColumns() {
    return partitionByColumns;
  }

  /** Returns the order-by collation within each partition. */
  public RelCollation getOrderByCollation() {
    return orderByCollation;
  }

  /** Returns constants referenced by window function expressions. */
  public List<RexLiteral> getConstants() {
    return constants;
  }

  /** Returns the output row type including window function result columns. */
  public RelDataType getOutputType() {
    return outputType;
  }

  @Override
  public List<PlanNode> getSources() {
    return List.of(source);
  }

  @Override
  public <R, C> R accept(PlanNodeVisitor<R, C> visitor, C context) {
    return visitor.visitWindow(this, context);
  }

  @Override
  public PlanNode replaceChildren(List<PlanNode> newChildren) {
    if (newChildren.size() != 1) {
      throw new IllegalArgumentException("WindowNode expects exactly 1 child");
    }
    return new WindowNode(
        getId(), newChildren.get(0), partitionByColumns, orderByCollation, constants, outputType);
  }

  @Override
  public String toString() {
    return "WindowNode{id="
        + getId()
        + ", partitionBy="
        + partitionByColumns
        + ", orderBy="
        + orderByCollation
        + "}";
  }
}
