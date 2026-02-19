/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.distributed.planner;

import java.util.List;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rex.RexNode;

/**
 * Plan node representing a join operation. Corresponds to Calcite's LogicalJoin. Supports INNER,
 * LEFT, RIGHT, FULL, SEMI, and ANTI join types.
 *
 * <p>In distributed execution, the join's two inputs are connected via exchange operators:
 *
 * <ul>
 *   <li>Hash join: both sides hash-partitioned on join key columns
 *   <li>Broadcast join: small (build) side broadcast to all nodes, large (probe) side stays local
 * </ul>
 *
 * <p>The build side is conventionally the right input, and the probe side is the left input.
 */
public class JoinNode extends PlanNode {

  private final PlanNode left;
  private final PlanNode right;
  private final RexNode condition;
  private final JoinRelType joinType;
  private final List<Integer> leftJoinKeys;
  private final List<Integer> rightJoinKeys;

  /**
   * Creates a JoinNode.
   *
   * @param id the plan node identifier
   * @param left the probe (left) side input
   * @param right the build (right) side input
   * @param condition the join condition expression
   * @param joinType the type of join (INNER, LEFT, RIGHT, FULL, SEMI, ANTI)
   * @param leftJoinKeys column indices from the left side that form the join key
   * @param rightJoinKeys column indices from the right side that form the join key
   */
  public JoinNode(
      PlanNodeId id,
      PlanNode left,
      PlanNode right,
      RexNode condition,
      JoinRelType joinType,
      List<Integer> leftJoinKeys,
      List<Integer> rightJoinKeys) {
    super(id);
    this.left = left;
    this.right = right;
    this.condition = condition;
    this.joinType = joinType;
    this.leftJoinKeys = List.copyOf(leftJoinKeys);
    this.rightJoinKeys = List.copyOf(rightJoinKeys);
  }

  /** Returns the probe (left) side input. */
  public PlanNode getLeft() {
    return left;
  }

  /** Returns the build (right) side input. */
  public PlanNode getRight() {
    return right;
  }

  /** Returns the join condition. */
  public RexNode getCondition() {
    return condition;
  }

  /** Returns the join type. */
  public JoinRelType getJoinType() {
    return joinType;
  }

  /** Returns the join key column indices from the left side. */
  public List<Integer> getLeftJoinKeys() {
    return leftJoinKeys;
  }

  /** Returns the join key column indices from the right side. */
  public List<Integer> getRightJoinKeys() {
    return rightJoinKeys;
  }

  @Override
  public List<PlanNode> getSources() {
    return List.of(left, right);
  }

  @Override
  public <R, C> R accept(PlanNodeVisitor<R, C> visitor, C context) {
    return visitor.visitJoin(this, context);
  }

  @Override
  public PlanNode replaceChildren(List<PlanNode> newChildren) {
    if (newChildren.size() != 2) {
      throw new IllegalArgumentException("JoinNode expects exactly 2 children");
    }
    return new JoinNode(
        getId(),
        newChildren.get(0),
        newChildren.get(1),
        condition,
        joinType,
        leftJoinKeys,
        rightJoinKeys);
  }

  @Override
  public String toString() {
    return "JoinNode{id="
        + getId()
        + ", type="
        + joinType
        + ", leftKeys="
        + leftJoinKeys
        + ", rightKeys="
        + rightJoinKeys
        + "}";
  }
}
