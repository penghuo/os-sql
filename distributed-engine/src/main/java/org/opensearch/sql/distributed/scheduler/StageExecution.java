/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.distributed.scheduler;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.sql.distributed.planner.plan.StageFragment;

/**
 * Holds the scheduled execution plan: a list of StageFragments along with their node assignments.
 * Produced by {@link StageScheduler} and consumed by the execution engine to dispatch fragments to
 * the appropriate nodes.
 */
public class StageExecution {

  private final List<StageFragment> fragments;
  private final Map<Integer, NodeAssignment> stageAssignments;
  private final DiscoveryNode coordinatorNode;

  /**
   * Creates a new StageExecution.
   *
   * @param fragments all stage fragments (leaf stages first, root last)
   * @param stageAssignments mapping from stageId to its node assignment
   * @param coordinatorNode the node acting as coordinator (runs the root stage)
   */
  public StageExecution(
      List<StageFragment> fragments,
      Map<Integer, NodeAssignment> stageAssignments,
      DiscoveryNode coordinatorNode) {
    this.fragments = List.copyOf(Objects.requireNonNull(fragments, "fragments is null"));
    this.stageAssignments =
        Map.copyOf(Objects.requireNonNull(stageAssignments, "stageAssignments is null"));
    this.coordinatorNode = Objects.requireNonNull(coordinatorNode, "coordinatorNode is null");
  }

  /** Returns all fragments in dependency order (leaf first, root last). */
  public List<StageFragment> getFragments() {
    return fragments;
  }

  /** Returns the node assignment for a given stage. */
  public NodeAssignment getAssignment(int stageId) {
    NodeAssignment assignment = stageAssignments.get(stageId);
    if (assignment == null) {
      throw new IllegalArgumentException("No assignment found for stage " + stageId);
    }
    return assignment;
  }

  /** Returns all stage assignments. */
  public Map<Integer, NodeAssignment> getStageAssignments() {
    return stageAssignments;
  }

  /** Returns the coordinator node (runs the root stage). */
  public DiscoveryNode getCoordinatorNode() {
    return coordinatorNode;
  }

  /** Returns the leaf stage fragment. In Phase 1, there is exactly one leaf stage. */
  public StageFragment getLeafStage() {
    return fragments.stream()
        .filter(StageFragment::isLeafStage)
        .findFirst()
        .orElseThrow(() -> new IllegalStateException("No leaf stage found"));
  }

  /** Returns the root stage fragment. In Phase 1, this is the last fragment. */
  public StageFragment getRootStage() {
    return fragments.stream()
        .filter(f -> !f.isLeafStage())
        .findFirst()
        .orElseThrow(() -> new IllegalStateException("No root stage found"));
  }

  @Override
  public String toString() {
    return "StageExecution{stages="
        + fragments.size()
        + ", coordinator="
        + coordinatorNode.getName()
        + "}";
  }
}
