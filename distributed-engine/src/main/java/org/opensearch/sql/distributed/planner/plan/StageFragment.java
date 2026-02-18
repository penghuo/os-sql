/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.distributed.planner.plan;

import java.util.List;
import org.opensearch.sql.distributed.planner.PlanNode;
import org.opensearch.sql.distributed.planner.RemoteSourceNode;

/**
 * Represents a fragment (stage) of the distributed execution plan. Each fragment executes as a unit
 * on a set of nodes.
 *
 * <p>In Phase 1 scatter-gather, there are always exactly 2 fragments:
 *
 * <ul>
 *   <li>Leaf stage (stageId=0): executes on data nodes (shard-holding nodes)
 *   <li>Root stage (stageId=1): executes on the coordinator node
 * </ul>
 */
public class StageFragment {

  private final int stageId;
  private final PlanNode root;
  private final PartitioningScheme outputPartitioning;
  private final List<RemoteSourceNode> remoteSourceNodes;

  public StageFragment(
      int stageId,
      PlanNode root,
      PartitioningScheme outputPartitioning,
      List<RemoteSourceNode> remoteSourceNodes) {
    this.stageId = stageId;
    this.root = root;
    this.outputPartitioning = outputPartitioning;
    this.remoteSourceNodes = List.copyOf(remoteSourceNodes);
  }

  /** Returns the stage identifier. */
  public int getStageId() {
    return stageId;
  }

  /** Returns the root PlanNode of this fragment's subtree. */
  public PlanNode getRoot() {
    return root;
  }

  /** Returns how this fragment's output is partitioned/distributed. */
  public PartitioningScheme getOutputPartitioning() {
    return outputPartitioning;
  }

  /** Returns references to upstream stages (via RemoteSourceNode). Empty for leaf stages. */
  public List<RemoteSourceNode> getRemoteSourceNodes() {
    return remoteSourceNodes;
  }

  /** Returns true if this is a leaf stage (no remote sources). */
  public boolean isLeafStage() {
    return remoteSourceNodes.isEmpty();
  }

  @Override
  public String toString() {
    return "StageFragment{stageId="
        + stageId
        + ", root="
        + root
        + ", partitioning="
        + outputPartitioning
        + ", isLeaf="
        + isLeafStage()
        + "}";
  }
}
