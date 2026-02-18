/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.distributed.explain;

import java.util.List;
import java.util.Map;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.sql.distributed.planner.PlanNode;
import org.opensearch.sql.distributed.planner.PlanNodeVisitor;
import org.opensearch.sql.distributed.planner.plan.StageFragment;
import org.opensearch.sql.distributed.scheduler.NodeAssignment;
import org.opensearch.sql.distributed.scheduler.StageExecution;
import org.opensearch.sql.executor.ExecutionEngine.ExplainResponse;
import org.opensearch.sql.executor.ExecutionEngine.ExplainResponseNodeV2;

/**
 * Generates an explain response for a distributed execution plan. Describes stages, operators per
 * stage, shard assignments, and exchange types.
 */
public class DistributedPlanExplainer {

  /**
   * Generate an ExplainResponse for the given distributed execution plan.
   *
   * @param execution the scheduled execution with stage assignments
   * @return an ExplainResponse containing the distributed plan description
   */
  public ExplainResponse explain(StageExecution execution) {
    StringBuilder logical = new StringBuilder();
    StringBuilder physical = new StringBuilder();

    logical.append("Distributed Scatter-Gather Plan\n");
    logical.append("===============================\n\n");

    physical.append("Stage Assignments\n");
    physical.append("=================\n\n");

    for (StageFragment fragment : execution.getFragments()) {
      int stageId = fragment.getStageId();
      boolean isLeaf = fragment.isLeafStage();

      logical.append(
          String.format(
              "Stage %d (%s):\n", stageId, isLeaf ? "LEAF - data nodes" : "ROOT - coordinator"));
      logical.append(explainPlanTree(fragment.getRoot(), "  "));
      logical.append("\n");

      NodeAssignment assignment = execution.getAssignment(stageId);
      physical.append(String.format("Stage %d:\n", stageId));

      if (isLeaf) {
        physical.append("  Distribution: SOURCE_DISTRIBUTED\n");
        for (Map.Entry<DiscoveryNode, List<Integer>> entry :
            assignment.getNodeToShards().entrySet()) {
          physical.append(
              String.format("  Node %s: shards %s\n", entry.getKey().getName(), entry.getValue()));
        }
      } else {
        physical.append("  Distribution: COORDINATOR_ONLY\n");
        physical.append(String.format("  Node: %s\n", execution.getCoordinatorNode().getName()));
      }
      physical.append("\n");
    }

    return new ExplainResponse(
        new ExplainResponseNodeV2(logical.toString(), physical.toString(), null));
  }

  /** Recursively explain a PlanNode tree as an indented string. */
  private String explainPlanTree(PlanNode node, String indent) {
    StringBuilder sb = new StringBuilder();
    PlanNodeDescriber describer = new PlanNodeDescriber();
    String description = node.accept(describer, null);
    sb.append(indent).append(description).append("\n");

    for (PlanNode child : node.getSources()) {
      sb.append(explainPlanTree(child, indent + "  "));
    }
    return sb.toString();
  }

  /** Visitor that produces a human-readable description of each PlanNode. */
  private static class PlanNodeDescriber extends PlanNodeVisitor<String, Void> {
    @Override
    public String visitPlan(PlanNode node, Void context) {
      return node.getClass().getSimpleName() + " [id=" + node.getId() + "]";
    }
  }
}
