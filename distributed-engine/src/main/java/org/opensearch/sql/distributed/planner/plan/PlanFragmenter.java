/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.distributed.planner.plan;

import java.util.ArrayList;
import java.util.List;
import org.opensearch.sql.distributed.planner.ExchangeNode;
import org.opensearch.sql.distributed.planner.PlanNode;
import org.opensearch.sql.distributed.planner.PlanNodeId;
import org.opensearch.sql.distributed.planner.PlanNodeVisitor;
import org.opensearch.sql.distributed.planner.RemoteSourceNode;

/**
 * Splits a PlanNode tree into StageFragments at ExchangeNode boundaries.
 *
 * <p>Adapted from Trino's {@code PlanFragmenter} (~850 LOC). Phase 1 produces exactly 2 fragments:
 *
 * <ul>
 *   <li>Leaf stage (stageId=0): everything below the GatherExchange (runs on data nodes)
 *   <li>Root stage (stageId=1): everything above the GatherExchange (runs on coordinator)
 * </ul>
 *
 * <p>The GatherExchange is replaced with a {@link RemoteSourceNode} in the root stage, which
 * references the leaf stage's output.
 */
public class PlanFragmenter {

  private int nextStageId = 0;

  /**
   * Fragments the plan tree into stages by cutting at ExchangeNode boundaries.
   *
   * @param root the plan tree with ExchangeNodes inserted by AddExchanges
   * @return list of StageFragments (leaf stages first, root stage last)
   */
  public List<StageFragment> fragment(PlanNode root) {
    List<StageFragment> fragments = new ArrayList<>();
    PlanNode rootStageTree = root.accept(new FragmentingVisitor(fragments), null);

    // Create the root stage fragment
    List<RemoteSourceNode> remoteSourceNodes = collectRemoteSourceNodes(rootStageTree);
    StageFragment rootFragment =
        new StageFragment(
            nextStageId++,
            rootStageTree,
            PartitioningScheme.singlePartitioning(),
            remoteSourceNodes);
    fragments.add(rootFragment);

    return fragments;
  }

  /**
   * Visitor that traverses the PlanNode tree and cuts at ExchangeNode boundaries. When an
   * ExchangeNode is encountered, the subtree below it becomes a leaf stage fragment, and the
   * ExchangeNode is replaced with a RemoteSourceNode.
   */
  private class FragmentingVisitor extends PlanNodeVisitor<PlanNode, Void> {

    private final List<StageFragment> fragments;

    FragmentingVisitor(List<StageFragment> fragments) {
      this.fragments = fragments;
    }

    @Override
    public PlanNode visitPlan(PlanNode node, Void context) {
      // Default: recurse into children
      List<PlanNode> newChildren =
          node.getSources().stream().map(child -> child.accept(this, context)).toList();
      if (newChildren.equals(node.getSources())) {
        return node;
      }
      return node.replaceChildren(newChildren);
    }

    @Override
    public PlanNode visitExchange(ExchangeNode node, Void context) {
      // The subtree below the exchange becomes a separate stage fragment.
      // First, recursively fragment the subtree (in case of nested exchanges in Phase 2).
      PlanNode leafStageRoot = node.getSource().accept(this, context);

      int leafStageId = nextStageId++;
      PartitioningScheme leafOutputPartitioning = node.getPartitioningScheme();
      List<RemoteSourceNode> leafRemoteSources = collectRemoteSourceNodes(leafStageRoot);

      StageFragment leafFragment =
          new StageFragment(leafStageId, leafStageRoot, leafOutputPartitioning, leafRemoteSources);
      fragments.add(leafFragment);

      // Replace the ExchangeNode with a RemoteSourceNode that references the leaf stage
      return new RemoteSourceNode(
          PlanNodeId.next("RemoteSource"), List.of(leafStageId), node.getExchangeType());
    }
  }

  /** Collects all RemoteSourceNode instances in the given subtree. */
  private List<RemoteSourceNode> collectRemoteSourceNodes(PlanNode root) {
    List<RemoteSourceNode> result = new ArrayList<>();
    collectRemoteSourceNodesRecursive(root, result);
    return result;
  }

  private void collectRemoteSourceNodesRecursive(PlanNode node, List<RemoteSourceNode> result) {
    if (node instanceof RemoteSourceNode) {
      result.add((RemoteSourceNode) node);
    }
    for (PlanNode child : node.getSources()) {
      collectRemoteSourceNodesRecursive(child, result);
    }
  }
}
