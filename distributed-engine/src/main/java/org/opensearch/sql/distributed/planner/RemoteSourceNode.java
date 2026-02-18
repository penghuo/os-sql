/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.distributed.planner;

import java.util.List;

/**
 * Plan node representing a reference to a remote stage's output. Created by the PlanFragmenter when
 * splitting the plan at ExchangeNode boundaries. The root stage uses RemoteSourceNode to reference
 * Pages produced by leaf stages.
 */
public class RemoteSourceNode extends PlanNode {

  private final List<Integer> sourceStageIds;
  private final ExchangeNode.ExchangeType exchangeType;

  public RemoteSourceNode(
      PlanNodeId id, List<Integer> sourceStageIds, ExchangeNode.ExchangeType exchangeType) {
    super(id);
    this.sourceStageIds = List.copyOf(sourceStageIds);
    this.exchangeType = exchangeType;
  }

  public List<Integer> getSourceStageIds() {
    return sourceStageIds;
  }

  public ExchangeNode.ExchangeType getExchangeType() {
    return exchangeType;
  }

  @Override
  public List<PlanNode> getSources() {
    return List.of();
  }

  @Override
  public <R, C> R accept(PlanNodeVisitor<R, C> visitor, C context) {
    return visitor.visitRemoteSource(this, context);
  }

  @Override
  public PlanNode replaceChildren(List<PlanNode> newChildren) {
    if (!newChildren.isEmpty()) {
      throw new IllegalArgumentException("RemoteSourceNode is a leaf node with no children");
    }
    return this;
  }

  @Override
  public String toString() {
    return "RemoteSourceNode{id="
        + getId()
        + ", sourceStages="
        + sourceStageIds
        + ", type="
        + exchangeType
        + "}";
  }
}
