/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.distributed.planner;

import java.util.List;
import org.opensearch.sql.distributed.planner.plan.PartitioningScheme;

/**
 * Plan node representing an exchange boundary between stages. The PlanFragmenter cuts the plan at
 * ExchangeNode boundaries to create separate stage fragments.
 *
 * <p>Phase 1 supports only GATHER exchange type (scatter-gather pattern). Phase 2 will add HASH and
 * BROADCAST exchange types.
 */
public class ExchangeNode extends PlanNode {

  /** Exchange distribution type. */
  public enum ExchangeType {
    /** All data gathered to a single coordinator node. */
    GATHER,
    /** Data hash-partitioned across nodes (Phase 2). */
    HASH,
    /** Data broadcast to all nodes (Phase 2). */
    BROADCAST
  }

  private final PlanNode source;
  private final ExchangeType exchangeType;
  private final PartitioningScheme partitioningScheme;

  public ExchangeNode(
      PlanNodeId id,
      PlanNode source,
      ExchangeType exchangeType,
      PartitioningScheme partitioningScheme) {
    super(id);
    this.source = source;
    this.exchangeType = exchangeType;
    this.partitioningScheme = partitioningScheme;
  }

  public PlanNode getSource() {
    return source;
  }

  public ExchangeType getExchangeType() {
    return exchangeType;
  }

  public PartitioningScheme getPartitioningScheme() {
    return partitioningScheme;
  }

  @Override
  public List<PlanNode> getSources() {
    return List.of(source);
  }

  @Override
  public <R, C> R accept(PlanNodeVisitor<R, C> visitor, C context) {
    return visitor.visitExchange(this, context);
  }

  @Override
  public PlanNode replaceChildren(List<PlanNode> newChildren) {
    if (newChildren.size() != 1) {
      throw new IllegalArgumentException("ExchangeNode expects exactly 1 child");
    }
    return new ExchangeNode(getId(), newChildren.get(0), exchangeType, partitioningScheme);
  }

  @Override
  public String toString() {
    return "ExchangeNode{id=" + getId() + ", type=" + exchangeType + "}";
  }
}
