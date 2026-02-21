/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.dqe.exchange;

import java.util.Iterator;
import java.util.List;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;

/**
 * Abstract base class for Exchange nodes in the Distributed Query Engine (DQE).
 *
 * <p>An Exchange node sits at the boundary between shard-local and coordinator-level execution. It
 * is responsible for:
 *
 * <ol>
 *   <li>Defining what runs on each shard ({@link #getShardPlan()})
 *   <li>Receiving shard results ({@link #setShardResults(List)})
 *   <li>Merging them into a single result stream ({@link #scan()})
 * </ol>
 */
public abstract class Exchange {

  /** The RelNode subtree that should be serialized and shipped to each shard for execution. */
  private final RelNode shardPlan;

  /** The row type of the output produced by {@link #scan()}. */
  private final RelDataType rowType;

  protected Exchange(RelNode shardPlan, RelDataType rowType) {
    this.shardPlan = shardPlan;
    this.rowType = rowType;
  }

  /** Returns the RelNode subtree to serialize and ship to shards. */
  public RelNode getShardPlan() {
    return shardPlan;
  }

  /**
   * Called by the coordinator after collecting results from all shards. Implementations should store
   * or process the results so that {@link #scan()} can iterate over them.
   *
   * @param results the results from each shard
   */
  public abstract void setShardResults(List<ShardResult> results);

  /**
   * Returns an iterator over the merged result rows. Must be called after {@link
   * #setShardResults(List)}.
   *
   * @return iterator of Object[] rows
   */
  public abstract Iterator<Object[]> scan();

  /** Returns the row type of the output. */
  public RelDataType getRowType() {
    return rowType;
  }
}
