/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.planner.distributed;

import org.apache.calcite.rel.RelNode;

/**
 * Estimates the cardinality (row count) of a query plan. Used by the {@link QueryRouter} to decide
 * whether distributed execution is beneficial.
 *
 * <p>The estimator uses two sources of information:
 *
 * <ol>
 *   <li>Calcite's built-in metadata (via {@code RelNode.estimateRowCount})
 *   <li>Index-level document counts from the {@link ShardSplitManager}
 * </ol>
 */
public class CardinalityEstimator {

  private final ShardSplitManager splitManager;

  public CardinalityEstimator(ShardSplitManager splitManager) {
    this.splitManager = splitManager;
  }

  /**
   * Estimates the number of rows the given plan will produce.
   *
   * <p>Attempts to use Calcite's metadata query first. If that is unavailable or produces an
   * invalid result, returns {@code -1} to indicate that the estimate is unknown.
   *
   * @param relNode the logical plan root
   * @return estimated row count, or -1 if unknown
   */
  public long estimateRowCount(RelNode relNode) {
    try {
      Double rowCount = relNode.estimateRowCount(relNode.getCluster().getMetadataQuery());
      if (rowCount != null && !rowCount.isNaN() && !rowCount.isInfinite() && rowCount >= 0) {
        return rowCount.longValue();
      }
    } catch (Exception e) {
      // Metadata query may not be available
    }
    return -1L;
  }

  /**
   * Returns the document count for the given index, using the {@link ShardSplitManager}.
   *
   * @param indexName the index to query
   * @return the document count
   */
  public long getIndexDocCount(String indexName) {
    return splitManager.getDocCount(indexName);
  }
}
