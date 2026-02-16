/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.planner.distributed;

import org.apache.calcite.rel.RelNode;

/**
 * Decides whether a query should use distributed execution or fall back to single-node execution.
 *
 * <p>The routing decision is based on three factors:
 *
 * <ol>
 *   <li>Whether distributed execution is enabled (configuration flag)
 *   <li>Whether the query plan contains operators that benefit from distribution (aggregation,
 *       sort, join)
 *   <li>Whether the index has enough documents to justify the overhead of distribution
 * </ol>
 */
public class QueryRouter {

  /** Default minimum document count threshold for distributed execution. */
  public static final long DEFAULT_DOC_COUNT_THRESHOLD = 10_000L;

  private final boolean distributedEnabled;
  private final FragmentPlanner fragmentPlanner;
  private final CardinalityEstimator cardinalityEstimator;
  private final long docCountThreshold;

  /**
   * Creates a QueryRouter with the default document count threshold.
   *
   * @param distributedEnabled whether distributed execution is enabled
   * @param fragmentPlanner the fragment planner for checking exchange requirements
   * @param cardinalityEstimator the cardinality estimator for doc count lookups
   */
  public QueryRouter(
      boolean distributedEnabled,
      FragmentPlanner fragmentPlanner,
      CardinalityEstimator cardinalityEstimator) {
    this(distributedEnabled, fragmentPlanner, cardinalityEstimator, DEFAULT_DOC_COUNT_THRESHOLD);
  }

  /**
   * Creates a QueryRouter with a custom document count threshold.
   *
   * @param distributedEnabled whether distributed execution is enabled
   * @param fragmentPlanner the fragment planner for checking exchange requirements
   * @param cardinalityEstimator the cardinality estimator for doc count lookups
   * @param docCountThreshold minimum document count to justify distributed execution
   */
  public QueryRouter(
      boolean distributedEnabled,
      FragmentPlanner fragmentPlanner,
      CardinalityEstimator cardinalityEstimator,
      long docCountThreshold) {
    this.distributedEnabled = distributedEnabled;
    this.fragmentPlanner = fragmentPlanner;
    this.cardinalityEstimator = cardinalityEstimator;
    this.docCountThreshold = docCountThreshold;
  }

  /**
   * Determines whether the given query plan should use distributed execution.
   *
   * @param relNode the logical plan root
   * @param indexName the index being queried
   * @return {@code true} if distributed execution should be used
   */
  public boolean shouldDistribute(RelNode relNode, String indexName) {
    if (!distributedEnabled) {
      return false;
    }
    if (!fragmentPlanner.requiresExchange(relNode)) {
      return false;
    }
    long docCount = cardinalityEstimator.getIndexDocCount(indexName);
    // If doc count is unknown (-1), default to distributed to be safe
    return docCount < 0 || docCount >= docCountThreshold;
  }
}
