/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.dqe.exchange;

/**
 * Describes how to merge partial aggregation results from shards into the final result on the
 * coordinator.
 */
public enum MergeFunction {
  /** Merge partial COUNT results by summing them. */
  SUM_COUNTS,

  /** Merge partial SUM results by summing them. */
  SUM_SUMS,

  /** Merge partial AVG results by dividing the sum of SUMs by the sum of COUNTs. */
  SUM_DIV_COUNT,

  /** Merge partial MIN results by taking the minimum. */
  MIN_OF,

  /** Merge partial MAX results by taking the maximum. */
  MAX_OF
}
