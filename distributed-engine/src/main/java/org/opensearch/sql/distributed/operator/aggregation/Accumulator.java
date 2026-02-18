/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.distributed.operator.aggregation;

import org.opensearch.sql.distributed.data.Block;

/**
 * Accumulates values for a single aggregation function across all groups. Ported from Trino's
 * accumulator concept but simplified for Phase 1.
 *
 * <p>Each Accumulator instance tracks state for multiple groups (identified by groupId). The
 * GroupByHash assigns groupIds to input rows.
 */
public interface Accumulator {

  /**
   * Adds input values for the given group IDs.
   *
   * @param groupIds group ID for each row position
   * @param groupCount total number of groups seen so far
   * @param block the input block (column) for this accumulator
   */
  void addInput(int[] groupIds, int groupCount, Block block);

  /**
   * Returns the result for the given group as a value that can be placed into an output block.
   *
   * @param groupId the group to get the result for
   * @return the aggregated value (Long, Double, or byte[])
   */
  Object getResult(int groupId);

  /**
   * Returns true if the result for the given group should be null. This handles cases like COUNT
   * returning 0 vs. AVG returning null for no input.
   */
  boolean isNull(int groupId);

  /** Returns the estimated memory usage in bytes. */
  long getEstimatedSizeInBytes();

  /** Ensures internal state arrays can hold at least the given number of groups. */
  void ensureCapacity(int groupCount);

  /** Returns the type of block this accumulator produces. */
  ResultType getResultType();

  enum ResultType {
    LONG,
    DOUBLE,
    VARIABLE_WIDTH
  }
}
