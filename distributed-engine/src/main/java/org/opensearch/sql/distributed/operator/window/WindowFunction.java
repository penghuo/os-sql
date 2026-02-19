/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.distributed.operator.window;

import org.opensearch.sql.distributed.data.Block;

/**
 * Interface for window functions. A window function processes a partition of rows (which share the
 * same partition key) and produces one output value per row.
 *
 * <p>Ported from Trino's window function interfaces (simplified: single interface for ranking,
 * value, and aggregate window functions).
 */
public interface WindowFunction {

  /**
   * Processes a partition and returns a Block containing one result value per row.
   *
   * @param partition the partition data (all pages flattened into per-column arrays)
   * @param partitionSize the number of rows in the partition
   * @param orderKeyChannels column indices used for ORDER BY (for rank comparisons)
   * @return a Block with {@code partitionSize} positions, one result per row
   */
  Block processPartition(PartitionData partition, int partitionSize, int[] orderKeyChannels);

  /** Returns a descriptive name for this window function (for diagnostics). */
  String getName();
}
