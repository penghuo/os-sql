/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.distributed.operator.window;

import java.util.Optional;
import org.opensearch.sql.distributed.data.Block;
import org.opensearch.sql.distributed.data.LongArrayBlock;

/**
 * DENSE_RANK() window function. Returns the dense rank of each row within a partition. Rows with
 * equal ORDER BY values receive the same rank, and the next rank increments by 1 (no gaps).
 *
 * <p>Example: values [10, 20, 20, 30] produce dense ranks [1, 2, 2, 3].
 */
public class DenseRankFunction implements WindowFunction {

  @Override
  public Block processPartition(
      PartitionData partition, int partitionSize, int[] orderKeyChannels) {
    long[] values = new long[partitionSize];
    if (partitionSize == 0) {
      return new LongArrayBlock(0, Optional.empty(), values);
    }

    long denseRank = 1;
    values[0] = denseRank;
    for (int i = 1; i < partitionSize; i++) {
      if (!partition.rowsEqual(i, i - 1, orderKeyChannels)) {
        denseRank++;
      }
      values[i] = denseRank;
    }
    return new LongArrayBlock(partitionSize, Optional.empty(), values);
  }

  @Override
  public String getName() {
    return "DENSE_RANK";
  }
}
