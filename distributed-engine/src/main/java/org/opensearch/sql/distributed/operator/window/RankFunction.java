/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.distributed.operator.window;

import java.util.Optional;
import org.opensearch.sql.distributed.data.Block;
import org.opensearch.sql.distributed.data.LongArrayBlock;

/**
 * RANK() window function. Returns the rank of each row within a partition. Rows with equal ORDER BY
 * values receive the same rank, and the next rank is the row number (leaving gaps).
 *
 * <p>Example: values [10, 20, 20, 30] produce ranks [1, 2, 2, 4].
 */
public class RankFunction implements WindowFunction {

  @Override
  public Block processPartition(
      PartitionData partition, int partitionSize, int[] orderKeyChannels) {
    long[] values = new long[partitionSize];
    if (partitionSize == 0) {
      return new LongArrayBlock(0, Optional.empty(), values);
    }

    values[0] = 1;
    for (int i = 1; i < partitionSize; i++) {
      if (partition.rowsEqual(i, i - 1, orderKeyChannels)) {
        values[i] = values[i - 1];
      } else {
        values[i] = i + 1; // rank = row number (1-based)
      }
    }
    return new LongArrayBlock(partitionSize, Optional.empty(), values);
  }

  @Override
  public String getName() {
    return "RANK";
  }
}
