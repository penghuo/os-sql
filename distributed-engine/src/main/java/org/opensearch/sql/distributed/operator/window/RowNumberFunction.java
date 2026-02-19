/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.distributed.operator.window;

import java.util.Optional;
import org.opensearch.sql.distributed.data.Block;
import org.opensearch.sql.distributed.data.LongArrayBlock;

/**
 * ROW_NUMBER() window function. Returns a sequential integer for each row within a partition,
 * starting at 1. The ordering is determined by the ORDER BY clause.
 */
public class RowNumberFunction implements WindowFunction {

  @Override
  public Block processPartition(
      PartitionData partition, int partitionSize, int[] orderKeyChannels) {
    long[] values = new long[partitionSize];
    for (int i = 0; i < partitionSize; i++) {
      values[i] = i + 1;
    }
    return new LongArrayBlock(partitionSize, Optional.empty(), values);
  }

  @Override
  public String getName() {
    return "ROW_NUMBER";
  }
}
