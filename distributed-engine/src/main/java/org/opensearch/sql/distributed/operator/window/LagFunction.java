/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.distributed.operator.window;

import java.util.Optional;
import org.opensearch.sql.distributed.data.Block;
import org.opensearch.sql.distributed.data.DoubleArrayBlock;
import org.opensearch.sql.distributed.data.LongArrayBlock;

/**
 * LAG(column, offset) window function. Returns the value of the specified column from a row that is
 * {@code offset} rows before the current row within the partition. Returns null if the offset goes
 * beyond the partition boundary.
 */
public class LagFunction implements WindowFunction {

  private final int valueChannel;
  private final int offset;
  private final boolean outputDouble;

  /**
   * Creates a LAG function.
   *
   * @param valueChannel the column index to read the lagged value from
   * @param offset the number of rows to look back (typically 1)
   * @param outputDouble true if the output should be DOUBLE type, false for LONG
   */
  public LagFunction(int valueChannel, int offset, boolean outputDouble) {
    this.valueChannel = valueChannel;
    this.offset = offset;
    this.outputDouble = outputDouble;
  }

  @Override
  public Block processPartition(
      PartitionData partition, int partitionSize, int[] orderKeyChannels) {
    if (outputDouble) {
      return processDouble(partition, partitionSize);
    }
    return processLong(partition, partitionSize);
  }

  private Block processLong(PartitionData partition, int partitionSize) {
    long[] values = new long[partitionSize];
    boolean[] nulls = new boolean[partitionSize];
    boolean hasNull = false;
    for (int i = 0; i < partitionSize; i++) {
      int sourceRow = i - offset;
      if (sourceRow < 0 || partition.isNull(sourceRow, valueChannel)) {
        nulls[i] = true;
        hasNull = true;
      } else {
        Object val = partition.getValue(sourceRow, valueChannel);
        values[i] = ((Number) val).longValue();
      }
    }
    return new LongArrayBlock(
        partitionSize, hasNull ? Optional.of(nulls) : Optional.empty(), values);
  }

  private Block processDouble(PartitionData partition, int partitionSize) {
    double[] values = new double[partitionSize];
    boolean[] nulls = new boolean[partitionSize];
    boolean hasNull = false;
    for (int i = 0; i < partitionSize; i++) {
      int sourceRow = i - offset;
      if (sourceRow < 0 || partition.isNull(sourceRow, valueChannel)) {
        nulls[i] = true;
        hasNull = true;
      } else {
        Object val = partition.getValue(sourceRow, valueChannel);
        values[i] = ((Number) val).doubleValue();
      }
    }
    return new DoubleArrayBlock(
        partitionSize, hasNull ? Optional.of(nulls) : Optional.empty(), values);
  }

  @Override
  public String getName() {
    return "LAG";
  }
}
