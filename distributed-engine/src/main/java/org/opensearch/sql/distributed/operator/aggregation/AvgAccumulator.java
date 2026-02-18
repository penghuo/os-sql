/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.distributed.operator.aggregation;

import java.util.Arrays;
import org.opensearch.sql.distributed.data.Block;

/** AVG accumulator. Tracks sum and count per group. Returns null for groups with no input. */
public class AvgAccumulator implements Accumulator {

  private double[] sums;
  private long[] counts;

  public AvgAccumulator() {
    this.sums = new double[64];
    this.counts = new long[64];
  }

  @Override
  public void addInput(int[] groupIds, int groupCount, Block block) {
    ensureCapacity(groupCount);
    if (block == null) {
      return;
    }
    for (int i = 0; i < groupIds.length; i++) {
      if (!block.isNull(i)) {
        int groupId = groupIds[i];
        sums[groupId] += SumAccumulator.extractDouble(block, i);
        counts[groupId]++;
      }
    }
  }

  @Override
  public Object getResult(int groupId) {
    if (counts[groupId] == 0) {
      return null;
    }
    return sums[groupId] / counts[groupId];
  }

  @Override
  public boolean isNull(int groupId) {
    return counts[groupId] == 0;
  }

  @Override
  public long getEstimatedSizeInBytes() {
    return (long) Double.BYTES * sums.length + (long) Long.BYTES * counts.length;
  }

  @Override
  public void ensureCapacity(int groupCount) {
    if (groupCount > sums.length) {
      int newLen = Math.max(sums.length * 2, groupCount);
      sums = Arrays.copyOf(sums, newLen);
      counts = Arrays.copyOf(counts, newLen);
    }
  }

  @Override
  public ResultType getResultType() {
    return ResultType.DOUBLE;
  }
}
