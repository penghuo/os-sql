/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.distributed.operator.aggregation;

import java.util.Arrays;
import org.opensearch.sql.distributed.data.Block;

/**
 * MAX accumulator. Tracks the maximum value per group. Returns null for groups with no input. Works
 * with numeric types (produces DOUBLE).
 */
public class MaxAccumulator implements Accumulator {

  private double[] maxes;
  private boolean[] hasValue;

  public MaxAccumulator() {
    this.maxes = new double[64];
    this.hasValue = new boolean[64];
    Arrays.fill(maxes, Double.NEGATIVE_INFINITY);
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
        double value = SumAccumulator.extractDouble(block, i);
        if (!hasValue[groupId] || value > maxes[groupId]) {
          maxes[groupId] = value;
          hasValue[groupId] = true;
        }
      }
    }
  }

  @Override
  public Object getResult(int groupId) {
    return hasValue[groupId] ? maxes[groupId] : null;
  }

  @Override
  public boolean isNull(int groupId) {
    return !hasValue[groupId];
  }

  @Override
  public long getEstimatedSizeInBytes() {
    return (long) Double.BYTES * maxes.length + hasValue.length;
  }

  @Override
  public void ensureCapacity(int groupCount) {
    if (groupCount > maxes.length) {
      int oldLen = maxes.length;
      int newLen = Math.max(maxes.length * 2, groupCount);
      maxes = Arrays.copyOf(maxes, newLen);
      hasValue = Arrays.copyOf(hasValue, newLen);
      Arrays.fill(maxes, oldLen, newLen, Double.NEGATIVE_INFINITY);
    }
  }

  @Override
  public ResultType getResultType() {
    return ResultType.DOUBLE;
  }
}
