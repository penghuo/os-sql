/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.distributed.operator.aggregation;

import java.util.Arrays;
import org.opensearch.sql.distributed.data.Block;

/**
 * MIN accumulator. Tracks the minimum value per group. Returns null for groups with no input. Works
 * with numeric types (produces DOUBLE).
 */
public class MinAccumulator implements Accumulator {

  private double[] mins;
  private boolean[] hasValue;

  public MinAccumulator() {
    this.mins = new double[64];
    this.hasValue = new boolean[64];
    Arrays.fill(mins, Double.POSITIVE_INFINITY);
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
        if (!hasValue[groupId] || value < mins[groupId]) {
          mins[groupId] = value;
          hasValue[groupId] = true;
        }
      }
    }
  }

  @Override
  public Object getResult(int groupId) {
    return hasValue[groupId] ? mins[groupId] : null;
  }

  @Override
  public boolean isNull(int groupId) {
    return !hasValue[groupId];
  }

  @Override
  public long getEstimatedSizeInBytes() {
    return (long) Double.BYTES * mins.length + hasValue.length;
  }

  @Override
  public void ensureCapacity(int groupCount) {
    if (groupCount > mins.length) {
      int oldLen = mins.length;
      int newLen = Math.max(mins.length * 2, groupCount);
      mins = Arrays.copyOf(mins, newLen);
      hasValue = Arrays.copyOf(hasValue, newLen);
      Arrays.fill(mins, oldLen, newLen, Double.POSITIVE_INFINITY);
    }
  }

  @Override
  public ResultType getResultType() {
    return ResultType.DOUBLE;
  }
}
