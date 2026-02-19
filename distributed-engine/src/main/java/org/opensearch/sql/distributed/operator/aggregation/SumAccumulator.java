/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.distributed.operator.aggregation;

import java.util.Arrays;
import org.opensearch.sql.distributed.data.Block;
import org.opensearch.sql.distributed.data.ByteArrayBlock;
import org.opensearch.sql.distributed.data.DoubleArrayBlock;
import org.opensearch.sql.distributed.data.IntArrayBlock;
import org.opensearch.sql.distributed.data.LongArrayBlock;
import org.opensearch.sql.distributed.data.ShortArrayBlock;

/**
 * SUM accumulator. Produces a DOUBLE result to handle mixed numeric types. Returns null for groups
 * with no non-null input.
 */
public class SumAccumulator implements Accumulator {

  private double[] sums;
  private boolean[] hasValue;
  private final boolean returnZeroForEmpty; // SUM0 semantics: return 0 instead of null

  public SumAccumulator() {
    this(false);
  }

  public SumAccumulator(boolean returnZeroForEmpty) {
    this.sums = new double[64];
    this.hasValue = new boolean[64];
    this.returnZeroForEmpty = returnZeroForEmpty;
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
        sums[groupId] += extractDouble(block, i);
        hasValue[groupId] = true;
      }
    }
  }

  @Override
  public Object getResult(int groupId) {
    if (hasValue[groupId]) {
      return sums[groupId];
    }
    // SUM0 returns 0 for groups with no non-null input (Calcite $SUM0 semantics)
    return returnZeroForEmpty ? 0.0 : null;
  }

  @Override
  public boolean isNull(int groupId) {
    return !hasValue[groupId] && !returnZeroForEmpty;
  }

  @Override
  public long getEstimatedSizeInBytes() {
    return (long) Double.BYTES * sums.length + hasValue.length;
  }

  @Override
  public void ensureCapacity(int groupCount) {
    if (groupCount > sums.length) {
      int newLen = Math.max(sums.length * 2, groupCount);
      sums = Arrays.copyOf(sums, newLen);
      hasValue = Arrays.copyOf(hasValue, newLen);
    }
  }

  @Override
  public ResultType getResultType() {
    return ResultType.DOUBLE;
  }

  static double extractDouble(Block block, int position) {
    Block resolved = BlockUtils.resolveValueBlock(block);
    int resolvedPos = BlockUtils.resolvePosition(block, position);
    if (resolved instanceof LongArrayBlock longBlock) {
      return longBlock.getLong(resolvedPos);
    }
    if (resolved instanceof DoubleArrayBlock doubleBlock) {
      return doubleBlock.getDouble(resolvedPos);
    }
    if (resolved instanceof IntArrayBlock intBlock) {
      return intBlock.getInt(resolvedPos);
    }
    if (resolved instanceof ShortArrayBlock shortBlock) {
      return shortBlock.getShort(resolvedPos);
    }
    if (resolved instanceof ByteArrayBlock byteBlock) {
      return byteBlock.getByte(resolvedPos);
    }
    throw new UnsupportedOperationException(
        "Cannot extract numeric value from: " + resolved.getClass().getSimpleName());
  }
}
