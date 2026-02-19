/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.distributed.operator.aggregation;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.opensearch.sql.distributed.data.Block;
import org.opensearch.sql.distributed.data.BooleanArrayBlock;
import org.opensearch.sql.distributed.data.ByteArrayBlock;
import org.opensearch.sql.distributed.data.DoubleArrayBlock;
import org.opensearch.sql.distributed.data.IntArrayBlock;
import org.opensearch.sql.distributed.data.LongArrayBlock;
import org.opensearch.sql.distributed.data.ShortArrayBlock;
import org.opensearch.sql.distributed.data.VariableWidthBlock;

/**
 * COUNT(DISTINCT column) accumulator. Tracks unique non-null values per group using a Set. Returns
 * the number of distinct non-null values.
 */
public class DistinctCountAccumulator implements Accumulator {

  private List<Set<Object>> distinctSets;

  public DistinctCountAccumulator() {
    this.distinctSets = new ArrayList<>();
    for (int i = 0; i < 64; i++) {
      distinctSets.add(new HashSet<>());
    }
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
        Object value = extractComparableValue(block, i);
        distinctSets.get(groupId).add(value);
      }
    }
  }

  @Override
  public Object getResult(int groupId) {
    return (long) distinctSets.get(groupId).size();
  }

  @Override
  public boolean isNull(int groupId) {
    return false; // COUNT DISTINCT never returns null, always returns 0+
  }

  @Override
  public long getEstimatedSizeInBytes() {
    long size = 0;
    for (Set<Object> set : distinctSets) {
      size += (long) set.size() * 32;
    }
    return size;
  }

  @Override
  public void ensureCapacity(int groupCount) {
    while (distinctSets.size() < groupCount) {
      distinctSets.add(new HashSet<>());
    }
  }

  @Override
  public ResultType getResultType() {
    return ResultType.LONG;
  }

  private static Object extractComparableValue(Block block, int position) {
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
    if (resolved instanceof BooleanArrayBlock boolBlock) {
      return boolBlock.getBoolean(resolvedPos);
    }
    if (resolved instanceof VariableWidthBlock varBlock) {
      return varBlock.getString(resolvedPos);
    }
    throw new UnsupportedOperationException(
        "Cannot extract comparable value from: " + resolved.getClass().getSimpleName());
  }
}
