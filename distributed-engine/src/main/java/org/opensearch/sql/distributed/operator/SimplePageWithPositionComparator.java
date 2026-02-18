/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.distributed.operator;

import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import org.opensearch.sql.distributed.data.Block;
import org.opensearch.sql.distributed.data.BooleanArrayBlock;
import org.opensearch.sql.distributed.data.ByteArrayBlock;
import org.opensearch.sql.distributed.data.DoubleArrayBlock;
import org.opensearch.sql.distributed.data.IntArrayBlock;
import org.opensearch.sql.distributed.data.LongArrayBlock;
import org.opensearch.sql.distributed.data.Page;
import org.opensearch.sql.distributed.data.ShortArrayBlock;
import org.opensearch.sql.distributed.data.VariableWidthBlock;

/**
 * Compares rows across pages by multiple sort channels. Ported from Trino's
 * io.trino.operator.SimplePageWithPositionComparator, simplified to use reflection-free typed
 * comparison.
 */
public class SimplePageWithPositionComparator implements PageWithPositionComparator {

  private final int[] sortChannels;
  private final SortOrder[] sortOrders;

  public SimplePageWithPositionComparator(List<Integer> sortChannels, List<SortOrder> sortOrders) {
    Objects.requireNonNull(sortChannels, "sortChannels is null");
    Objects.requireNonNull(sortOrders, "sortOrders is null");
    if (sortChannels.size() != sortOrders.size()) {
      throw new IllegalArgumentException("sortChannels and sortOrders must have the same size");
    }
    this.sortChannels = sortChannels.stream().mapToInt(Integer::intValue).toArray();
    this.sortOrders = sortOrders.toArray(new SortOrder[0]);
  }

  @Override
  public int compareTo(Page left, int leftPosition, Page right, int rightPosition) {
    for (int i = 0; i < sortChannels.length; i++) {
      int channel = sortChannels[i];
      SortOrder order = sortOrders[i];

      Block leftBlock = left.getBlock(channel);
      Block rightBlock = right.getBlock(channel);

      boolean leftIsNull = leftBlock.isNull(leftPosition);
      boolean rightIsNull = rightBlock.isNull(rightPosition);

      if (leftIsNull && rightIsNull) {
        continue;
      }
      if (leftIsNull) {
        return order.isNullsFirst() ? -1 : 1;
      }
      if (rightIsNull) {
        return order.isNullsFirst() ? 1 : -1;
      }

      int result = compareValues(leftBlock, leftPosition, rightBlock, rightPosition);
      if (result != 0) {
        return order.isAscending() ? result : -result;
      }
    }
    return 0;
  }

  @SuppressWarnings("unchecked")
  private static int compareValues(
      Block leftBlock, int leftPosition, Block rightBlock, int rightPosition) {
    // Unwrap dictionary/RLE blocks to get to the value block
    Block leftResolved = resolveBlock(leftBlock, leftPosition);
    int leftResolvedPos = resolvePosition(leftBlock, leftPosition);
    Block rightResolved = resolveBlock(rightBlock, rightPosition);
    int rightResolvedPos = resolvePosition(rightBlock, rightPosition);

    if (leftResolved instanceof LongArrayBlock leftLong
        && rightResolved instanceof LongArrayBlock rightLong) {
      return Long.compare(leftLong.getLong(leftResolvedPos), rightLong.getLong(rightResolvedPos));
    }
    if (leftResolved instanceof DoubleArrayBlock leftDouble
        && rightResolved instanceof DoubleArrayBlock rightDouble) {
      return Double.compare(
          leftDouble.getDouble(leftResolvedPos), rightDouble.getDouble(rightResolvedPos));
    }
    if (leftResolved instanceof IntArrayBlock leftInt
        && rightResolved instanceof IntArrayBlock rightInt) {
      return Integer.compare(leftInt.getInt(leftResolvedPos), rightInt.getInt(rightResolvedPos));
    }
    if (leftResolved instanceof ShortArrayBlock leftShort
        && rightResolved instanceof ShortArrayBlock rightShort) {
      return Short.compare(
          leftShort.getShort(leftResolvedPos), rightShort.getShort(rightResolvedPos));
    }
    if (leftResolved instanceof ByteArrayBlock leftByte
        && rightResolved instanceof ByteArrayBlock rightByte) {
      return Byte.compare(leftByte.getByte(leftResolvedPos), rightByte.getByte(rightResolvedPos));
    }
    if (leftResolved instanceof BooleanArrayBlock leftBool
        && rightResolved instanceof BooleanArrayBlock rightBool) {
      return Boolean.compare(
          leftBool.getBoolean(leftResolvedPos), rightBool.getBoolean(rightResolvedPos));
    }
    if (leftResolved instanceof VariableWidthBlock leftVar
        && rightResolved instanceof VariableWidthBlock rightVar) {
      return Arrays.compare(leftVar.getSlice(leftResolvedPos), rightVar.getSlice(rightResolvedPos));
    }
    throw new UnsupportedOperationException(
        "Cannot compare blocks of type: " + leftResolved.getClass().getSimpleName());
  }

  /**
   * Resolves to the underlying value block. For DictionaryBlock/RLE, this follows the indirection.
   */
  private static Block resolveBlock(Block block, int position) {
    if (block instanceof org.opensearch.sql.distributed.data.DictionaryBlock dictBlock) {
      return dictBlock.getDictionary();
    }
    if (block instanceof org.opensearch.sql.distributed.data.RunLengthEncodedBlock rleBlock) {
      return rleBlock.getValue();
    }
    return block;
  }

  /** Resolves the actual position within the underlying value block. */
  private static int resolvePosition(Block block, int position) {
    if (block instanceof org.opensearch.sql.distributed.data.DictionaryBlock dictBlock) {
      return dictBlock.getId(position);
    }
    if (block instanceof org.opensearch.sql.distributed.data.RunLengthEncodedBlock) {
      return 0;
    }
    return position;
  }
}
