/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.distributed.operator.aggregation;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
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
 * Simplified GroupByHash for Phase 1. Uses a HashMap with composite key (GroupKey) instead of
 * Trino's bytecode-generated flat hash strategy. No spill support.
 *
 * <p>Maps each unique combination of group key values to a sequential groupId (0, 1, 2, ...).
 */
public class GroupByHash {

  private final int[] groupByChannels;
  private final Map<GroupKey, Integer> keyToGroupId;
  private int nextGroupId;

  /** Stores group key values for each groupId (for output reconstruction). */
  private final List<List<Object>> groupKeyValues;

  public GroupByHash(int[] groupByChannels, int expectedGroups) {
    this.groupByChannels = Objects.requireNonNull(groupByChannels, "groupByChannels is null");
    this.keyToGroupId = new HashMap<>(expectedGroups);
    this.nextGroupId = 0;
    this.groupKeyValues = new java.util.ArrayList<>();
  }

  /**
   * Assigns groupIds to each position in the page.
   *
   * @param page the input page
   * @return array of groupIds, one per position
   */
  public int[] getGroupIds(Page page) {
    int positionCount = page.getPositionCount();
    int[] groupIds = new int[positionCount];

    for (int position = 0; position < positionCount; position++) {
      GroupKey key = extractKey(page, position);
      Integer groupId = keyToGroupId.get(key);
      if (groupId == null) {
        groupId = nextGroupId++;
        keyToGroupId.put(key, groupId);
        groupKeyValues.add(key.values);
      }
      groupIds[position] = groupId;
    }
    return groupIds;
  }

  /** Returns the number of distinct groups. */
  public int getGroupCount() {
    return nextGroupId;
  }

  /** Returns the estimated memory usage in bytes. */
  public long getEstimatedSizeInBytes() {
    // Rough estimate
    return (long) keyToGroupId.size() * 100L;
  }

  /** Returns the key values for the given groupId. Used when building the output page. */
  public List<Object> getGroupKeyValues(int groupId) {
    return groupKeyValues.get(groupId);
  }

  /** Returns the group-by channel indices. */
  public int[] getGroupByChannels() {
    return groupByChannels;
  }

  /**
   * Builds output blocks for the group key columns. One block per group-by channel, with one
   * position per group.
   */
  public Block[] buildGroupKeyBlocks(Page samplePage) {
    int groupCount = getGroupCount();
    Block[] keyBlocks = new Block[groupByChannels.length];

    for (int col = 0; col < groupByChannels.length; col++) {
      Block sampleBlock = BlockUtils.resolveValueBlock(samplePage.getBlock(groupByChannels[col]));
      keyBlocks[col] = buildKeyBlock(col, groupCount, sampleBlock);
    }
    return keyBlocks;
  }

  private Block buildKeyBlock(int keyIndex, int groupCount, Block sampleBlock) {
    if (sampleBlock instanceof LongArrayBlock) {
      long[] values = new long[groupCount];
      boolean[] nulls = null;
      boolean hasNull = false;
      for (int g = 0; g < groupCount; g++) {
        Object val = groupKeyValues.get(g).get(keyIndex);
        if (val == null) {
          if (nulls == null) nulls = new boolean[groupCount];
          nulls[g] = true;
          hasNull = true;
        } else {
          values[g] = ((Number) val).longValue();
        }
      }
      return new LongArrayBlock(
          groupCount, hasNull ? Optional.of(nulls) : Optional.empty(), values);
    }
    if (sampleBlock instanceof DoubleArrayBlock) {
      double[] values = new double[groupCount];
      boolean[] nulls = null;
      boolean hasNull = false;
      for (int g = 0; g < groupCount; g++) {
        Object val = groupKeyValues.get(g).get(keyIndex);
        if (val == null) {
          if (nulls == null) nulls = new boolean[groupCount];
          nulls[g] = true;
          hasNull = true;
        } else {
          values[g] = ((Number) val).doubleValue();
        }
      }
      return new DoubleArrayBlock(
          groupCount, hasNull ? Optional.of(nulls) : Optional.empty(), values);
    }
    if (sampleBlock instanceof IntArrayBlock) {
      int[] values = new int[groupCount];
      boolean[] nulls = null;
      boolean hasNull = false;
      for (int g = 0; g < groupCount; g++) {
        Object val = groupKeyValues.get(g).get(keyIndex);
        if (val == null) {
          if (nulls == null) nulls = new boolean[groupCount];
          nulls[g] = true;
          hasNull = true;
        } else {
          values[g] = ((Number) val).intValue();
        }
      }
      return new IntArrayBlock(groupCount, hasNull ? Optional.of(nulls) : Optional.empty(), values);
    }
    if (sampleBlock instanceof BooleanArrayBlock) {
      boolean[] values = new boolean[groupCount];
      boolean[] nulls = null;
      boolean hasNull = false;
      for (int g = 0; g < groupCount; g++) {
        Object val = groupKeyValues.get(g).get(keyIndex);
        if (val == null) {
          if (nulls == null) nulls = new boolean[groupCount];
          nulls[g] = true;
          hasNull = true;
        } else {
          values[g] = (Boolean) val;
        }
      }
      return new BooleanArrayBlock(
          groupCount, hasNull ? Optional.of(nulls) : Optional.empty(), values);
    }
    if (sampleBlock instanceof VariableWidthBlock) {
      return buildVariableWidthKeyBlock(keyIndex, groupCount);
    }
    throw new UnsupportedOperationException(
        "Unsupported key block type: " + sampleBlock.getClass().getSimpleName());
  }

  private VariableWidthBlock buildVariableWidthKeyBlock(int keyIndex, int groupCount) {
    int totalBytes = 0;
    boolean hasNull = false;
    for (int g = 0; g < groupCount; g++) {
      Object val = groupKeyValues.get(g).get(keyIndex);
      if (val == null) {
        hasNull = true;
      } else {
        totalBytes += ((byte[]) val).length;
      }
    }
    byte[] slice = new byte[totalBytes];
    int[] offsets = new int[groupCount + 1];
    boolean[] nulls = hasNull ? new boolean[groupCount] : null;
    int byteOffset = 0;
    for (int g = 0; g < groupCount; g++) {
      Object val = groupKeyValues.get(g).get(keyIndex);
      if (val == null) {
        nulls[g] = true;
        offsets[g + 1] = byteOffset;
      } else {
        byte[] data = (byte[]) val;
        System.arraycopy(data, 0, slice, byteOffset, data.length);
        byteOffset += data.length;
        offsets[g + 1] = byteOffset;
      }
    }
    return new VariableWidthBlock(
        groupCount, slice, offsets, hasNull ? Optional.of(nulls) : Optional.empty());
  }

  private GroupKey extractKey(Page page, int position) {
    List<Object> values = new java.util.ArrayList<>(groupByChannels.length);
    for (int channel : groupByChannels) {
      Block block = page.getBlock(channel);
      if (block.isNull(position)) {
        values.add(null);
      } else {
        values.add(extractValue(block, position));
      }
    }
    return new GroupKey(values);
  }

  private static Object extractValue(Block block, int position) {
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
      return varBlock.getSlice(resolvedPos);
    }
    throw new UnsupportedOperationException(
        "Unsupported block type for group key: " + resolved.getClass().getSimpleName());
  }

  /** Composite key for grouping. Uses value-based equality. */
  private static final class GroupKey {
    final List<Object> values;
    private final int hash;

    GroupKey(List<Object> values) {
      this.values = values;
      this.hash = computeHash(values);
    }

    private static int computeHash(List<Object> values) {
      int h = 1;
      for (Object value : values) {
        if (value == null) {
          h = 31 * h;
        } else if (value instanceof byte[] bytes) {
          h = 31 * h + Arrays.hashCode(bytes);
        } else {
          h = 31 * h + value.hashCode();
        }
      }
      return h;
    }

    @Override
    public boolean equals(Object other) {
      if (this == other) return true;
      if (!(other instanceof GroupKey that)) return false;
      if (values.size() != that.values.size()) return false;
      for (int i = 0; i < values.size(); i++) {
        Object a = values.get(i);
        Object b = that.values.get(i);
        if (a == null && b == null) continue;
        if (a == null || b == null) return false;
        if (a instanceof byte[] aBytes && b instanceof byte[] bBytes) {
          if (!Arrays.equals(aBytes, bBytes)) return false;
        } else if (!a.equals(b)) {
          return false;
        }
      }
      return true;
    }

    @Override
    public int hashCode() {
      return hash;
    }
  }
}
