/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.distributed.operator.window;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import org.opensearch.sql.distributed.data.Block;
import org.opensearch.sql.distributed.data.BooleanArrayBlock;
import org.opensearch.sql.distributed.data.ByteArrayBlock;
import org.opensearch.sql.distributed.data.DictionaryBlock;
import org.opensearch.sql.distributed.data.DoubleArrayBlock;
import org.opensearch.sql.distributed.data.IntArrayBlock;
import org.opensearch.sql.distributed.data.LongArrayBlock;
import org.opensearch.sql.distributed.data.Page;
import org.opensearch.sql.distributed.data.RunLengthEncodedBlock;
import org.opensearch.sql.distributed.data.ShortArrayBlock;
import org.opensearch.sql.distributed.data.VariableWidthBlock;

/**
 * Holds all rows for a single window partition, flattened from multiple pages. Provides access to
 * values at any (row, channel) position within the partition.
 */
public class PartitionData {

  private final List<Page> pages;
  private final int[] pageIndices;
  private final int[] positionsInPage;
  private final int size;

  private PartitionData(List<Page> pages, int[] pageIndices, int[] positionsInPage, int size) {
    this.pages = pages;
    this.pageIndices = pageIndices;
    this.positionsInPage = positionsInPage;
    this.size = size;
  }

  /** Creates a PartitionData from a list of pages belonging to the same partition. */
  public static PartitionData fromPages(List<Page> pages) {
    Objects.requireNonNull(pages, "pages is null");
    int totalSize = 0;
    for (Page page : pages) {
      totalSize += page.getPositionCount();
    }

    int[] pageIdxArr = new int[totalSize];
    int[] posInPageArr = new int[totalSize];
    int flatIdx = 0;
    for (int pIdx = 0; pIdx < pages.size(); pIdx++) {
      int positions = pages.get(pIdx).getPositionCount();
      for (int pos = 0; pos < positions; pos++) {
        pageIdxArr[flatIdx] = pIdx;
        posInPageArr[flatIdx] = pos;
        flatIdx++;
      }
    }

    return new PartitionData(new ArrayList<>(pages), pageIdxArr, posInPageArr, totalSize);
  }

  /** Returns the number of rows in this partition. */
  public int getSize() {
    return size;
  }

  /** Returns the block for the given channel at the given flat row index. */
  public Block getBlock(int rowIndex, int channel) {
    return pages.get(pageIndices[rowIndex]).getBlock(channel);
  }

  /** Returns the position within the block's page for the given flat row index. */
  public int getPositionInPage(int rowIndex) {
    return positionsInPage[rowIndex];
  }

  /** Returns true if the value at (rowIndex, channel) is null. */
  public boolean isNull(int rowIndex, int channel) {
    Block block = getBlock(rowIndex, channel);
    return block.isNull(positionsInPage[rowIndex]);
  }

  /** Extracts the value at (rowIndex, channel) as an Object. Supports all block types. */
  public Object getValue(int rowIndex, int channel) {
    Block block = getBlock(rowIndex, channel);
    int pos = positionsInPage[rowIndex];

    if (block.isNull(pos)) {
      return null;
    }

    Block resolved = resolveValueBlock(block);
    int resolvedPos = resolvePosition(block, pos);

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
        "Unsupported block type: " + resolved.getClass().getSimpleName());
  }

  /**
   * Returns true if two rows have equal values across the given channels. Used for ORDER BY
   * comparison in ranking functions.
   */
  public boolean rowsEqual(int row1, int row2, int[] channels) {
    for (int channel : channels) {
      Object v1 = getValue(row1, channel);
      Object v2 = getValue(row2, channel);
      if (v1 == null && v2 == null) continue;
      if (v1 == null || v2 == null) return false;
      if (v1 instanceof byte[] b1 && v2 instanceof byte[] b2) {
        if (!Arrays.equals(b1, b2)) return false;
      } else if (!v1.equals(v2)) {
        return false;
      }
    }
    return true;
  }

  /** Returns the source pages. */
  public List<Page> getPages() {
    return pages;
  }

  /** Returns the number of channels. */
  public int getChannelCount() {
    return pages.isEmpty() ? 0 : pages.get(0).getChannelCount();
  }

  private static Block resolveValueBlock(Block block) {
    if (block instanceof DictionaryBlock dict) {
      return dict.getDictionary();
    }
    if (block instanceof RunLengthEncodedBlock rle) {
      return rle.getValue();
    }
    return block;
  }

  private static int resolvePosition(Block block, int position) {
    if (block instanceof DictionaryBlock dict) {
      return dict.getId(position);
    }
    if (block instanceof RunLengthEncodedBlock) {
      return 0;
    }
    return position;
  }
}
