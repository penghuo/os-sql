/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.distributed.operator;

import java.util.Objects;
import org.opensearch.sql.distributed.data.Block;
import org.opensearch.sql.distributed.data.Page;
import org.opensearch.sql.distributed.data.ValueBlock;

/**
 * A PageProjection that simply selects a column from the input page. The most common projection
 * type.
 */
public class ColumnProjection implements PageProjection {

  private final int channel;

  public ColumnProjection(int channel) {
    if (channel < 0) {
      throw new IllegalArgumentException("channel must be non-negative");
    }
    this.channel = channel;
  }

  @Override
  public Block project(Page page, boolean[] selectedPositions) {
    Objects.requireNonNull(page, "page is null");
    Block block = page.getBlock(channel);

    // If all positions are selected, return the block as-is
    if (allSelected(selectedPositions, page.getPositionCount())) {
      return block;
    }

    // Count selected positions
    int selectedCount = 0;
    for (boolean selected : selectedPositions) {
      if (selected) {
        selectedCount++;
      }
    }

    // Build position array for copyPositions
    int[] positions = new int[selectedCount];
    int idx = 0;
    for (int i = 0; i < selectedPositions.length; i++) {
      if (selectedPositions[i]) {
        positions[idx++] = i;
      }
    }

    // Use copyPositions if it's a ValueBlock, otherwise use getRegion approach
    if (block instanceof ValueBlock valueBlock) {
      return valueBlock.copyPositions(positions, 0, selectedCount);
    }

    // For DictionaryBlock/RLE, fall back to building a new block via single values
    // This is a simplified approach; production code would optimize further
    return copyBlockPositions(block, positions, selectedCount);
  }

  private static boolean allSelected(boolean[] selected, int positionCount) {
    if (selected.length != positionCount) {
      return false;
    }
    for (boolean s : selected) {
      if (!s) {
        return false;
      }
    }
    return true;
  }

  private static Block copyBlockPositions(Block block, int[] positions, int count) {
    // Get the single value blocks and reconstruct
    // For Dictionary/RLE blocks, extract the underlying value blocks
    if (block instanceof org.opensearch.sql.distributed.data.DictionaryBlock dict) {
      ValueBlock dictionary = dict.getDictionary();
      int[] newIds = new int[count];
      for (int i = 0; i < count; i++) {
        newIds[i] = dict.getId(positions[i]);
      }
      return dictionary.copyPositions(newIds, 0, count);
    }
    if (block instanceof org.opensearch.sql.distributed.data.RunLengthEncodedBlock rle) {
      return new org.opensearch.sql.distributed.data.RunLengthEncodedBlock(rle.getValue(), count);
    }
    throw new UnsupportedOperationException(
        "Cannot copy positions for block type: " + block.getClass().getSimpleName());
  }

  public int getChannel() {
    return channel;
  }
}
