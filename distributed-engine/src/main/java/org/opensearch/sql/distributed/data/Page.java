/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.distributed.data;

import java.util.Arrays;
import java.util.Objects;

/**
 * A collection of Blocks, one per column, all with the same number of positions. This is the unit
 * of data exchange between operators. Ported from Trino's io.trino.spi.Page.
 */
public class Page {

  public static final int MAX_PAGE_SIZE_IN_BYTES = 1024 * 1024;

  private final Block[] blocks;
  private final int positionCount;

  public Page(Block... blocks) {
    this(determinePositionCount(blocks), blocks);
  }

  public Page(int positionCount, Block... blocks) {
    if (positionCount < 0) {
      throw new IllegalArgumentException("positionCount is negative");
    }
    this.positionCount = positionCount;
    this.blocks = Objects.requireNonNull(blocks, "blocks is null").clone();
    for (Block block : this.blocks) {
      if (block.getPositionCount() != positionCount) {
        throw new IllegalArgumentException(
            String.format(
                "Block position count (%d) does not match page position count (%d)",
                block.getPositionCount(), positionCount));
      }
    }
  }

  public int getPositionCount() {
    return positionCount;
  }

  public int getChannelCount() {
    return blocks.length;
  }

  public Block getBlock(int channel) {
    return blocks[channel];
  }

  public long getSizeInBytes() {
    long size = 0;
    for (Block block : blocks) {
      size += block.getSizeInBytes();
    }
    return size;
  }

  public long getRetainedSizeInBytes() {
    long size = 0;
    for (Block block : blocks) {
      size += block.getRetainedSizeInBytes();
    }
    return size;
  }

  /** Returns a page that is a view of a region of this page. Shares underlying block data. */
  public Page getRegion(int positionOffset, int length) {
    if (positionOffset < 0 || length < 0 || positionOffset + length > positionCount) {
      throw new IndexOutOfBoundsException(
          String.format(
              "Invalid region: offset=%d, length=%d, positionCount=%d",
              positionOffset, length, positionCount));
    }
    if (length == positionCount) {
      return this;
    }
    Block[] regionBlocks = new Block[blocks.length];
    for (int i = 0; i < blocks.length; i++) {
      regionBlocks[i] = blocks[i].getRegion(positionOffset, length);
    }
    return new Page(length, regionBlocks);
  }

  /** Returns a new page containing only the specified columns. */
  public Page getColumns(int... columns) {
    Block[] selectedBlocks = new Block[columns.length];
    for (int i = 0; i < columns.length; i++) {
      selectedBlocks[i] = blocks[columns[i]];
    }
    return new Page(positionCount, selectedBlocks);
  }

  /** Returns a new page with the given block appended as an additional column. */
  public Page appendColumn(Block block) {
    if (block.getPositionCount() != positionCount) {
      throw new IllegalArgumentException("Block position count does not match page");
    }
    Block[] newBlocks = Arrays.copyOf(blocks, blocks.length + 1);
    newBlocks[blocks.length] = block;
    return new Page(positionCount, newBlocks);
  }

  /** Returns a new page with the given block prepended as the first column. */
  public Page prependColumn(Block block) {
    if (block.getPositionCount() != positionCount) {
      throw new IllegalArgumentException("Block position count does not match page");
    }
    Block[] newBlocks = new Block[blocks.length + 1];
    newBlocks[0] = block;
    System.arraycopy(blocks, 0, newBlocks, 1, blocks.length);
    return new Page(positionCount, newBlocks);
  }

  private static int determinePositionCount(Block[] blocks) {
    if (blocks.length == 0) {
      return 0;
    }
    return blocks[0].getPositionCount();
  }
}
