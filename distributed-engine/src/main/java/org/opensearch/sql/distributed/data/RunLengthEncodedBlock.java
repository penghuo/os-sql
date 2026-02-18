/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.distributed.data;

import java.util.List;
import java.util.Objects;

/**
 * A block where all positions have the same value. Stores only a single-position block and a
 * position count. Ported from Trino's io.trino.spi.block.RunLengthEncodedBlock.
 */
public final class RunLengthEncodedBlock implements Block {

  private static final int INSTANCE_SIZE = 32;

  private final Block value;
  private final int positionCount;

  public RunLengthEncodedBlock(Block value, int positionCount) {
    if (value.getPositionCount() != 1) {
      throw new IllegalArgumentException(
          "value block must have exactly 1 position, has " + value.getPositionCount());
    }
    if (positionCount < 0) {
      throw new IllegalArgumentException("positionCount is negative");
    }
    this.value = Objects.requireNonNull(value, "value is null");
    this.positionCount = positionCount;
  }

  /** Returns the single-position block that all positions map to. */
  public Block getValue() {
    return value;
  }

  @Override
  public int getPositionCount() {
    return positionCount;
  }

  @Override
  public boolean mayHaveNull() {
    return value.mayHaveNull();
  }

  @Override
  public boolean isNull(int position) {
    Objects.checkIndex(position, positionCount);
    return value.isNull(0);
  }

  @Override
  public long getSizeInBytes() {
    return value.getSizeInBytes();
  }

  @Override
  public long getRetainedSizeInBytes() {
    return INSTANCE_SIZE + value.getRetainedSizeInBytes();
  }

  @Override
  public RunLengthEncodedBlock getRegion(int positionOffset, int length) {
    Objects.checkFromIndexSize(positionOffset, length, positionCount);
    return new RunLengthEncodedBlock(value, length);
  }

  @Override
  public Block getSingleValueBlock(int position) {
    Objects.checkIndex(position, positionCount);
    return value;
  }

  @Override
  public List<Block> getChildren() {
    return List.of(value);
  }
}
