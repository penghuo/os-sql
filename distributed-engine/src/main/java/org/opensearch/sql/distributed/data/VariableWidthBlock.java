/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.distributed.data;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

/**
 * A block for variable-width data (VARCHAR, VARBINARY). Values are stored as contiguous bytes with
 * an offset array. Ported from Trino's io.trino.spi.block.VariableWidthBlock.
 */
public final class VariableWidthBlock implements ValueBlock {

  private static final int INSTANCE_SIZE = 96;

  private final int arrayOffset;
  private final int positionCount;
  private final byte[] slice;
  private final int[] offsets; // length >= arrayOffset + positionCount + 1
  private final boolean[] valueIsNull;

  public VariableWidthBlock(
      int positionCount, byte[] slice, int[] offsets, Optional<boolean[]> valueIsNull) {
    this(0, positionCount, slice, offsets, valueIsNull.orElse(null));
  }

  VariableWidthBlock(
      int arrayOffset, int positionCount, byte[] slice, int[] offsets, boolean[] valueIsNull) {
    if (positionCount < 0) {
      throw new IllegalArgumentException("positionCount is negative");
    }
    this.arrayOffset = arrayOffset;
    this.positionCount = positionCount;
    this.slice = Objects.requireNonNull(slice, "slice is null");
    this.offsets = Objects.requireNonNull(offsets, "offsets is null");
    this.valueIsNull = valueIsNull;
    if (arrayOffset + positionCount + 1 > offsets.length) {
      throw new IllegalArgumentException(
          "offsets length is less than arrayOffset + positionCount + 1");
    }
    if (valueIsNull != null && arrayOffset + positionCount > valueIsNull.length) {
      throw new IllegalArgumentException(
          "valueIsNull length is less than arrayOffset + positionCount");
    }
  }

  /** Returns a copy of the bytes for the value at the given position. */
  public byte[] getSlice(int position) {
    Objects.checkIndex(position, positionCount);
    int start = offsets[arrayOffset + position];
    int end = offsets[arrayOffset + position + 1];
    return Arrays.copyOfRange(slice, start, end);
  }

  /** Returns the length in bytes of the value at the given position. */
  public int getSliceLength(int position) {
    Objects.checkIndex(position, positionCount);
    return offsets[arrayOffset + position + 1] - offsets[arrayOffset + position];
  }

  /** Returns the value at the given position as a UTF-8 string. */
  public String getString(int position) {
    return new String(getSlice(position), StandardCharsets.UTF_8);
  }

  @Override
  public int getPositionCount() {
    return positionCount;
  }

  @Override
  public boolean mayHaveNull() {
    return valueIsNull != null;
  }

  @Override
  public boolean isNull(int position) {
    Objects.checkIndex(position, positionCount);
    return valueIsNull != null && valueIsNull[arrayOffset + position];
  }

  @Override
  public long getSizeInBytes() {
    int dataStart = offsets[arrayOffset];
    int dataEnd = offsets[arrayOffset + positionCount];
    return (dataEnd - dataStart)
        + (long) Integer.BYTES * (positionCount + 1)
        + (valueIsNull != null ? positionCount : 0);
  }

  @Override
  public long getRetainedSizeInBytes() {
    return INSTANCE_SIZE
        + slice.length
        + (long) Integer.BYTES * offsets.length
        + (valueIsNull != null ? (long) valueIsNull.length : 0);
  }

  @Override
  public VariableWidthBlock getRegion(int positionOffset, int length) {
    Objects.checkFromIndexSize(positionOffset, length, positionCount);
    return new VariableWidthBlock(
        arrayOffset + positionOffset, length, slice, offsets, valueIsNull);
  }

  @Override
  public VariableWidthBlock getSingleValueBlock(int position) {
    Objects.checkIndex(position, positionCount);
    if (isNull(position)) {
      return new VariableWidthBlock(0, 1, new byte[0], new int[] {0, 0}, new boolean[] {true});
    }
    byte[] data = getSlice(position);
    return new VariableWidthBlock(0, 1, data, new int[] {0, data.length}, null);
  }

  @Override
  public VariableWidthBlock copyPositions(int[] positions, int offset, int length) {
    int totalBytes = 0;
    for (int i = 0; i < length; i++) {
      int pos = positions[offset + i];
      Objects.checkIndex(pos, positionCount);
      totalBytes += getSliceLength(pos);
    }
    byte[] newSlice = new byte[totalBytes];
    int[] newOffsets = new int[length + 1];
    boolean[] newNulls = valueIsNull != null ? new boolean[length] : null;
    int byteOffset = 0;
    for (int i = 0; i < length; i++) {
      int pos = positions[offset + i];
      int start = offsets[arrayOffset + pos];
      int len = offsets[arrayOffset + pos + 1] - start;
      System.arraycopy(slice, start, newSlice, byteOffset, len);
      newOffsets[i] = byteOffset;
      byteOffset += len;
      if (newNulls != null) {
        newNulls[i] = valueIsNull[arrayOffset + pos];
      }
    }
    newOffsets[length] = byteOffset;
    return new VariableWidthBlock(0, length, newSlice, newOffsets, newNulls);
  }

  @Override
  public List<Block> getChildren() {
    return Collections.emptyList();
  }

  public byte[] getRawSlice() {
    return slice;
  }

  public int[] getRawOffsets() {
    return offsets;
  }

  public int getArrayOffset() {
    return arrayOffset;
  }
}
