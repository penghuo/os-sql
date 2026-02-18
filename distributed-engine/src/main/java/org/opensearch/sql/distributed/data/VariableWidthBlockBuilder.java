/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.distributed.data;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Optional;

/** Builder for creating VariableWidthBlock instances incrementally. */
public class VariableWidthBlockBuilder implements BlockBuilder {

  private static final int INITIAL_CAPACITY = 64;
  private static final int INITIAL_SLICE_CAPACITY = 256;

  private byte[] slice;
  private int[] offsets;
  private boolean[] valueIsNull;
  private int positionCount;
  private int sliceOffset;
  private boolean hasNull;

  public VariableWidthBlockBuilder() {
    this(INITIAL_CAPACITY);
  }

  public VariableWidthBlockBuilder(int expectedEntries) {
    slice = new byte[INITIAL_SLICE_CAPACITY];
    offsets = new int[expectedEntries + 1];
    valueIsNull = new boolean[expectedEntries];
    offsets[0] = 0;
  }

  public VariableWidthBlockBuilder appendBytes(byte[] value) {
    return appendBytes(value, 0, value.length);
  }

  public VariableWidthBlockBuilder appendBytes(byte[] value, int offset, int length) {
    ensureSliceCapacity(length);
    ensurePositionCapacity();
    System.arraycopy(value, offset, slice, sliceOffset, length);
    sliceOffset += length;
    valueIsNull[positionCount] = false;
    positionCount++;
    offsets[positionCount] = sliceOffset;
    return this;
  }

  public VariableWidthBlockBuilder appendString(String value) {
    return appendBytes(value.getBytes(StandardCharsets.UTF_8));
  }

  @Override
  public VariableWidthBlockBuilder appendNull() {
    ensurePositionCapacity();
    valueIsNull[positionCount] = true;
    hasNull = true;
    positionCount++;
    offsets[positionCount] = sliceOffset;
    return this;
  }

  @Override
  public int getPositionCount() {
    return positionCount;
  }

  @Override
  public long getSizeInBytes() {
    return sliceOffset + (long) Integer.BYTES * (positionCount + 1) + (hasNull ? positionCount : 0);
  }

  @Override
  public VariableWidthBlock build() {
    return new VariableWidthBlock(
        positionCount,
        Arrays.copyOf(slice, sliceOffset),
        Arrays.copyOf(offsets, positionCount + 1),
        hasNull ? Optional.of(Arrays.copyOf(valueIsNull, positionCount)) : Optional.empty());
  }

  private void ensurePositionCapacity() {
    if (positionCount >= valueIsNull.length) {
      int newCapacity = Math.max(valueIsNull.length * 2, positionCount + 1);
      valueIsNull = Arrays.copyOf(valueIsNull, newCapacity);
      offsets = Arrays.copyOf(offsets, newCapacity + 1);
    }
  }

  private void ensureSliceCapacity(int additionalBytes) {
    if (sliceOffset + additionalBytes > slice.length) {
      int newCapacity = Math.max(slice.length * 2, sliceOffset + additionalBytes);
      slice = Arrays.copyOf(slice, newCapacity);
    }
  }
}
