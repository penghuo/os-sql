/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.distributed.data;

import java.util.Arrays;
import java.util.Optional;

/** Builder for creating IntArrayBlock instances incrementally. */
public class IntBlockBuilder implements BlockBuilder {

  private static final int INITIAL_CAPACITY = 64;

  private int[] values;
  private boolean[] valueIsNull;
  private int positionCount;
  private boolean hasNull;

  public IntBlockBuilder() {
    this(INITIAL_CAPACITY);
  }

  public IntBlockBuilder(int expectedEntries) {
    values = new int[expectedEntries];
    valueIsNull = new boolean[expectedEntries];
  }

  public IntBlockBuilder appendInt(int value) {
    ensureCapacity();
    values[positionCount] = value;
    valueIsNull[positionCount] = false;
    positionCount++;
    return this;
  }

  @Override
  public IntBlockBuilder appendNull() {
    ensureCapacity();
    values[positionCount] = 0;
    valueIsNull[positionCount] = true;
    hasNull = true;
    positionCount++;
    return this;
  }

  @Override
  public int getPositionCount() {
    return positionCount;
  }

  @Override
  public long getSizeInBytes() {
    return (long) Integer.BYTES * positionCount + (hasNull ? positionCount : 0);
  }

  @Override
  public IntArrayBlock build() {
    return new IntArrayBlock(
        positionCount,
        hasNull ? Optional.of(Arrays.copyOf(valueIsNull, positionCount)) : Optional.empty(),
        Arrays.copyOf(values, positionCount));
  }

  private void ensureCapacity() {
    if (positionCount >= values.length) {
      int newCapacity = Math.max(values.length * 2, positionCount + 1);
      values = Arrays.copyOf(values, newCapacity);
      valueIsNull = Arrays.copyOf(valueIsNull, newCapacity);
    }
  }
}
