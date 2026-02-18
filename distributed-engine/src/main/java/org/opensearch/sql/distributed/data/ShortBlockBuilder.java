/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.distributed.data;

import java.util.Arrays;
import java.util.Optional;

/** Builder for creating ShortArrayBlock instances incrementally. */
public class ShortBlockBuilder implements BlockBuilder {

  private static final int INITIAL_CAPACITY = 64;

  private short[] values;
  private boolean[] valueIsNull;
  private int positionCount;
  private boolean hasNull;

  public ShortBlockBuilder() {
    this(INITIAL_CAPACITY);
  }

  public ShortBlockBuilder(int expectedEntries) {
    values = new short[expectedEntries];
    valueIsNull = new boolean[expectedEntries];
  }

  public ShortBlockBuilder appendShort(short value) {
    ensureCapacity();
    values[positionCount] = value;
    valueIsNull[positionCount] = false;
    positionCount++;
    return this;
  }

  @Override
  public ShortBlockBuilder appendNull() {
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
    return (long) Short.BYTES * positionCount + (hasNull ? positionCount : 0);
  }

  @Override
  public ShortArrayBlock build() {
    return new ShortArrayBlock(
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
