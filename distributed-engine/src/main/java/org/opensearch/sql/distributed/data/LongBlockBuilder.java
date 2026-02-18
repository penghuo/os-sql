/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.distributed.data;

import java.util.Arrays;
import java.util.Optional;

/** Builder for creating LongArrayBlock instances incrementally. */
public class LongBlockBuilder implements BlockBuilder {

  private static final int INITIAL_CAPACITY = 64;

  private long[] values;
  private boolean[] valueIsNull;
  private int positionCount;
  private boolean hasNull;

  public LongBlockBuilder() {
    this(INITIAL_CAPACITY);
  }

  public LongBlockBuilder(int expectedEntries) {
    values = new long[expectedEntries];
    valueIsNull = new boolean[expectedEntries];
  }

  public LongBlockBuilder appendLong(long value) {
    ensureCapacity();
    values[positionCount] = value;
    valueIsNull[positionCount] = false;
    positionCount++;
    return this;
  }

  @Override
  public LongBlockBuilder appendNull() {
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
    return (long) Long.BYTES * positionCount + (hasNull ? positionCount : 0);
  }

  @Override
  public LongArrayBlock build() {
    return new LongArrayBlock(
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
