/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.distributed.data;

import java.util.Arrays;
import java.util.Optional;

/** Builder for creating ByteArrayBlock instances incrementally. */
public class ByteBlockBuilder implements BlockBuilder {

  private static final int INITIAL_CAPACITY = 64;

  private byte[] values;
  private boolean[] valueIsNull;
  private int positionCount;
  private boolean hasNull;

  public ByteBlockBuilder() {
    this(INITIAL_CAPACITY);
  }

  public ByteBlockBuilder(int expectedEntries) {
    values = new byte[expectedEntries];
    valueIsNull = new boolean[expectedEntries];
  }

  public ByteBlockBuilder appendByte(byte value) {
    ensureCapacity();
    values[positionCount] = value;
    valueIsNull[positionCount] = false;
    positionCount++;
    return this;
  }

  @Override
  public ByteBlockBuilder appendNull() {
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
    return (long) positionCount + (hasNull ? positionCount : 0);
  }

  @Override
  public ByteArrayBlock build() {
    return new ByteArrayBlock(
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
