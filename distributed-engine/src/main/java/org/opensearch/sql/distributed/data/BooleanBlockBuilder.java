/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.distributed.data;

import java.util.Arrays;
import java.util.Optional;

/** Builder for creating BooleanArrayBlock instances incrementally. */
public class BooleanBlockBuilder implements BlockBuilder {

  private static final int INITIAL_CAPACITY = 64;

  private boolean[] values;
  private boolean[] valueIsNull;
  private int positionCount;
  private boolean hasNull;

  public BooleanBlockBuilder() {
    this(INITIAL_CAPACITY);
  }

  public BooleanBlockBuilder(int expectedEntries) {
    values = new boolean[expectedEntries];
    valueIsNull = new boolean[expectedEntries];
  }

  public BooleanBlockBuilder appendBoolean(boolean value) {
    ensureCapacity();
    values[positionCount] = value;
    valueIsNull[positionCount] = false;
    positionCount++;
    return this;
  }

  @Override
  public BooleanBlockBuilder appendNull() {
    ensureCapacity();
    values[positionCount] = false;
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
  public BooleanArrayBlock build() {
    return new BooleanArrayBlock(
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
