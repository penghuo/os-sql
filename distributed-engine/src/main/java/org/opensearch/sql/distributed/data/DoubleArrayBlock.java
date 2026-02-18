/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.distributed.data;

import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

/** A block backed by a double array. Used for DOUBLE/FLOAT types. */
public final class DoubleArrayBlock implements ValueBlock {

  private static final int INSTANCE_SIZE = 64;

  private final int arrayOffset;
  private final int positionCount;
  private final boolean[] valueIsNull;
  private final double[] values;

  public DoubleArrayBlock(int positionCount, Optional<boolean[]> valueIsNull, double[] values) {
    this(0, positionCount, valueIsNull.orElse(null), values);
  }

  DoubleArrayBlock(int arrayOffset, int positionCount, boolean[] valueIsNull, double[] values) {
    if (positionCount < 0) {
      throw new IllegalArgumentException("positionCount is negative");
    }
    this.arrayOffset = arrayOffset;
    this.positionCount = positionCount;
    this.valueIsNull = valueIsNull;
    this.values = Objects.requireNonNull(values, "values is null");
    if (arrayOffset + positionCount > values.length) {
      throw new IllegalArgumentException("values length is less than arrayOffset + positionCount");
    }
    if (valueIsNull != null && arrayOffset + positionCount > valueIsNull.length) {
      throw new IllegalArgumentException(
          "valueIsNull length is less than arrayOffset + positionCount");
    }
  }

  public double getDouble(int position) {
    Objects.checkIndex(position, positionCount);
    return values[arrayOffset + position];
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
    return (long) Double.BYTES * positionCount + (valueIsNull != null ? positionCount : 0);
  }

  @Override
  public long getRetainedSizeInBytes() {
    return INSTANCE_SIZE
        + (long) Double.BYTES * values.length
        + (valueIsNull != null ? (long) valueIsNull.length : 0);
  }

  @Override
  public DoubleArrayBlock getRegion(int positionOffset, int length) {
    Objects.checkFromIndexSize(positionOffset, length, positionCount);
    return new DoubleArrayBlock(arrayOffset + positionOffset, length, valueIsNull, values);
  }

  @Override
  public DoubleArrayBlock getSingleValueBlock(int position) {
    Objects.checkIndex(position, positionCount);
    boolean[] newNulls = isNull(position) ? new boolean[] {true} : null;
    return new DoubleArrayBlock(0, 1, newNulls, new double[] {values[arrayOffset + position]});
  }

  @Override
  public DoubleArrayBlock copyPositions(int[] positions, int offset, int length) {
    double[] newValues = new double[length];
    boolean[] newNulls = valueIsNull != null ? new boolean[length] : null;
    for (int i = 0; i < length; i++) {
      int pos = positions[offset + i];
      Objects.checkIndex(pos, positionCount);
      newValues[i] = values[arrayOffset + pos];
      if (newNulls != null) {
        newNulls[i] = valueIsNull[arrayOffset + pos];
      }
    }
    return new DoubleArrayBlock(0, length, newNulls, newValues);
  }

  @Override
  public List<Block> getChildren() {
    return Collections.emptyList();
  }

  public double[] getRawValues() {
    return values;
  }

  public int getArrayOffset() {
    return arrayOffset;
  }
}
