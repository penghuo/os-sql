/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.apache.spark.util.collection.unsafe.sort;

import org.apache.spark.unsafe.Platform;
import org.apache.spark.unsafe.array.LongArray;
import org.apache.spark.util.collection.SortDataFormat;

/**
 * Supports sorting an array of (record pointer, key prefix) pairs.
 * Used in {@link UnsafeInMemorySorter}.
 * <p>
 * Within each long[] buffer, position {@code 2 * i} holds a pointer to the record at
 * index {@code i}, while position {@code 2 * i + 1} in the array holds an 8-byte key prefix.
 */
public final class UnsafeSortDataFormat
  extends SortDataFormat<RecordPointerAndKeyPrefix, LongArray> {

  private final LongArray buffer;

  public UnsafeSortDataFormat(LongArray buffer) {
    this.buffer = buffer;
  }

  @Override
  public RecordPointerAndKeyPrefix getKey(LongArray data, int pos) {
    // Since we re-use keys, this method shouldn't be called.
    throw new UnsupportedOperationException();
  }

  @Override
  public RecordPointerAndKeyPrefix newKey() {
    return new RecordPointerAndKeyPrefix();
  }

  @Override
  public RecordPointerAndKeyPrefix getKey(LongArray data, int pos,
                                          RecordPointerAndKeyPrefix reuse) {
    reuse.recordPointer = data.get(pos * 2);
    reuse.keyPrefix = data.get(pos * 2 + 1);
    return reuse;
  }

  @Override
  public void swap(LongArray data, int pos0, int pos1) {
    long tempPointer = data.get(pos0 * 2);
    long tempKeyPrefix = data.get(pos0 * 2 + 1);
    data.set(pos0 * 2, data.get(pos1 * 2));
    data.set(pos0 * 2 + 1, data.get(pos1 * 2 + 1));
    data.set(pos1 * 2, tempPointer);
    data.set(pos1 * 2 + 1, tempKeyPrefix);
  }

  @Override
  public void copyElement(LongArray src, int srcPos, LongArray dst, int dstPos) {
    dst.set(dstPos * 2, src.get(srcPos * 2));
    dst.set(dstPos * 2 + 1, src.get(srcPos * 2 + 1));
  }

  @Override
  public void copyRange(LongArray src, int srcPos, LongArray dst, int dstPos, int length) {
    Platform.copyMemory(
      src.getBaseObject(),
      src.getBaseOffset() + srcPos * 16L,
      dst.getBaseObject(),
      dst.getBaseOffset() + dstPos * 16L,
      length * 16L);
  }

  @Override
  public LongArray allocate(int length) {
    assert (length * 2L <= buffer.size()) :
      "the buffer is smaller than required: " + buffer.size() + " < " + (length * 2);
    return buffer;
  }

}
