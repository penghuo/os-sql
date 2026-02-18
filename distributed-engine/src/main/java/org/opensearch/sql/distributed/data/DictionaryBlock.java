/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.distributed.data;

import java.util.Arrays;
import java.util.List;
import java.util.Objects;

/**
 * A block that stores values as references into a dictionary. Saves memory when many positions
 * share the same value. Ported from Trino's io.trino.spi.block.DictionaryBlock.
 */
public final class DictionaryBlock implements Block {

  private static final int INSTANCE_SIZE = 64;

  private final int positionCount;
  private final ValueBlock dictionary;
  private final int[] ids;

  public DictionaryBlock(int positionCount, ValueBlock dictionary, int[] ids) {
    if (positionCount < 0) {
      throw new IllegalArgumentException("positionCount is negative");
    }
    this.positionCount = positionCount;
    this.dictionary = Objects.requireNonNull(dictionary, "dictionary is null");
    this.ids = Objects.requireNonNull(ids, "ids is null");
    if (ids.length < positionCount) {
      throw new IllegalArgumentException("ids length is less than positionCount");
    }
  }

  public ValueBlock getDictionary() {
    return dictionary;
  }

  public int getId(int position) {
    Objects.checkIndex(position, positionCount);
    return ids[position];
  }

  @Override
  public int getPositionCount() {
    return positionCount;
  }

  @Override
  public boolean mayHaveNull() {
    return dictionary.mayHaveNull();
  }

  @Override
  public boolean isNull(int position) {
    Objects.checkIndex(position, positionCount);
    return dictionary.isNull(ids[position]);
  }

  @Override
  public long getSizeInBytes() {
    return (long) Integer.BYTES * positionCount + dictionary.getSizeInBytes();
  }

  @Override
  public long getRetainedSizeInBytes() {
    return INSTANCE_SIZE + (long) Integer.BYTES * ids.length + dictionary.getRetainedSizeInBytes();
  }

  @Override
  public DictionaryBlock getRegion(int positionOffset, int length) {
    Objects.checkFromIndexSize(positionOffset, length, positionCount);
    int[] newIds = Arrays.copyOfRange(ids, positionOffset, positionOffset + length);
    return new DictionaryBlock(length, dictionary, newIds);
  }

  @Override
  public Block getSingleValueBlock(int position) {
    Objects.checkIndex(position, positionCount);
    return dictionary.getSingleValueBlock(ids[position]);
  }

  @Override
  public List<Block> getChildren() {
    return List.of(dictionary);
  }
}
