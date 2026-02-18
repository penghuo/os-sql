/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.distributed.data;

import org.opensearch.sql.distributed.lucene.ColumnMapping;

/**
 * Builder for constructing {@link Block} instances incrementally. Each BlockBuilder is specialized
 * for a particular data type (long, int, double, variable-width, etc.).
 *
 * <p>The builder accumulates values via type-specific append methods, then produces a final
 * immutable Block via {@link #build()}.
 */
public interface BlockBuilder {

  /** Appends a null value at the next position. */
  BlockBuilder appendNull();

  /** Appends a long value. Only valid for LONG block builders. */
  default BlockBuilder appendLong(long value) {
    throw new UnsupportedOperationException("appendLong not supported by " + getClass().getName());
  }

  /** Appends an int value. Only valid for INT block builders. */
  default BlockBuilder appendInt(int value) {
    throw new UnsupportedOperationException("appendInt not supported by " + getClass().getName());
  }

  /** Appends a short value. Only valid for SHORT block builders. */
  default BlockBuilder appendShort(short value) {
    throw new UnsupportedOperationException("appendShort not supported by " + getClass().getName());
  }

  /** Appends a byte value. Only valid for BYTE block builders. */
  default BlockBuilder appendByte(byte value) {
    throw new UnsupportedOperationException("appendByte not supported by " + getClass().getName());
  }

  /** Appends a double value. Only valid for DOUBLE block builders. */
  default BlockBuilder appendDouble(double value) {
    throw new UnsupportedOperationException(
        "appendDouble not supported by " + getClass().getName());
  }

  /** Appends a float value (stored as double). Only valid for DOUBLE block builders. */
  default BlockBuilder appendFloat(float value) {
    return appendDouble(value);
  }

  /** Appends a boolean value. Only valid for BOOLEAN block builders. */
  default BlockBuilder appendBoolean(boolean value) {
    throw new UnsupportedOperationException(
        "appendBoolean not supported by " + getClass().getName());
  }

  /** Appends raw bytes. Only valid for VARIABLE_WIDTH block builders. */
  default BlockBuilder appendBytes(byte[] bytes) {
    return appendBytes(bytes, 0, bytes.length);
  }

  /** Appends raw bytes with offset and length. Only valid for VARIABLE_WIDTH block builders. */
  default BlockBuilder appendBytes(byte[] bytes, int offset, int length) {
    throw new UnsupportedOperationException("appendBytes not supported by " + getClass().getName());
  }

  /** Returns the current number of positions appended. */
  int getPositionCount();

  /** Returns the approximate size in bytes of the data accumulated so far. */
  long getSizeInBytes();

  /** Builds and returns the immutable Block from accumulated values. */
  Block build();

  /**
   * Creates a BlockBuilder appropriate for the given block type and expected capacity.
   *
   * @param blockType the target block type from ColumnMapping
   * @param expectedSize expected number of positions (hint for initial capacity)
   * @return a new BlockBuilder instance
   */
  static BlockBuilder create(ColumnMapping.BlockType blockType, int expectedSize) {
    return switch (blockType) {
      case LONG -> new LongBlockBuilder(expectedSize);
      case INT -> new IntBlockBuilder(expectedSize);
      case SHORT -> new ShortBlockBuilder(expectedSize);
      case BYTE -> new ByteBlockBuilder(expectedSize);
      case DOUBLE, FLOAT -> new DoubleBlockBuilder(expectedSize);
      case BOOLEAN -> new BooleanBlockBuilder(expectedSize);
      case VARIABLE_WIDTH -> new VariableWidthBlockBuilder(expectedSize);
    };
  }
}
