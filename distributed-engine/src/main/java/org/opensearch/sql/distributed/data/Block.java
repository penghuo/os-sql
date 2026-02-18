/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.distributed.data;

import java.util.List;

/**
 * Columnar data block. Ported from Trino's io.trino.spi.block.Block. A Block represents a column
 * (or partial column) of data with a fixed number of positions, each of which may be null.
 */
public sealed interface Block permits ValueBlock, DictionaryBlock, RunLengthEncodedBlock {

  /** Returns the number of positions in this block. */
  int getPositionCount();

  /** Returns true if this block may contain null values. */
  boolean mayHaveNull();

  /** Returns true if the value at the given position is null. */
  boolean isNull(int position);

  /** Returns the approximate size of this block in bytes (data only, no overhead). */
  long getSizeInBytes();

  /** Returns the total retained size in bytes, including object overhead and backing arrays. */
  long getRetainedSizeInBytes();

  /** Returns a block that is a view of a region of this block. Shares underlying data. */
  Block getRegion(int positionOffset, int length);

  /** Returns a new single-position block containing the value at the given position. */
  Block getSingleValueBlock(int position);

  /** Returns child blocks (e.g., dictionary for DictionaryBlock). Empty for value blocks. */
  List<Block> getChildren();
}
