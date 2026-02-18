/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.distributed.data;

/**
 * A Block that directly holds values (as opposed to wrapping another block like DictionaryBlock or
 * RunLengthEncodedBlock). Ported from Trino's io.trino.spi.block.ValueBlock.
 */
public sealed interface ValueBlock extends Block
    permits LongArrayBlock,
        IntArrayBlock,
        DoubleArrayBlock,
        ByteArrayBlock,
        ShortArrayBlock,
        BooleanArrayBlock,
        VariableWidthBlock {

  @Override
  ValueBlock getRegion(int positionOffset, int length);

  @Override
  ValueBlock getSingleValueBlock(int position);

  /** Copies the values at the given positions into a new block. */
  ValueBlock copyPositions(int[] positions, int offset, int length);
}
