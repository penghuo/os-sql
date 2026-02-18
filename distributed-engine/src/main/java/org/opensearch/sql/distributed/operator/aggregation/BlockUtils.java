/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.distributed.operator.aggregation;

import org.opensearch.sql.distributed.data.Block;
import org.opensearch.sql.distributed.data.DictionaryBlock;
import org.opensearch.sql.distributed.data.RunLengthEncodedBlock;

/** Utility methods for working with blocks in aggregation accumulators. */
final class BlockUtils {

  private BlockUtils() {}

  /** Resolves a block to its underlying value block (unwraps Dictionary/RLE). */
  static Block resolveValueBlock(Block block) {
    if (block instanceof DictionaryBlock dict) {
      return dict.getDictionary();
    }
    if (block instanceof RunLengthEncodedBlock rle) {
      return rle.getValue();
    }
    return block;
  }

  /** Resolves the actual position within the underlying value block. */
  static int resolvePosition(Block block, int position) {
    if (block instanceof DictionaryBlock dict) {
      return dict.getId(position);
    }
    if (block instanceof RunLengthEncodedBlock) {
      return 0;
    }
    return position;
  }
}
