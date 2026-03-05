/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.dqe.function.aggregate;

import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;

/** Stateful accumulator that processes Block-level input and produces an aggregate result. */
public interface Accumulator {

  /** Add values from a Block to this accumulator. */
  void addBlock(Block block, int positionCount);

  /** Write the final aggregate result to the given BlockBuilder. */
  void writeFinalTo(BlockBuilder builder);
}
