/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.dqe.function.scalar;

import io.trino.spi.block.Block;

/**
 * Vectorized scalar function contract. Receives input Blocks (one per argument) and the row count,
 * returns an output Block covering all positions.
 */
@FunctionalInterface
public interface ScalarFunctionImplementation {
  Block evaluate(Block[] arguments, int positionCount);
}
