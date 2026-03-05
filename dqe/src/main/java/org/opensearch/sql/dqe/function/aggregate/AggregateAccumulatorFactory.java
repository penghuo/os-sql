/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.dqe.function.aggregate;

import io.trino.spi.type.Type;

/** Factory for creating aggregate accumulators. */
public interface AggregateAccumulatorFactory {

  /** Create a new accumulator instance. */
  Accumulator createAccumulator();

  /** The intermediate (partial aggregation) type. */
  Type getIntermediateType();

  /** The final output type. */
  Type getOutputType();
}
