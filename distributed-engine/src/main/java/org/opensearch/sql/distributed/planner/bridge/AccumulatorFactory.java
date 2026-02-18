/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.distributed.planner.bridge;

import java.util.function.Supplier;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.sql.SqlKind;
import org.opensearch.sql.distributed.operator.aggregation.Accumulator;
import org.opensearch.sql.distributed.operator.aggregation.AvgAccumulator;
import org.opensearch.sql.distributed.operator.aggregation.CountAccumulator;
import org.opensearch.sql.distributed.operator.aggregation.MaxAccumulator;
import org.opensearch.sql.distributed.operator.aggregation.MinAccumulator;
import org.opensearch.sql.distributed.operator.aggregation.SumAccumulator;

/**
 * Maps Calcite {@link AggregateCall} to distributed engine {@link Accumulator} instances. Phase 1
 * supports COUNT, SUM, AVG, MIN, MAX.
 */
public final class AccumulatorFactory {

  private AccumulatorFactory() {}

  /**
   * Creates an Accumulator supplier for the given AggregateCall.
   *
   * @param aggCall the Calcite aggregate call
   * @return a Supplier that creates fresh Accumulator instances
   * @throws UnsupportedOperationException for unsupported aggregate functions
   */
  public static Supplier<Accumulator> createAccumulatorSupplier(AggregateCall aggCall) {
    SqlKind kind = aggCall.getAggregation().getKind();
    switch (kind) {
      case COUNT:
        // COUNT(*) has empty argList, COUNT(column) has one arg
        boolean countAll = aggCall.getArgList().isEmpty();
        return () -> new CountAccumulator(countAll);
      case SUM:
      case SUM0:
        return SumAccumulator::new;
      case AVG:
        return AvgAccumulator::new;
      case MIN:
        return MinAccumulator::new;
      case MAX:
        return MaxAccumulator::new;
      default:
        throw new UnsupportedOperationException(
            "Unsupported aggregate function: "
                + aggCall.getAggregation().getName()
                + " (kind: "
                + kind
                + ")");
    }
  }

  /**
   * Resolves the input channel index for an AggregateCall.
   *
   * @param aggCall the Calcite aggregate call
   * @return the input channel index, or -1 for COUNT(*)
   */
  public static int resolveInputChannel(AggregateCall aggCall) {
    if (aggCall.getArgList().isEmpty()) {
      return -1; // COUNT(*)
    }
    return aggCall.getArgList().get(0);
  }
}
