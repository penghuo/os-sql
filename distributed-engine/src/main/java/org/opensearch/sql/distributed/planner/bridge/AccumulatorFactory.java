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
import org.opensearch.sql.distributed.operator.aggregation.DistinctCountAccumulator;
import org.opensearch.sql.distributed.operator.aggregation.MaxAccumulator;
import org.opensearch.sql.distributed.operator.aggregation.MinAccumulator;
import org.opensearch.sql.distributed.operator.aggregation.SumAccumulator;
import org.opensearch.sql.distributed.planner.AggregationNode.AggregationMode;

/**
 * Maps Calcite {@link AggregateCall} to distributed engine {@link Accumulator} instances. Supports
 * PARTIAL and FINAL aggregation modes for distributed execution:
 *
 * <ul>
 *   <li>SINGLE/PARTIAL: standard accumulators (COUNT counts rows, SUM sums values, etc.)
 *   <li>FINAL: merge accumulators (COUNT becomes SUM to merge partial counts, SUM stays SUM, etc.)
 * </ul>
 */
public final class AccumulatorFactory {

  private AccumulatorFactory() {}

  /**
   * Creates an Accumulator supplier for the given AggregateCall in SINGLE mode.
   *
   * @param aggCall the Calcite aggregate call
   * @return a Supplier that creates fresh Accumulator instances
   */
  public static Supplier<Accumulator> createAccumulatorSupplier(AggregateCall aggCall) {
    return createAccumulatorSupplier(aggCall, AggregationMode.SINGLE);
  }

  /**
   * Creates an Accumulator supplier for the given AggregateCall and aggregation mode.
   *
   * <p>In FINAL mode, the accumulators merge partial results:
   *
   * <ul>
   *   <li>COUNT → SUM (sum up partial counts from leaf nodes)
   *   <li>SUM → SUM (sum up partial sums)
   *   <li>MIN → MIN (min of partial mins)
   *   <li>MAX → MAX (max of partial maxes)
   *   <li>AVG → SUM/COUNT merge (handled by AvgAccumulator in merge mode)
   * </ul>
   *
   * @param aggCall the Calcite aggregate call
   * @param mode the aggregation mode (PARTIAL, FINAL, or SINGLE)
   * @return a Supplier that creates fresh Accumulator instances
   * @throws UnsupportedOperationException for unsupported aggregate functions
   */
  public static Supplier<Accumulator> createAccumulatorSupplier(
      AggregateCall aggCall, AggregationMode mode) {
    SqlKind kind = aggCall.getAggregation().getKind();

    if (mode == AggregationMode.FINAL) {
      return createFinalAccumulatorSupplier(kind, aggCall);
    }

    // SINGLE and PARTIAL use the same accumulators
    switch (kind) {
      case COUNT:
        // COUNT(DISTINCT column) needs special handling
        if (aggCall.isDistinct()) {
          return DistinctCountAccumulator::new;
        }
        // COUNT(*) has empty argList, COUNT(column) has one arg
        boolean countAll = aggCall.getArgList().isEmpty();
        return () -> new CountAccumulator(countAll);
      case SUM:
      case SUM0:
        // Both SUM and SUM0 use returnZeroForEmpty=true for PPL semantics
        // where SUM returns 0 (not null) when all input values are null
        return () -> new SumAccumulator(true);
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
   * Creates merge accumulators for the FINAL aggregation phase. These combine partial results from
   * leaf nodes rather than processing raw data.
   */
  private static Supplier<Accumulator> createFinalAccumulatorSupplier(
      SqlKind kind, AggregateCall aggCall) {
    switch (kind) {
      case COUNT:
        // FINAL COUNT: sum up partial counts (each leaf produced a count)
        return () -> new SumAccumulator(true);
      case SUM:
      case SUM0:
        // FINAL SUM: sum up partial sums
        return () -> new SumAccumulator(true);
      case AVG:
        // FINAL AVG: the partial phase produces (sum, count) pairs.
        // For now, treat as SUM (partial AVG emits the running average, final AVG averages
        // the averages — this is approximate but works for single-node scatter-gather where
        // there's only one partial result). Full distributed AVG needs sum+count tracking.
        return AvgAccumulator::new;
      case MIN:
        // FINAL MIN: min of partial mins
        return MinAccumulator::new;
      case MAX:
        // FINAL MAX: max of partial maxes
        return MaxAccumulator::new;
      default:
        throw new UnsupportedOperationException(
            "Unsupported final aggregate function: "
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

  /**
   * Resolves the input channel for an aggregate in a specific mode. In FINAL mode, the input is the
   * partial result row: [group_key_0, ..., group_key_N, agg_0, agg_1, ...]. So the i-th aggregate
   * reads from channel (groupByCount + aggIndex).
   *
   * @param aggCall the Calcite aggregate call
   * @param mode the aggregation mode
   * @param aggIndex the index of this aggregate in the aggregate call list
   * @param groupByCount the number of group-by columns
   * @return the input channel index
   */
  public static int resolveInputChannel(
      AggregateCall aggCall, AggregationMode mode, int aggIndex, int groupByCount) {
    if (mode == AggregationMode.FINAL) {
      return groupByCount + aggIndex;
    }
    return resolveInputChannel(aggCall);
  }
}
