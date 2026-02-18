/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.distributed.operator;

import com.google.common.util.concurrent.ListenableFuture;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import org.opensearch.sql.distributed.context.OperatorContext;
import org.opensearch.sql.distributed.data.Block;
import org.opensearch.sql.distributed.data.DoubleArrayBlock;
import org.opensearch.sql.distributed.data.LongArrayBlock;
import org.opensearch.sql.distributed.data.Page;
import org.opensearch.sql.distributed.operator.aggregation.Accumulator;
import org.opensearch.sql.distributed.operator.aggregation.GroupByHash;

/**
 * Hash-based aggregation operator. Groups input rows by key columns and computes aggregate
 * functions (COUNT, SUM, AVG, MIN, MAX) per group. Ported from Trino's
 * io.trino.operator.HashAggregationOperator.
 *
 * <p>Phase 1: InMemoryHashAggregationBuilder only (no spill to disk).
 *
 * <p>Output schema: [groupKey1, groupKey2, ..., agg1, agg2, ...]
 */
public class HashAggregationOperator implements Operator {

  private enum State {
    NEEDS_INPUT,
    HAS_OUTPUT,
    FINISHED
  }

  private final OperatorContext operatorContext;
  private final GroupByHash groupByHash;
  private final List<Accumulator> accumulators;
  private final int[] aggregateInputChannels;

  private State state;
  private Page firstPage; // stored for type inference during output

  public HashAggregationOperator(
      OperatorContext operatorContext,
      int[] groupByChannels,
      int[] aggregateInputChannels,
      List<Accumulator> accumulators,
      int expectedGroups) {
    this.operatorContext = Objects.requireNonNull(operatorContext, "operatorContext is null");
    this.groupByHash = new GroupByHash(groupByChannels, expectedGroups);
    this.accumulators = List.copyOf(Objects.requireNonNull(accumulators, "accumulators is null"));
    this.aggregateInputChannels =
        Objects.requireNonNull(aggregateInputChannels, "aggregateInputChannels is null");
    if (aggregateInputChannels.length != accumulators.size()) {
      throw new IllegalArgumentException(
          "aggregateInputChannels length must match accumulators size");
    }
    this.state = State.NEEDS_INPUT;
  }

  @Override
  public ListenableFuture<?> isBlocked() {
    return NOT_BLOCKED;
  }

  @Override
  public boolean needsInput() {
    return state == State.NEEDS_INPUT;
  }

  @Override
  public void addInput(Page page) {
    if (state != State.NEEDS_INPUT) {
      throw new IllegalStateException("Operator does not need input");
    }
    Objects.requireNonNull(page, "page is null");
    if (page.getPositionCount() == 0) {
      return;
    }

    if (firstPage == null) {
      firstPage = page;
    }

    // Assign group IDs
    int[] groupIds = groupByHash.getGroupIds(page);
    int groupCount = groupByHash.getGroupCount();

    // Feed each accumulator
    for (int i = 0; i < accumulators.size(); i++) {
      int channel = aggregateInputChannels[i];
      Block inputBlock = (channel >= 0) ? page.getBlock(channel) : null;
      accumulators.get(i).addInput(groupIds, groupCount, inputBlock);
    }
  }

  @Override
  public Page getOutput() {
    if (state == State.NEEDS_INPUT) {
      return null;
    }
    if (state == State.HAS_OUTPUT) {
      state = State.FINISHED;
      return buildOutputPage();
    }
    return null;
  }

  @Override
  public void finish() {
    if (state == State.NEEDS_INPUT) {
      state = State.HAS_OUTPUT;
    }
  }

  @Override
  public boolean isFinished() {
    return state == State.FINISHED;
  }

  @Override
  public OperatorContext getOperatorContext() {
    return operatorContext;
  }

  @Override
  public void close() {
    state = State.FINISHED;
  }

  private boolean isGlobalAggregation() {
    return groupByHash.getGroupByChannels().length == 0;
  }

  private Page buildOutputPage() {
    int groupCount = groupByHash.getGroupCount();
    if (groupCount == 0) {
      // For global aggregation (no group-by key), SQL/PPL semantics require a single output row
      // with default values: COUNT→0, SUM/AVG/MIN/MAX→null
      if (isGlobalAggregation()) {
        return buildGlobalAggDefaultPage();
      }
      return null;
    }

    // Build group key blocks
    Block[] keyBlocks =
        (firstPage != null) ? groupByHash.buildGroupKeyBlocks(firstPage) : new Block[0];

    // Build accumulator result blocks
    Block[] aggBlocks = new Block[accumulators.size()];
    for (int i = 0; i < accumulators.size(); i++) {
      aggBlocks[i] = buildAccumulatorBlock(accumulators.get(i), groupCount);
    }

    // Combine: [key1, key2, ..., agg1, agg2, ...]
    Block[] allBlocks = new Block[keyBlocks.length + aggBlocks.length];
    System.arraycopy(keyBlocks, 0, allBlocks, 0, keyBlocks.length);
    System.arraycopy(aggBlocks, 0, allBlocks, keyBlocks.length, aggBlocks.length);

    return new Page(groupCount, allBlocks);
  }

  /**
   * Builds a single-row output page for global aggregation with no input. Each accumulator produces
   * its default value: COUNT→0, others→null.
   */
  private Page buildGlobalAggDefaultPage() {
    Block[] aggBlocks = new Block[accumulators.size()];
    for (int i = 0; i < accumulators.size(); i++) {
      Accumulator acc = accumulators.get(i);
      // Ensure capacity for 1 group (group 0) so getResult/isNull work
      acc.ensureCapacity(1);
      aggBlocks[i] = buildAccumulatorBlock(acc, 1);
    }
    return new Page(1, aggBlocks);
  }

  private static Block buildAccumulatorBlock(Accumulator accumulator, int groupCount) {
    switch (accumulator.getResultType()) {
      case LONG:
        return buildLongAccBlock(accumulator, groupCount);
      case DOUBLE:
        return buildDoubleAccBlock(accumulator, groupCount);
      default:
        throw new UnsupportedOperationException(
            "Unsupported accumulator result type: " + accumulator.getResultType());
    }
  }

  private static LongArrayBlock buildLongAccBlock(Accumulator accumulator, int groupCount) {
    long[] values = new long[groupCount];
    boolean[] nulls = null;
    boolean hasNull = false;
    for (int g = 0; g < groupCount; g++) {
      if (accumulator.isNull(g)) {
        if (nulls == null) nulls = new boolean[groupCount];
        nulls[g] = true;
        hasNull = true;
      } else {
        values[g] = ((Number) accumulator.getResult(g)).longValue();
      }
    }
    return new LongArrayBlock(groupCount, hasNull ? Optional.of(nulls) : Optional.empty(), values);
  }

  private static DoubleArrayBlock buildDoubleAccBlock(Accumulator accumulator, int groupCount) {
    double[] values = new double[groupCount];
    boolean[] nulls = null;
    boolean hasNull = false;
    for (int g = 0; g < groupCount; g++) {
      if (accumulator.isNull(g)) {
        if (nulls == null) nulls = new boolean[groupCount];
        nulls[g] = true;
        hasNull = true;
      } else {
        values[g] = ((Number) accumulator.getResult(g)).doubleValue();
      }
    }
    return new DoubleArrayBlock(
        groupCount, hasNull ? Optional.of(nulls) : Optional.empty(), values);
  }

  /** Factory for creating HashAggregationOperator instances. */
  public static class HashAggregationOperatorFactory implements OperatorFactory {
    private final int[] groupByChannels;
    private final int[] aggregateInputChannels;
    private final List<java.util.function.Supplier<Accumulator>> accumulatorFactories;
    private final int expectedGroups;
    private boolean closed;

    public HashAggregationOperatorFactory(
        int[] groupByChannels,
        int[] aggregateInputChannels,
        List<java.util.function.Supplier<Accumulator>> accumulatorFactories,
        int expectedGroups) {
      this.groupByChannels = Objects.requireNonNull(groupByChannels);
      this.aggregateInputChannels = Objects.requireNonNull(aggregateInputChannels);
      this.accumulatorFactories = List.copyOf(Objects.requireNonNull(accumulatorFactories));
      this.expectedGroups = expectedGroups;
    }

    @Override
    public Operator createOperator(OperatorContext operatorContext) {
      if (closed) {
        throw new IllegalStateException("Factory is already closed");
      }
      List<Accumulator> accumulators =
          accumulatorFactories.stream()
              .map(java.util.function.Supplier::get)
              .collect(java.util.stream.Collectors.toList());
      return new HashAggregationOperator(
          operatorContext, groupByChannels, aggregateInputChannels, accumulators, expectedGroups);
    }

    @Override
    public void noMoreOperators() {
      closed = true;
    }
  }
}
