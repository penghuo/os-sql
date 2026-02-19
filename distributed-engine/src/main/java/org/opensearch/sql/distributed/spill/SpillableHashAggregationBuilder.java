/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.distributed.spill;

import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Supplier;
import org.opensearch.sql.distributed.context.OperatorContext;
import org.opensearch.sql.distributed.data.Block;
import org.opensearch.sql.distributed.data.DoubleArrayBlock;
import org.opensearch.sql.distributed.data.LongArrayBlock;
import org.opensearch.sql.distributed.data.Page;
import org.opensearch.sql.distributed.memory.MemoryPool;
import org.opensearch.sql.distributed.operator.Operator;
import org.opensearch.sql.distributed.operator.OperatorFactory;
import org.opensearch.sql.distributed.operator.aggregation.Accumulator;
import org.opensearch.sql.distributed.operator.aggregation.GroupByHash;

/**
 * Extends the Phase 1 in-memory {@link
 * org.opensearch.sql.distributed.operator.HashAggregationOperator} with spill-to-disk support. When
 * the MemoryPool signals memory pressure (revocable memory exceeds threshold), this operator:
 *
 * <ol>
 *   <li>Flushes the current hash table contents to a spill file via {@link FileSingleStreamSpiller}
 *   <li>Clears the hash table and frees revocable memory
 *   <li>Continues processing input into a fresh hash table
 * </ol>
 *
 * <p>On finish(), all spill files are merged: each file is re-read, and for each group across
 * files, accumulators are combined (e.g., summing partial sums, taking min of partial mins).
 *
 * <p>Output schema: [groupKey1, groupKey2, ..., agg1, agg2, ...]
 */
public class SpillableHashAggregationBuilder implements Operator, MemoryRevocableOperator {

  private enum State {
    NEEDS_INPUT,
    SPILLING,
    HAS_OUTPUT,
    FINISHED
  }

  private final OperatorContext operatorContext;
  private final int[] groupByChannels;
  private final int[] aggregateInputChannels;
  private final List<Supplier<Accumulator>> accumulatorFactories;
  private final int expectedGroups;
  private final MemoryPool memoryPool;
  private final SpillerFactory spillerFactory;
  private final long revocableMemoryThreshold;

  /** Current in-memory state. */
  private GroupByHash groupByHash;

  private List<Accumulator> accumulators;
  private Page firstPage;

  /** Spill state. */
  private final List<FileSingleStreamSpiller> spillFiles;

  private ListenableFuture<Void> spillFuture;
  private long currentRevocableMemory;

  private State state;

  public SpillableHashAggregationBuilder(
      OperatorContext operatorContext,
      int[] groupByChannels,
      int[] aggregateInputChannels,
      List<Supplier<Accumulator>> accumulatorFactories,
      int expectedGroups,
      MemoryPool memoryPool,
      SpillerFactory spillerFactory,
      long revocableMemoryThreshold) {
    this.operatorContext = Objects.requireNonNull(operatorContext);
    this.groupByChannels = Objects.requireNonNull(groupByChannels);
    this.aggregateInputChannels = Objects.requireNonNull(aggregateInputChannels);
    this.accumulatorFactories = List.copyOf(Objects.requireNonNull(accumulatorFactories));
    this.expectedGroups = expectedGroups;
    this.memoryPool = Objects.requireNonNull(memoryPool);
    this.spillerFactory = Objects.requireNonNull(spillerFactory);
    this.revocableMemoryThreshold = revocableMemoryThreshold;
    this.spillFiles = new ArrayList<>();
    this.currentRevocableMemory = 0;
    this.state = State.NEEDS_INPUT;

    initializeHashTable();
  }

  private void initializeHashTable() {
    this.groupByHash = new GroupByHash(groupByChannels, expectedGroups);
    this.accumulators = new ArrayList<>();
    for (Supplier<Accumulator> factory : accumulatorFactories) {
      accumulators.add(factory.get());
    }
  }

  @Override
  public ListenableFuture<?> isBlocked() {
    if (state == State.SPILLING && spillFuture != null && !spillFuture.isDone()) {
      return spillFuture;
    }
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
    Objects.requireNonNull(page);
    if (page.getPositionCount() == 0) {
      return;
    }

    if (firstPage == null) {
      firstPage = page;
    }

    // Assign group IDs and accumulate
    int[] groupIds = groupByHash.getGroupIds(page);
    int groupCount = groupByHash.getGroupCount();

    for (int i = 0; i < accumulators.size(); i++) {
      int channel = aggregateInputChannels[i];
      Block inputBlock = (channel >= 0) ? page.getBlock(channel) : null;
      accumulators.get(i).addInput(groupIds, groupCount, inputBlock);
    }

    // Update revocable memory estimate
    updateRevocableMemory();

    // Check if we need to spill
    if (currentRevocableMemory > revocableMemoryThreshold) {
      startMemoryRevoke();
      finishMemoryRevoke();
    }
  }

  private void updateRevocableMemory() {
    long hashSize = groupByHash.getEstimatedSizeInBytes();
    long accSize = 0;
    for (Accumulator acc : accumulators) {
      accSize += acc.getEstimatedSizeInBytes();
    }
    currentRevocableMemory = hashSize + accSize;
  }

  @Override
  public long getRevocableMemoryUsage() {
    return currentRevocableMemory;
  }

  @Override
  public ListenableFuture<Void> startMemoryRevoke() {
    if (groupByHash.getGroupCount() == 0) {
      spillFuture = Futures.immediateVoidFuture();
      return spillFuture;
    }

    // Flush current hash table to spill file
    FileSingleStreamSpiller spiller = spillerFactory.create();
    spillFiles.add(spiller);

    Page partialResult = buildPartialOutputPage();
    if (partialResult != null) {
      spiller.spill(partialResult);
    }

    spillFuture = Futures.immediateVoidFuture();
    return spillFuture;
  }

  @Override
  public void finishMemoryRevoke() {
    // Clear the hash table and accumulators, start fresh
    initializeHashTable();
    firstPage = null;
    currentRevocableMemory = 0;
    state = State.NEEDS_INPUT;
  }

  @Override
  public Page getOutput() {
    if (state == State.NEEDS_INPUT) {
      return null;
    }
    if (state == State.HAS_OUTPUT) {
      state = State.FINISHED;
      if (spillFiles.isEmpty()) {
        return buildFinalOutputPage();
      }
      return mergeSplitsAndBuild();
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
    for (FileSingleStreamSpiller spiller : spillFiles) {
      spiller.close();
    }
    spillFiles.clear();
  }

  /**
   * Builds a partial output page from the current in-memory hash table. Used when spilling to disk
   * — writes the current partial aggregation results.
   */
  private Page buildPartialOutputPage() {
    int groupCount = groupByHash.getGroupCount();
    if (groupCount == 0) {
      return null;
    }

    Block[] keyBlocks =
        (firstPage != null) ? groupByHash.buildGroupKeyBlocks(firstPage) : new Block[0];
    Block[] aggBlocks = new Block[accumulators.size()];
    for (int i = 0; i < accumulators.size(); i++) {
      aggBlocks[i] = buildAccumulatorBlock(accumulators.get(i), groupCount);
    }

    Block[] allBlocks = new Block[keyBlocks.length + aggBlocks.length];
    System.arraycopy(keyBlocks, 0, allBlocks, 0, keyBlocks.length);
    System.arraycopy(aggBlocks, 0, allBlocks, keyBlocks.length, aggBlocks.length);

    return new Page(groupCount, allBlocks);
  }

  /** Builds the final output page when no spilling occurred. */
  private Page buildFinalOutputPage() {
    int groupCount = groupByHash.getGroupCount();
    if (groupCount == 0) {
      if (isGlobalAggregation()) {
        return buildGlobalAggDefaultPage();
      }
      return null;
    }
    return buildPartialOutputPage();
  }

  /**
   * Merges all spill files with the current in-memory partial state. Re-reads each spill file and
   * combines accumulators across all partials.
   */
  private Page mergeSplitsAndBuild() {
    // First, get the current in-memory partial as a page
    Page currentPartial = buildPartialOutputPage();

    // Collect all partial pages
    List<Page> allPartials = new ArrayList<>();
    for (FileSingleStreamSpiller spiller : spillFiles) {
      Iterator<Page> spilledPages = spiller.getSpilledPages();
      while (spilledPages.hasNext()) {
        allPartials.add(spilledPages.next());
      }
    }
    if (currentPartial != null) {
      allPartials.add(currentPartial);
    }

    if (allPartials.isEmpty()) {
      if (isGlobalAggregation()) {
        return buildGlobalAggDefaultPage();
      }
      return null;
    }

    // Re-aggregate all partials using a fresh hash table
    GroupByHash mergeHash = new GroupByHash(buildMergeGroupByChannels(), expectedGroups);
    List<Accumulator> mergeAccumulators = new ArrayList<>();
    for (Supplier<Accumulator> factory : accumulatorFactories) {
      mergeAccumulators.add(factory.get());
    }

    Page mergeFirstPage = null;
    int keyColumnCount = groupByChannels.length;

    for (Page partial : allPartials) {
      if (mergeFirstPage == null) {
        mergeFirstPage = partial;
      }
      int posCount = partial.getPositionCount();
      int[] groupIds = mergeHash.getGroupIds(partial);
      int groupCount = mergeHash.getGroupCount();

      // The partial page layout is [key1, key2, ..., agg1, agg2, ...]
      // We need to combine accumulators from the partial results
      for (int i = 0; i < mergeAccumulators.size(); i++) {
        int aggChannel = keyColumnCount + i;
        Block inputBlock = partial.getBlock(aggChannel);
        mergeAccumulators.get(i).addInput(groupIds, groupCount, inputBlock);
      }
    }

    // Build the final merged output
    int finalGroupCount = mergeHash.getGroupCount();
    if (finalGroupCount == 0) {
      if (isGlobalAggregation()) {
        return buildGlobalAggDefaultPage();
      }
      return null;
    }

    Block[] keyBlocks =
        (mergeFirstPage != null) ? mergeHash.buildGroupKeyBlocks(mergeFirstPage) : new Block[0];
    Block[] aggBlocks = new Block[mergeAccumulators.size()];
    for (int i = 0; i < mergeAccumulators.size(); i++) {
      aggBlocks[i] = buildAccumulatorBlock(mergeAccumulators.get(i), finalGroupCount);
    }

    Block[] allBlocks = new Block[keyBlocks.length + aggBlocks.length];
    System.arraycopy(keyBlocks, 0, allBlocks, 0, keyBlocks.length);
    System.arraycopy(aggBlocks, 0, allBlocks, keyBlocks.length, aggBlocks.length);

    // Close spill files
    for (FileSingleStreamSpiller spiller : spillFiles) {
      spiller.close();
    }
    spillFiles.clear();

    return new Page(finalGroupCount, allBlocks);
  }

  /** When merging partials, the key columns are [0, 1, ..., keyCount-1] in the partial page. */
  private int[] buildMergeGroupByChannels() {
    int[] channels = new int[groupByChannels.length];
    for (int i = 0; i < channels.length; i++) {
      channels[i] = i;
    }
    return channels;
  }

  private boolean isGlobalAggregation() {
    return groupByChannels.length == 0;
  }

  private Page buildGlobalAggDefaultPage() {
    Block[] aggBlocks = new Block[accumulators.size()];
    for (int i = 0; i < accumulators.size(); i++) {
      Accumulator acc = accumulators.get(i);
      acc.ensureCapacity(1);
      aggBlocks[i] = buildAccumulatorBlock(acc, 1);
    }
    return new Page(1, aggBlocks);
  }

  private static Block buildAccumulatorBlock(Accumulator accumulator, int groupCount) {
    return switch (accumulator.getResultType()) {
      case LONG -> buildLongAccBlock(accumulator, groupCount);
      case DOUBLE -> buildDoubleAccBlock(accumulator, groupCount);
      default ->
          throw new UnsupportedOperationException(
              "Unsupported accumulator result type: " + accumulator.getResultType());
    };
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

  /** Factory for creating SpillableHashAggregationBuilder instances. */
  public static class Factory implements OperatorFactory {
    private final int[] groupByChannels;
    private final int[] aggregateInputChannels;
    private final List<Supplier<Accumulator>> accumulatorFactories;
    private final int expectedGroups;
    private final MemoryPool memoryPool;
    private final SpillerFactory spillerFactory;
    private final long revocableMemoryThreshold;
    private boolean closed;

    public Factory(
        int[] groupByChannels,
        int[] aggregateInputChannels,
        List<Supplier<Accumulator>> accumulatorFactories,
        int expectedGroups,
        MemoryPool memoryPool,
        SpillerFactory spillerFactory,
        long revocableMemoryThreshold) {
      this.groupByChannels = Objects.requireNonNull(groupByChannels);
      this.aggregateInputChannels = Objects.requireNonNull(aggregateInputChannels);
      this.accumulatorFactories = List.copyOf(Objects.requireNonNull(accumulatorFactories));
      this.expectedGroups = expectedGroups;
      this.memoryPool = Objects.requireNonNull(memoryPool);
      this.spillerFactory = Objects.requireNonNull(spillerFactory);
      this.revocableMemoryThreshold = revocableMemoryThreshold;
    }

    @Override
    public Operator createOperator(OperatorContext operatorContext) {
      if (closed) {
        throw new IllegalStateException("Factory is already closed");
      }
      return new SpillableHashAggregationBuilder(
          operatorContext,
          groupByChannels,
          aggregateInputChannels,
          accumulatorFactories,
          expectedGroups,
          memoryPool,
          spillerFactory,
          revocableMemoryThreshold);
    }

    @Override
    public void noMoreOperators() {
      closed = true;
    }
  }
}
