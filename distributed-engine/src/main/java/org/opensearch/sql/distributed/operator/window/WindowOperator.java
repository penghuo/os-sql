/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.distributed.operator.window;

import com.google.common.util.concurrent.ListenableFuture;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import org.opensearch.sql.distributed.context.OperatorContext;
import org.opensearch.sql.distributed.data.Block;
import org.opensearch.sql.distributed.data.Page;
import org.opensearch.sql.distributed.operator.Operator;
import org.opensearch.sql.distributed.operator.OperatorFactory;

/**
 * Window function operator. Accumulates all input rows (assumed to be pre-partitioned and
 * pre-sorted by upstream exchanges/sorts), computes window functions across each partition, and
 * emits output pages with the original columns plus computed window columns.
 *
 * <p>Lifecycle: NEEDS_INPUT (accumulating) -> PROCESSING (computing) -> HAS_OUTPUT (emitting) ->
 * FINISHED.
 *
 * <p>Ported from Trino's io.trino.operator.WindowOperator (simplified: single-partition
 * accumulation, no spill, no pipelined partitioning).
 */
public class WindowOperator implements Operator {

  private enum State {
    NEEDS_INPUT,
    PROCESSING,
    HAS_OUTPUT,
    FINISHED
  }

  private final OperatorContext operatorContext;
  private final int[] partitionKeyChannels;
  private final int[] orderKeyChannels;
  private final List<WindowFunction> windowFunctions;

  private State state;
  private final List<Page> accumulatedPages;
  private List<Page> outputPages;
  private int outputPageIndex;

  /**
   * Creates a new WindowOperator.
   *
   * @param operatorContext the operator context
   * @param partitionKeyChannels column indices for PARTITION BY (empty for single partition)
   * @param orderKeyChannels column indices for ORDER BY
   * @param windowFunctions the window functions to compute
   */
  public WindowOperator(
      OperatorContext operatorContext,
      int[] partitionKeyChannels,
      int[] orderKeyChannels,
      List<WindowFunction> windowFunctions) {
    this.operatorContext = Objects.requireNonNull(operatorContext, "operatorContext is null");
    this.partitionKeyChannels =
        Objects.requireNonNull(partitionKeyChannels, "partitionKeyChannels is null");
    this.orderKeyChannels = Objects.requireNonNull(orderKeyChannels, "orderKeyChannels is null");
    this.windowFunctions =
        List.copyOf(Objects.requireNonNull(windowFunctions, "windowFunctions is null"));
    this.state = State.NEEDS_INPUT;
    this.accumulatedPages = new ArrayList<>();
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
      throw new IllegalStateException("Operator does not need input, state=" + state);
    }
    Objects.requireNonNull(page, "page is null");
    if (page.getPositionCount() == 0) {
      return;
    }

    accumulatedPages.add(page);
    operatorContext.recordInputPositions(page.getPositionCount());
  }

  @Override
  public Page getOutput() {
    if (state == State.PROCESSING) {
      processPartitions();
      state = State.HAS_OUTPUT;
      outputPageIndex = 0;
    }
    if (state == State.HAS_OUTPUT) {
      if (outputPageIndex < outputPages.size()) {
        Page result = outputPages.get(outputPageIndex++);
        operatorContext.recordOutputPositions(result.getPositionCount());
        if (outputPageIndex >= outputPages.size()) {
          state = State.FINISHED;
          outputPages = null;
        }
        return result;
      }
      state = State.FINISHED;
      return null;
    }
    return null;
  }

  @Override
  public void finish() {
    if (state == State.NEEDS_INPUT) {
      state = State.PROCESSING;
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
    accumulatedPages.clear();
    outputPages = null;
  }

  /** Processes all accumulated pages, splitting into partitions and computing window functions. */
  private void processPartitions() {
    if (accumulatedPages.isEmpty()) {
      outputPages = List.of();
      return;
    }

    // If no partition keys, treat everything as one partition
    if (partitionKeyChannels.length == 0) {
      PartitionData partition = PartitionData.fromPages(accumulatedPages);
      outputPages = processPartition(partition);
      return;
    }

    // Split pages into partitions by partition key boundaries.
    // Assumption: input is pre-sorted by partition keys (from upstream HashExchange/sort).
    outputPages = new ArrayList<>();
    List<Page> currentPartitionPages = new ArrayList<>();
    Object[] currentKey = null;

    for (Page page : accumulatedPages) {
      for (int pos = 0; pos < page.getPositionCount(); pos++) {
        Object[] key = extractPartitionKey(page, pos);
        if (currentKey == null) {
          currentKey = key;
        }

        if (!keysEqual(currentKey, key)) {
          // Emit current partition
          PartitionData partition = PartitionData.fromPages(currentPartitionPages);
          outputPages.addAll(processPartition(partition));
          currentPartitionPages = new ArrayList<>();
          currentKey = key;
        }

        // Add this row to current partition as a single-row page
        // (Optimization: batch consecutive rows from same page)
        currentPartitionPages.add(page.getRegion(pos, 1));
      }
    }

    // Process final partition
    if (!currentPartitionPages.isEmpty()) {
      PartitionData partition = PartitionData.fromPages(currentPartitionPages);
      outputPages.addAll(processPartition(partition));
    }
  }

  /**
   * Processes a single partition: computes all window functions and builds output pages with
   * original columns + window result columns appended.
   */
  private List<Page> processPartition(PartitionData partition) {
    int partitionSize = partition.getSize();
    if (partitionSize == 0) {
      return List.of();
    }

    // Compute window function results
    Block[] windowBlocks = new Block[windowFunctions.size()];
    for (int i = 0; i < windowFunctions.size(); i++) {
      windowBlocks[i] =
          windowFunctions.get(i).processPartition(partition, partitionSize, orderKeyChannels);
    }

    // Build output pages: original columns + window columns
    List<Page> results = new ArrayList<>();
    int channelCount = partition.getChannelCount();
    int offset = 0;

    for (Page sourcePage : partition.getPages()) {
      int pageSize = sourcePage.getPositionCount();
      Block[] allBlocks = new Block[channelCount + windowFunctions.size()];

      // Copy original blocks
      for (int col = 0; col < channelCount; col++) {
        allBlocks[col] = sourcePage.getBlock(col);
      }

      // Add window function result blocks (sliced to this page's region)
      for (int w = 0; w < windowFunctions.size(); w++) {
        allBlocks[channelCount + w] = windowBlocks[w].getRegion(offset, pageSize);
      }

      results.add(new Page(pageSize, allBlocks));
      offset += pageSize;
    }

    return results;
  }

  private Object[] extractPartitionKey(Page page, int position) {
    Object[] key = new Object[partitionKeyChannels.length];
    for (int i = 0; i < partitionKeyChannels.length; i++) {
      Block block = page.getBlock(partitionKeyChannels[i]);
      if (block.isNull(position)) {
        key[i] = null;
      } else {
        key[i] = extractValue(block, position);
      }
    }
    return key;
  }

  private static boolean keysEqual(Object[] key1, Object[] key2) {
    if (key1.length != key2.length) return false;
    for (int i = 0; i < key1.length; i++) {
      Object a = key1[i];
      Object b = key2[i];
      if (a == null && b == null) continue;
      if (a == null || b == null) return false;
      if (a instanceof byte[] ab && b instanceof byte[] bb) {
        if (!Arrays.equals(ab, bb)) return false;
      } else if (!a.equals(b)) {
        return false;
      }
    }
    return true;
  }

  private static Object extractValue(Block block, int position) {
    Block resolved = resolveValueBlock(block);
    int resolvedPos = resolvePosition(block, position);

    if (resolved instanceof org.opensearch.sql.distributed.data.LongArrayBlock longBlock) {
      return longBlock.getLong(resolvedPos);
    }
    if (resolved instanceof org.opensearch.sql.distributed.data.DoubleArrayBlock doubleBlock) {
      return doubleBlock.getDouble(resolvedPos);
    }
    if (resolved instanceof org.opensearch.sql.distributed.data.IntArrayBlock intBlock) {
      return intBlock.getInt(resolvedPos);
    }
    if (resolved instanceof org.opensearch.sql.distributed.data.ShortArrayBlock shortBlock) {
      return shortBlock.getShort(resolvedPos);
    }
    if (resolved instanceof org.opensearch.sql.distributed.data.ByteArrayBlock byteBlock) {
      return byteBlock.getByte(resolvedPos);
    }
    if (resolved instanceof org.opensearch.sql.distributed.data.BooleanArrayBlock boolBlock) {
      return boolBlock.getBoolean(resolvedPos);
    }
    if (resolved instanceof org.opensearch.sql.distributed.data.VariableWidthBlock varBlock) {
      return varBlock.getSlice(resolvedPos);
    }
    throw new UnsupportedOperationException(
        "Unsupported block type: " + resolved.getClass().getSimpleName());
  }

  private static Block resolveValueBlock(Block block) {
    if (block instanceof org.opensearch.sql.distributed.data.DictionaryBlock dict) {
      return dict.getDictionary();
    }
    if (block instanceof org.opensearch.sql.distributed.data.RunLengthEncodedBlock rle) {
      return rle.getValue();
    }
    return block;
  }

  private static int resolvePosition(Block block, int position) {
    if (block instanceof org.opensearch.sql.distributed.data.DictionaryBlock dict) {
      return dict.getId(position);
    }
    if (block instanceof org.opensearch.sql.distributed.data.RunLengthEncodedBlock) {
      return 0;
    }
    return position;
  }

  /** Factory for creating {@link WindowOperator} instances. */
  public static class WindowOperatorFactory implements OperatorFactory {
    private final int[] partitionKeyChannels;
    private final int[] orderKeyChannels;
    private final List<WindowFunction> windowFunctionPrototypes;
    private boolean closed;

    public WindowOperatorFactory(
        int[] partitionKeyChannels, int[] orderKeyChannels, List<WindowFunction> windowFunctions) {
      this.partitionKeyChannels = Objects.requireNonNull(partitionKeyChannels);
      this.orderKeyChannels = Objects.requireNonNull(orderKeyChannels);
      this.windowFunctionPrototypes = List.copyOf(Objects.requireNonNull(windowFunctions));
    }

    @Override
    public Operator createOperator(OperatorContext operatorContext) {
      if (closed) {
        throw new IllegalStateException("Factory is already closed");
      }
      return new WindowOperator(
          operatorContext, partitionKeyChannels, orderKeyChannels, windowFunctionPrototypes);
    }

    @Override
    public void noMoreOperators() {
      closed = true;
    }
  }
}
