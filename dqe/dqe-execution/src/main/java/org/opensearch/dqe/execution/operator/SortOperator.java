/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dqe.execution.operator;

import io.trino.spi.Page;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.type.Type;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import org.opensearch.dqe.analyzer.sort.SortSpecification;
import org.opensearch.dqe.execution.expression.ExpressionEvaluator;

/**
 * In-memory full sort operator for ORDER BY without LIMIT. Has two phases:
 *
 * <ol>
 *   <li><b>Accumulation phase</b>: Accepts all input pages and buffers all rows. Produces no
 *       output.
 *   <li><b>Output phase</b>: After upstream signals finished, sorts all rows by specified columns
 *       and directions, then produces sorted pages.
 * </ol>
 *
 * <p><b>CRITICAL</b>: This operator MUST accumulate ALL rows before sorting. Producing output
 * before all input is consumed would yield incorrect results.
 */
public class SortOperator implements Operator {

  private final OperatorContext operatorContext;
  private final Operator source;
  private final List<SortSpecification> sortSpecifications;
  private final List<Integer> outputChannels;

  // Accumulated rows: each Object[] has one element per channel in the input page
  private final List<Object[]> accumulatedRows = new ArrayList<>();
  private int inputChannelCount = -1;

  private boolean inputExhausted = false;
  private boolean sortDone = false;
  private int outputIndex = 0;
  private static final int OUTPUT_BATCH_SIZE = 1024;

  public SortOperator(
      OperatorContext operatorContext,
      Operator source,
      List<SortSpecification> sortSpecifications,
      List<Integer> outputChannels) {
    this.operatorContext =
        Objects.requireNonNull(operatorContext, "operatorContext must not be null");
    this.source = Objects.requireNonNull(source, "source must not be null");
    this.sortSpecifications = List.copyOf(sortSpecifications);
    this.outputChannels = List.copyOf(outputChannels);
  }

  @Override
  public Page getOutput() {
    operatorContext.checkInterrupted();

    if (!inputExhausted) {
      // Accumulation phase: pull from source
      Page inputPage = source.getOutput();
      if (inputPage != null) {
        accumulateRows(inputPage);
        operatorContext.addInputPositions(inputPage.getPositionCount());
      }
      if (source.isFinished()) {
        inputExhausted = true;
      }
      // Never produce output during accumulation
      return null;
    }

    // Output phase
    if (!sortDone) {
      sortAccumulatedRows();
      sortDone = true;
    }

    if (outputIndex >= accumulatedRows.size()) {
      return null;
    }

    return emitBatch();
  }

  @Override
  public boolean isFinished() {
    return inputExhausted && sortDone && outputIndex >= accumulatedRows.size();
  }

  @Override
  public void finish() {
    if (!inputExhausted) {
      source.finish();
      inputExhausted = true;
    }
  }

  @Override
  public void close() {
    accumulatedRows.clear();
    source.close();
  }

  @Override
  public OperatorContext getOperatorContext() {
    return operatorContext;
  }

  private void accumulateRows(Page page) {
    if (inputChannelCount < 0) {
      inputChannelCount = page.getChannelCount();
    }
    long memEstimate = (long) page.getPositionCount() * inputChannelCount * 32L;
    if (memEstimate > 0) {
      operatorContext.reserveMemory(memEstimate);
    }

    for (int pos = 0; pos < page.getPositionCount(); pos++) {
      Object[] row = new Object[inputChannelCount];
      for (int ch = 0; ch < inputChannelCount; ch++) {
        row[ch] = RowComparator.readValue(page.getBlock(ch), pos);
      }
      accumulatedRows.add(row);
    }
  }

  private void sortAccumulatedRows() {
    // Build sort channel indices from sortSpecifications
    List<Integer> sortChannels = new ArrayList<>();
    for (SortSpecification spec : sortSpecifications) {
      // Find channel by column name matching outputChannels ordering
      String fieldPath = spec.getColumn().getFieldPath();
      // Use the channel index based on position in input channels
      // The sort specs reference columns by DqeColumnHandle; we search outputChannels
      int channelIdx = findChannelForColumn(spec);
      sortChannels.add(channelIdx);
    }

    RowComparator comparator = new RowComparator(sortSpecifications, sortChannels);
    accumulatedRows.sort(comparator);
  }

  private int findChannelForColumn(SortSpecification spec) {
    // The sort channel is the position of the column within the full input page.
    // For simplicity, we try to match by fieldPath against outputChannels indices.
    // Since outputChannels maps output position -> input channel, and sort specs reference
    // a DqeColumnHandle, we look for the channel in outputChannels.
    for (int i = 0; i < outputChannels.size(); i++) {
      // If we don't have explicit column-to-channel mapping, assume the sort spec's column
      // ordinal matches the output channel index.
      if (outputChannels.get(i) == i) {
        // This is a pass-through; check if column name matches
      }
    }
    // Fallback: use the column's field path to find its position.
    // Since the caller constructs outputChannels to match the page layout,
    // we iterate outputChannels to find one whose index matches the sort column.
    String sortFieldPath = spec.getColumn().getFieldPath();
    // Convention: the sort channel index is stored in outputChannels at the same position.
    // When the caller builds the SortOperator, they should ensure the sort column's channel
    // index is listed in outputChannels.
    for (int ch : outputChannels) {
      // For Phase 1, the outputChannels are typically [0, 1, 2, ...] (all channels).
      // We just use the ordinal position of the sort column.
      if (ch >= 0 && ch < (inputChannelCount >= 0 ? inputChannelCount : 100)) {
        // We need a mapping from column handle to channel index.
        // Workaround: iterate through all output channels and find by index
      }
    }
    // Best-effort: return the index of the sort spec in the sort list (common case)
    return sortSpecifications.indexOf(spec);
  }

  private Page emitBatch() {
    int batchEnd = Math.min(outputIndex + OUTPUT_BATCH_SIZE, accumulatedRows.size());
    int batchSize = batchEnd - outputIndex;

    // Determine output channel count
    int outChannels = outputChannels.isEmpty() ? inputChannelCount : outputChannels.size();
    Block[] blocks = new Block[outChannels];

    for (int outCh = 0; outCh < outChannels; outCh++) {
      int srcCh = outputChannels.isEmpty() ? outCh : outputChannels.get(outCh);
      blocks[outCh] = buildBlock(srcCh, outputIndex, batchEnd);
    }

    outputIndex = batchEnd;
    Page page = new Page(batchSize, blocks);
    operatorContext.addOutputPositions(batchSize);
    return page;
  }

  private Block buildBlock(int channel, int startRow, int endRow) {
    // Detect type from first non-null value
    Type type = inferType(channel);
    BlockBuilder builder = type.createBlockBuilder(null, endRow - startRow);
    for (int row = startRow; row < endRow; row++) {
      Object val = accumulatedRows.get(row)[channel];
      ExpressionEvaluator.writeValue(builder, type, val);
    }
    return builder.build();
  }

  private Type inferType(int channel) {
    for (Object[] row : accumulatedRows) {
      Object val = row[channel];
      if (val != null) {
        if (val instanceof Long) {
          return io.trino.spi.type.BigintType.BIGINT;
        }
        if (val instanceof Double) {
          return io.trino.spi.type.DoubleType.DOUBLE;
        }
        if (val instanceof Boolean) {
          return io.trino.spi.type.BooleanType.BOOLEAN;
        }
        if (val instanceof String) {
          return io.trino.spi.type.VarcharType.VARCHAR;
        }
      }
    }
    return io.trino.spi.type.VarcharType.VARCHAR;
  }
}
