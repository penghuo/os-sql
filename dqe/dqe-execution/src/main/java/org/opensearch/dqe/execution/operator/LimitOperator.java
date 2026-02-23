/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dqe.execution.operator;

import io.trino.spi.Page;
import io.trino.spi.block.Block;
import java.util.Objects;

/**
 * Simple limit/offset operator. Skips the first {@code offset} rows, then emits up to {@code limit}
 * rows. Signals finished once the limit is reached.
 */
public class LimitOperator implements Operator {

  private final OperatorContext operatorContext;
  private final Operator source;
  private final long limit;
  private final long offset;

  private long skippedRows = 0;
  private long emittedRows = 0;
  private boolean finished = false;

  public LimitOperator(OperatorContext operatorContext, Operator source, long limit, long offset) {
    this.operatorContext =
        Objects.requireNonNull(operatorContext, "operatorContext must not be null");
    this.source = Objects.requireNonNull(source, "source must not be null");
    this.limit = limit;
    this.offset = offset;
  }

  @Override
  public Page getOutput() {
    if (finished) {
      return null;
    }
    operatorContext.checkInterrupted();

    while (true) {
      Page inputPage = source.getOutput();
      if (inputPage == null) {
        if (source.isFinished()) {
          finished = true;
        }
        return null;
      }

      operatorContext.addInputPositions(inputPage.getPositionCount());
      int positionCount = inputPage.getPositionCount();

      // Handle offset: skip rows
      if (skippedRows < offset) {
        long toSkip = offset - skippedRows;
        if (toSkip >= positionCount) {
          skippedRows += positionCount;
          continue; // Skip entire page
        }
        // Partial skip: take rows from toSkip onward
        int startPos = (int) toSkip;
        skippedRows = offset;
        inputPage = slicePage(inputPage, startPos, positionCount - startPos);
        positionCount = inputPage.getPositionCount();
      }

      // Handle limit: cap output rows
      long remaining = limit - emittedRows;
      if (remaining <= 0) {
        finished = true;
        return null;
      }

      if (positionCount > remaining) {
        inputPage = slicePage(inputPage, 0, (int) remaining);
      }

      emittedRows += inputPage.getPositionCount();
      operatorContext.addOutputPositions(inputPage.getPositionCount());

      if (emittedRows >= limit) {
        finished = true;
      }

      return inputPage;
    }
  }

  @Override
  public boolean isFinished() {
    return finished;
  }

  @Override
  public void finish() {
    source.finish();
  }

  @Override
  public void close() {
    source.close();
  }

  @Override
  public OperatorContext getOperatorContext() {
    return operatorContext;
  }

  /** Slices a page to keep only rows from startPos to startPos+length-1. */
  private static Page slicePage(Page page, int startPos, int length) {
    if (startPos == 0 && length == page.getPositionCount()) {
      return page;
    }
    int[] positions = new int[length];
    for (int i = 0; i < length; i++) {
      positions[i] = startPos + i;
    }
    Block[] blocks = new Block[page.getChannelCount()];
    for (int ch = 0; ch < page.getChannelCount(); ch++) {
      blocks[ch] = page.getBlock(ch).copyPositions(positions, 0, length);
    }
    return new Page(length, blocks);
  }
}
