/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dqe.execution.operator;

import io.trino.spi.Page;
import io.trino.spi.block.Block;
import io.trino.spi.type.Type;
import java.util.Objects;
import org.opensearch.dqe.execution.expression.ExpressionEvaluator;

/**
 * Post-scan filter operator that evaluates predicate expressions against each row in a Page and
 * produces a filtered Page containing only matching rows.
 */
public class FilterOperator implements Operator {

  private final OperatorContext operatorContext;
  private final Operator source;
  private final ExpressionEvaluator filterPredicate;
  private boolean finished = false;

  public FilterOperator(
      OperatorContext operatorContext, Operator source, ExpressionEvaluator filterPredicate) {
    this.operatorContext =
        Objects.requireNonNull(operatorContext, "operatorContext must not be null");
    this.source = Objects.requireNonNull(source, "source must not be null");
    this.filterPredicate =
        Objects.requireNonNull(filterPredicate, "filterPredicate must not be null");
  }

  @Override
  public Page getOutput() {
    if (finished) {
      return null;
    }
    operatorContext.checkInterrupted();

    Page inputPage = source.getOutput();
    if (inputPage == null) {
      if (source.isFinished()) {
        finished = true;
      }
      return null;
    }

    operatorContext.addInputPositions(inputPage.getPositionCount());

    // Evaluate filter for each row and collect positions that pass
    int[] selectedPositions = new int[inputPage.getPositionCount()];
    int selectedCount = 0;
    for (int i = 0; i < inputPage.getPositionCount(); i++) {
      Object result = filterPredicate.evaluate(inputPage, i);
      if (result instanceof Boolean && (Boolean) result) {
        selectedPositions[selectedCount++] = i;
      }
    }

    if (selectedCount == 0) {
      return null; // No rows passed the filter; caller will call again
    }

    if (selectedCount == inputPage.getPositionCount()) {
      // All rows passed; return original page
      operatorContext.addOutputPositions(inputPage.getPositionCount());
      return inputPage;
    }

    // Build filtered page by selecting only matching positions
    int channelCount = inputPage.getChannelCount();
    Block[] filteredBlocks = new Block[channelCount];
    for (int channel = 0; channel < channelCount; channel++) {
      Block block = inputPage.getBlock(channel);
      Type type =
          block.getClass().getSimpleName().contains("Int128")
              ? io.trino.spi.type.VarcharType.VARCHAR
              : guessBlockType(block);
      io.trino.spi.block.BlockBuilder builder =
          block.getLoadedBlock().getClass().getSimpleName().isEmpty() ? null : null;
      // Use block.copyPositions for efficiency
      filteredBlocks[channel] = block.copyPositions(selectedPositions, 0, selectedCount);
    }

    Page result = new Page(selectedCount, filteredBlocks);
    operatorContext.addOutputPositions(result.getPositionCount());
    return result;
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

  private static Type guessBlockType(Block block) {
    return io.trino.spi.type.VarcharType.VARCHAR;
  }
}
