/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dqe.execution.operator;

import io.trino.spi.Page;
import io.trino.spi.block.Block;
import java.util.List;
import java.util.Objects;
import org.opensearch.dqe.execution.expression.ExpressionEvaluator;

/**
 * Projection operator that computes output expressions and produces a Page with only the requested
 * output columns. Uses ExpressionEvaluator for each output expression.
 */
public class ProjectOperator implements Operator {

  private final OperatorContext operatorContext;
  private final Operator source;
  private final List<ExpressionEvaluator> projections;
  private boolean finished = false;

  public ProjectOperator(
      OperatorContext operatorContext, Operator source, List<ExpressionEvaluator> projections) {
    this.operatorContext =
        Objects.requireNonNull(operatorContext, "operatorContext must not be null");
    this.source = Objects.requireNonNull(source, "source must not be null");
    this.projections =
        List.copyOf(Objects.requireNonNull(projections, "projections must not be null"));
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

    Block[] outputBlocks = new Block[projections.size()];
    for (int i = 0; i < projections.size(); i++) {
      outputBlocks[i] = projections.get(i).evaluateAll(inputPage);
    }

    Page result = new Page(inputPage.getPositionCount(), outputBlocks);
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
}
