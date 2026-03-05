/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dqe.exchange.gather;

import io.trino.spi.Page;
import java.util.Objects;
import org.opensearch.dqe.execution.operator.Operator;
import org.opensearch.dqe.execution.operator.OperatorContext;
import org.opensearch.dqe.parser.DqeErrorCode;
import org.opensearch.dqe.parser.DqeException;

/**
 * Adapts a {@link GatherExchangeSource} (blocking {@code getNextPage()}) into the pull-based {@link
 * Operator} interface used by the coordinator pipeline.
 */
public class ExchangeSourceOperator implements Operator {

  private final OperatorContext operatorContext;
  private final GatherExchangeSource source;
  private boolean finished;

  /**
   * Create an exchange source operator.
   *
   * @param operatorContext the operator context for memory tracking and cancellation
   * @param source the gather exchange source to read pages from
   */
  public ExchangeSourceOperator(OperatorContext operatorContext, GatherExchangeSource source) {
    this.operatorContext =
        Objects.requireNonNull(operatorContext, "operatorContext must not be null");
    this.source = Objects.requireNonNull(source, "source must not be null");
    this.finished = false;
  }

  @Override
  public Page getOutput() {
    operatorContext.checkInterrupted();

    if (finished) {
      return null;
    }

    try {
      Page page = source.getNextPage();
      if (page == null) {
        finished = true;
        return null;
      }
      operatorContext.addInputPositions(page.getPositionCount());
      operatorContext.addOutputPositions(page.getPositionCount());
      return page;
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new DqeException(
          "Exchange source interrupted for query [" + operatorContext.getQueryId() + "]",
          DqeErrorCode.EXECUTION_ERROR,
          e);
    }
  }

  @Override
  public boolean isFinished() {
    return finished || source.isFinished();
  }

  @Override
  public void finish() {
    finished = true;
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
