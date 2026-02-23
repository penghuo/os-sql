/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dqe.execution.driver;

import io.trino.spi.Page;
import java.io.Closeable;
import java.util.List;
import java.util.Objects;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import org.opensearch.dqe.execution.operator.Operator;
import org.opensearch.dqe.execution.operator.OperatorContext;

/**
 * A single-threaded execution unit that drives a pipeline. Pulls pages from the pipeline's output
 * operator and delivers them to a consumer (e.g., exchange sink or result collector).
 */
public class Driver implements Closeable {

  private static final int MAX_PAGES_PER_YIELD = 16;

  private final Pipeline pipeline;
  private final Consumer<Page> outputConsumer;
  private boolean finished = false;

  public Driver(Pipeline pipeline, Consumer<Page> outputConsumer) {
    this.pipeline = Objects.requireNonNull(pipeline, "pipeline must not be null");
    this.outputConsumer = Objects.requireNonNull(outputConsumer, "outputConsumer must not be null");
  }

  /**
   * Runs the execution loop. Pulls pages from the pipeline's output operator and delivers them to
   * the consumer. Yields after MAX_PAGES_PER_YIELD pages.
   *
   * @return true if more work remains, false if pipeline is finished
   */
  public boolean process() {
    if (finished) {
      return false;
    }

    int pagesProduced = 0;
    Operator outputOp = pipeline.getOutput();

    while (pagesProduced < MAX_PAGES_PER_YIELD) {
      if (outputOp.isFinished()) {
        finished = true;
        return false;
      }

      Page page = outputOp.getOutput();
      if (page != null) {
        outputConsumer.accept(page);
        pagesProduced++;
      } else if (outputOp.isFinished()) {
        finished = true;
        return false;
      } else {
        // No data available yet; yield to let other drivers run
        break;
      }
    }

    return !finished;
  }

  /** Returns true when the pipeline is fully finished. */
  public boolean isFinished() {
    return finished;
  }

  @Override
  public void close() {
    pipeline.close();
    finished = true;
  }

  /** Returns operator contexts for all operators in the pipeline. */
  public List<OperatorContext> getOperatorContexts() {
    return pipeline.getOperators().stream()
        .map(Operator::getOperatorContext)
        .collect(Collectors.toList());
  }

  /** Returns the pipeline. */
  public Pipeline getPipeline() {
    return pipeline;
  }
}
