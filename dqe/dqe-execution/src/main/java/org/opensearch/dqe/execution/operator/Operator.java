/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dqe.execution.operator;

import io.trino.spi.Page;

/**
 * Base interface for all physical operators in the DQE execution engine. Operators use a pull-based
 * iteration model: the downstream consumer calls {@link #getOutput()} to pull data.
 *
 * <p>Lifecycle: create -> repeated getOutput/isFinished -> finish -> close
 */
public interface Operator extends AutoCloseable {

  /**
   * Returns the next page of results, or {@code null} if data is not ready yet. A null return does
   * NOT mean finished; the caller must check {@link #isFinished()}.
   */
  Page getOutput();

  /** Returns true when the operator has produced all output and will never produce more. */
  boolean isFinished();

  /**
   * Signals that no more input will arrive. Called by Driver when upstream signals isFinished().
   * Operators with accumulation phases (Sort, TopN) begin producing output after this.
   */
  void finish();

  /** Releases all resources held by this operator. */
  @Override
  void close();

  /** Returns the operator context for memory tracking and cancellation. */
  OperatorContext getOperatorContext();
}
