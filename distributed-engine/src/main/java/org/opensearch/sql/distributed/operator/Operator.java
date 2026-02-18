/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.distributed.operator;

import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import org.opensearch.sql.distributed.context.OperatorContext;
import org.opensearch.sql.distributed.data.Page;

/**
 * Core operator interface for the distributed execution engine. Ported from Trino's
 * io.trino.operator.Operator.
 *
 * <p>Operators form a pipeline where Pages flow from source to sink. The Driver moves data between
 * operators using the cooperative scheduling contract: isBlocked/needsInput/addInput/getOutput.
 */
public interface Operator extends AutoCloseable {

  /** A pre-resolved future indicating the operator is not blocked. */
  ListenableFuture<Void> NOT_BLOCKED = Futures.immediateVoidFuture();

  /**
   * Returns a future that is resolved when the operator is no longer blocked. Returns {@link
   * #NOT_BLOCKED} when the operator can proceed immediately.
   */
  ListenableFuture<?> isBlocked();

  /** Returns true if the operator can accept input via {@link #addInput(Page)}. */
  boolean needsInput();

  /** Provides a page of input data to this operator. Only valid when needsInput() is true. */
  void addInput(Page page);

  /** Returns the next page of output, or null if no output is available. */
  Page getOutput();

  /**
   * Signals that no more input will be provided. After this call, the operator should produce
   * remaining output and eventually return true from isFinished().
   */
  void finish();

  /** Returns true when the operator has finished producing output. */
  boolean isFinished();

  /** Returns the context for this operator instance. */
  OperatorContext getOperatorContext();

  @Override
  void close() throws Exception;
}
