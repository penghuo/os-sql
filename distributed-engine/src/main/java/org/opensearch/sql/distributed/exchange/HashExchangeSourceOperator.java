/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.distributed.exchange;

import com.google.common.util.concurrent.ListenableFuture;
import org.opensearch.sql.distributed.context.OperatorContext;
import org.opensearch.sql.distributed.data.Page;
import org.opensearch.sql.distributed.operator.SourceOperator;

/**
 * Source operator on the receiving side of a hash exchange. Reads pages from a shared {@link
 * OutputBuffer} that has been populated by the hash-partitioned exchange sender. This operator sits
 * at the start of an intermediate stage pipeline.
 *
 * <p>Thread-safe: the underlying OutputBuffer handles concurrent access.
 */
public class HashExchangeSourceOperator implements SourceOperator {

  private final OperatorContext operatorContext;
  private final OutputBuffer inputBuffer;

  /**
   * Creates a source operator that reads from a hash-partitioned input buffer.
   *
   * @param operatorContext the operator context
   * @param inputBuffer the buffer containing pages assigned to this partition
   */
  public HashExchangeSourceOperator(OperatorContext operatorContext, OutputBuffer inputBuffer) {
    this.operatorContext = operatorContext;
    this.inputBuffer = inputBuffer;
  }

  @Override
  public ListenableFuture<?> isBlocked() {
    return inputBuffer.isBlocked();
  }

  @Override
  public Page getOutput() {
    Page page = inputBuffer.poll();
    if (page != null) {
      operatorContext.recordOutputPositions(page.getPositionCount());
    }
    return page;
  }

  @Override
  public void finish() {
    // The buffer is finished by the sender side
  }

  @Override
  public boolean isFinished() {
    return inputBuffer.isFinished();
  }

  @Override
  public OperatorContext getOperatorContext() {
    return operatorContext;
  }

  @Override
  public void close() throws Exception {
    // Nothing to close; buffer lifecycle managed externally
  }
}
