/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.distributed.operator.join;

import com.google.common.util.concurrent.ListenableFuture;
import java.util.Objects;
import org.opensearch.sql.distributed.context.OperatorContext;
import org.opensearch.sql.distributed.data.Page;
import org.opensearch.sql.distributed.operator.Operator;
import org.opensearch.sql.distributed.operator.OperatorFactory;

/**
 * Builds a {@link JoinHash} from build-side Pages. The hash table is consumed by the corresponding
 * {@link LookupJoinOperator} on the probe side.
 *
 * <p>Lifecycle: CONSUMING_INPUT (accumulating pages) -> LOOKUP_SOURCE_READY (hash table built) ->
 * FINISHED.
 *
 * <p>Ported from Trino's io.trino.operator.join.HashBuilderOperator (simplified: no spill, no
 * bytecode-generated hash, reflection-based).
 */
public class HashBuilderOperator implements Operator {

  private enum State {
    /** Accumulating build-side pages. */
    CONSUMING_INPUT,
    /** Hash table is built and ready for the probe side. */
    LOOKUP_SOURCE_READY,
    /** Operator is finished. */
    FINISHED
  }

  private final OperatorContext operatorContext;
  private final int[] hashKeyChannels;
  private final PagesHash pagesHash;

  private State state;
  private JoinHash joinHash;
  private long memoryReserved;

  /**
   * Creates a new HashBuilderOperator.
   *
   * @param operatorContext the operator context for memory tracking
   * @param hashKeyChannels column indices in build-side pages used as hash keys
   */
  public HashBuilderOperator(OperatorContext operatorContext, int[] hashKeyChannels) {
    this.operatorContext = Objects.requireNonNull(operatorContext, "operatorContext is null");
    this.hashKeyChannels = Objects.requireNonNull(hashKeyChannels, "hashKeyChannels is null");
    this.pagesHash = new PagesHash(hashKeyChannels);
    this.state = State.CONSUMING_INPUT;
    this.memoryReserved = 0;
  }

  @Override
  public ListenableFuture<?> isBlocked() {
    return NOT_BLOCKED;
  }

  @Override
  public boolean needsInput() {
    return state == State.CONSUMING_INPUT;
  }

  @Override
  public void addInput(Page page) {
    if (state != State.CONSUMING_INPUT) {
      throw new IllegalStateException("Operator is not consuming input, state=" + state);
    }
    Objects.requireNonNull(page, "page is null");
    if (page.getPositionCount() == 0) {
      return;
    }

    pagesHash.addPage(page);
    operatorContext.recordInputPositions(page.getPositionCount());

    // Track memory for the accumulated page
    long pageSize = page.getRetainedSizeInBytes();
    operatorContext.reserveMemory(pageSize);
    memoryReserved += pageSize;
  }

  /**
   * Returns null — the hash builder does not produce output Pages. Its output is the built {@link
   * JoinHash}, retrieved via {@link #getJoinHash()}.
   */
  @Override
  public Page getOutput() {
    return null;
  }

  /** Signals that no more input will be provided. Builds the hash table from accumulated pages. */
  @Override
  public void finish() {
    if (state == State.CONSUMING_INPUT) {
      // Build the hash table
      pagesHash.build();
      joinHash = new JoinHash(pagesHash);

      // Reserve additional memory for the hash table structure
      long hashOverhead = pagesHash.getEstimatedSizeInBytes();
      operatorContext.reserveMemory(hashOverhead);
      memoryReserved += hashOverhead;

      state = State.LOOKUP_SOURCE_READY;
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
    if (state != State.FINISHED) {
      state = State.FINISHED;
      if (memoryReserved > 0) {
        operatorContext.freeMemory(memoryReserved);
        memoryReserved = 0;
      }
    }
  }

  /**
   * Returns the built {@link JoinHash}. Only valid after {@link #finish()} has been called.
   *
   * @throws IllegalStateException if the hash table has not been built yet
   */
  public JoinHash getJoinHash() {
    if (joinHash == null) {
      throw new IllegalStateException("JoinHash is not ready; finish() has not been called");
    }
    return joinHash;
  }

  /**
   * Marks this operator as finished. Called after the probe side has consumed the JoinHash and no
   * longer needs it.
   */
  public void destroy() {
    close();
  }

  /** Factory for creating {@link HashBuilderOperator} instances. */
  public static class HashBuilderOperatorFactory implements OperatorFactory {
    private final int[] hashKeyChannels;
    private boolean closed;
    private HashBuilderOperator lastCreated;

    public HashBuilderOperatorFactory(int[] hashKeyChannels) {
      this.hashKeyChannels = Objects.requireNonNull(hashKeyChannels);
    }

    @Override
    public Operator createOperator(OperatorContext operatorContext) {
      if (closed) {
        throw new IllegalStateException("Factory is already closed");
      }
      lastCreated = new HashBuilderOperator(operatorContext, hashKeyChannels);
      return lastCreated;
    }

    @Override
    public void noMoreOperators() {
      closed = true;
    }

    /** Returns the last created operator. Used to retrieve the JoinHash for the probe side. */
    public HashBuilderOperator getLastCreated() {
      return lastCreated;
    }
  }
}
