/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.distributed.operator;

import com.google.common.util.concurrent.ListenableFuture;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import org.opensearch.sql.distributed.context.OperatorContext;
import org.opensearch.sql.distributed.data.Page;

/**
 * Returns the top N rows from the source sorted according to the specified ordering. Uses a
 * heap-based approach: collects all input, maintains a PagesIndex, sorts, and emits top N. Ported
 * from Trino's io.trino.operator.TopNOperator.
 *
 * <p>Phase 1: fully in-memory, no spill-to-disk support.
 */
public class TopNOperator implements Operator {

  private enum State {
    NEEDS_INPUT,
    HAS_OUTPUT,
    FINISHED
  }

  private final OperatorContext operatorContext;
  private final int n;
  private final PageWithPositionComparator comparator;
  private final PagesIndex pagesIndex;

  private State state;
  private Iterator<Page> outputIterator;

  public TopNOperator(
      OperatorContext operatorContext, int n, PageWithPositionComparator comparator) {
    this.operatorContext = Objects.requireNonNull(operatorContext, "operatorContext is null");
    if (n < 0) {
      throw new IllegalArgumentException("n must be non-negative, got: " + n);
    }
    this.n = n;
    this.comparator = Objects.requireNonNull(comparator, "comparator is null");
    this.pagesIndex = new PagesIndex(Math.min(n, 1024));
    this.state = (n == 0) ? State.FINISHED : State.NEEDS_INPUT;
  }

  @Override
  public ListenableFuture<?> isBlocked() {
    return NOT_BLOCKED;
  }

  @Override
  public boolean needsInput() {
    return state == State.NEEDS_INPUT;
  }

  @Override
  public void addInput(Page page) {
    if (state != State.NEEDS_INPUT) {
      throw new IllegalStateException("Operator does not need input");
    }
    Objects.requireNonNull(page, "page is null");
    if (page.getPositionCount() > 0) {
      pagesIndex.addPage(page);
    }
  }

  @Override
  public Page getOutput() {
    if (state == State.NEEDS_INPUT) {
      return null;
    }
    if (state == State.HAS_OUTPUT) {
      if (outputIterator == null) {
        // Sort and produce only top N
        pagesIndex.sort(comparator);
        int outputCount = Math.min(n, pagesIndex.getPositionCount());
        outputIterator =
            pagesIndex.getSortedPages(0, outputCount, PagesIndex.DEFAULT_OUTPUT_PAGE_SIZE);
      }
      if (outputIterator.hasNext()) {
        return outputIterator.next();
      }
      state = State.FINISHED;
    }
    return null;
  }

  @Override
  public void finish() {
    if (state == State.NEEDS_INPUT) {
      state = State.HAS_OUTPUT;
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
    pagesIndex.clear();
    outputIterator = null;
    state = State.FINISHED;
  }

  /** Factory for creating TopNOperator instances. */
  public static class TopNOperatorFactory implements OperatorFactory {
    private final int n;
    private final List<Integer> sortChannels;
    private final List<SortOrder> sortOrders;
    private boolean closed;

    public TopNOperatorFactory(int n, List<Integer> sortChannels, List<SortOrder> sortOrders) {
      this.n = n;
      this.sortChannels = Objects.requireNonNull(sortChannels, "sortChannels is null");
      this.sortOrders = Objects.requireNonNull(sortOrders, "sortOrders is null");
    }

    @Override
    public Operator createOperator(OperatorContext operatorContext) {
      if (closed) {
        throw new IllegalStateException("Factory is already closed");
      }
      return new TopNOperator(
          operatorContext, n, new SimplePageWithPositionComparator(sortChannels, sortOrders));
    }

    @Override
    public void noMoreOperators() {
      closed = true;
    }
  }
}
