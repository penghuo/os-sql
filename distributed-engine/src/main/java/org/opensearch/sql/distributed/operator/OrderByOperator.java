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
 * Full in-memory sort operator. Collects all input pages, sorts them according to the specified
 * ordering, then emits sorted pages. Ported from Trino's io.trino.operator.OrderByOperator.
 *
 * <p>Phase 1: fully in-memory, no spill-to-disk support.
 */
public class OrderByOperator implements Operator {

  private enum State {
    NEEDS_INPUT,
    HAS_OUTPUT,
    FINISHED
  }

  private final OperatorContext operatorContext;
  private final PageWithPositionComparator comparator;
  private final PagesIndex pagesIndex;
  private final int[] outputChannels;

  private State state;
  private Iterator<Page> outputIterator;

  public OrderByOperator(
      OperatorContext operatorContext,
      PageWithPositionComparator comparator,
      int[] outputChannels) {
    this.operatorContext = Objects.requireNonNull(operatorContext, "operatorContext is null");
    this.comparator = Objects.requireNonNull(comparator, "comparator is null");
    this.outputChannels = Objects.requireNonNull(outputChannels, "outputChannels is null");
    this.pagesIndex = new PagesIndex(1024);
    this.state = State.NEEDS_INPUT;
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
        pagesIndex.sort(comparator);
        outputIterator = pagesIndex.getSortedPages();
      }
      if (outputIterator.hasNext()) {
        return outputIterator.next();
      }
      state = State.FINISHED;
    }
    return null;
  }

  private boolean needsProjection() {
    if (pagesIndex.getPositionCount() == 0) {
      return false;
    }
    Page firstPage = pagesIndex.getPageForAddress(0);
    int pageChannelCount = firstPage.getChannelCount();
    if (outputChannels.length != pageChannelCount) {
      return true; // different column count, needs projection
    }
    // Check if outputChannels is an identity mapping [0, 1, 2, ...]
    for (int i = 0; i < outputChannels.length; i++) {
      if (outputChannels[i] != i) {
        return true;
      }
    }
    return false;
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

  /** Factory for creating OrderByOperator instances. */
  public static class OrderByOperatorFactory implements OperatorFactory {
    private final List<Integer> sortChannels;
    private final List<SortOrder> sortOrders;
    private final int[] outputChannels;
    private boolean closed;

    public OrderByOperatorFactory(
        List<Integer> sortChannels, List<SortOrder> sortOrders, int[] outputChannels) {
      this.sortChannels = Objects.requireNonNull(sortChannels, "sortChannels is null");
      this.sortOrders = Objects.requireNonNull(sortOrders, "sortOrders is null");
      this.outputChannels = Objects.requireNonNull(outputChannels, "outputChannels is null");
    }

    @Override
    public Operator createOperator(OperatorContext operatorContext) {
      if (closed) {
        throw new IllegalStateException("Factory is already closed");
      }
      return new OrderByOperator(
          operatorContext,
          new SimplePageWithPositionComparator(sortChannels, sortOrders),
          outputChannels);
    }

    @Override
    public void noMoreOperators() {
      closed = true;
    }
  }
}
