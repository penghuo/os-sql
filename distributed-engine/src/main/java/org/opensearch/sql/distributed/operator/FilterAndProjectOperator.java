/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.distributed.operator;

import com.google.common.util.concurrent.ListenableFuture;
import java.util.List;
import java.util.Objects;
import org.opensearch.sql.distributed.context.OperatorContext;
import org.opensearch.sql.distributed.data.Block;
import org.opensearch.sql.distributed.data.Page;

/**
 * Combined filter and project operator. Applies a filter predicate to each row and then projects
 * the selected columns/expressions. Ported from Trino's
 * io.trino.operator.project.FilterAndProjectOperator.
 *
 * <p>This is a streaming operator: it processes each input page immediately and outputs filtered
 * pages. It does not buffer all input.
 */
public class FilterAndProjectOperator implements Operator {

  private final OperatorContext operatorContext;
  private final PageFilter filter;
  private final List<PageProjection> projections;

  private boolean finishing;
  private boolean finished;
  private Page pendingOutput;

  public FilterAndProjectOperator(
      OperatorContext operatorContext, PageFilter filter, List<PageProjection> projections) {
    this.operatorContext = Objects.requireNonNull(operatorContext, "operatorContext is null");
    this.filter = Objects.requireNonNull(filter, "filter is null");
    this.projections = List.copyOf(Objects.requireNonNull(projections, "projections is null"));
  }

  @Override
  public ListenableFuture<?> isBlocked() {
    return NOT_BLOCKED;
  }

  @Override
  public boolean needsInput() {
    return !finishing && pendingOutput == null;
  }

  @Override
  public void addInput(Page page) {
    if (!needsInput()) {
      throw new IllegalStateException("Operator does not need input");
    }
    Objects.requireNonNull(page, "page is null");

    if (page.getPositionCount() == 0) {
      return;
    }

    // Evaluate filter
    boolean[] selected = filter.filter(page);

    // Count selected positions
    int selectedCount = countSelected(selected, page.getPositionCount());
    if (selectedCount == 0) {
      return;
    }

    // Apply projections
    Block[] outputBlocks = new Block[projections.size()];
    for (int i = 0; i < projections.size(); i++) {
      outputBlocks[i] = projections.get(i).project(page, selected);
    }

    pendingOutput = new Page(selectedCount, outputBlocks);
  }

  @Override
  public Page getOutput() {
    if (pendingOutput != null) {
      Page output = pendingOutput;
      pendingOutput = null;
      return output;
    }
    return null;
  }

  @Override
  public void finish() {
    finishing = true;
    if (pendingOutput == null) {
      finished = true;
    }
  }

  @Override
  public boolean isFinished() {
    return finished && pendingOutput == null;
  }

  @Override
  public OperatorContext getOperatorContext() {
    return operatorContext;
  }

  @Override
  public void close() {
    pendingOutput = null;
    finished = true;
    finishing = true;
  }

  private static int countSelected(boolean[] selected, int positionCount) {
    int count = 0;
    int limit = Math.min(selected.length, positionCount);
    for (int i = 0; i < limit; i++) {
      if (selected[i]) {
        count++;
      }
    }
    return count;
  }

  /** Factory for creating FilterAndProjectOperator instances. */
  public static class FilterAndProjectOperatorFactory implements OperatorFactory {
    private final PageFilter filter;
    private final List<PageProjection> projections;
    private boolean closed;

    public FilterAndProjectOperatorFactory(PageFilter filter, List<PageProjection> projections) {
      this.filter = Objects.requireNonNull(filter, "filter is null");
      this.projections = List.copyOf(Objects.requireNonNull(projections, "projections is null"));
    }

    @Override
    public Operator createOperator(OperatorContext operatorContext) {
      if (closed) {
        throw new IllegalStateException("Factory is already closed");
      }
      return new FilterAndProjectOperator(operatorContext, filter, projections);
    }

    @Override
    public void noMoreOperators() {
      closed = true;
    }
  }
}
