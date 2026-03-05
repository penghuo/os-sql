/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.dqe.operator;

import io.trino.spi.Page;
import io.trino.spi.block.Block;
import io.trino.spi.type.BooleanType;
import org.opensearch.sql.dqe.function.expression.BlockExpression;

/**
 * Physical operator that filters rows based on a vectorized {@link BlockExpression} predicate.
 *
 * <p>Uses Trino's {@link Page#copyPositions(int[], int, int)} for efficient row selection.
 */
public class FilterOperator implements Operator {

  private final Operator source;
  private final BlockExpression predicate;

  /**
   * Create a FilterOperator with a vectorized BlockExpression predicate.
   *
   * @param source child operator providing input pages
   * @param predicate expression that produces a BOOLEAN Block for filtering
   */
  public FilterOperator(Operator source, BlockExpression predicate) {
    this.source = source;
    this.predicate = predicate;
  }

  @Override
  public Page processNextBatch() {
    while (true) {
      Page page = source.processNextBatch();
      if (page == null) {
        return null;
      }

      int positionCount = page.getPositionCount();
      int[] selectedPositions = new int[positionCount];
      int selectedCount = 0;

      Block filterResult = predicate.evaluate(page);
      for (int pos = 0; pos < positionCount; pos++) {
        if (!filterResult.isNull(pos) && BooleanType.BOOLEAN.getBoolean(filterResult, pos)) {
          selectedPositions[selectedCount++] = pos;
        }
      }

      if (selectedCount == 0) {
        continue;
      }

      if (selectedCount == positionCount) {
        return page;
      }

      return page.copyPositions(selectedPositions, 0, selectedCount);
    }
  }

  @Override
  public void close() {
    source.close();
  }
}
