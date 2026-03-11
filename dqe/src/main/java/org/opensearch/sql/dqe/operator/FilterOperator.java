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
 * <p>Uses Trino's {@link Page#copyPositions(int[], int, int)} for efficient row selection. Includes
 * optimizations for highly selective filters (common in ClickBench) where most rows are filtered
 * out, and for pass-through cases where all rows match.
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
      Block filterResult = predicate.evaluate(page);

      // Check if we can skip the entire page (no rows pass) or return it unchanged (all pass)
      boolean mayHaveNull = filterResult.mayHaveNull();

      // Build selection vector
      int[] selectedPositions = new int[positionCount];
      int selectedCount = 0;

      if (mayHaveNull) {
        for (int pos = 0; pos < positionCount; pos++) {
          if (!filterResult.isNull(pos) && BooleanType.BOOLEAN.getBoolean(filterResult, pos)) {
            selectedPositions[selectedCount++] = pos;
          }
        }
      } else {
        // No nulls: skip isNull check for every position
        for (int pos = 0; pos < positionCount; pos++) {
          if (BooleanType.BOOLEAN.getBoolean(filterResult, pos)) {
            selectedPositions[selectedCount++] = pos;
          }
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
