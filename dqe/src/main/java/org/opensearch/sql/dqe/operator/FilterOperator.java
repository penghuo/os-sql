/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.dqe.operator;

import io.trino.spi.Page;
import io.trino.spi.block.Block;
import io.trino.spi.type.BooleanType;
import java.util.function.BiFunction;
import org.opensearch.sql.dqe.function.expression.BlockExpression;

/**
 * Physical operator that filters rows based on a predicate. Supports two modes: a row-at-a-time
 * BiFunction predicate (legacy) and a vectorized BlockExpression predicate (new).
 *
 * <p>Uses Trino's {@link Page#copyPositions(int[], int, int)} for efficient row selection.
 */
public class FilterOperator implements Operator {

  private final Operator source;
  private final BiFunction<Page, Integer, Boolean> predicate;
  private final BlockExpression blockPredicate;

  /**
   * Create a FilterOperator with a row-at-a-time predicate.
   *
   * @param source child operator providing input pages
   * @param predicate function that receives (page, position) and returns true to keep the row
   */
  public FilterOperator(Operator source, BiFunction<Page, Integer, Boolean> predicate) {
    this.source = source;
    this.predicate = predicate;
    this.blockPredicate = null;
  }

  /**
   * Create a FilterOperator with a vectorized BlockExpression predicate.
   *
   * @param source child operator providing input pages
   * @param blockPredicate expression that produces a BOOLEAN Block for filtering
   */
  public FilterOperator(Operator source, BlockExpression blockPredicate) {
    this.source = source;
    this.predicate = null;
    this.blockPredicate = blockPredicate;
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

      if (blockPredicate != null) {
        Block filterResult = blockPredicate.evaluate(page);
        for (int pos = 0; pos < positionCount; pos++) {
          if (!filterResult.isNull(pos) && BooleanType.BOOLEAN.getBoolean(filterResult, pos)) {
            selectedPositions[selectedCount++] = pos;
          }
        }
      } else {
        for (int pos = 0; pos < positionCount; pos++) {
          if (predicate.apply(page, pos)) {
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
