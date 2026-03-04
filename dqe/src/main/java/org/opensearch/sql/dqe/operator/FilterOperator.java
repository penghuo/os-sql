/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.dqe.operator;

import io.trino.spi.Page;
import java.util.function.BiFunction;

/**
 * Physical operator that filters rows based on a predicate function. The predicate receives a page
 * and a row position, returning true to keep the row.
 *
 * <p>Uses Trino's {@link Page#copyPositions(int[], int, int)} for efficient row selection.
 */
public class FilterOperator implements Operator {

  private final Operator source;
  private final BiFunction<Page, Integer, Boolean> predicate;

  /**
   * Create a FilterOperator.
   *
   * @param source child operator providing input pages
   * @param predicate function that receives (page, position) and returns true to keep the row
   */
  public FilterOperator(Operator source, BiFunction<Page, Integer, Boolean> predicate) {
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

      for (int pos = 0; pos < positionCount; pos++) {
        if (predicate.apply(page, pos)) {
          selectedPositions[selectedCount++] = pos;
        }
      }

      if (selectedCount == 0) {
        // All rows filtered out, try next page
        continue;
      }

      if (selectedCount == positionCount) {
        // All rows kept, return as-is
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
