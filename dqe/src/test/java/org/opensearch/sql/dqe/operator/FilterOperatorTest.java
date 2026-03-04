/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.dqe.operator;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.opensearch.sql.dqe.operator.TestPageSource.buildBigintPage;

import io.trino.spi.Page;
import io.trino.spi.type.BigintType;
import java.util.List;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

@DisplayName("FilterOperator")
class FilterOperatorTest {

  @Test
  @DisplayName("Keeps only rows matching predicate")
  void filtersRows() {
    // Page with values 0, 1, 2, 3, 4
    Page input = buildBigintPage(5);
    Operator source = new TestPageSource(List.of(input));
    // Keep only rows where value >= 3
    FilterOperator filter =
        new FilterOperator(
            source, (page, pos) -> BigintType.BIGINT.getLong(page.getBlock(0), pos) >= 3);

    Page result = filter.processNextBatch();
    assertEquals(2, result.getPositionCount());
    assertEquals(3L, BigintType.BIGINT.getLong(result.getBlock(0), 0));
    assertEquals(4L, BigintType.BIGINT.getLong(result.getBlock(0), 1));
  }

  @Test
  @DisplayName("Returns null when all rows filtered out")
  void allFilteredOut() {
    Page input = buildBigintPage(3);
    Operator source = new TestPageSource(List.of(input));
    FilterOperator filter = new FilterOperator(source, (page, pos) -> false);

    assertNull(filter.processNextBatch());
  }

  @Test
  @DisplayName("Passes through all rows when predicate always true")
  void allRowsMatch() {
    Page input = buildBigintPage(4);
    Operator source = new TestPageSource(List.of(input));
    FilterOperator filter = new FilterOperator(source, (page, pos) -> true);

    Page result = filter.processNextBatch();
    assertEquals(4, result.getPositionCount());
  }

  @Test
  @DisplayName("Skips empty pages and finds matching rows in subsequent pages")
  void skipsFilteredPages() {
    Page page1 = buildBigintPage(3); // values 0, 1, 2
    Page page2 = buildBigintPage(5); // values 0, 1, 2, 3, 4
    Operator source = new TestPageSource(List.of(page1, page2));
    // Keep only values >= 3 (none in page1, two in page2)
    FilterOperator filter =
        new FilterOperator(
            source, (page, pos) -> BigintType.BIGINT.getLong(page.getBlock(0), pos) >= 3);

    Page result = filter.processNextBatch();
    assertEquals(2, result.getPositionCount());
    assertEquals(3L, BigintType.BIGINT.getLong(result.getBlock(0), 0));
    assertEquals(4L, BigintType.BIGINT.getLong(result.getBlock(0), 1));
  }

  @Test
  @DisplayName("Returns null for empty source")
  void emptySource() {
    Operator source = new TestPageSource(List.of());
    FilterOperator filter = new FilterOperator(source, (page, pos) -> true);

    assertNull(filter.processNextBatch());
  }
}
