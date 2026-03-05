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
import io.trino.sql.tree.ComparisonExpression;
import java.util.List;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.opensearch.sql.dqe.function.expression.BlockExpression;
import org.opensearch.sql.dqe.function.expression.ColumnReference;
import org.opensearch.sql.dqe.function.expression.ComparisonBlockExpression;
import org.opensearch.sql.dqe.function.expression.ConstantExpression;

@DisplayName("FilterOperator")
class FilterOperatorTest {

  /** col0 >= threshold */
  private static BlockExpression geq(long threshold) {
    return new ComparisonBlockExpression(
        ComparisonExpression.Operator.GREATER_THAN_OR_EQUAL,
        new ColumnReference(0, BigintType.BIGINT),
        new ConstantExpression(threshold, BigintType.BIGINT));
  }

  /** Always-true predicate: col0 >= 0 (all values in test pages are non-negative) */
  private static BlockExpression alwaysTrue() {
    return geq(0);
  }

  /** Always-false predicate: col0 >= Long.MAX_VALUE */
  private static BlockExpression alwaysFalse() {
    return geq(Long.MAX_VALUE);
  }

  @Test
  @DisplayName("Keeps only rows matching predicate")
  void filtersRows() {
    Page input = buildBigintPage(5); // values 0, 1, 2, 3, 4
    Operator source = new TestPageSource(List.of(input));
    FilterOperator filter = new FilterOperator(source, geq(3));

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
    FilterOperator filter = new FilterOperator(source, alwaysFalse());

    assertNull(filter.processNextBatch());
  }

  @Test
  @DisplayName("Passes through all rows when predicate always true")
  void allRowsMatch() {
    Page input = buildBigintPage(4);
    Operator source = new TestPageSource(List.of(input));
    FilterOperator filter = new FilterOperator(source, alwaysTrue());

    Page result = filter.processNextBatch();
    assertEquals(4, result.getPositionCount());
  }

  @Test
  @DisplayName("Skips empty pages and finds matching rows in subsequent pages")
  void skipsFilteredPages() {
    Page page1 = buildBigintPage(3); // values 0, 1, 2
    Page page2 = buildBigintPage(5); // values 0, 1, 2, 3, 4
    Operator source = new TestPageSource(List.of(page1, page2));
    FilterOperator filter = new FilterOperator(source, geq(3));

    Page result = filter.processNextBatch();
    assertEquals(2, result.getPositionCount());
    assertEquals(3L, BigintType.BIGINT.getLong(result.getBlock(0), 0));
    assertEquals(4L, BigintType.BIGINT.getLong(result.getBlock(0), 1));
  }

  @Test
  @DisplayName("Returns null for empty source")
  void emptySource() {
    Operator source = new TestPageSource(List.of());
    FilterOperator filter = new FilterOperator(source, alwaysTrue());

    assertNull(filter.processNextBatch());
  }
}
