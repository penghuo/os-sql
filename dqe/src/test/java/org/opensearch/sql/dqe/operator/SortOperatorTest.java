/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.dqe.operator;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.opensearch.sql.dqe.operator.TestPageSource.buildBigintPageWithValues;

import io.trino.spi.Page;
import io.trino.spi.type.BigintType;
import io.trino.spi.type.VarcharType;
import java.util.List;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

@DisplayName("SortOperator")
class SortOperatorTest {

  @Test
  @DisplayName("Sorts rows by bigint column ascending")
  void sortAscending() {
    Page input = buildBigintPageWithValues(3L, 1L, 2L);
    Operator source = new TestPageSource(List.of(input));
    SortOperator sort =
        new SortOperator(source, List.of(0), List.of(true), List.of(BigintType.BIGINT));

    Page result = sort.processNextBatch();
    assertEquals(3, result.getPositionCount());
    assertEquals(1L, BigintType.BIGINT.getLong(result.getBlock(0), 0));
    assertEquals(2L, BigintType.BIGINT.getLong(result.getBlock(0), 1));
    assertEquals(3L, BigintType.BIGINT.getLong(result.getBlock(0), 2));
  }

  @Test
  @DisplayName("Sorts rows by bigint column descending")
  void sortDescending() {
    Page input = buildBigintPageWithValues(1L, 3L, 2L);
    Operator source = new TestPageSource(List.of(input));
    SortOperator sort =
        new SortOperator(source, List.of(0), List.of(false), List.of(BigintType.BIGINT));

    Page result = sort.processNextBatch();
    assertEquals(3, result.getPositionCount());
    assertEquals(3L, BigintType.BIGINT.getLong(result.getBlock(0), 0));
    assertEquals(2L, BigintType.BIGINT.getLong(result.getBlock(0), 1));
    assertEquals(1L, BigintType.BIGINT.getLong(result.getBlock(0), 2));
  }

  @Test
  @DisplayName("Sorts across multiple input pages")
  void sortAcrossPages() {
    Page page1 = buildBigintPageWithValues(5L, 3L);
    Page page2 = buildBigintPageWithValues(1L, 4L, 2L);
    Operator source = new TestPageSource(List.of(page1, page2));
    SortOperator sort =
        new SortOperator(source, List.of(0), List.of(true), List.of(BigintType.BIGINT));

    Page result = sort.processNextBatch();
    assertEquals(5, result.getPositionCount());
    assertEquals(1L, BigintType.BIGINT.getLong(result.getBlock(0), 0));
    assertEquals(2L, BigintType.BIGINT.getLong(result.getBlock(0), 1));
    assertEquals(3L, BigintType.BIGINT.getLong(result.getBlock(0), 2));
    assertEquals(4L, BigintType.BIGINT.getLong(result.getBlock(0), 3));
    assertEquals(5L, BigintType.BIGINT.getLong(result.getBlock(0), 4));
  }

  @Test
  @DisplayName("Returns null after first call (all data emitted in one page)")
  void returnsNullAfterFirst() {
    Page input = buildBigintPageWithValues(2L, 1L);
    Operator source = new TestPageSource(List.of(input));
    SortOperator sort =
        new SortOperator(source, List.of(0), List.of(true), List.of(BigintType.BIGINT));

    sort.processNextBatch();
    assertNull(sort.processNextBatch());
  }

  @Test
  @DisplayName("Returns null for empty source")
  void emptySource() {
    Operator source = new TestPageSource(List.of());
    SortOperator sort =
        new SortOperator(source, List.of(0), List.of(true), List.of(BigintType.BIGINT));

    assertNull(sort.processNextBatch());
  }

  @Test
  @DisplayName("Sorts multi-column page by VARCHAR category then BIGINT value")
  void sortMultiColumn() {
    // Build a page with [VARCHAR, BIGINT] columns
    Page input = TestPageSource.buildCategoryValuePage("b", 2L, "a", 3L, "b", 1L, "a", 1L);
    Operator source = new TestPageSource(List.of(input));
    SortOperator sort =
        new SortOperator(
            source,
            List.of(0, 1), // sort by category ASC, then value ASC
            List.of(true, true),
            List.of(VarcharType.VARCHAR, BigintType.BIGINT));

    Page result = sort.processNextBatch();
    assertEquals(4, result.getPositionCount());
    // Expected order: ("a",1), ("a",3), ("b",1), ("b",2)
    assertEquals("a", VarcharType.VARCHAR.getSlice(result.getBlock(0), 0).toStringUtf8());
    assertEquals(1L, BigintType.BIGINT.getLong(result.getBlock(1), 0));
    assertEquals("a", VarcharType.VARCHAR.getSlice(result.getBlock(0), 1).toStringUtf8());
    assertEquals(3L, BigintType.BIGINT.getLong(result.getBlock(1), 1));
    assertEquals("b", VarcharType.VARCHAR.getSlice(result.getBlock(0), 2).toStringUtf8());
    assertEquals(1L, BigintType.BIGINT.getLong(result.getBlock(1), 2));
    assertEquals("b", VarcharType.VARCHAR.getSlice(result.getBlock(0), 3).toStringUtf8());
    assertEquals(2L, BigintType.BIGINT.getLong(result.getBlock(1), 3));
  }
}
