/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.distributed.operator;

import static org.junit.jupiter.api.Assertions.*;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.opensearch.sql.distributed.context.OperatorContext;
import org.opensearch.sql.distributed.data.LongArrayBlock;
import org.opensearch.sql.distributed.data.Page;

class TopNOperatorTest {

  private static final OperatorContext CTX = new OperatorContext(0, "TopN");

  @Test
  @DisplayName("Top-5 from 10 rows")
  void testTop5From10() {
    TopNOperator operator =
        new TopNOperator(
            CTX,
            5,
            new SimplePageWithPositionComparator(List.of(0), List.of(SortOrder.ASC_NULLS_LAST)));

    // Add 10 rows: 10, 9, 8, ..., 1
    operator.addInput(createLongPage(10, 9, 8, 7, 6));
    operator.addInput(createLongPage(5, 4, 3, 2, 1));
    operator.finish();

    List<Long> values = collectOutput(operator);
    assertEquals(5, values.size());
    // Top 5 ascending: 1, 2, 3, 4, 5
    assertEquals(List.of(1L, 2L, 3L, 4L, 5L), values);
  }

  @Test
  @DisplayName("Top-5 descending")
  void testTop5Descending() {
    TopNOperator operator =
        new TopNOperator(
            CTX,
            5,
            new SimplePageWithPositionComparator(List.of(0), List.of(SortOrder.DESC_NULLS_LAST)));

    operator.addInput(createLongPage(1, 2, 3, 4, 5, 6, 7, 8, 9, 10));
    operator.finish();

    List<Long> values = collectOutput(operator);
    assertEquals(5, values.size());
    assertEquals(List.of(10L, 9L, 8L, 7L, 6L), values);
  }

  @Test
  @DisplayName("N greater than input size")
  void testNGreaterThanInput() {
    TopNOperator operator =
        new TopNOperator(
            CTX,
            100,
            new SimplePageWithPositionComparator(List.of(0), List.of(SortOrder.ASC_NULLS_LAST)));

    operator.addInput(createLongPage(3, 1, 2));
    operator.finish();

    List<Long> values = collectOutput(operator);
    assertEquals(3, values.size());
    assertEquals(List.of(1L, 2L, 3L), values);
  }

  @Test
  @DisplayName("N = 0 produces no output")
  void testNZero() {
    TopNOperator operator =
        new TopNOperator(
            CTX,
            0,
            new SimplePageWithPositionComparator(List.of(0), List.of(SortOrder.ASC_NULLS_LAST)));

    assertTrue(operator.isFinished());
    assertNull(operator.getOutput());
  }

  @Test
  @DisplayName("Null ordering: NULLS_FIRST")
  void testNullsFirst() {
    TopNOperator operator =
        new TopNOperator(
            CTX,
            3,
            new SimplePageWithPositionComparator(List.of(0), List.of(SortOrder.ASC_NULLS_FIRST)));

    long[] values = {1, 0, 3, 0, 5};
    boolean[] nulls = {false, true, false, true, false};
    Page page = new Page(new LongArrayBlock(5, Optional.of(nulls), values));
    operator.addInput(page);
    operator.finish();

    List<Long> output = new ArrayList<>();
    List<Boolean> outputNulls = new ArrayList<>();
    Page outPage;
    while ((outPage = operator.getOutput()) != null) {
      LongArrayBlock block = (LongArrayBlock) outPage.getBlock(0);
      for (int i = 0; i < outPage.getPositionCount(); i++) {
        outputNulls.add(block.isNull(i));
        output.add(block.isNull(i) ? null : block.getLong(i));
      }
    }
    assertEquals(3, output.size());
    // NULLS_FIRST: nulls come first
    assertTrue(outputNulls.get(0));
    assertTrue(outputNulls.get(1));
    assertFalse(outputNulls.get(2));
  }

  @Test
  @DisplayName("Multi-column sort")
  void testMultiColumnSort() {
    TopNOperator operator =
        new TopNOperator(
            CTX,
            4,
            new SimplePageWithPositionComparator(
                List.of(0, 1), List.of(SortOrder.ASC_NULLS_LAST, SortOrder.DESC_NULLS_LAST)));

    // (col0, col1): (1,3), (1,1), (2,2), (2,4), (3,1)
    long[] col0 = {1, 1, 2, 2, 3};
    long[] col1 = {3, 1, 2, 4, 1};
    Page page =
        new Page(
            new LongArrayBlock(5, Optional.empty(), col0),
            new LongArrayBlock(5, Optional.empty(), col1));
    operator.addInput(page);
    operator.finish();

    // Sorted: (1,3), (1,1), (2,4), (2,2) — col0 ASC, col1 DESC
    List<long[]> rows = collectMultiColumnOutput(operator, 2);
    assertEquals(4, rows.size());
    assertArrayEquals(new long[] {1, 3}, rows.get(0));
    assertArrayEquals(new long[] {1, 1}, rows.get(1));
    assertArrayEquals(new long[] {2, 4}, rows.get(2));
    assertArrayEquals(new long[] {2, 2}, rows.get(3));
  }

  @Test
  @DisplayName("Operator lifecycle: finish and close")
  void testLifecycle() {
    TopNOperator operator =
        new TopNOperator(
            CTX,
            5,
            new SimplePageWithPositionComparator(List.of(0), List.of(SortOrder.ASC_NULLS_LAST)));

    assertTrue(operator.needsInput());
    assertFalse(operator.isFinished());

    operator.addInput(createLongPage(1, 2, 3));
    operator.finish();

    assertFalse(operator.needsInput());
    assertNotNull(operator.getOutput());
    assertNull(operator.getOutput()); // no more
    assertTrue(operator.isFinished());
  }

  // --- Helper methods ---

  private static Page createLongPage(long... values) {
    return new Page(new LongArrayBlock(values.length, Optional.empty(), values));
  }

  private static List<Long> collectOutput(Operator operator) {
    List<Long> values = new ArrayList<>();
    Page page;
    while ((page = operator.getOutput()) != null) {
      LongArrayBlock block = (LongArrayBlock) page.getBlock(0);
      for (int i = 0; i < page.getPositionCount(); i++) {
        values.add(block.getLong(i));
      }
    }
    return values;
  }

  private static List<long[]> collectMultiColumnOutput(Operator operator, int numCols) {
    List<long[]> rows = new ArrayList<>();
    Page page;
    while ((page = operator.getOutput()) != null) {
      for (int i = 0; i < page.getPositionCount(); i++) {
        long[] row = new long[numCols];
        for (int c = 0; c < numCols; c++) {
          row[c] = ((LongArrayBlock) page.getBlock(c)).getLong(i);
        }
        rows.add(row);
      }
    }
    return rows;
  }
}
