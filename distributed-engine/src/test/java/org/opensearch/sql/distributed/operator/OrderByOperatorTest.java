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

class OrderByOperatorTest {

  private static final OperatorContext CTX = new OperatorContext(0, "OrderBy");

  @Test
  @DisplayName("Single column sort ASC")
  void testSingleColumnAsc() {
    int[] outputChannels = {0};
    OrderByOperator operator =
        new OrderByOperator(
            CTX,
            new SimplePageWithPositionComparator(List.of(0), List.of(SortOrder.ASC_NULLS_LAST)),
            outputChannels);

    operator.addInput(createLongPage(5, 3, 1, 4, 2));
    operator.finish();

    List<Long> values = collectLongOutput(operator);
    assertEquals(List.of(1L, 2L, 3L, 4L, 5L), values);
  }

  @Test
  @DisplayName("Single column sort DESC")
  void testSingleColumnDesc() {
    int[] outputChannels = {0};
    OrderByOperator operator =
        new OrderByOperator(
            CTX,
            new SimplePageWithPositionComparator(List.of(0), List.of(SortOrder.DESC_NULLS_LAST)),
            outputChannels);

    operator.addInput(createLongPage(1, 5, 3, 2, 4));
    operator.finish();

    List<Long> values = collectLongOutput(operator);
    assertEquals(List.of(5L, 4L, 3L, 2L, 1L), values);
  }

  @Test
  @DisplayName("Multi-column sort")
  void testMultiColumnSort() {
    int[] outputChannels = {0, 1};
    OrderByOperator operator =
        new OrderByOperator(
            CTX,
            new SimplePageWithPositionComparator(
                List.of(0, 1), List.of(SortOrder.ASC_NULLS_LAST, SortOrder.ASC_NULLS_LAST)),
            outputChannels);

    long[] col0 = {2, 1, 2, 1, 3};
    long[] col1 = {2, 2, 1, 1, 1};
    operator.addInput(
        new Page(
            new LongArrayBlock(5, Optional.empty(), col0),
            new LongArrayBlock(5, Optional.empty(), col1)));
    operator.finish();

    List<long[]> rows = collectMultiColumnOutput(operator, 2);
    assertEquals(5, rows.size());
    assertArrayEquals(new long[] {1, 1}, rows.get(0));
    assertArrayEquals(new long[] {1, 2}, rows.get(1));
    assertArrayEquals(new long[] {2, 1}, rows.get(2));
    assertArrayEquals(new long[] {2, 2}, rows.get(3));
    assertArrayEquals(new long[] {3, 1}, rows.get(4));
  }

  @Test
  @DisplayName("Null ordering")
  void testNullOrdering() {
    int[] outputChannels = {0};
    OrderByOperator operator =
        new OrderByOperator(
            CTX,
            new SimplePageWithPositionComparator(List.of(0), List.of(SortOrder.ASC_NULLS_FIRST)),
            outputChannels);

    long[] values = {3, 0, 1, 0, 2};
    boolean[] nulls = {false, true, false, true, false};
    operator.addInput(new Page(new LongArrayBlock(5, Optional.of(nulls), values)));
    operator.finish();

    List<Long> result = new ArrayList<>();
    List<Boolean> resultNulls = new ArrayList<>();
    Page page;
    while ((page = operator.getOutput()) != null) {
      LongArrayBlock block = (LongArrayBlock) page.getBlock(0);
      for (int i = 0; i < page.getPositionCount(); i++) {
        resultNulls.add(block.isNull(i));
        result.add(block.isNull(i) ? null : block.getLong(i));
      }
    }
    assertEquals(5, result.size());
    assertTrue(resultNulls.get(0));
    assertTrue(resultNulls.get(1));
    assertEquals(1L, result.get(2));
    assertEquals(2L, result.get(3));
    assertEquals(3L, result.get(4));
  }

  @Test
  @DisplayName("Large input requiring multiple pages")
  void testLargeInput() {
    int[] outputChannels = {0};
    OrderByOperator operator =
        new OrderByOperator(
            CTX,
            new SimplePageWithPositionComparator(List.of(0), List.of(SortOrder.ASC_NULLS_LAST)),
            outputChannels);

    // Add 5000 rows in 5 pages
    for (int p = 0; p < 5; p++) {
      long[] vals = new long[1000];
      for (int i = 0; i < 1000; i++) {
        vals[i] = 5000 - (p * 1000 + i); // descending
      }
      operator.addInput(createLongPage(vals));
    }
    operator.finish();

    List<Long> values = collectLongOutput(operator);
    assertEquals(5000, values.size());
    for (int i = 1; i < values.size(); i++) {
      assertTrue(values.get(i) >= values.get(i - 1), "Not sorted at index " + i);
    }
    assertEquals(1L, values.get(0));
    assertEquals(5000L, values.get(values.size() - 1));
  }

  @Test
  @DisplayName("Empty input")
  void testEmptyInput() {
    int[] outputChannels = {0};
    OrderByOperator operator =
        new OrderByOperator(
            CTX,
            new SimplePageWithPositionComparator(List.of(0), List.of(SortOrder.ASC_NULLS_LAST)),
            outputChannels);

    operator.finish();
    assertNull(operator.getOutput());
    assertTrue(operator.isFinished());
  }

  // --- Helper methods ---

  private static Page createLongPage(long... values) {
    return new Page(new LongArrayBlock(values.length, Optional.empty(), values));
  }

  private static List<Long> collectLongOutput(Operator operator) {
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
