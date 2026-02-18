/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.distributed.operator;

import static org.junit.jupiter.api.Assertions.*;

import java.util.List;
import java.util.Optional;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.opensearch.sql.distributed.context.OperatorContext;
import org.opensearch.sql.distributed.data.LongArrayBlock;
import org.opensearch.sql.distributed.data.Page;

class FilterAndProjectOperatorTest {

  private static final OperatorContext CTX = new OperatorContext(0, "FilterAndProject");

  @Test
  @DisplayName("Filter true: all rows pass")
  void testFilterAllTrue() {
    FilterAndProjectOperator operator =
        new FilterAndProjectOperator(CTX, PageFilter.ALWAYS_TRUE, List.of(new ColumnProjection(0)));

    Page input = createLongPage(1, 2, 3);
    operator.addInput(input);

    Page output = operator.getOutput();
    assertNotNull(output);
    assertEquals(3, output.getPositionCount());
    LongArrayBlock block = (LongArrayBlock) output.getBlock(0);
    assertEquals(1L, block.getLong(0));
    assertEquals(2L, block.getLong(1));
    assertEquals(3L, block.getLong(2));
  }

  @Test
  @DisplayName("Filter false: no rows pass")
  void testFilterAllFalse() {
    FilterAndProjectOperator operator =
        new FilterAndProjectOperator(
            CTX,
            page -> new boolean[page.getPositionCount()], // all false
            List.of(new ColumnProjection(0)));

    operator.addInput(createLongPage(1, 2, 3));
    assertNull(operator.getOutput());
  }

  @Test
  @DisplayName("Filter with null handling")
  void testFilterWithNulls() {
    FilterAndProjectOperator operator =
        new FilterAndProjectOperator(
            CTX,
            page -> {
              boolean[] result = new boolean[page.getPositionCount()];
              LongArrayBlock block = (LongArrayBlock) page.getBlock(0);
              for (int i = 0; i < page.getPositionCount(); i++) {
                result[i] = !block.isNull(i) && block.getLong(i) > 2;
              }
              return result;
            },
            List.of(new ColumnProjection(0)));

    long[] values = {1, 0, 3, 0, 5};
    boolean[] nulls = {false, true, false, true, false};
    operator.addInput(new Page(new LongArrayBlock(5, Optional.of(nulls), values)));

    Page output = operator.getOutput();
    assertNotNull(output);
    assertEquals(2, output.getPositionCount());
    LongArrayBlock block = (LongArrayBlock) output.getBlock(0);
    assertEquals(3L, block.getLong(0));
    assertEquals(5L, block.getLong(1));
  }

  @Test
  @DisplayName("Project column subset")
  void testProjectColumnSubset() {
    FilterAndProjectOperator operator =
        new FilterAndProjectOperator(
            CTX, PageFilter.ALWAYS_TRUE, List.of(new ColumnProjection(1))); // only column 1

    long[] col0 = {1, 2, 3};
    long[] col1 = {10, 20, 30};
    operator.addInput(
        new Page(
            new LongArrayBlock(3, Optional.empty(), col0),
            new LongArrayBlock(3, Optional.empty(), col1)));

    Page output = operator.getOutput();
    assertNotNull(output);
    assertEquals(1, output.getChannelCount());
    assertEquals(3, output.getPositionCount());
    LongArrayBlock block = (LongArrayBlock) output.getBlock(0);
    assertEquals(10L, block.getLong(0));
    assertEquals(20L, block.getLong(1));
    assertEquals(30L, block.getLong(2));
  }

  @Test
  @DisplayName("Empty page input")
  void testEmptyPageInput() {
    FilterAndProjectOperator operator =
        new FilterAndProjectOperator(CTX, PageFilter.ALWAYS_TRUE, List.of(new ColumnProjection(0)));

    Page emptyPage = new Page(0, new LongArrayBlock(0, Optional.empty(), new long[0]));
    operator.addInput(emptyPage);
    assertNull(operator.getOutput()); // empty pages produce no output
  }

  @Test
  @DisplayName("Operator lifecycle")
  void testLifecycle() {
    FilterAndProjectOperator operator =
        new FilterAndProjectOperator(CTX, PageFilter.ALWAYS_TRUE, List.of(new ColumnProjection(0)));

    assertTrue(operator.needsInput());
    assertFalse(operator.isFinished());

    operator.addInput(createLongPage(1));
    assertFalse(operator.needsInput()); // has pending output

    assertNotNull(operator.getOutput());
    assertTrue(operator.needsInput()); // can accept more input

    operator.finish();
    assertTrue(operator.isFinished());
  }

  // --- Helper ---

  private static Page createLongPage(long... values) {
    return new Page(new LongArrayBlock(values.length, Optional.empty(), values));
  }
}
