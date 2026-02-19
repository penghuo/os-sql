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
import org.opensearch.sql.distributed.operator.window.AggregateWindowFunction;
import org.opensearch.sql.distributed.operator.window.DenseRankFunction;
import org.opensearch.sql.distributed.operator.window.LagFunction;
import org.opensearch.sql.distributed.operator.window.LeadFunction;
import org.opensearch.sql.distributed.operator.window.RankFunction;
import org.opensearch.sql.distributed.operator.window.RowNumberFunction;
import org.opensearch.sql.distributed.operator.window.WindowOperator;

/**
 * Tests for WindowOperator: ROW_NUMBER, RANK, DENSE_RANK, LAG/LEAD, SUM OVER, ROWS frame, RANGE
 * frame, single partition, multiple partitions.
 */
class WindowOperatorTest {

  private static final OperatorContext CTX = new OperatorContext(0, "Window");

  /** Collects all output pages from a WindowOperator. */
  private List<Page> collectOutput(WindowOperator op) {
    op.finish();
    List<Page> pages = new ArrayList<>();
    Page output;
    while ((output = op.getOutput()) != null) {
      pages.add(output);
    }
    assertTrue(op.isFinished());
    return pages;
  }

  /** Extracts all long values from a specific channel across multiple pages. */
  private long[] collectLongs(List<Page> pages, int channel) {
    int total = pages.stream().mapToInt(Page::getPositionCount).sum();
    long[] result = new long[total];
    int idx = 0;
    for (Page page : pages) {
      LongArrayBlock block = (LongArrayBlock) page.getBlock(channel);
      for (int i = 0; i < page.getPositionCount(); i++) {
        result[idx++] = block.getLong(i);
      }
    }
    return result;
  }

  /** Checks if a position is null across pages for a given channel. */
  private boolean[] collectNulls(List<Page> pages, int channel) {
    int total = pages.stream().mapToInt(Page::getPositionCount).sum();
    boolean[] result = new boolean[total];
    int idx = 0;
    for (Page page : pages) {
      for (int i = 0; i < page.getPositionCount(); i++) {
        result[idx++] = page.getBlock(channel).isNull(i);
      }
    }
    return result;
  }

  // ==================== ROW_NUMBER ====================

  @Test
  @DisplayName("ROW_NUMBER: assigns sequential numbers within each partition")
  void rowNumberWithinPartitions() {
    WindowOperator op =
        new WindowOperator(
            CTX,
            new int[] {0}, // PARTITION BY dept (col 0)
            new int[] {1}, // ORDER BY salary (col 1)
            List.of(new RowNumberFunction()));

    // dept=1: salary=100,200,300; dept=2: salary=150,250
    op.addInput(
        new Page(
            new LongArrayBlock(5, Optional.empty(), new long[] {1, 1, 1, 2, 2}),
            new LongArrayBlock(5, Optional.empty(), new long[] {100, 200, 300, 150, 250})));

    List<Page> output = collectOutput(op);
    // Output has 3 channels: dept, salary, row_number
    long[] rowNums = collectLongs(output, 2);
    // dept=1: 1,2,3; dept=2: 1,2
    assertArrayEquals(new long[] {1, 2, 3, 1, 2}, rowNums);

    op.close();
  }

  @Test
  @DisplayName("ROW_NUMBER: single partition (no PARTITION BY)")
  void rowNumberSinglePartition() {
    WindowOperator op =
        new WindowOperator(
            CTX,
            new int[] {}, // no PARTITION BY
            new int[] {0}, // ORDER BY col 0
            List.of(new RowNumberFunction()));

    op.addInput(new Page(new LongArrayBlock(4, Optional.empty(), new long[] {10, 20, 30, 40})));

    List<Page> output = collectOutput(op);
    long[] rowNums = collectLongs(output, 1); // appended after col 0
    assertArrayEquals(new long[] {1, 2, 3, 4}, rowNums);

    op.close();
  }

  // ==================== RANK ====================

  @Test
  @DisplayName("RANK: ties get same rank, gaps after ties")
  void rankWithTies() {
    WindowOperator op =
        new WindowOperator(
            CTX,
            new int[] {}, // single partition
            new int[] {0}, // ORDER BY col 0
            List.of(new RankFunction()));

    // Values: 100, 100, 200, 300, 300
    op.addInput(
        new Page(new LongArrayBlock(5, Optional.empty(), new long[] {100, 100, 200, 300, 300})));

    List<Page> output = collectOutput(op);
    long[] ranks = collectLongs(output, 1);
    // Ranks: 1, 1, 3, 4, 4
    assertArrayEquals(new long[] {1, 1, 3, 4, 4}, ranks);

    op.close();
  }

  @Test
  @DisplayName("RANK: no ties produces sequential ranks")
  void rankNoTies() {
    WindowOperator op =
        new WindowOperator(CTX, new int[] {}, new int[] {0}, List.of(new RankFunction()));

    op.addInput(new Page(new LongArrayBlock(3, Optional.empty(), new long[] {10, 20, 30})));

    List<Page> output = collectOutput(op);
    long[] ranks = collectLongs(output, 1);
    assertArrayEquals(new long[] {1, 2, 3}, ranks);

    op.close();
  }

  // ==================== DENSE_RANK ====================

  @Test
  @DisplayName("DENSE_RANK: ties get same rank, no gaps")
  void denseRankWithTies() {
    WindowOperator op =
        new WindowOperator(CTX, new int[] {}, new int[] {0}, List.of(new DenseRankFunction()));

    // Values: 100, 100, 200, 300, 300
    op.addInput(
        new Page(new LongArrayBlock(5, Optional.empty(), new long[] {100, 100, 200, 300, 300})));

    List<Page> output = collectOutput(op);
    long[] ranks = collectLongs(output, 1);
    // Dense ranks: 1, 1, 2, 3, 3
    assertArrayEquals(new long[] {1, 1, 2, 3, 3}, ranks);

    op.close();
  }

  // ==================== LAG / LEAD ====================

  @Test
  @DisplayName("LAG: returns previous row value within partition")
  void lagWithinPartition() {
    WindowOperator op =
        new WindowOperator(
            CTX,
            new int[] {0}, // PARTITION BY dept
            new int[] {1}, // ORDER BY salary
            List.of(new LagFunction(1, 1, false))); // LAG(salary, 1)

    // dept=1: salary=100,200,300
    op.addInput(
        new Page(
            new LongArrayBlock(3, Optional.empty(), new long[] {1, 1, 1}),
            new LongArrayBlock(3, Optional.empty(), new long[] {100, 200, 300})));

    List<Page> output = collectOutput(op);
    boolean[] nulls = collectNulls(output, 2);
    long[] values = collectLongs(output, 2);
    // LAG(salary,1): null, 100, 200
    assertTrue(nulls[0], "First row LAG should be null");
    assertEquals(100L, values[1]);
    assertEquals(200L, values[2]);

    op.close();
  }

  @Test
  @DisplayName("LEAD: returns next row value within partition")
  void leadWithinPartition() {
    WindowOperator op =
        new WindowOperator(
            CTX,
            new int[] {0}, // PARTITION BY dept
            new int[] {1}, // ORDER BY salary
            List.of(new LeadFunction(1, 1, false))); // LEAD(salary, 1)

    op.addInput(
        new Page(
            new LongArrayBlock(3, Optional.empty(), new long[] {1, 1, 1}),
            new LongArrayBlock(3, Optional.empty(), new long[] {100, 200, 300})));

    List<Page> output = collectOutput(op);
    boolean[] nulls = collectNulls(output, 2);
    long[] values = collectLongs(output, 2);
    // LEAD(salary,1): 200, 300, null
    assertEquals(200L, values[0]);
    assertEquals(300L, values[1]);
    assertTrue(nulls[2], "Last row LEAD should be null");

    op.close();
  }

  @Test
  @DisplayName("LAG with offset 2")
  void lagWithOffset2() {
    WindowOperator op =
        new WindowOperator(
            CTX,
            new int[] {},
            new int[] {0},
            List.of(new LagFunction(0, 2, false))); // LAG(col0, 2)

    op.addInput(new Page(new LongArrayBlock(4, Optional.empty(), new long[] {10, 20, 30, 40})));

    List<Page> output = collectOutput(op);
    boolean[] nulls = collectNulls(output, 1);
    long[] values = collectLongs(output, 1);
    // LAG(col0,2): null, null, 10, 20
    assertTrue(nulls[0]);
    assertTrue(nulls[1]);
    assertEquals(10L, values[2]);
    assertEquals(20L, values[3]);

    op.close();
  }

  // ==================== SUM OVER ====================

  @Test
  @DisplayName("SUM OVER: running sum within partition")
  void sumOverRunningSum() {
    WindowOperator op =
        new WindowOperator(
            CTX,
            new int[] {0},
            new int[] {1},
            List.of(
                new AggregateWindowFunction(
                    AggregateWindowFunction.AggregateType.SUM,
                    1,
                    AggregateWindowFunction.FrameType.ROWS_UNBOUNDED_PRECEDING_TO_CURRENT_ROW,
                    false))); // SUM(salary) long output

    // dept=1: salary=100,200,300
    op.addInput(
        new Page(
            new LongArrayBlock(3, Optional.empty(), new long[] {1, 1, 1}),
            new LongArrayBlock(3, Optional.empty(), new long[] {100, 200, 300})));

    List<Page> output = collectOutput(op);
    long[] sums = collectLongs(output, 2);
    // Running sum: 100, 300, 600
    assertArrayEquals(new long[] {100, 300, 600}, sums);

    op.close();
  }

  @Test
  @DisplayName("SUM OVER: total partition sum (no ORDER BY)")
  void sumOverTotalPartition() {
    WindowOperator op =
        new WindowOperator(
            CTX,
            new int[] {0},
            new int[] {},
            List.of(
                new AggregateWindowFunction(
                    AggregateWindowFunction.AggregateType.SUM,
                    1,
                    AggregateWindowFunction.FrameType
                        .ROWS_UNBOUNDED_PRECEDING_TO_UNBOUNDED_FOLLOWING,
                    false)));

    // dept=1: salary=100,200,300; dept=2: salary=150,250
    op.addInput(
        new Page(
            new LongArrayBlock(5, Optional.empty(), new long[] {1, 1, 1, 2, 2}),
            new LongArrayBlock(5, Optional.empty(), new long[] {100, 200, 300, 150, 250})));

    List<Page> output = collectOutput(op);
    long[] sums = collectLongs(output, 2);
    // dept=1 total=600, dept=2 total=400
    assertArrayEquals(new long[] {600, 600, 600, 400, 400}, sums);

    op.close();
  }

  // ==================== EDGE CASES ====================

  @Test
  @DisplayName("Empty input produces no output")
  void emptyInput() {
    WindowOperator op =
        new WindowOperator(CTX, new int[] {}, new int[] {0}, List.of(new RowNumberFunction()));

    List<Page> output = collectOutput(op);
    assertTrue(output.isEmpty());

    op.close();
  }

  @Test
  @DisplayName("Single row per partition")
  void singleRowPartition() {
    WindowOperator op =
        new WindowOperator(
            CTX, new int[] {}, new int[] {0}, List.of(new RowNumberFunction(), new RankFunction()));

    op.addInput(new Page(new LongArrayBlock(1, Optional.empty(), new long[] {42})));

    List<Page> output = collectOutput(op);
    // Output: col0, row_number, rank
    long[] rowNums = collectLongs(output, 1);
    long[] ranks = collectLongs(output, 2);
    assertArrayEquals(new long[] {1}, rowNums);
    assertArrayEquals(new long[] {1}, ranks);

    op.close();
  }

  @Test
  @DisplayName("Multiple window functions in same query")
  void multipleWindowFunctions() {
    WindowOperator op =
        new WindowOperator(
            CTX,
            new int[] {},
            new int[] {0},
            List.of(new RowNumberFunction(), new RankFunction(), new DenseRankFunction()));

    // Values: 10, 10, 20
    op.addInput(new Page(new LongArrayBlock(3, Optional.empty(), new long[] {10, 10, 20})));

    List<Page> output = collectOutput(op);
    // Output: col0, row_number, rank, dense_rank
    long[] rowNums = collectLongs(output, 1);
    long[] ranks = collectLongs(output, 2);
    long[] denseRanks = collectLongs(output, 3);

    assertArrayEquals(new long[] {1, 2, 3}, rowNums);
    assertArrayEquals(new long[] {1, 1, 3}, ranks);
    assertArrayEquals(new long[] {1, 1, 2}, denseRanks);

    op.close();
  }

  @Test
  @DisplayName("Operator lifecycle contract")
  void operatorLifecycle() {
    WindowOperator op =
        new WindowOperator(CTX, new int[] {}, new int[] {0}, List.of(new RowNumberFunction()));

    // Initial state
    assertTrue(op.needsInput());
    assertFalse(op.isFinished());
    assertTrue(op.isBlocked().isDone());
    assertNull(op.getOutput(), "No output before finish");

    // Add input
    op.addInput(new Page(new LongArrayBlock(2, Optional.empty(), new long[] {1, 2})));
    assertTrue(op.needsInput());

    // Finish triggers processing
    op.finish();
    assertFalse(op.needsInput());

    // Drain output
    Page output = op.getOutput();
    assertNotNull(output);
    assertTrue(op.isFinished());

    op.close();
  }

  @Test
  @DisplayName("addInput throws after finish")
  void addInputThrowsAfterFinish() {
    WindowOperator op =
        new WindowOperator(CTX, new int[] {}, new int[] {0}, List.of(new RowNumberFunction()));

    op.finish();

    assertThrows(
        IllegalStateException.class,
        () -> op.addInput(new Page(new LongArrayBlock(1, Optional.empty(), new long[] {1}))));

    op.close();
  }

  @Test
  @DisplayName("Empty page is skipped")
  void emptyPageSkipped() {
    WindowOperator op =
        new WindowOperator(CTX, new int[] {}, new int[] {0}, List.of(new RowNumberFunction()));

    op.addInput(new Page(new LongArrayBlock(0, Optional.empty(), new long[0])));
    op.addInput(new Page(new LongArrayBlock(2, Optional.empty(), new long[] {10, 20})));

    List<Page> output = collectOutput(op);
    long[] rowNums = collectLongs(output, 1);
    assertArrayEquals(new long[] {1, 2}, rowNums);

    op.close();
  }
}
