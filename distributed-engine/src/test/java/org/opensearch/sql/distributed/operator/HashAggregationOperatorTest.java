/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.distributed.operator;

import static org.junit.jupiter.api.Assertions.*;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.opensearch.sql.distributed.context.OperatorContext;
import org.opensearch.sql.distributed.data.DoubleArrayBlock;
import org.opensearch.sql.distributed.data.LongArrayBlock;
import org.opensearch.sql.distributed.data.Page;
import org.opensearch.sql.distributed.operator.aggregation.AvgAccumulator;
import org.opensearch.sql.distributed.operator.aggregation.CountAccumulator;
import org.opensearch.sql.distributed.operator.aggregation.MaxAccumulator;
import org.opensearch.sql.distributed.operator.aggregation.MinAccumulator;
import org.opensearch.sql.distributed.operator.aggregation.SumAccumulator;

class HashAggregationOperatorTest {

  private static final OperatorContext CTX = new OperatorContext(0, "HashAgg");

  @Test
  @DisplayName("COUNT(*) with single group key")
  void testCountStar() {
    // Schema: col0=groupKey, col1=value
    // Groups: 1 appears 3 times, 2 appears 2 times
    HashAggregationOperator operator =
        createAggOperator(
            new int[] {0}, // group by col0
            new int[] {-1}, // -1 for COUNT(*)
            List.of(new CountAccumulator(true)));

    long[] groupKeys = {1, 2, 1, 2, 1};
    long[] values = {10, 20, 30, 40, 50};
    operator.addInput(
        new Page(
            new LongArrayBlock(5, Optional.empty(), groupKeys),
            new LongArrayBlock(5, Optional.empty(), values)));
    operator.finish();

    Page output = operator.getOutput();
    assertNotNull(output);
    assertEquals(2, output.getPositionCount());

    // Extract results as map: groupKey -> count
    Map<Long, Long> results = new HashMap<>();
    LongArrayBlock keys = (LongArrayBlock) output.getBlock(0);
    LongArrayBlock counts = (LongArrayBlock) output.getBlock(1);
    for (int i = 0; i < output.getPositionCount(); i++) {
      results.put(keys.getLong(i), counts.getLong(i));
    }
    assertEquals(3L, results.get(1L));
    assertEquals(2L, results.get(2L));
  }

  @Test
  @DisplayName("SUM with single group key")
  void testSum() {
    HashAggregationOperator operator =
        createAggOperator(new int[] {0}, new int[] {1}, List.of(new SumAccumulator()));

    long[] groupKeys = {1, 2, 1, 2, 1};
    long[] values = {10, 20, 30, 40, 50};
    operator.addInput(
        new Page(
            new LongArrayBlock(5, Optional.empty(), groupKeys),
            new LongArrayBlock(5, Optional.empty(), values)));
    operator.finish();

    Page output = operator.getOutput();
    assertNotNull(output);

    Map<Long, Double> results = new HashMap<>();
    LongArrayBlock keys = (LongArrayBlock) output.getBlock(0);
    DoubleArrayBlock sums = (DoubleArrayBlock) output.getBlock(1);
    for (int i = 0; i < output.getPositionCount(); i++) {
      results.put(keys.getLong(i), sums.getDouble(i));
    }
    assertEquals(90.0, results.get(1L)); // 10+30+50
    assertEquals(60.0, results.get(2L)); // 20+40
  }

  @Test
  @DisplayName("AVG with single group key")
  void testAvg() {
    HashAggregationOperator operator =
        createAggOperator(new int[] {0}, new int[] {1}, List.of(new AvgAccumulator()));

    long[] groupKeys = {1, 2, 1, 2, 1};
    long[] values = {10, 20, 30, 40, 50};
    operator.addInput(
        new Page(
            new LongArrayBlock(5, Optional.empty(), groupKeys),
            new LongArrayBlock(5, Optional.empty(), values)));
    operator.finish();

    Page output = operator.getOutput();
    assertNotNull(output);

    Map<Long, Double> results = new HashMap<>();
    LongArrayBlock keys = (LongArrayBlock) output.getBlock(0);
    DoubleArrayBlock avgs = (DoubleArrayBlock) output.getBlock(1);
    for (int i = 0; i < output.getPositionCount(); i++) {
      results.put(keys.getLong(i), avgs.getDouble(i));
    }
    assertEquals(30.0, results.get(1L)); // (10+30+50)/3
    assertEquals(30.0, results.get(2L)); // (20+40)/2
  }

  @Test
  @DisplayName("MIN and MAX")
  void testMinMax() {
    HashAggregationOperator operator =
        createAggOperator(
            new int[] {0}, new int[] {1, 1}, List.of(new MinAccumulator(), new MaxAccumulator()));

    long[] groupKeys = {1, 2, 1, 2, 1};
    long[] values = {50, 20, 10, 40, 30};
    operator.addInput(
        new Page(
            new LongArrayBlock(5, Optional.empty(), groupKeys),
            new LongArrayBlock(5, Optional.empty(), values)));
    operator.finish();

    Page output = operator.getOutput();
    assertNotNull(output);

    LongArrayBlock keys = (LongArrayBlock) output.getBlock(0);
    DoubleArrayBlock mins = (DoubleArrayBlock) output.getBlock(1);
    DoubleArrayBlock maxes = (DoubleArrayBlock) output.getBlock(2);

    Map<Long, Double> minResults = new HashMap<>();
    Map<Long, Double> maxResults = new HashMap<>();
    for (int i = 0; i < output.getPositionCount(); i++) {
      minResults.put(keys.getLong(i), mins.getDouble(i));
      maxResults.put(keys.getLong(i), maxes.getDouble(i));
    }
    assertEquals(10.0, minResults.get(1L));
    assertEquals(50.0, maxResults.get(1L));
    assertEquals(20.0, minResults.get(2L));
    assertEquals(40.0, maxResults.get(2L));
  }

  @Test
  @DisplayName("Null keys form their own group")
  void testNullKeys() {
    HashAggregationOperator operator =
        createAggOperator(new int[] {0}, new int[] {-1}, List.of(new CountAccumulator(true)));

    long[] groupKeys = {1, 0, 1, 0, 2};
    boolean[] nulls = {false, true, false, true, false};
    operator.addInput(
        new Page(
            new LongArrayBlock(5, Optional.of(nulls), groupKeys),
            new LongArrayBlock(5, Optional.empty(), new long[] {10, 20, 30, 40, 50})));
    operator.finish();

    Page output = operator.getOutput();
    assertNotNull(output);
    assertEquals(3, output.getPositionCount()); // groups: 1, null, 2
  }

  @Test
  @DisplayName("High-cardinality groups")
  void testHighCardinalityGroups() {
    HashAggregationOperator operator =
        createAggOperator(new int[] {0}, new int[] {-1}, List.of(new CountAccumulator(true)));

    // 1000 unique groups
    long[] groupKeys = new long[1000];
    for (int i = 0; i < 1000; i++) {
      groupKeys[i] = i;
    }
    operator.addInput(
        new Page(
            new LongArrayBlock(1000, Optional.empty(), groupKeys),
            new LongArrayBlock(1000, Optional.empty(), new long[1000])));
    operator.finish();

    Page output = operator.getOutput();
    assertNotNull(output);
    assertEquals(1000, output.getPositionCount());
  }

  @Test
  @DisplayName("Multi-column group key")
  void testMultiColumnGroupKey() {
    HashAggregationOperator operator =
        new HashAggregationOperator(
            CTX,
            new int[] {0, 1}, // group by col0 AND col1
            new int[] {-1}, // COUNT(*)
            List.of(new CountAccumulator(true)),
            16);

    // (1,A), (1,B), (1,A), (2,A)
    long[] col0 = {1, 1, 1, 2};
    long[] col1 = {10, 20, 10, 10};
    operator.addInput(
        new Page(
            new LongArrayBlock(4, Optional.empty(), col0),
            new LongArrayBlock(4, Optional.empty(), col1)));
    operator.finish();

    Page output = operator.getOutput();
    assertNotNull(output);
    assertEquals(3, output.getPositionCount()); // 3 unique (col0,col1) combos
  }

  @Test
  @DisplayName("Empty input")
  void testEmptyInput() {
    HashAggregationOperator operator =
        createAggOperator(new int[] {0}, new int[] {-1}, List.of(new CountAccumulator(true)));

    operator.finish();
    Page output = operator.getOutput();
    assertNull(output); // no groups -> null output
    assertTrue(operator.isFinished());
  }

  @Test
  @DisplayName("Global aggregation: COUNT(*) with no group-by key")
  void testGlobalCountStar() {
    // stats count() — no group-by channels
    HashAggregationOperator operator =
        new HashAggregationOperator(
            CTX,
            new int[] {}, // no group-by
            new int[] {-1}, // COUNT(*)
            List.of(new CountAccumulator(true)),
            1);

    long[] values = {10, 20, 30, 40, 50};
    operator.addInput(new Page(new LongArrayBlock(5, Optional.empty(), values)));
    operator.finish();

    Page output = operator.getOutput();
    assertNotNull(output);
    assertEquals(1, output.getPositionCount());
    // No key blocks, only aggregation block
    assertEquals(1, output.getChannelCount());
    LongArrayBlock counts = (LongArrayBlock) output.getBlock(0);
    assertEquals(5L, counts.getLong(0));
  }

  @Test
  @DisplayName("Global aggregation on empty input returns default row")
  void testGlobalAggEmptyInput() {
    // stats count(), avg(field) on empty table
    HashAggregationOperator operator =
        new HashAggregationOperator(
            CTX,
            new int[] {}, // no group-by (global)
            new int[] {-1, -1}, // COUNT(*), AVG (no input column for this test)
            List.of(new CountAccumulator(true), new AvgAccumulator()),
            1);

    // No input added
    operator.finish();

    Page output = operator.getOutput();
    assertNotNull(output, "Global aggregation on empty input must produce a row");
    assertEquals(1, output.getPositionCount());
    assertEquals(2, output.getChannelCount());

    // COUNT(*) on empty → 0
    LongArrayBlock countBlock = (LongArrayBlock) output.getBlock(0);
    assertEquals(0L, countBlock.getLong(0));

    // AVG on empty → null
    DoubleArrayBlock avgBlock = (DoubleArrayBlock) output.getBlock(1);
    assertTrue(avgBlock.isNull(0));
  }

  @Test
  @DisplayName("Global aggregation: multiple aggregations, no group-by")
  void testGlobalMultipleAggs() {
    // stats count(), sum(val), avg(val), min(val), max(val)
    HashAggregationOperator operator =
        new HashAggregationOperator(
            CTX,
            new int[] {},
            new int[] {-1, 0, 0, 0, 0},
            List.of(
                new CountAccumulator(true),
                new SumAccumulator(),
                new AvgAccumulator(),
                new MinAccumulator(),
                new MaxAccumulator()),
            1);

    long[] values = {10, 20, 30};
    operator.addInput(new Page(new LongArrayBlock(3, Optional.empty(), values)));
    operator.finish();

    Page output = operator.getOutput();
    assertNotNull(output);
    assertEquals(1, output.getPositionCount());
    assertEquals(5, output.getChannelCount());

    assertEquals(3L, ((LongArrayBlock) output.getBlock(0)).getLong(0)); // COUNT
    assertEquals(60.0, ((DoubleArrayBlock) output.getBlock(1)).getDouble(0)); // SUM
    assertEquals(20.0, ((DoubleArrayBlock) output.getBlock(2)).getDouble(0)); // AVG
    assertEquals(10.0, ((DoubleArrayBlock) output.getBlock(3)).getDouble(0)); // MIN
    assertEquals(30.0, ((DoubleArrayBlock) output.getBlock(4)).getDouble(0)); // MAX
  }

  // --- Helper methods ---

  private static HashAggregationOperator createAggOperator(
      int[] groupByChannels,
      int[] aggregateInputChannels,
      List<org.opensearch.sql.distributed.operator.aggregation.Accumulator> accumulators) {
    return new HashAggregationOperator(
        CTX, groupByChannels, aggregateInputChannels, accumulators, 16);
  }
}
