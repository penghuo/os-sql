/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.dqe.exchange;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.mockito.Mockito.mock;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class MergeAggregateExchangeTest {

  private final RelNode mockPlan = mock(RelNode.class);
  private final RelDataType mockRowType = mock(RelDataType.class);

  @Test
  @DisplayName("COUNT merge: SUM of partial counts across shards")
  void testCountMerge() {
    // groupCount=1 (group by col 0), one aggregate: SUM_COUNTS on col 1
    MergeAggregateExchange exchange =
        new MergeAggregateExchange(
            mockPlan,
            mockRowType,
            Collections.singletonList(MergeFunction.SUM_COUNTS),
            1);

    // Shard 0: group "A" count=5, group "B" count=3
    // Shard 1: group "A" count=7, group "B" count=2
    List<Object[]> shard0 = Arrays.asList(new Object[] {"A", 5L}, new Object[] {"B", 3L});
    List<Object[]> shard1 = Arrays.asList(new Object[] {"A", 7L}, new Object[] {"B", 2L});

    exchange.setShardResults(
        Arrays.asList(
            new ShardResult(0, shard0, mockRowType), new ShardResult(1, shard1, mockRowType)));

    List<Object[]> result = toList(exchange.scan());
    assertEquals(2, result.size());

    // Find group A and B
    Object[] groupA = findGroup(result, "A");
    Object[] groupB = findGroup(result, "B");
    assertEquals(12L, groupA[1]); // 5 + 7
    assertEquals(5L, groupB[1]); // 3 + 2
  }

  @Test
  @DisplayName("SUM merge: SUM of partial sums across shards")
  void testSumMerge() {
    MergeAggregateExchange exchange =
        new MergeAggregateExchange(
            mockPlan,
            mockRowType,
            Collections.singletonList(MergeFunction.SUM_SUMS),
            1);

    List<Object[]> shard0 = Collections.singletonList(new Object[] {"X", 100.5});
    List<Object[]> shard1 = Collections.singletonList(new Object[] {"X", 200.3});

    exchange.setShardResults(
        Arrays.asList(
            new ShardResult(0, shard0, mockRowType), new ShardResult(1, shard1, mockRowType)));

    List<Object[]> result = toList(exchange.scan());
    assertEquals(1, result.size());
    assertEquals(300.8, (Double) result.get(0)[1], 0.001);
  }

  @Test
  @DisplayName("MIN merge: MIN of partial mins across shards")
  void testMinMerge() {
    MergeAggregateExchange exchange =
        new MergeAggregateExchange(
            mockPlan,
            mockRowType,
            Collections.singletonList(MergeFunction.MIN_OF),
            1);

    List<Object[]> shard0 = Collections.singletonList(new Object[] {"G1", 10});
    List<Object[]> shard1 = Collections.singletonList(new Object[] {"G1", 3});
    List<Object[]> shard2 = Collections.singletonList(new Object[] {"G1", 7});

    exchange.setShardResults(
        Arrays.asList(
            new ShardResult(0, shard0, mockRowType),
            new ShardResult(1, shard1, mockRowType),
            new ShardResult(2, shard2, mockRowType)));

    List<Object[]> result = toList(exchange.scan());
    assertEquals(1, result.size());
    assertEquals(3, result.get(0)[1]);
  }

  @Test
  @DisplayName("MAX merge: MAX of partial maxes across shards")
  void testMaxMerge() {
    MergeAggregateExchange exchange =
        new MergeAggregateExchange(
            mockPlan,
            mockRowType,
            Collections.singletonList(MergeFunction.MAX_OF),
            1);

    List<Object[]> shard0 = Collections.singletonList(new Object[] {"G1", 10});
    List<Object[]> shard1 = Collections.singletonList(new Object[] {"G1", 25});
    List<Object[]> shard2 = Collections.singletonList(new Object[] {"G1", 7});

    exchange.setShardResults(
        Arrays.asList(
            new ShardResult(0, shard0, mockRowType),
            new ShardResult(1, shard1, mockRowType),
            new ShardResult(2, shard2, mockRowType)));

    List<Object[]> result = toList(exchange.scan());
    assertEquals(1, result.size());
    assertEquals(25, result.get(0)[1]);
  }

  @Test
  @DisplayName("AVG merge: SUM(sums) / SUM(counts) from two input columns per shard")
  void testAvgMerge() {
    // SUM_DIV_COUNT consumes two columns (sum, count) and produces one output column
    MergeAggregateExchange exchange =
        new MergeAggregateExchange(
            mockPlan,
            mockRowType,
            Collections.singletonList(MergeFunction.SUM_DIV_COUNT),
            1);

    // group "A": shard0 sum=30.0 count=3, shard1 sum=50.0 count=5
    // Expected avg = (30+50)/(3+5) = 80/8 = 10.0
    List<Object[]> shard0 = Collections.singletonList(new Object[] {"A", 30.0, 3L});
    List<Object[]> shard1 = Collections.singletonList(new Object[] {"A", 50.0, 5L});

    exchange.setShardResults(
        Arrays.asList(
            new ShardResult(0, shard0, mockRowType), new ShardResult(1, shard1, mockRowType)));

    List<Object[]> result = toList(exchange.scan());
    assertEquals(1, result.size());
    // Output row: [group_key, avg_result]
    assertEquals("A", result.get(0)[0]);
    assertEquals(10.0, (Double) result.get(0)[1], 0.001);
  }

  @Test
  @DisplayName("AVG merge with multiple groups")
  void testAvgMergeMultipleGroups() {
    MergeAggregateExchange exchange =
        new MergeAggregateExchange(
            mockPlan,
            mockRowType,
            Collections.singletonList(MergeFunction.SUM_DIV_COUNT),
            1);

    // Shard 0: group "A" sum=10 count=2, group "B" sum=20 count=4
    // Shard 1: group "A" sum=30 count=3, group "B" sum=40 count=6
    List<Object[]> shard0 =
        Arrays.asList(new Object[] {"A", 10.0, 2L}, new Object[] {"B", 20.0, 4L});
    List<Object[]> shard1 =
        Arrays.asList(new Object[] {"A", 30.0, 3L}, new Object[] {"B", 40.0, 6L});

    exchange.setShardResults(
        Arrays.asList(
            new ShardResult(0, shard0, mockRowType), new ShardResult(1, shard1, mockRowType)));

    List<Object[]> result = toList(exchange.scan());
    assertEquals(2, result.size());

    Object[] groupA = findGroup(result, "A");
    Object[] groupB = findGroup(result, "B");
    assertEquals(8.0, (Double) groupA[1], 0.001); // (10+30)/(2+3) = 40/5 = 8
    assertEquals(6.0, (Double) groupB[1], 0.001); // (20+40)/(4+6) = 60/10 = 6
  }

  @Test
  @DisplayName("NULL handling: all-NULL shard values for COUNT yield 0 (not null)")
  void testAllNullCount() {
    MergeAggregateExchange exchange =
        new MergeAggregateExchange(
            mockPlan,
            mockRowType,
            Collections.singletonList(MergeFunction.SUM_COUNTS),
            1);

    List<Object[]> shard0 = Collections.singletonList(new Object[] {"A", null});
    List<Object[]> shard1 = Collections.singletonList(new Object[] {"A", null});

    exchange.setShardResults(
        Arrays.asList(
            new ShardResult(0, shard0, mockRowType), new ShardResult(1, shard1, mockRowType)));

    List<Object[]> result = toList(exchange.scan());
    assertEquals(1, result.size());
    assertEquals(0L, result.get(0)[1]);
  }

  @Test
  @DisplayName("NULL handling: all-NULL shard values for SUM yield 0 (matches OpenSearch behavior)")
  void testAllNullSum() {
    MergeAggregateExchange exchange =
        new MergeAggregateExchange(
            mockPlan,
            mockRowType,
            Collections.singletonList(MergeFunction.SUM_SUMS),
            1);

    List<Object[]> shard0 = Collections.singletonList(new Object[] {"A", null});
    List<Object[]> shard1 = Collections.singletonList(new Object[] {"A", null});

    exchange.setShardResults(
        Arrays.asList(
            new ShardResult(0, shard0, mockRowType), new ShardResult(1, shard1, mockRowType)));

    List<Object[]> result = toList(exchange.scan());
    assertEquals(1, result.size());
    assertEquals(0L, result.get(0)[1]);
  }

  @Test
  @DisplayName("NULL handling: mixed null and non-null values for SUM")
  void testMixedNullSum() {
    MergeAggregateExchange exchange =
        new MergeAggregateExchange(
            mockPlan,
            mockRowType,
            Collections.singletonList(MergeFunction.SUM_SUMS),
            1);

    List<Object[]> shard0 = Collections.singletonList(new Object[] {"A", null});
    List<Object[]> shard1 = Collections.singletonList(new Object[] {"A", 42.0});

    exchange.setShardResults(
        Arrays.asList(
            new ShardResult(0, shard0, mockRowType), new ShardResult(1, shard1, mockRowType)));

    List<Object[]> result = toList(exchange.scan());
    assertEquals(1, result.size());
    assertEquals(42.0, (Double) result.get(0)[1], 0.001);
  }

  @Test
  @DisplayName("NULL handling: all-NULL shard for MIN yields null")
  void testAllNullMin() {
    MergeAggregateExchange exchange =
        new MergeAggregateExchange(
            mockPlan,
            mockRowType,
            Collections.singletonList(MergeFunction.MIN_OF),
            1);

    List<Object[]> shard0 = Collections.singletonList(new Object[] {"A", null});
    List<Object[]> shard1 = Collections.singletonList(new Object[] {"A", null});

    exchange.setShardResults(
        Arrays.asList(
            new ShardResult(0, shard0, mockRowType), new ShardResult(1, shard1, mockRowType)));

    List<Object[]> result = toList(exchange.scan());
    assertEquals(1, result.size());
    assertNull(result.get(0)[1]);
  }

  @Test
  @DisplayName("NULL handling: mixed null and non-null for MAX")
  void testMixedNullMax() {
    MergeAggregateExchange exchange =
        new MergeAggregateExchange(
            mockPlan,
            mockRowType,
            Collections.singletonList(MergeFunction.MAX_OF),
            1);

    List<Object[]> shard0 = Collections.singletonList(new Object[] {"A", null});
    List<Object[]> shard1 = Collections.singletonList(new Object[] {"A", 99});

    exchange.setShardResults(
        Arrays.asList(
            new ShardResult(0, shard0, mockRowType), new ShardResult(1, shard1, mockRowType)));

    List<Object[]> result = toList(exchange.scan());
    assertEquals(1, result.size());
    assertEquals(99, result.get(0)[1]);
  }

  @Test
  @DisplayName("Empty group on one shard, present on another")
  void testGroupOnlyOnOneShard() {
    MergeAggregateExchange exchange =
        new MergeAggregateExchange(
            mockPlan,
            mockRowType,
            Collections.singletonList(MergeFunction.SUM_COUNTS),
            1);

    // Shard 0: has group "A" and "B"
    // Shard 1: only has group "A"
    List<Object[]> shard0 = Arrays.asList(new Object[] {"A", 5L}, new Object[] {"B", 3L});
    List<Object[]> shard1 = Collections.singletonList(new Object[] {"A", 7L});

    exchange.setShardResults(
        Arrays.asList(
            new ShardResult(0, shard0, mockRowType), new ShardResult(1, shard1, mockRowType)));

    List<Object[]> result = toList(exchange.scan());
    assertEquals(2, result.size());

    Object[] groupA = findGroup(result, "A");
    Object[] groupB = findGroup(result, "B");
    assertEquals(12L, groupA[1]); // 5 + 7
    assertEquals(3L, groupB[1]); // only from shard 0
  }

  @Test
  @DisplayName("No group keys (groupCount=0) merges all rows into a single group")
  void testNoGroupKeys() {
    MergeAggregateExchange exchange =
        new MergeAggregateExchange(
            mockPlan,
            mockRowType,
            Collections.singletonList(MergeFunction.SUM_COUNTS),
            0);

    // No group keys, just aggregate column
    List<Object[]> shard0 = Collections.singletonList(new Object[] {10L});
    List<Object[]> shard1 = Collections.singletonList(new Object[] {20L});

    exchange.setShardResults(
        Arrays.asList(
            new ShardResult(0, shard0, mockRowType), new ShardResult(1, shard1, mockRowType)));

    List<Object[]> result = toList(exchange.scan());
    assertEquals(1, result.size());
    assertEquals(30L, result.get(0)[0]); // 10 + 20
  }

  @Test
  @DisplayName("Multiple aggregate functions in a single exchange")
  void testMultipleMergeFunctions() {
    // groupCount=1, three aggregate functions: SUM_COUNTS, SUM_SUMS, MAX_OF
    MergeAggregateExchange exchange =
        new MergeAggregateExchange(
            mockPlan,
            mockRowType,
            Arrays.asList(MergeFunction.SUM_COUNTS, MergeFunction.SUM_SUMS, MergeFunction.MAX_OF),
            1);

    // Row layout: [group_key, count, sum, max]
    List<Object[]> shard0 = Collections.singletonList(new Object[] {"G1", 5L, 100.0, 50});
    List<Object[]> shard1 = Collections.singletonList(new Object[] {"G1", 3L, 60.0, 80});

    exchange.setShardResults(
        Arrays.asList(
            new ShardResult(0, shard0, mockRowType), new ShardResult(1, shard1, mockRowType)));

    List<Object[]> result = toList(exchange.scan());
    assertEquals(1, result.size());
    assertEquals("G1", result.get(0)[0]);
    assertEquals(8L, result.get(0)[1]); // SUM_COUNTS: 5 + 3
    assertEquals(160.0, (Double) result.get(0)[2], 0.001); // SUM_SUMS: 100 + 60
    assertEquals(80, result.get(0)[3]); // MAX_OF: max(50, 80)
  }

  @Test
  @DisplayName("MergeAggregateExchange with empty shard results")
  void testEmptyShardResults() {
    MergeAggregateExchange exchange =
        new MergeAggregateExchange(
            mockPlan,
            mockRowType,
            Collections.singletonList(MergeFunction.SUM_COUNTS),
            1);

    exchange.setShardResults(Collections.emptyList());

    List<Object[]> result = toList(exchange.scan());
    assertEquals(0, result.size());
  }

  @Test
  @DisplayName("NULL handling: AVG with all null sums and counts")
  void testAvgAllNull() {
    MergeAggregateExchange exchange =
        new MergeAggregateExchange(
            mockPlan,
            mockRowType,
            Collections.singletonList(MergeFunction.SUM_DIV_COUNT),
            1);

    List<Object[]> shard0 = Collections.singletonList(new Object[] {"A", null, null});
    List<Object[]> shard1 = Collections.singletonList(new Object[] {"A", null, null});

    exchange.setShardResults(
        Arrays.asList(
            new ShardResult(0, shard0, mockRowType), new ShardResult(1, shard1, mockRowType)));

    List<Object[]> result = toList(exchange.scan());
    assertEquals(1, result.size());
    assertNull(result.get(0)[1]);
  }

  @Test
  @DisplayName("Integer SUM preserves long type when all inputs are integral")
  void testSumPreservesLongType() {
    MergeAggregateExchange exchange =
        new MergeAggregateExchange(
            mockPlan,
            mockRowType,
            Collections.singletonList(MergeFunction.SUM_SUMS),
            1);

    List<Object[]> shard0 = Collections.singletonList(new Object[] {"A", 10L});
    List<Object[]> shard1 = Collections.singletonList(new Object[] {"A", 20L});

    exchange.setShardResults(
        Arrays.asList(
            new ShardResult(0, shard0, mockRowType), new ShardResult(1, shard1, mockRowType)));

    List<Object[]> result = toList(exchange.scan());
    assertEquals(30L, result.get(0)[1]);
  }

  private Object[] findGroup(List<Object[]> rows, Object groupKey) {
    for (Object[] row : rows) {
      if (groupKey.equals(row[0])) {
        return row;
      }
    }
    throw new AssertionError("Group not found: " + groupKey);
  }

  private List<Object[]> toList(Iterator<Object[]> iter) {
    List<Object[]> result = new ArrayList<>();
    iter.forEachRemaining(result::add);
    return result;
  }
}
