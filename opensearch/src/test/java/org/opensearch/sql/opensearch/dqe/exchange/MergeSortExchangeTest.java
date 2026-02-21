/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.dqe.exchange;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.mockito.Mockito.mock;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.RelFieldCollation.Direction;
import org.apache.calcite.rel.RelFieldCollation.NullDirection;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class MergeSortExchangeTest {

  private final RelNode mockPlan = mock(RelNode.class);
  private final RelDataType mockRowType = mock(RelDataType.class);

  @Test
  @DisplayName("MergeSortExchange with pre-sorted shard results produces global ASC order")
  void testGlobalAscOrder() {
    RelFieldCollation collation =
        new RelFieldCollation(0, Direction.ASCENDING, NullDirection.LAST);
    MergeSortExchange exchange =
        new MergeSortExchange(mockPlan, mockRowType, Collections.singletonList(collation), 0);

    // Shard 0: [1, 3, 5], Shard 1: [2, 4, 6]
    List<Object[]> shard0 =
        Arrays.asList(new Object[] {1, "a"}, new Object[] {3, "c"}, new Object[] {5, "e"});
    List<Object[]> shard1 =
        Arrays.asList(new Object[] {2, "b"}, new Object[] {4, "d"}, new Object[] {6, "f"});

    exchange.setShardResults(
        Arrays.asList(
            new ShardResult(0, shard0, mockRowType), new ShardResult(1, shard1, mockRowType)));

    List<Object[]> result = toList(exchange.scan());
    assertEquals(6, result.size());
    for (int i = 0; i < result.size(); i++) {
      assertEquals(i + 1, result.get(i)[0]);
    }
  }

  @Test
  @DisplayName("MergeSortExchange with DESC order produces globally descending output")
  void testGlobalDescOrder() {
    RelFieldCollation collation =
        new RelFieldCollation(0, Direction.DESCENDING, NullDirection.FIRST);
    MergeSortExchange exchange =
        new MergeSortExchange(mockPlan, mockRowType, Collections.singletonList(collation), 0);

    // Shard 0: [6, 4, 2], Shard 1: [5, 3, 1]
    List<Object[]> shard0 =
        Arrays.asList(new Object[] {6, "f"}, new Object[] {4, "d"}, new Object[] {2, "b"});
    List<Object[]> shard1 =
        Arrays.asList(new Object[] {5, "e"}, new Object[] {3, "c"}, new Object[] {1, "a"});

    exchange.setShardResults(
        Arrays.asList(
            new ShardResult(0, shard0, mockRowType), new ShardResult(1, shard1, mockRowType)));

    List<Object[]> result = toList(exchange.scan());
    assertEquals(6, result.size());
    for (int i = 0; i < result.size(); i++) {
      assertEquals(6 - i, result.get(i)[0]);
    }
  }

  @Test
  @DisplayName("MergeSortExchange with limit returns at most K rows")
  void testWithLimit() {
    RelFieldCollation collation =
        new RelFieldCollation(0, Direction.ASCENDING, NullDirection.LAST);
    MergeSortExchange exchange =
        new MergeSortExchange(mockPlan, mockRowType, Collections.singletonList(collation), 3);

    List<Object[]> shard0 =
        Arrays.asList(new Object[] {1, "a"}, new Object[] {3, "c"}, new Object[] {5, "e"});
    List<Object[]> shard1 =
        Arrays.asList(new Object[] {2, "b"}, new Object[] {4, "d"}, new Object[] {6, "f"});

    exchange.setShardResults(
        Arrays.asList(
            new ShardResult(0, shard0, mockRowType), new ShardResult(1, shard1, mockRowType)));

    List<Object[]> result = toList(exchange.scan());
    assertEquals(3, result.size());
    assertEquals(1, result.get(0)[0]);
    assertEquals(2, result.get(1)[0]);
    assertEquals(3, result.get(2)[0]);
  }

  @Test
  @DisplayName("MergeSortExchange ASC with NULLS FIRST places nulls before non-nulls")
  void testAscNullsFirst() {
    RelFieldCollation collation =
        new RelFieldCollation(0, Direction.ASCENDING, NullDirection.FIRST);
    MergeSortExchange exchange =
        new MergeSortExchange(mockPlan, mockRowType, Collections.singletonList(collation), 0);

    List<Object[]> shard0 = Arrays.asList(new Object[] {null, "x"}, new Object[] {3, "c"});
    List<Object[]> shard1 = Arrays.asList(new Object[] {1, "a"}, new Object[] {5, "e"});

    exchange.setShardResults(
        Arrays.asList(
            new ShardResult(0, shard0, mockRowType), new ShardResult(1, shard1, mockRowType)));

    List<Object[]> result = toList(exchange.scan());
    assertEquals(4, result.size());
    assertNull(result.get(0)[0]);
    assertEquals(1, result.get(1)[0]);
    assertEquals(3, result.get(2)[0]);
    assertEquals(5, result.get(3)[0]);
  }

  @Test
  @DisplayName("MergeSortExchange ASC with NULLS LAST places nulls after non-nulls")
  void testAscNullsLast() {
    RelFieldCollation collation =
        new RelFieldCollation(0, Direction.ASCENDING, NullDirection.LAST);
    MergeSortExchange exchange =
        new MergeSortExchange(mockPlan, mockRowType, Collections.singletonList(collation), 0);

    List<Object[]> shard0 = Arrays.asList(new Object[] {1, "a"}, new Object[] {null, "x"});
    List<Object[]> shard1 = Arrays.asList(new Object[] {2, "b"}, new Object[] {null, "y"});

    exchange.setShardResults(
        Arrays.asList(
            new ShardResult(0, shard0, mockRowType), new ShardResult(1, shard1, mockRowType)));

    List<Object[]> result = toList(exchange.scan());
    assertEquals(4, result.size());
    assertEquals(1, result.get(0)[0]);
    assertEquals(2, result.get(1)[0]);
    assertNull(result.get(2)[0]);
    assertNull(result.get(3)[0]);
  }

  @Test
  @DisplayName("MergeSortExchange DESC with NULLS FIRST places nulls before non-nulls")
  void testDescNullsFirst() {
    RelFieldCollation collation =
        new RelFieldCollation(0, Direction.DESCENDING, NullDirection.FIRST);
    MergeSortExchange exchange =
        new MergeSortExchange(mockPlan, mockRowType, Collections.singletonList(collation), 0);

    List<Object[]> shard0 = Arrays.asList(new Object[] {null, "x"}, new Object[] {3, "c"});
    List<Object[]> shard1 = Arrays.asList(new Object[] {5, "e"}, new Object[] {1, "a"});

    exchange.setShardResults(
        Arrays.asList(
            new ShardResult(0, shard0, mockRowType), new ShardResult(1, shard1, mockRowType)));

    List<Object[]> result = toList(exchange.scan());
    assertEquals(4, result.size());
    assertNull(result.get(0)[0]);
    assertEquals(5, result.get(1)[0]);
    assertEquals(3, result.get(2)[0]);
    assertEquals(1, result.get(3)[0]);
  }

  @Test
  @DisplayName("MergeSortExchange DESC with NULLS LAST places nulls after non-nulls")
  void testDescNullsLast() {
    RelFieldCollation collation =
        new RelFieldCollation(0, Direction.DESCENDING, NullDirection.LAST);
    MergeSortExchange exchange =
        new MergeSortExchange(mockPlan, mockRowType, Collections.singletonList(collation), 0);

    List<Object[]> shard0 = Arrays.asList(new Object[] {5, "e"}, new Object[] {null, "x"});
    List<Object[]> shard1 = Arrays.asList(new Object[] {3, "c"}, new Object[] {null, "y"});

    exchange.setShardResults(
        Arrays.asList(
            new ShardResult(0, shard0, mockRowType), new ShardResult(1, shard1, mockRowType)));

    List<Object[]> result = toList(exchange.scan());
    assertEquals(4, result.size());
    assertEquals(5, result.get(0)[0]);
    assertEquals(3, result.get(1)[0]);
    assertNull(result.get(2)[0]);
    assertNull(result.get(3)[0]);
  }

  @Test
  @DisplayName("MergeSortExchange with UNSPECIFIED null direction defaults correctly")
  void testUnspecifiedNullDirection() {
    // ASC + UNSPECIFIED => NULLS LAST
    RelFieldCollation ascCollation =
        new RelFieldCollation(0, Direction.ASCENDING, NullDirection.UNSPECIFIED);
    MergeSortExchange ascExchange =
        new MergeSortExchange(
            mockPlan, mockRowType, Collections.singletonList(ascCollation), 0);

    List<Object[]> shard0 = Arrays.asList(new Object[] {1}, new Object[] {null});
    List<Object[]> shard1 = Collections.singletonList(new Object[] {2});

    ascExchange.setShardResults(
        Arrays.asList(
            new ShardResult(0, shard0, mockRowType), new ShardResult(1, shard1, mockRowType)));

    List<Object[]> ascResult = toList(ascExchange.scan());
    assertEquals(1, ascResult.get(0)[0]);
    assertEquals(2, ascResult.get(1)[0]);
    assertNull(ascResult.get(2)[0]);

    // DESC + UNSPECIFIED => NULLS FIRST
    RelFieldCollation descCollation =
        new RelFieldCollation(0, Direction.DESCENDING, NullDirection.UNSPECIFIED);
    MergeSortExchange descExchange =
        new MergeSortExchange(
            mockPlan, mockRowType, Collections.singletonList(descCollation), 0);

    List<Object[]> shard2 = Arrays.asList(new Object[] {null}, new Object[] {3});
    List<Object[]> shard3 = Collections.singletonList(new Object[] {1});

    descExchange.setShardResults(
        Arrays.asList(
            new ShardResult(0, shard2, mockRowType), new ShardResult(1, shard3, mockRowType)));

    List<Object[]> descResult = toList(descExchange.scan());
    assertNull(descResult.get(0)[0]);
    assertEquals(3, descResult.get(1)[0]);
    assertEquals(1, descResult.get(2)[0]);
  }

  @Test
  @DisplayName("MergeSortExchange with multiple sort keys")
  void testMultipleSortKeys() {
    RelFieldCollation col0 = new RelFieldCollation(0, Direction.ASCENDING, NullDirection.LAST);
    RelFieldCollation col1 = new RelFieldCollation(1, Direction.DESCENDING, NullDirection.FIRST);
    MergeSortExchange exchange =
        new MergeSortExchange(mockPlan, mockRowType, Arrays.asList(col0, col1), 0);

    // Shard 0: (1,10), (1,5), (2,8)
    List<Object[]> shard0 =
        Arrays.asList(new Object[] {1, 10}, new Object[] {1, 5}, new Object[] {2, 8});
    // Shard 1: (1,7), (2,12)
    List<Object[]> shard1 = Arrays.asList(new Object[] {1, 7}, new Object[] {2, 12});

    exchange.setShardResults(
        Arrays.asList(
            new ShardResult(0, shard0, mockRowType), new ShardResult(1, shard1, mockRowType)));

    List<Object[]> result = toList(exchange.scan());
    assertEquals(5, result.size());
    // col0 ASC first, then col1 DESC for ties
    assertArrayEquals(new Object[] {1, 10}, result.get(0));
    assertArrayEquals(new Object[] {1, 7}, result.get(1));
    assertArrayEquals(new Object[] {1, 5}, result.get(2));
    assertArrayEquals(new Object[] {2, 12}, result.get(3));
    assertArrayEquals(new Object[] {2, 8}, result.get(4));
  }

  @Test
  @DisplayName("MergeSortExchange with empty shards")
  void testEmptyShards() {
    RelFieldCollation collation =
        new RelFieldCollation(0, Direction.ASCENDING, NullDirection.LAST);
    MergeSortExchange exchange =
        new MergeSortExchange(mockPlan, mockRowType, Collections.singletonList(collation), 0);

    exchange.setShardResults(
        Arrays.asList(
            new ShardResult(0, Collections.emptyList(), mockRowType),
            new ShardResult(1, Collections.emptyList(), mockRowType)));

    List<Object[]> result = toList(exchange.scan());
    assertEquals(0, result.size());
  }

  @Test
  @DisplayName("MergeSortExchange limit larger than total rows returns all rows")
  void testLimitLargerThanTotal() {
    RelFieldCollation collation =
        new RelFieldCollation(0, Direction.ASCENDING, NullDirection.LAST);
    MergeSortExchange exchange =
        new MergeSortExchange(mockPlan, mockRowType, Collections.singletonList(collation), 100);

    List<Object[]> shard0 = Arrays.asList(new Object[] {1}, new Object[] {3});
    List<Object[]> shard1 = Collections.singletonList(new Object[] {2});

    exchange.setShardResults(
        Arrays.asList(
            new ShardResult(0, shard0, mockRowType), new ShardResult(1, shard1, mockRowType)));

    List<Object[]> result = toList(exchange.scan());
    assertEquals(3, result.size());
  }

  @Test
  @DisplayName("MergeSortExchange with all null values")
  void testAllNulls() {
    RelFieldCollation collation =
        new RelFieldCollation(0, Direction.ASCENDING, NullDirection.LAST);
    MergeSortExchange exchange =
        new MergeSortExchange(mockPlan, mockRowType, Collections.singletonList(collation), 0);

    List<Object[]> shard0 = Collections.singletonList(new Object[] {null});
    List<Object[]> shard1 = Collections.singletonList(new Object[] {null});

    exchange.setShardResults(
        Arrays.asList(
            new ShardResult(0, shard0, mockRowType), new ShardResult(1, shard1, mockRowType)));

    List<Object[]> result = toList(exchange.scan());
    assertEquals(2, result.size());
    assertNull(result.get(0)[0]);
    assertNull(result.get(1)[0]);
  }

  @Test
  @DisplayName("MergeSortExchange uses shard ID as tiebreaker for equal sort values")
  void testShardIdTiebreaker() {
    RelFieldCollation collation =
        new RelFieldCollation(0, Direction.ASCENDING, NullDirection.LAST);
    MergeSortExchange exchange =
        new MergeSortExchange(mockPlan, mockRowType, Collections.singletonList(collation), 0);

    // Both shards have rows with equal sort key (value=5). Shard 0 should come before shard 2.
    List<Object[]> shard0 = Collections.singletonList(new Object[] {5, "from_shard_0"});
    List<Object[]> shard1 = Collections.singletonList(new Object[] {5, "from_shard_2"});

    exchange.setShardResults(
        Arrays.asList(
            new ShardResult(0, shard0, mockRowType), new ShardResult(2, shard1, mockRowType)));

    List<Object[]> result = toList(exchange.scan());
    assertEquals(2, result.size());
    // Lower shard ID (0) should come first for equal sort keys
    assertEquals("from_shard_0", result.get(0)[1]);
    assertEquals("from_shard_2", result.get(1)[1]);
  }

  @Test
  @DisplayName("MergeSortExchange shard ID tiebreaker with DESC ordering")
  void testShardIdTiebreakerDesc() {
    RelFieldCollation collation =
        new RelFieldCollation(0, Direction.DESCENDING, NullDirection.FIRST);
    MergeSortExchange exchange =
        new MergeSortExchange(mockPlan, mockRowType, Collections.singletonList(collation), 0);

    // Both shards have rows with equal sort key (value=10).
    List<Object[]> shard0 = Arrays.asList(new Object[] {10, "s0_a"}, new Object[] {5, "s0_b"});
    List<Object[]> shard1 = Arrays.asList(new Object[] {10, "s1_a"}, new Object[] {3, "s1_b"});

    exchange.setShardResults(
        Arrays.asList(
            new ShardResult(0, shard0, mockRowType), new ShardResult(1, shard1, mockRowType)));

    List<Object[]> result = toList(exchange.scan());
    assertEquals(4, result.size());
    // For tied value=10 DESC, lower shard ID (0) comes first
    assertEquals("s0_a", result.get(0)[1]);
    assertEquals("s1_a", result.get(1)[1]);
    assertEquals(5, result.get(2)[0]);
    assertEquals(3, result.get(3)[0]);
  }

  private List<Object[]> toList(Iterator<Object[]> iter) {
    List<Object[]> result = new ArrayList<>();
    iter.forEachRemaining(result::add);
    return result;
  }
}
