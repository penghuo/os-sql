/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.dqe.exchange;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
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

class ConcatExchangeTest {

  private final RelNode mockPlan = mock(RelNode.class);
  private final RelDataType mockRowType = mock(RelDataType.class);

  @Test
  @DisplayName("ConcatExchange with 0 shards returns no rows")
  void testZeroShards() {
    ConcatExchange exchange = new ConcatExchange(mockPlan, mockRowType);
    exchange.setShardResults(Collections.emptyList());

    Iterator<Object[]> iter = exchange.scan();
    assertFalse(iter.hasNext());
  }

  @Test
  @DisplayName("ConcatExchange with 1 shard returns all rows from that shard")
  void testOneShard() {
    ConcatExchange exchange = new ConcatExchange(mockPlan, mockRowType);

    List<Object[]> rows = Arrays.asList(new Object[] {"a", 1}, new Object[] {"b", 2});

    exchange.setShardResults(
        Collections.singletonList(new ShardResult(0, rows, mockRowType)));

    List<Object[]> result = toList(exchange.scan());
    assertEquals(2, result.size());
    assertArrayEquals(new Object[] {"a", 1}, result.get(0));
    assertArrayEquals(new Object[] {"b", 2}, result.get(1));
  }

  @Test
  @DisplayName("ConcatExchange with N shards returns all rows from all shards")
  void testMultipleShards() {
    ConcatExchange exchange = new ConcatExchange(mockPlan, mockRowType);

    List<Object[]> shard0Rows = Arrays.asList(new Object[] {"a", 1}, new Object[] {"b", 2});

    List<Object[]> shard1Rows = Collections.singletonList(new Object[] {"c", 3});

    List<Object[]> shard2Rows =
        Arrays.asList(new Object[] {"d", 4}, new Object[] {"e", 5}, new Object[] {"f", 6});

    exchange.setShardResults(
        Arrays.asList(
            new ShardResult(0, shard0Rows, mockRowType),
            new ShardResult(1, shard1Rows, mockRowType),
            new ShardResult(2, shard2Rows, mockRowType)));

    List<Object[]> result = toList(exchange.scan());
    assertEquals(6, result.size());
  }

  @Test
  @DisplayName("ConcatExchange with empty shard results returns no rows")
  void testShardsWithEmptyResults() {
    ConcatExchange exchange = new ConcatExchange(mockPlan, mockRowType);

    exchange.setShardResults(
        Arrays.asList(
            new ShardResult(0, Collections.emptyList(), mockRowType),
            new ShardResult(1, Collections.emptyList(), mockRowType)));

    List<Object[]> result = toList(exchange.scan());
    assertEquals(0, result.size());
  }

  @Test
  @DisplayName("ConcatExchange getShardPlan returns the provided plan")
  void testGetShardPlan() {
    ConcatExchange exchange = new ConcatExchange(mockPlan, mockRowType);
    assertEquals(mockPlan, exchange.getShardPlan());
  }

  @Test
  @DisplayName("ConcatExchange getRowType returns the provided row type")
  void testGetRowType() {
    ConcatExchange exchange = new ConcatExchange(mockPlan, mockRowType);
    assertEquals(mockRowType, exchange.getRowType());
  }

  private List<Object[]> toList(Iterator<Object[]> iter) {
    List<Object[]> result = new ArrayList<>();
    iter.forEachRemaining(result::add);
    return result;
  }
}
