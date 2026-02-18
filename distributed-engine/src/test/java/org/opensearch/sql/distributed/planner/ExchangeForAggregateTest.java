/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.distributed.planner;

import static org.junit.jupiter.api.Assertions.*;

import java.util.List;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.logical.LogicalAggregate;
import org.apache.calcite.util.ImmutableBitSet;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.opensearch.sql.distributed.planner.plan.AddExchanges;

/** Tests that AddExchanges inserts GatherExchange for aggregation plans. */
class ExchangeForAggregateTest extends ConverterTestBase {

  private final AddExchanges addExchanges = new AddExchanges();

  @Test
  @DisplayName("AggregationNode splits into PARTIAL + GatherExchange + FINAL")
  void aggregationGetsExchange() {
    // Build: AggregationNode(SINGLE) -> Scan
    var scan = createTableScan("test", "name", "age", "status");
    AggregateCall countCall = countCall();
    var agg =
        LogicalAggregate.create(scan, List.of(), ImmutableBitSet.of(2), null, List.of(countCall));

    PlanNode planNode = converter.convert(agg);
    PlanNode withExchange = addExchanges.addExchanges(planNode);

    // Root should be FINAL aggregation
    assertInstanceOf(AggregationNode.class, withExchange);
    AggregationNode finalAgg = (AggregationNode) withExchange;
    assertEquals(AggregationNode.AggregationMode.FINAL, finalAgg.getMode());

    // Below FINAL should be GatherExchange
    assertInstanceOf(ExchangeNode.class, finalAgg.getSource());
    ExchangeNode exchange = (ExchangeNode) finalAgg.getSource();
    assertEquals(ExchangeNode.ExchangeType.GATHER, exchange.getExchangeType());

    // Below exchange should be PARTIAL aggregation
    assertInstanceOf(AggregationNode.class, exchange.getSource());
    AggregationNode partialAgg = (AggregationNode) exchange.getSource();
    assertEquals(AggregationNode.AggregationMode.PARTIAL, partialAgg.getMode());

    // Below PARTIAL should be the scan
    assertInstanceOf(LuceneTableScanNode.class, partialAgg.getSource());
  }

  @Test
  @DisplayName("Aggregation preserves group set and agg calls through split")
  void aggregationPreservesSemantics() {
    var scan = createTableScan("test", "name", "age", "status");
    AggregateCall countCall = countCall();
    ImmutableBitSet groupSet = ImmutableBitSet.of(2);
    var agg = LogicalAggregate.create(scan, List.of(), groupSet, null, List.of(countCall));

    PlanNode planNode = converter.convert(agg);
    PlanNode withExchange = addExchanges.addExchanges(planNode);

    AggregationNode finalAgg = (AggregationNode) withExchange;
    assertEquals(groupSet, finalAgg.getGroupSet());
    assertEquals(1, finalAgg.getAggregateCalls().size());

    ExchangeNode exchange = (ExchangeNode) finalAgg.getSource();
    AggregationNode partialAgg = (AggregationNode) exchange.getSource();
    assertEquals(groupSet, partialAgg.getGroupSet());
    assertEquals(1, partialAgg.getAggregateCalls().size());
  }
}
