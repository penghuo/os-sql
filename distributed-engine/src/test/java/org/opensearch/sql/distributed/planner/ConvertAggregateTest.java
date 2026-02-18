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

/**
 * Tests that LogicalAggregate is correctly converted to AggregationNode with group keys and agg
 * functions preserved.
 */
class ConvertAggregateTest extends ConverterTestBase {

  @Test
  @DisplayName("LogicalAggregate -> AggregationNode with SINGLE mode")
  void convertSimpleAggregate() {
    // source=test | stats count() by status
    var scan = createTableScan("test", "name", "age", "status");
    AggregateCall countCall = countCall();
    ImmutableBitSet groupSet = ImmutableBitSet.of(2); // group by status
    LogicalAggregate agg =
        LogicalAggregate.create(scan, List.of(), groupSet, null, List.of(countCall));

    PlanNode result = converter.convert(agg);

    assertInstanceOf(AggregationNode.class, result);
    AggregationNode aggNode = (AggregationNode) result;
    assertEquals(AggregationNode.AggregationMode.SINGLE, aggNode.getMode());
    assertEquals(groupSet, aggNode.getGroupSet());
    assertEquals(1, aggNode.getAggregateCalls().size());
    assertInstanceOf(LuceneTableScanNode.class, aggNode.getSource());
  }

  @Test
  @DisplayName("Aggregate without group by produces AggregationNode with empty group set")
  void convertAggregateNoGroupBy() {
    // source=test | stats count()
    var scan = createTableScan("test", "name", "age");
    AggregateCall countCall = countCall();
    ImmutableBitSet groupSet = ImmutableBitSet.of();
    LogicalAggregate agg =
        LogicalAggregate.create(scan, List.of(), groupSet, null, List.of(countCall));

    PlanNode result = converter.convert(agg);

    AggregationNode aggNode = (AggregationNode) result;
    assertTrue(aggNode.getGroupSet().isEmpty());
    assertEquals(1, aggNode.getAggregateCalls().size());
  }
}
