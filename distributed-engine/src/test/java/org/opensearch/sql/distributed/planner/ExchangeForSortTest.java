/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.distributed.planner;

import static org.junit.jupiter.api.Assertions.*;

import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.logical.LogicalSort;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.opensearch.sql.distributed.planner.plan.AddExchanges;

/** Tests that AddExchanges inserts GatherExchange for sort and topN plans. */
class ExchangeForSortTest extends ConverterTestBase {

  private final AddExchanges addExchanges = new AddExchanges();

  @Test
  @DisplayName("SortNode gets local sort + GatherExchange + merge sort")
  void sortGetsExchange() {
    // source=test | sort age
    var scan = createTableScan("test", "name", "age");
    var collation = RelCollations.of(new RelFieldCollation(1));
    var sort = LogicalSort.create(scan, collation, null, null);

    PlanNode planNode = converter.convert(sort);
    PlanNode withExchange = addExchanges.addExchanges(planNode);

    // Root should be SortNode (merge-sort on coordinator)
    assertInstanceOf(SortNode.class, withExchange);
    SortNode rootSort = (SortNode) withExchange;

    // Below sort should be GatherExchange
    assertInstanceOf(ExchangeNode.class, rootSort.getSource());
    ExchangeNode exchange = (ExchangeNode) rootSort.getSource();
    assertEquals(ExchangeNode.ExchangeType.GATHER, exchange.getExchangeType());

    // Below exchange should be local SortNode
    assertInstanceOf(SortNode.class, exchange.getSource());
    SortNode localSort = (SortNode) exchange.getSource();
    assertInstanceOf(LuceneTableScanNode.class, localSort.getSource());
  }

  @Test
  @DisplayName("TopNNode gets local topN + GatherExchange + final topN")
  void topNGetsExchange() {
    // source=test | sort age | head 10
    var scan = createTableScan("test", "name", "age");
    var collation = RelCollations.of(new RelFieldCollation(1));
    var sort = LogicalSort.create(scan, collation, null, exactLiteral(10));

    PlanNode planNode = converter.convert(sort);
    PlanNode withExchange = addExchanges.addExchanges(planNode);

    // Root should be TopNNode
    assertInstanceOf(TopNNode.class, withExchange);
    TopNNode rootTopN = (TopNNode) withExchange;
    assertEquals(10, rootTopN.getLimit());

    // Below topN should be GatherExchange
    assertInstanceOf(ExchangeNode.class, rootTopN.getSource());
    ExchangeNode exchange = (ExchangeNode) rootTopN.getSource();

    // Below exchange should be local TopNNode
    assertInstanceOf(TopNNode.class, exchange.getSource());
    TopNNode localTopN = (TopNNode) exchange.getSource();
    assertEquals(10, localTopN.getLimit());
  }

  @Test
  @DisplayName("ensureGatherExchange wraps filter-only plan")
  void filterOnlyGetsWrappedInExchange() {
    // source=test | where age > 30
    var scan = createTableScan("test", "name", "age");
    var filter = LogicalFilter.create(scan, greaterThan(fieldRef(1), literal(30)));

    PlanNode planNode = converter.convert(filter);
    PlanNode withExchange = addExchanges.addExchanges(planNode);

    // Filter doesn't insert exchange by itself
    withExchange = addExchanges.ensureGatherExchange(withExchange);

    // Should be wrapped in GatherExchange
    assertInstanceOf(ExchangeNode.class, withExchange);
    ExchangeNode exchange = (ExchangeNode) withExchange;
    assertEquals(ExchangeNode.ExchangeType.GATHER, exchange.getExchangeType());
    assertInstanceOf(FilterNode.class, exchange.getSource());
  }

  @Test
  @DisplayName("ensureGatherExchange is idempotent when exchange exists")
  void existingExchangeNotDuplicated() {
    var scan = createTableScan("test", "name", "age");
    var collation = RelCollations.of(new RelFieldCollation(1));
    var sort = LogicalSort.create(scan, collation, null, null);

    PlanNode planNode = converter.convert(sort);
    PlanNode withExchange = addExchanges.addExchanges(planNode);
    PlanNode ensured = addExchanges.ensureGatherExchange(withExchange);

    // Should NOT add another exchange
    assertEquals(withExchange, ensured);
  }
}
