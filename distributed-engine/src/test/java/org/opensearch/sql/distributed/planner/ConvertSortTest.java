/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.distributed.planner;

import static org.junit.jupiter.api.Assertions.*;

import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.logical.LogicalSort;
import org.apache.calcite.rex.RexLiteral;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/** Tests that LogicalSort is correctly converted to SortNode or TopNNode. */
class ConvertSortTest extends ConverterTestBase {

  @Test
  @DisplayName("LogicalSort with collation only -> SortNode")
  void convertSortOnly() {
    // source=test | sort age
    var scan = createTableScan("test", "name", "age");
    var collation = RelCollations.of(new RelFieldCollation(1));
    LogicalSort sort = LogicalSort.create(scan, collation, null, null);

    PlanNode result = converter.convert(sort);

    assertInstanceOf(SortNode.class, result);
    SortNode sortNode = (SortNode) result;
    assertEquals(1, sortNode.getCollation().getFieldCollations().size());
    assertEquals(1, sortNode.getCollation().getFieldCollations().get(0).getFieldIndex());
  }

  @Test
  @DisplayName("LogicalSort with collation and fetch -> TopNNode")
  void convertSortWithLimit() {
    // source=test | sort age | head 10
    var scan = createTableScan("test", "name", "age");
    var collation = RelCollations.of(new RelFieldCollation(1));
    RexLiteral fetch = exactLiteral(10);
    LogicalSort sort = LogicalSort.create(scan, collation, null, fetch);

    PlanNode result = converter.convert(sort);

    assertInstanceOf(TopNNode.class, result);
    TopNNode topN = (TopNNode) result;
    assertEquals(10, topN.getLimit());
    assertNotNull(topN.getCollation());
  }

  @Test
  @DisplayName("LogicalSort with fetch only -> LimitNode")
  void convertLimitOnly() {
    // source=test | head 20
    var scan = createTableScan("test", "name", "age");
    RexLiteral fetch = exactLiteral(20);
    LogicalSort sort = LogicalSort.create(scan, RelCollations.EMPTY, null, fetch);

    PlanNode result = converter.convert(sort);

    assertInstanceOf(LimitNode.class, result);
    LimitNode limitNode = (LimitNode) result;
    assertEquals(20, limitNode.getLimit());
    assertEquals(0, limitNode.getOffset());
  }

  @Test
  @DisplayName("LogicalSort with fetch and offset -> LimitNode with offset")
  void convertLimitWithOffset() {
    var scan = createTableScan("test", "name", "age");
    RexLiteral fetch = exactLiteral(10);
    RexLiteral offset = exactLiteral(5);
    LogicalSort sort = LogicalSort.create(scan, RelCollations.EMPTY, offset, fetch);

    PlanNode result = converter.convert(sort);

    assertInstanceOf(LimitNode.class, result);
    LimitNode limitNode = (LimitNode) result;
    assertEquals(10, limitNode.getLimit());
    assertEquals(5, limitNode.getOffset());
  }

  @Test
  @DisplayName("Sort direction is preserved (DESC)")
  void convertDescSort() {
    var scan = createTableScan("test", "name", "age");
    var collation =
        RelCollations.of(new RelFieldCollation(1, RelFieldCollation.Direction.DESCENDING));
    LogicalSort sort = LogicalSort.create(scan, collation, null, null);

    PlanNode result = converter.convert(sort);

    assertInstanceOf(SortNode.class, result);
    SortNode sortNode = (SortNode) result;
    assertEquals(
        RelFieldCollation.Direction.DESCENDING,
        sortNode.getCollation().getFieldCollations().get(0).getDirection());
  }
}
