/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.distributed.planner;

import static org.junit.jupiter.api.Assertions.*;

import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rex.RexNode;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/** Tests that LogicalFilter is correctly converted to FilterNode with predicate preserved. */
class ConvertFilterTest extends ConverterTestBase {

  @Test
  @DisplayName("LogicalFilter -> FilterNode with predicate preserved")
  void convertSimpleFilter() {
    // source=test | where age > 30
    RexNode predicate = greaterThan(fieldRef(1), literal(30));
    LogicalFilter filter = LogicalFilter.create(createTableScan("test", "name", "age"), predicate);

    PlanNode result = converter.convert(filter);

    assertInstanceOf(FilterNode.class, result);
    FilterNode filterNode = (FilterNode) result;
    assertNotNull(filterNode.getPredicate());
    assertEquals(predicate, filterNode.getPredicate());
    assertInstanceOf(LuceneTableScanNode.class, filterNode.getSource());
  }

  @Test
  @DisplayName("Filter predicate references are preserved correctly")
  void convertFilterPredicateReferences() {
    RexNode predicate = equals(fieldRef(0), literalString("hello"));
    LogicalFilter filter = LogicalFilter.create(createTableScan("test", "name", "age"), predicate);

    PlanNode result = converter.convert(filter);

    FilterNode filterNode = (FilterNode) result;
    assertEquals(predicate.toString(), filterNode.getPredicate().toString());
  }

  @Test
  @DisplayName("Filter on top of filter creates nested FilterNode")
  void convertNestedFilter() {
    RexNode predicate1 = greaterThan(fieldRef(1), literal(30));
    RexNode predicate2 = equals(fieldRef(0), literalString("test"));
    LogicalFilter innerFilter =
        LogicalFilter.create(createTableScan("test", "name", "age"), predicate1);
    LogicalFilter outerFilter = LogicalFilter.create(innerFilter, predicate2);

    PlanNode result = converter.convert(outerFilter);

    assertInstanceOf(FilterNode.class, result);
    FilterNode outer = (FilterNode) result;
    assertInstanceOf(FilterNode.class, outer.getSource());
    FilterNode inner = (FilterNode) outer.getSource();
    assertInstanceOf(LuceneTableScanNode.class, inner.getSource());
  }
}
