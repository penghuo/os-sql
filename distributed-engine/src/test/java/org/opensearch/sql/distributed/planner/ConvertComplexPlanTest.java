/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.distributed.planner;

import static org.junit.jupiter.api.Assertions.*;

import java.util.List;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.logical.LogicalAggregate;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.logical.LogicalSort;
import org.apache.calcite.util.ImmutableBitSet;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/** Tests that complex multi-level RelNode trees are correctly converted. */
class ConvertComplexPlanTest extends ConverterTestBase {

  @Test
  @DisplayName("Filter + Project -> FilterNode(ProjectNode(Scan))")
  void convertFilterProject() {
    // source=test | where age > 30 | fields name
    var scan = createTableScan("test", "name", "age");
    var filter = LogicalFilter.create(scan, greaterThan(fieldRef(1), literal(30)));
    var project = createProject(filter, List.of(fieldRef(0)), "name");

    PlanNode result = converter.convert(project);

    assertInstanceOf(ProjectNode.class, result);
    ProjectNode proj = (ProjectNode) result;
    assertInstanceOf(FilterNode.class, proj.getSource());
    FilterNode filt = (FilterNode) proj.getSource();
    assertInstanceOf(LuceneTableScanNode.class, filt.getSource());
  }

  @Test
  @DisplayName("Filter + Aggregate -> AggregationNode(FilterNode(Scan))")
  void convertFilterAggregate() {
    // source=test | where age > 25 | stats count() by status
    var scan = createTableScan("test", "name", "age", "status");
    var filter = LogicalFilter.create(scan, greaterThan(fieldRef(1), literal(25)));
    AggregateCall countCall = countCall();
    var agg =
        LogicalAggregate.create(filter, List.of(), ImmutableBitSet.of(2), null, List.of(countCall));

    PlanNode result = converter.convert(agg);

    assertInstanceOf(AggregationNode.class, result);
    AggregationNode aggNode = (AggregationNode) result;
    assertInstanceOf(FilterNode.class, aggNode.getSource());
  }

  @Test
  @DisplayName("Sort + Limit -> TopNNode(Scan)")
  void convertSortLimit() {
    // source=test | sort age | head 10
    var scan = createTableScan("test", "name", "age");
    var collation = RelCollations.of(new RelFieldCollation(1));
    var sort = LogicalSort.create(scan, collation, null, exactLiteral(10));

    PlanNode result = converter.convert(sort);

    assertInstanceOf(TopNNode.class, result);
    TopNNode topN = (TopNNode) result;
    assertEquals(10, topN.getLimit());
    assertInstanceOf(LuceneTableScanNode.class, topN.getSource());
  }

  @Test
  @DisplayName("Filter + Sort + Limit creates full pipeline")
  void convertFilterSortLimit() {
    // source=test | where age > 20 | sort age | head 5
    var scan = createTableScan("test", "name", "age");
    var filter = LogicalFilter.create(scan, greaterThan(fieldRef(1), literal(20)));
    var collation = RelCollations.of(new RelFieldCollation(1));
    var sort = LogicalSort.create(filter, collation, null, exactLiteral(5));

    PlanNode result = converter.convert(sort);

    assertInstanceOf(TopNNode.class, result);
    TopNNode topN = (TopNNode) result;
    assertInstanceOf(FilterNode.class, topN.getSource());
    FilterNode filt = (FilterNode) topN.getSource();
    assertInstanceOf(LuceneTableScanNode.class, filt.getSource());
  }

  @Test
  @DisplayName("All PlanNodes have unique IDs")
  void planNodeIdsAreUnique() {
    var scan = createTableScan("test", "name", "age");
    var filter = LogicalFilter.create(scan, greaterThan(fieldRef(1), literal(30)));
    var project = createProject(filter, List.of(fieldRef(0)), "name");

    PlanNode result = converter.convert(project);

    ProjectNode proj = (ProjectNode) result;
    FilterNode filt = (FilterNode) proj.getSource();
    LuceneTableScanNode scanNode = (LuceneTableScanNode) filt.getSource();

    assertNotEquals(proj.getId(), filt.getId());
    assertNotEquals(filt.getId(), scanNode.getId());
    assertNotEquals(proj.getId(), scanNode.getId());
  }
}
