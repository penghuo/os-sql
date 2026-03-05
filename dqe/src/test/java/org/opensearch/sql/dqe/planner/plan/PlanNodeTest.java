/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.dqe.planner.plan;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNull;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.List;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.core.common.io.stream.InputStreamStreamInput;

@DisplayName("Plan node serialization round-trip")
class PlanNodeTest {

  @Test
  @DisplayName("TableScanNode round-trips through StreamOutput/StreamInput")
  void tableScanRoundTrip() throws IOException {
    TableScanNode original = new TableScanNode("logs", List.of("category", "status"));

    BytesStreamOutput out = new BytesStreamOutput();
    DqePlanNode.writePlanNode(out, original);

    InputStreamStreamInput in =
        new InputStreamStreamInput(new ByteArrayInputStream(out.bytes().toBytesRef().bytes));
    DqePlanNode deserialized = DqePlanNode.readPlanNode(in);

    assertInstanceOf(TableScanNode.class, deserialized);
    TableScanNode scan = (TableScanNode) deserialized;
    assertEquals("logs", scan.getIndexName());
    assertEquals(List.of("category", "status"), scan.getColumns());
  }

  @Test
  @DisplayName("TableScanNode with dslFilter round-trips correctly")
  void tableScanWithDslFilterRoundTrip() throws IOException {
    TableScanNode original =
        new TableScanNode("logs", List.of("status"), "{\"term\":{\"status\":200}}");

    BytesStreamOutput out = new BytesStreamOutput();
    DqePlanNode.writePlanNode(out, original);

    InputStreamStreamInput in =
        new InputStreamStreamInput(new ByteArrayInputStream(out.bytes().toBytesRef().bytes));
    DqePlanNode deserialized = DqePlanNode.readPlanNode(in);

    assertInstanceOf(TableScanNode.class, deserialized);
    TableScanNode scan = (TableScanNode) deserialized;
    assertEquals("logs", scan.getIndexName());
    assertEquals(List.of("status"), scan.getColumns());
    assertEquals("{\"term\":{\"status\":200}}", scan.getDslFilter());
  }

  @Test
  @DisplayName("TableScanNode with null dslFilter round-trips correctly")
  void tableScanWithNullDslFilterRoundTrip() throws IOException {
    TableScanNode original = new TableScanNode("logs", List.of("category", "status"));

    BytesStreamOutput out = new BytesStreamOutput();
    DqePlanNode.writePlanNode(out, original);

    InputStreamStreamInput in =
        new InputStreamStreamInput(new ByteArrayInputStream(out.bytes().toBytesRef().bytes));
    DqePlanNode deserialized = DqePlanNode.readPlanNode(in);

    assertInstanceOf(TableScanNode.class, deserialized);
    TableScanNode scan = (TableScanNode) deserialized;
    assertEquals("logs", scan.getIndexName());
    assertEquals(List.of("category", "status"), scan.getColumns());
    assertNull(scan.getDslFilter());
  }

  @Test
  @DisplayName("FilterNode round-trips with child and predicate")
  void filterNodeRoundTrip() throws IOException {
    TableScanNode scan = new TableScanNode("logs", List.of("status"));
    FilterNode filter = new FilterNode(scan, "status = 200");

    BytesStreamOutput out = new BytesStreamOutput();
    DqePlanNode.writePlanNode(out, filter);

    InputStreamStreamInput in =
        new InputStreamStreamInput(new ByteArrayInputStream(out.bytes().toBytesRef().bytes));
    DqePlanNode deserialized = DqePlanNode.readPlanNode(in);

    assertInstanceOf(FilterNode.class, deserialized);
    FilterNode result = (FilterNode) deserialized;
    assertEquals("status = 200", result.getPredicateString());
    assertInstanceOf(TableScanNode.class, result.getChild());
  }

  @Test
  @DisplayName("ProjectNode round-trips with child and output columns")
  void projectNodeRoundTrip() throws IOException {
    TableScanNode scan = new TableScanNode("orders", List.of("id", "amount", "status"));
    ProjectNode project = new ProjectNode(scan, List.of("id", "amount"));

    BytesStreamOutput out = new BytesStreamOutput();
    DqePlanNode.writePlanNode(out, project);

    InputStreamStreamInput in =
        new InputStreamStreamInput(new ByteArrayInputStream(out.bytes().toBytesRef().bytes));
    DqePlanNode deserialized = DqePlanNode.readPlanNode(in);

    assertInstanceOf(ProjectNode.class, deserialized);
    ProjectNode result = (ProjectNode) deserialized;
    assertEquals(List.of("id", "amount"), result.getOutputColumns());
    assertInstanceOf(TableScanNode.class, result.getChild());
  }

  @Test
  @DisplayName("AggregationNode round-trips with groupBy, aggregates, and step")
  void aggregationNodeRoundTrip() throws IOException {
    TableScanNode scan = new TableScanNode("sales", List.of("category", "amount"));
    AggregationNode agg =
        new AggregationNode(
            scan,
            List.of("category"),
            List.of("COUNT(*)", "SUM(amount)"),
            AggregationNode.Step.PARTIAL);

    BytesStreamOutput out = new BytesStreamOutput();
    DqePlanNode.writePlanNode(out, agg);

    InputStreamStreamInput in =
        new InputStreamStreamInput(new ByteArrayInputStream(out.bytes().toBytesRef().bytes));
    DqePlanNode deserialized = DqePlanNode.readPlanNode(in);

    assertInstanceOf(AggregationNode.class, deserialized);
    AggregationNode result = (AggregationNode) deserialized;
    assertEquals(List.of("category"), result.getGroupByKeys());
    assertEquals(List.of("COUNT(*)", "SUM(amount)"), result.getAggregateFunctions());
    assertEquals(AggregationNode.Step.PARTIAL, result.getStep());
    assertInstanceOf(TableScanNode.class, result.getChild());
  }

  @Test
  @DisplayName("SortNode round-trips with sort keys and directions")
  void sortNodeRoundTrip() throws IOException {
    TableScanNode scan = new TableScanNode("events", List.of("timestamp", "severity"));
    SortNode sort = new SortNode(scan, List.of("timestamp", "severity"), List.of(false, true));

    BytesStreamOutput out = new BytesStreamOutput();
    DqePlanNode.writePlanNode(out, sort);

    InputStreamStreamInput in =
        new InputStreamStreamInput(new ByteArrayInputStream(out.bytes().toBytesRef().bytes));
    DqePlanNode deserialized = DqePlanNode.readPlanNode(in);

    assertInstanceOf(SortNode.class, deserialized);
    SortNode result = (SortNode) deserialized;
    assertEquals(List.of("timestamp", "severity"), result.getSortKeys());
    assertEquals(List.of(false, true), result.getAscending());
    assertInstanceOf(TableScanNode.class, result.getChild());
  }

  @Test
  @DisplayName("LimitNode round-trips with child and count")
  void limitNodeRoundTrip() throws IOException {
    TableScanNode scan = new TableScanNode("logs", List.of("message"));
    LimitNode limit = new LimitNode(scan, 100);

    BytesStreamOutput out = new BytesStreamOutput();
    DqePlanNode.writePlanNode(out, limit);

    InputStreamStreamInput in =
        new InputStreamStreamInput(new ByteArrayInputStream(out.bytes().toBytesRef().bytes));
    DqePlanNode deserialized = DqePlanNode.readPlanNode(in);

    assertInstanceOf(LimitNode.class, deserialized);
    LimitNode result = (LimitNode) deserialized;
    assertEquals(100, result.getCount());
    assertInstanceOf(TableScanNode.class, result.getChild());
  }

  @Test
  @DisplayName("Nested plan tree round-trips correctly")
  void nestedPlanRoundTrip() throws IOException {
    TableScanNode scan = new TableScanNode("logs", List.of("category", "status"));
    FilterNode filter = new FilterNode(scan, "status = 200");
    LimitNode limit = new LimitNode(filter, 10);

    BytesStreamOutput out = new BytesStreamOutput();
    DqePlanNode.writePlanNode(out, limit);

    InputStreamStreamInput in =
        new InputStreamStreamInput(new ByteArrayInputStream(out.bytes().toBytesRef().bytes));
    DqePlanNode deserialized = DqePlanNode.readPlanNode(in);

    assertInstanceOf(LimitNode.class, deserialized);
    LimitNode limitResult = (LimitNode) deserialized;
    assertEquals(10, limitResult.getCount());
    assertInstanceOf(FilterNode.class, limitResult.getChild());
    assertInstanceOf(TableScanNode.class, ((FilterNode) limitResult.getChild()).getChild());
  }
}
