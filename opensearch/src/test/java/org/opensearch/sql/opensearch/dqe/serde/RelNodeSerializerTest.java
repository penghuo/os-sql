/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.dqe.serde;

import static org.junit.jupiter.api.Assertions.*;
import static org.opensearch.sql.calcite.utils.OpenSearchTypeFactory.TYPE_FACTORY;
import com.google.common.collect.ImmutableList;
import java.math.BigDecimal;
import java.util.Collections;
import java.util.List;
import org.apache.calcite.plan.ConventionTraitDef;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.volcano.VolcanoPlanner;
import org.apache.calcite.rel.*;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.logical.*;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.*;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.ImmutableBitSet;
import org.junit.jupiter.api.*;

@DisplayNameGeneration(DisplayNameGenerator.ReplaceUnderscores.class)
public class RelNodeSerializerTest {
  private RexBuilder rexBuilder;
  private RelOptCluster cluster;
  private RelDataType rowType;
  private LogicalValues valuesRel;

  @BeforeEach void setUp() {
    rexBuilder = new RexBuilder(TYPE_FACTORY);
    VolcanoPlanner planner = new VolcanoPlanner();
    planner.addRelTraitDef(ConventionTraitDef.INSTANCE);
    cluster = RelOptCluster.create(planner, rexBuilder);
    rowType = TYPE_FACTORY.builder()
        .add("name", TYPE_FACTORY.createSqlType(SqlTypeName.VARCHAR))
        .add("age", TYPE_FACTORY.createSqlType(SqlTypeName.INTEGER))
        .add("salary", TYPE_FACTORY.createSqlType(SqlTypeName.DOUBLE))
        .add("active", TYPE_FACTORY.createSqlType(SqlTypeName.BOOLEAN)).build();
    valuesRel = LogicalValues.createEmpty(cluster, rowType);
  }

  @Test @DisplayName("Round-trip for LogicalFilter") void roundTrip_logicalFilter() {
    RexNode c = rexBuilder.makeCall(SqlStdOperatorTable.GREATER_THAN, rexBuilder.makeInputRef(rowType.getFieldList().get(1).getType(), 1), rexBuilder.makeLiteral(25, TYPE_FACTORY.createSqlType(SqlTypeName.INTEGER)));
    LogicalFilter f = LogicalFilter.create(valuesRel, c);
    String j = RelNodeSerializer.serialize(f); assertNotNull(j); assertTrue(j.contains("LogicalFilter"));
    RelNode d = RelNodeSerializer.deserialize(j, cluster, null); assertNotNull(d);
    assertEquals(f.getRowType().getFieldCount(), d.getRowType().getFieldCount());
    assertEquals(f.getRowType().getFieldNames(), d.getRowType().getFieldNames());
  }

  @Test @DisplayName("Round-trip for LogicalProject") void roundTrip_logicalProject() {
    List<RexNode> p = List.of(rexBuilder.makeInputRef(rowType.getFieldList().get(0).getType(), 0), rexBuilder.makeInputRef(rowType.getFieldList().get(1).getType(), 1));
    LogicalProject proj = LogicalProject.create(valuesRel, Collections.emptyList(), p, List.of("name", "age"));
    String j = RelNodeSerializer.serialize(proj); assertNotNull(j); assertTrue(j.contains("LogicalProject"));
    RelNode d = RelNodeSerializer.deserialize(j, cluster, null); assertNotNull(d);
    assertEquals(2, d.getRowType().getFieldCount()); assertEquals(List.of("name", "age"), d.getRowType().getFieldNames());
  }

  @Test @DisplayName("Round-trip for Aggregate with GROUP BY and multiple agg calls") void roundTrip_logicalAggregate_groupBy_multipleAggCalls() {
    AggregateCall cnt = AggregateCall.create(SqlStdOperatorTable.COUNT, false, false, false, ImmutableList.of(), ImmutableList.of(), -1, null, RelCollations.EMPTY, TYPE_FACTORY.createSqlType(SqlTypeName.BIGINT), "cnt");
    AggregateCall sum = AggregateCall.create(SqlStdOperatorTable.SUM, false, false, false, ImmutableList.of(), ImmutableList.of(2), -1, null, RelCollations.EMPTY, TYPE_FACTORY.createSqlType(SqlTypeName.DOUBLE), "total_salary");
    AggregateCall avg = AggregateCall.create(SqlStdOperatorTable.AVG, false, false, false, ImmutableList.of(), ImmutableList.of(1), -1, null, RelCollations.EMPTY, TYPE_FACTORY.createSqlType(SqlTypeName.DOUBLE), "avg_age");
    LogicalAggregate agg = LogicalAggregate.create(valuesRel, Collections.emptyList(), ImmutableBitSet.of(0), null, List.of(cnt, sum, avg));
    String j = RelNodeSerializer.serialize(agg); assertNotNull(j); assertTrue(j.contains("LogicalAggregate"));
    RelNode d = RelNodeSerializer.deserialize(j, cluster, null); assertNotNull(d);
    assertEquals(agg.getRowType().getFieldCount(), d.getRowType().getFieldCount()); assertEquals(agg.getRowType().getFieldNames(), d.getRowType().getFieldNames());
  }

  @Test @DisplayName("Round-trip for Sort with ASC/DESC, NULLS FIRST/LAST") void roundTrip_logicalSort_ascDesc_nullsFirstLast() {
    LogicalSort s = LogicalSort.create(valuesRel, RelCollations.of(new RelFieldCollation(0, RelFieldCollation.Direction.ASCENDING, RelFieldCollation.NullDirection.FIRST), new RelFieldCollation(1, RelFieldCollation.Direction.DESCENDING, RelFieldCollation.NullDirection.LAST)), null, null);
    String j = RelNodeSerializer.serialize(s); assertNotNull(j); assertTrue(j.contains("LogicalSort"));
    RelNode d = RelNodeSerializer.deserialize(j, cluster, null); assertNotNull(d);
    assertEquals(s.getRowType().getFieldNames(), d.getRowType().getFieldNames());
  }

  @Test @DisplayName("Round-trip for LogicalCalc") void roundTrip_logicalCalc() {
    RexProgramBuilder pb = new RexProgramBuilder(rowType, rexBuilder);
    pb.addProject(rexBuilder.makeInputRef(rowType.getFieldList().get(0).getType(), 0), "name");
    pb.addProject(rexBuilder.makeInputRef(rowType.getFieldList().get(1).getType(), 1), "age");
    pb.addCondition(rexBuilder.makeCall(SqlStdOperatorTable.GREATER_THAN, rexBuilder.makeInputRef(rowType.getFieldList().get(1).getType(), 1), rexBuilder.makeLiteral(25, TYPE_FACTORY.createSqlType(SqlTypeName.INTEGER))));
    LogicalCalc calc = LogicalCalc.create(valuesRel, pb.getProgram());
    String j = RelNodeSerializer.serialize(calc); assertNotNull(j); assertTrue(j.contains("LogicalCalc"));
    RelNode d = RelNodeSerializer.deserialize(j, cluster, null); assertNotNull(d);
    assertEquals(2, d.getRowType().getFieldCount()); assertEquals(List.of("name", "age"), d.getRowType().getFieldNames());
  }

  @Test @DisplayName("Round-trip for Filter with complex RexCall (abs(age) > 10)") void roundTrip_filter_complexRexCall() {
    RexNode abs = rexBuilder.makeCall(SqlStdOperatorTable.ABS, rexBuilder.makeInputRef(rowType.getFieldList().get(1).getType(), 1));
    LogicalFilter f = LogicalFilter.create(valuesRel, rexBuilder.makeCall(SqlStdOperatorTable.GREATER_THAN, abs, rexBuilder.makeLiteral(10, TYPE_FACTORY.createSqlType(SqlTypeName.INTEGER))));
    RelNode d = RelNodeSerializer.deserialize(RelNodeSerializer.serialize(f), cluster, null);
    assertNotNull(d); assertEquals(f.getRowType().getFieldCount(), d.getRowType().getFieldCount());
  }

  @Test @DisplayName("Round-trip preserves RelDataType") void roundTrip_preservesRelDataType() {
    RelDataType rich = TYPE_FACTORY.builder().add("nullable_name", TYPE_FACTORY.createTypeWithNullability(TYPE_FACTORY.createSqlType(SqlTypeName.VARCHAR), true)).add("non_null_age", TYPE_FACTORY.createSqlType(SqlTypeName.INTEGER)).add("nullable_salary", TYPE_FACTORY.createTypeWithNullability(TYPE_FACTORY.createSqlType(SqlTypeName.DOUBLE), true)).build();
    LogicalValues rv = LogicalValues.createEmpty(cluster, rich);
    List<RexNode> ps = List.of(rexBuilder.makeInputRef(rich.getFieldList().get(0).getType(), 0), rexBuilder.makeInputRef(rich.getFieldList().get(1).getType(), 1), rexBuilder.makeInputRef(rich.getFieldList().get(2).getType(), 2));
    LogicalProject proj = LogicalProject.create(rv, Collections.emptyList(), ps, List.of("nullable_name", "non_null_age", "nullable_salary"));
    RelNode d = RelNodeSerializer.deserialize(RelNodeSerializer.serialize(proj), cluster, null); assertNotNull(d);
    RelDataType dt = d.getRowType();
    assertEquals(List.of("nullable_name", "non_null_age", "nullable_salary"), dt.getFieldNames());
    assertEquals(SqlTypeName.VARCHAR, dt.getFieldList().get(0).getType().getSqlTypeName());
    assertTrue(dt.getFieldList().get(0).getType().isNullable()); assertFalse(dt.getFieldList().get(1).getType().isNullable()); assertTrue(dt.getFieldList().get(2).getType().isNullable());
  }

  @Test @DisplayName("RexLiteral VARCHAR") void roundTrip_rexLiteral_varchar() { assertNotNull(RelNodeSerializer.deserialize(RelNodeSerializer.serialize(LogicalFilter.create(valuesRel, rexBuilder.makeCall(SqlStdOperatorTable.EQUALS, rexBuilder.makeInputRef(rowType.getFieldList().get(0).getType(), 0), rexBuilder.makeLiteral("hello")))), cluster, null)); }
  @Test @DisplayName("RexLiteral INTEGER") void roundTrip_rexLiteral_integer() { assertNotNull(RelNodeSerializer.deserialize(RelNodeSerializer.serialize(LogicalFilter.create(valuesRel, rexBuilder.makeCall(SqlStdOperatorTable.EQUALS, rexBuilder.makeInputRef(rowType.getFieldList().get(1).getType(), 1), rexBuilder.makeLiteral(42, TYPE_FACTORY.createSqlType(SqlTypeName.INTEGER))))), cluster, null)); }
  @Test @DisplayName("RexLiteral DOUBLE") void roundTrip_rexLiteral_double() { assertNotNull(RelNodeSerializer.deserialize(RelNodeSerializer.serialize(LogicalFilter.create(valuesRel, rexBuilder.makeCall(SqlStdOperatorTable.GREATER_THAN, rexBuilder.makeInputRef(rowType.getFieldList().get(2).getType(), 2), rexBuilder.makeApproxLiteral(BigDecimal.valueOf(50000.50))))), cluster, null)); }
  @Test @DisplayName("RexLiteral BOOLEAN") void roundTrip_rexLiteral_boolean() { assertNotNull(RelNodeSerializer.deserialize(RelNodeSerializer.serialize(LogicalFilter.create(valuesRel, rexBuilder.makeCall(SqlStdOperatorTable.EQUALS, rexBuilder.makeInputRef(rowType.getFieldList().get(3).getType(), 3), rexBuilder.makeLiteral(true)))), cluster, null)); }
  @Test @DisplayName("RexLiteral DATE") void roundTrip_rexLiteral_date() { RelDataType drt = TYPE_FACTORY.builder().add("d", TYPE_FACTORY.createSqlType(SqlTypeName.DATE)).build(); assertNotNull(RelNodeSerializer.deserialize(RelNodeSerializer.serialize(LogicalFilter.create(LogicalValues.createEmpty(cluster, drt), rexBuilder.makeCall(SqlStdOperatorTable.EQUALS, rexBuilder.makeInputRef(drt.getFieldList().get(0).getType(), 0), rexBuilder.makeDateLiteral(new org.apache.calcite.util.DateString("2024-01-15"))))), cluster, null)); }
  @Test @DisplayName("RexLiteral TIMESTAMP") void roundTrip_rexLiteral_timestamp() { RelDataType trt = TYPE_FACTORY.builder().add("ts", TYPE_FACTORY.createSqlType(SqlTypeName.TIMESTAMP)).build(); assertNotNull(RelNodeSerializer.deserialize(RelNodeSerializer.serialize(LogicalFilter.create(LogicalValues.createEmpty(cluster, trt), rexBuilder.makeCall(SqlStdOperatorTable.EQUALS, rexBuilder.makeInputRef(trt.getFieldList().get(0).getType(), 0), rexBuilder.makeTimestampLiteral(new org.apache.calcite.util.TimestampString("2024-01-15 10:30:00"), 0)))), cluster, null)); }

  @Test @DisplayName("Operator table function round-trip (UPPER)") void roundTrip_withOperatorTableFunction() {
    LogicalProject proj = LogicalProject.create(valuesRel, Collections.emptyList(), List.of(rexBuilder.makeCall(SqlStdOperatorTable.UPPER, rexBuilder.makeInputRef(rowType.getFieldList().get(0).getType(), 0))), List.of("upper_name"));
    RelNode d = RelNodeSerializer.deserialize(RelNodeSerializer.serialize(proj), cluster, null);
    assertNotNull(d); assertEquals(1, d.getRowType().getFieldCount()); assertEquals("upper_name", d.getRowType().getFieldNames().get(0));
  }

  @Test @DisplayName("Round-trip for PPL UDF (JSON_EXTRACT)") void roundTrip_pplUdf() {
    // Test that PPL user-defined functions survive serialization round-trip
    LogicalProject proj = LogicalProject.create(valuesRel, Collections.emptyList(),
        List.of(rexBuilder.makeCall(
            org.opensearch.sql.expression.function.PPLBuiltinOperators.JSON_EXTRACT,
            rexBuilder.makeInputRef(rowType.getFieldList().get(0).getType(), 0),
            rexBuilder.makeLiteral("$.key"))),
        List.of("extracted"));
    String j = RelNodeSerializer.serialize(proj);
    assertNotNull(j);
    assertTrue(j.contains("JSON_EXTRACT"));
    RelNode d = RelNodeSerializer.deserialize(j, cluster, null);
    assertNotNull(d);
    assertEquals(1, d.getRowType().getFieldCount());
    assertEquals("extracted", d.getRowType().getFieldNames().get(0));
  }

  @Test @DisplayName("Round-trip for LogicalSystemLimit with QUERY_SIZE_LIMIT") void roundTrip_logicalSystemLimit() {
    // LogicalSystemLimit.create() requires RelCollationTraitDef to be registered in the planner
    VolcanoPlanner collationPlanner = new VolcanoPlanner();
    collationPlanner.addRelTraitDef(ConventionTraitDef.INSTANCE);
    collationPlanner.addRelTraitDef(RelCollationTraitDef.INSTANCE);
    RelOptCluster collationCluster = RelOptCluster.create(collationPlanner, rexBuilder);
    LogicalValues collationValues = LogicalValues.createEmpty(collationCluster, rowType);
    org.opensearch.sql.calcite.plan.rel.LogicalSystemLimit sysLimit =
        org.opensearch.sql.calcite.plan.rel.LogicalSystemLimit.create(
            org.opensearch.sql.calcite.plan.rel.LogicalSystemLimit.SystemLimitType.QUERY_SIZE_LIMIT,
            collationValues,
            rexBuilder.makeLiteral(10000, TYPE_FACTORY.createSqlType(SqlTypeName.INTEGER)));
    String j = RelNodeSerializer.serialize(sysLimit);
    assertNotNull(j);
    assertTrue(j.contains("LogicalSystemLimit"));
    assertTrue(j.contains("QUERY_SIZE_LIMIT"));
    RelNode d = RelNodeSerializer.deserialize(j, collationCluster, null);
    assertNotNull(d);
    assertInstanceOf(org.opensearch.sql.calcite.plan.rel.LogicalSystemLimit.class, d);
    org.opensearch.sql.calcite.plan.rel.LogicalSystemLimit deserialized =
        (org.opensearch.sql.calcite.plan.rel.LogicalSystemLimit) d;
    assertEquals(
        org.opensearch.sql.calcite.plan.rel.LogicalSystemLimit.SystemLimitType.QUERY_SIZE_LIMIT,
        deserialized.getType());
    assertEquals(sysLimit.getRowType().getFieldCount(), d.getRowType().getFieldCount());
    assertEquals(sysLimit.getRowType().getFieldNames(), d.getRowType().getFieldNames());
  }

  @Test @DisplayName("Malformed JSON error") void deserialize_malformed() { assertThrows(IllegalArgumentException.class, () -> RelNodeSerializer.deserialize("not valid json", cluster, null)); }
  @Test @DisplayName("Empty rels error") void deserialize_emptyRels() { assertThrows(Exception.class, () -> RelNodeSerializer.deserialize("{\"rels\":[]}", cluster, null)); }

  @Test @DisplayName("Sort with offset and fetch") void roundTrip_sort_withOffsetAndFetch() {
    LogicalSort s = LogicalSort.create(valuesRel, RelCollations.of(new RelFieldCollation(1, RelFieldCollation.Direction.ASCENDING, RelFieldCollation.NullDirection.LAST)), rexBuilder.makeLiteral(10, TYPE_FACTORY.createSqlType(SqlTypeName.INTEGER)), rexBuilder.makeLiteral(20, TYPE_FACTORY.createSqlType(SqlTypeName.INTEGER)));
    RelNode d = RelNodeSerializer.deserialize(RelNodeSerializer.serialize(s), cluster, null);
    assertNotNull(d); assertEquals(s.getRowType().getFieldCount(), d.getRowType().getFieldCount());
  }

  @Test @DisplayName("Composite plan: Filter -> Project -> Sort") void roundTrip_compositePlan() {
    LogicalFilter f = LogicalFilter.create(valuesRel, rexBuilder.makeCall(SqlStdOperatorTable.GREATER_THAN, rexBuilder.makeInputRef(rowType.getFieldList().get(1).getType(), 1), rexBuilder.makeLiteral(25, TYPE_FACTORY.createSqlType(SqlTypeName.INTEGER))));
    LogicalProject proj = LogicalProject.create(f, Collections.emptyList(), List.of(rexBuilder.makeInputRef(f.getRowType().getFieldList().get(0).getType(), 0), rexBuilder.makeInputRef(f.getRowType().getFieldList().get(2).getType(), 2)), List.of("name", "salary"));
    LogicalSort s = LogicalSort.create(proj, RelCollations.of(new RelFieldCollation(1, RelFieldCollation.Direction.DESCENDING, RelFieldCollation.NullDirection.LAST)), null, null);
    RelNode d = RelNodeSerializer.deserialize(RelNodeSerializer.serialize(s), cluster, null);
    assertNotNull(d); assertEquals(2, d.getRowType().getFieldCount()); assertEquals(List.of("name", "salary"), d.getRowType().getFieldNames());
  }
}
