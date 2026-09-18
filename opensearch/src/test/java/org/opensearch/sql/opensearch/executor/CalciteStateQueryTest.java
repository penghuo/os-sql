/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.executor;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.math.BigDecimal;
import java.util.List;
import java.util.Map;
import org.apache.calcite.adapter.enumerable.EnumerableCalc;
import org.apache.calcite.adapter.enumerable.EnumerableConvention;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Enumerator;
import org.apache.calcite.plan.Contexts;
import org.apache.calcite.plan.RelTraitDef;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexProgramBuilder;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.tools.Frameworks;
import org.apache.calcite.tools.Programs;
import org.apache.calcite.tools.RelBuilder;
import org.junit.jupiter.api.Test;
import org.opensearch.sql.calcite.CalcitePlanContext;
import org.opensearch.sql.calcite.SysLimit;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.data.model.ExprValueUtils;
import org.opensearch.sql.executor.ExecutionEngine.QueryResponse;
import org.opensearch.sql.executor.QueryType;
import org.opensearch.sql.opensearch.storage.scan.CalciteEnumerableStateStoreScan;

class CalciteStateQueryTest {

  @Test
  void cached_plan_reads_the_latest_store_generation_on_each_query() throws Exception {
    CalcitePlanContext planningContext = createContext();
    StateStore store = new StateStore(StateStore.UpdateMode.APPEND);
    CalciteEnumerableStateStoreScan scan = createScan(planningContext, store);
    CalciteStateQuery query = new CalciteStateQuery(scan, store);

    store.publish(List.of(row(1)));
    QueryResponse first = query.query(createContext());

    store.publish(List.of(row(2)));
    QueryResponse second = query.query(createContext());

    assertEquals(List.of(1), values(first));
    assertEquals(List.of(1, 2), values(second));
  }

  @Test
  void enumerator_reads_one_consistent_snapshot_generation() {
    CalcitePlanContext context = createContext();
    StateStore store = new StateStore(StateStore.UpdateMode.APPEND);
    CalciteEnumerableStateStoreScan scan = createScan(context, store);
    store.publish(List.of(row(1)));

    Enumerable<Object> enumerable = scan.scan();
    Enumerator<Object> first = enumerable.enumerator();
    store.publish(List.of(row(2)));

    assertTrue(first.moveNext());
    assertEquals(1, first.current());
    assertFalse(first.moveNext());

    Enumerator<Object> second = enumerable.enumerator();
    assertTrue(second.moveNext());
    assertEquals(1, second.current());
    assertTrue(second.moveNext());
    assertEquals(2, second.current());
    assertFalse(second.moveNext());
  }

  @Test
  void cached_plan_applies_coordinator_post_processing_to_each_snapshot() throws Exception {
    CalcitePlanContext planningContext = createContext();
    StateStore store = new StateStore(StateStore.UpdateMode.REPLACE);
    CalciteEnumerableStateStoreScan scan = createScan(planningContext, store);
    RexNode value = planningContext.rexBuilder.makeInputRef(scan, 0);
    RexNode doubled =
        planningContext.rexBuilder.makeCall(
            SqlStdOperatorTable.MULTIPLY,
            value,
            planningContext.rexBuilder.makeExactLiteral(BigDecimal.valueOf(2)));
    RexProgramBuilder program =
        new RexProgramBuilder(scan.getRowType(), planningContext.rexBuilder);
    program.addProject(value, "value");
    program.addProject(doubled, "doubled");
    EnumerableCalc calc = EnumerableCalc.create(scan, program.getProgram());
    CalciteStateQuery query = new CalciteStateQuery(calc, store);

    store.publish(List.of(row(3)));
    QueryResponse first = query.query(createContext());
    store.publish(List.of(row(7)));
    QueryResponse second = query.query(createContext());

    assertEquals(3, integer(first, "value"));
    assertEquals(6, integer(first, "doubled"));
    assertEquals(7, integer(second, "value"));
    assertEquals(14, integer(second, "doubled"));
  }

  private static CalciteEnumerableStateStoreScan createScan(
      CalcitePlanContext context, StateStore store) {
    RelDataType rowType =
        context.relBuilder.getTypeFactory().builder().add("value", SqlTypeName.INTEGER).build();
    return new CalciteEnumerableStateStoreScan(
        context.relBuilder.getCluster(),
        context.relBuilder.getCluster().traitSetOf(EnumerableConvention.INSTANCE),
        rowType,
        store);
  }

  private static CalcitePlanContext createContext() {
    SchemaPlus rootSchema = Frameworks.createRootSchema(true);
    Frameworks.ConfigBuilder config =
        Frameworks.newConfigBuilder()
            .parserConfig(SqlParser.Config.DEFAULT)
            .defaultSchema(rootSchema)
            .traitDefs((List<RelTraitDef>) null)
            .programs(Programs.heuristicJoinOrder(Programs.RULE_SET, true, 2))
            .context(Contexts.of(RelBuilder.Config.DEFAULT));
    return CalcitePlanContext.create(config.build(), SysLimit.DEFAULT, QueryType.PPL);
  }

  private static ExprValue row(int value) {
    return ExprValueUtils.tupleValue(Map.of("value", value));
  }

  private static List<Integer> values(QueryResponse response) {
    return response.getResults().stream()
        .map(row -> row.tupleValue().get("value").integerValue())
        .toList();
  }

  private static int integer(QueryResponse response, String field) {
    return response.getResults().getFirst().tupleValue().get(field).integerValue();
  }
}
