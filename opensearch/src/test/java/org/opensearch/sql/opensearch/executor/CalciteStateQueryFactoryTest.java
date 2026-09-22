/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.executor;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.List;
import org.apache.calcite.adapter.enumerable.EnumerableCalc;
import org.apache.calcite.adapter.enumerable.EnumerableConvention;
import org.apache.calcite.adapter.enumerable.EnumerableSort;
import org.apache.calcite.plan.Contexts;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelTraitDef;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexProgramBuilder;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.tools.Frameworks;
import org.apache.calcite.tools.Programs;
import org.apache.calcite.tools.RelBuilder;
import org.junit.jupiter.api.Test;
import org.opensearch.sql.calcite.CalcitePlanContext;
import org.opensearch.sql.calcite.SysLimit;
import org.opensearch.sql.executor.QueryType;
import org.opensearch.sql.opensearch.storage.OpenSearchIndex;
import org.opensearch.sql.opensearch.storage.scan.CalciteEnumerableIndexScan;
import org.opensearch.sql.opensearch.storage.scan.CalciteEnumerableStateStoreScan;
import org.opensearch.sql.opensearch.storage.scan.context.AggSpec;
import org.opensearch.sql.opensearch.storage.scan.context.PushDownContext;

class CalciteStateQueryFactoryTest {

  @Test
  void non_aggregation_uses_root_append_identity_plan() {
    CalciteEnumerableIndexScan scan = createIndexScan(null);

    CalciteStateQueryFactory.Created created = CalciteStateQueryFactory.create(scan);

    assertEquals(CalciteStateQueryFactory.Producer.ROOT_APPEND, created.producer());
    assertEquals(StateStore.UpdateMode.APPEND, created.query().stateStore().updateMode());
    assertInstanceOf(CalciteEnumerableStateStoreScan.class, created.query().physicalPlan());
  }

  @Test
  void composite_aggregation_uses_root_append_identity_plan() {
    AggSpec aggSpec = mock(AggSpec.class);
    when(aggSpec.isCompositeAggregation()).thenReturn(true);
    CalciteEnumerableIndexScan scan = createIndexScan(aggSpec);

    CalciteStateQueryFactory.Created created = CalciteStateQueryFactory.create(scan);

    assertEquals(CalciteStateQueryFactory.Producer.ROOT_APPEND, created.producer());
    assertEquals(StateStore.UpdateMode.APPEND, created.query().stateStore().updateMode());
  }

  @Test
  void pushed_aggregation_replaces_leaf_and_retains_row_local_suffix() {
    AggSpec aggSpec = mock(AggSpec.class);
    when(aggSpec.isCompositeAggregation()).thenReturn(false);
    CalciteEnumerableIndexScan scan = createIndexScan(aggSpec);
    RexProgramBuilder program =
        new RexProgramBuilder(scan.getRowType(), scan.getCluster().getRexBuilder());
    program.addIdentity();
    EnumerableCalc calc = EnumerableCalc.create(scan, program.getProgram());

    CalciteStateQueryFactory.Created created = CalciteStateQueryFactory.create(calc);

    assertEquals(CalciteStateQueryFactory.Producer.AGGREGATION_REPLACE, created.producer());
    assertEquals(StateStore.UpdateMode.REPLACE, created.query().stateStore().updateMode());
    EnumerableCalc rewritten =
        assertInstanceOf(EnumerableCalc.class, created.query().physicalPlan());
    assertInstanceOf(CalciteEnumerableStateStoreScan.class, rewritten.getInput());
  }

  @Test
  void ordered_sort_is_not_exposed_as_partial_state_query() {
    CalciteEnumerableIndexScan scan = createIndexScan(null);
    EnumerableSort sort =
        EnumerableSort.create(scan, RelCollations.of(new RelFieldCollation(0)), null, null);

    assertNull(CalciteStateQueryFactory.create(sort));
  }

  private static CalciteEnumerableIndexScan createIndexScan(AggSpec aggSpec) {
    CalcitePlanContext context = createContext();
    RelDataType rowType =
        context.relBuilder.getTypeFactory().builder().add("value", SqlTypeName.INTEGER).build();
    OpenSearchIndex index = mock(OpenSearchIndex.class);
    PushDownContext pushDownContext = new PushDownContext(index);
    pushDownContext.setAggSpec(aggSpec);
    return new CalciteEnumerableIndexScan(
        context.relBuilder.getCluster(),
        context.relBuilder.getCluster().traitSetOf(EnumerableConvention.INSTANCE),
        List.of(),
        mock(RelOptTable.class),
        index,
        rowType,
        pushDownContext);
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
}
