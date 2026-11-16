/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.ppl.calcite;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.lessThan;
import static org.hamcrest.Matchers.not;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

import java.lang.management.ManagementFactory;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.List;
import org.apache.calcite.DataContext;
import org.apache.calcite.adapter.enumerable.EnumerableRules;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Linq4j;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.plan.RelTraitDef;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Values;
import org.apache.calcite.rel.rules.PruneEmptyRules;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.schema.ScannableTable;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.impl.AbstractTable;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.test.CalciteAssert;
import org.apache.calcite.tools.Frameworks;
import org.apache.calcite.tools.Programs;
import org.junit.Test;
import org.opensearch.sql.calcite.CalcitePlanContext;
import org.opensearch.sql.calcite.plan.rel.EmptyValuesFactory;
import org.opensearch.sql.calcite.plan.rule.EmptyValuesRules;
import org.opensearch.sql.calcite.utils.CalciteToolsHelper;

/**
 * A filter that folds to FALSE becomes an empty {@code Values} over every field of the source,
 * including each sub-field of an object field. Planning it must not cost more as the source gains
 * fields.
 */
public class CalcitePPLWideEmptyValuesTest extends CalcitePPLAbstractTest {

  private static final String OTEL_LOGS_TABLE = "otel_logs";
  private static final int ATTRIBUTE_COUNT = 4000;

  /** Allocation budget for planning and running one query over {@link #OTEL_LOGS_TABLE}. */
  private static final long MAX_ALLOCATED_BYTES = 64L << 20;

  public CalcitePPLWideEmptyValuesTest() {
    super(CalciteAssert.SchemaSpec.SCOTT_WITH_TEMPORAL);
  }

  @Override
  protected Frameworks.ConfigBuilder config(CalciteAssert.SchemaSpec... schemaSpecs) {
    final SchemaPlus rootSchema = Frameworks.createRootSchema(true);
    final SchemaPlus schema = CalciteAssert.addSchema(rootSchema, schemaSpecs);
    schema.add(OTEL_LOGS_TABLE, new OtelLogsTable(ATTRIBUTE_COUNT));
    return Frameworks.newConfigBuilder()
        .parserConfig(SqlParser.Config.DEFAULT)
        .defaultSchema(schema)
        .traitDefs((List<RelTraitDef>) null)
        .programs(Programs.heuristicJoinOrder(Programs.RULE_SET, true, 2));
  }

  @Test
  public void testFilterFoldedToFalseHasNoCollations() {
    RelNode root = getRelNodeRaw("source=EMP | where 'a' = 'b' | eval x = 1");
    assertTrue(root.getInput(0) instanceof Values);
    assertTrue(
        root.getTraitSet().getCollations().stream()
            .allMatch(c -> c.getFieldCollations().isEmpty()));
    assertThat(root.getCluster().getMetadataQuery().collations(root), empty());
  }

  @Test
  public void testNonEmptyValuesKeepCollations() {
    CalcitePlanContext context = createBuilderContext();
    RelNode values = context.relBuilder.values(new String[] {"a"}, 1, 2).build();
    assertThat(values.getCluster().getMetadataQuery().collations(values), not(empty()));
  }

  @Test
  public void testLiteralComparisonFoldedToFalseOnManyAttributes() throws Exception {
    verifyEmptyWithinBudget("source=otel_logs | where 'a' = 'b' | eval x = 1 | head 10");
  }

  @Test
  public void testNumericFieldComparedToEmptyStringOnManyAttributes() throws Exception {
    verifyEmptyWithinBudget(
        "source=otel_logs | where isnotnull(severityNumber) and severityNumber != ''"
            + " | eval x = 1 | head 10");
  }

  @Test
  public void testContradictoryFiltersMergedOnManyAttributes() throws Exception {
    verifyEmptyWithinBudget(
        "source=otel_logs | where severityText = 'a' | where severityText = 'b' | head 10");
  }

  @Test
  public void testFilterFoldedToFalseWithProjectionsAndSortOnManyAttributes() throws Exception {
    verifyEmptyWithinBudget(
        "source=otel_logs | where 'a' = 'b' | eval x = 1, y = 2 | sort severityText"
            + " | fields severityText, x, y");
  }

  @Test
  public void testPlannerUsesEmptyValuesRules() {
    List<RelOptRule> rules = createBuilderContext().relBuilder.getCluster().getPlanner().getRules();
    for (RelOptRule original :
        List.of(
            PruneEmptyRules.PROJECT_INSTANCE,
            PruneEmptyRules.FILTER_INSTANCE,
            PruneEmptyRules.SORT_INSTANCE,
            PruneEmptyRules.JOIN_LEFT_INSTANCE,
            PruneEmptyRules.SORT_FETCH_ZERO_INSTANCE)) {
      RelOptRule registered =
          rules.stream()
              .filter(r -> r.toString().equals(original.toString()))
              .findFirst()
              .orElseThrow();
      assertSame(
          original.toString(),
          EmptyValuesFactory.LOGICAL_BUILDER,
          ((RelRule<?>) registered).config.relBuilderFactory());
    }
    assertTrue(rules.stream().noneMatch(r -> r == EnumerableRules.ENUMERABLE_VALUES_RULE));
    assertTrue(rules.stream().anyMatch(r -> r == EmptyValuesRules.ENUMERABLE_VALUES_RULE));
  }

  /**
   * Plans and runs the query as production does, within {@link #MAX_ALLOCATED_BYTES}. The first run
   * also pays for class loading and code generation, so only the second is measured.
   */
  private void verifyEmptyWithinBudget(String ppl) throws Exception {
    assertEquals(0, planAndRun(ppl));

    com.sun.management.ThreadMXBean threads =
        (com.sun.management.ThreadMXBean) ManagementFactory.getThreadMXBean();
    long threadId = Thread.currentThread().getId();
    long allocatedBefore = threads.getThreadAllocatedBytes(threadId);
    assertEquals(0, planAndRun(ppl));
    assertThat(
        ppl,
        threads.getThreadAllocatedBytes(threadId) - allocatedBefore,
        lessThan(MAX_ALLOCATED_BYTES));
  }

  /** Plans the query, runs it, and returns the number of rows. */
  private int planAndRun(String ppl) throws Exception {
    CalcitePlanContext context = createBuilderContext();
    RelNode rel = CalciteToolsHelper.optimize(getRelNodeRaw(context, ppl), context);
    int rows = 0;
    try (PreparedStatement statement = CalciteToolsHelper.OpenSearchRelRunners.run(context, rel);
        ResultSet resultSet = statement.executeQuery()) {
      while (resultSet.next()) {
        rows++;
      }
    }
    return rows;
  }

  /**
   * OpenTelemetry logs table with 16 top-level fields, as an OpenSearch index exposes it: each
   * object field is a map column, and each of its sub-fields is a column of its own, so {@code
   * attributes} with many sub-fields makes the row type wide.
   */
  private static class OtelLogsTable extends AbstractTable implements ScannableTable {
    private final int attributeCount;

    OtelLogsTable(int attributeCount) {
      this.attributeCount = attributeCount;
    }

    @Override
    public RelDataType getRowType(RelDataTypeFactory typeFactory) {
      RelDataType string = nullable(typeFactory, typeFactory.createSqlType(SqlTypeName.VARCHAR));
      RelDataType bigint = nullable(typeFactory, typeFactory.createSqlType(SqlTypeName.BIGINT));
      RelDataType timestamp =
          nullable(typeFactory, typeFactory.createSqlType(SqlTypeName.TIMESTAMP));
      RelDataType object =
          nullable(
              typeFactory,
              typeFactory.createMapType(
                  typeFactory.createSqlType(SqlTypeName.VARCHAR),
                  typeFactory.createSqlType(SqlTypeName.ANY)));
      RelDataTypeFactory.Builder builder = typeFactory.builder();
      builder.add("@timestamp", timestamp);
      builder.add("observedTimestamp", timestamp);
      builder.add("time", timestamp);
      builder.add("traceId", string);
      builder.add("spanId", string);
      builder.add("severityText", string);
      builder.add("severityNumber", bigint);
      builder.add("body", string);
      builder.add("flags", bigint);
      builder.add("droppedAttributesCount", bigint);
      builder.add("schemaUrl", string);
      builder.add("serviceName", string);
      builder.add("event", object);
      builder.add("instrumentationScope", object);
      builder.add("resource", object);
      builder.add("attributes", object);
      builder.add("event.domain", string);
      builder.add("event.name", string);
      builder.add("instrumentationScope.name", string);
      builder.add("instrumentationScope.version", string);
      builder.add("resource.attributes", object);
      builder.add("resource.attributes.service.name", string);
      builder.add("resource.attributes.host.name", string);
      for (int i = 0; i < attributeCount; i++) {
        builder.add("attributes.attr_" + i, string);
      }
      return builder.build();
    }

    private static RelDataType nullable(RelDataTypeFactory typeFactory, RelDataType type) {
      return typeFactory.createTypeWithNullability(type, true);
    }

    @Override
    public Enumerable<Object[]> scan(DataContext root) {
      return Linq4j.emptyEnumerable();
    }
  }
}
