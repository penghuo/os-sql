/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.ppl.calcite.sqlnode;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

import com.google.common.collect.ImmutableList;
import java.util.List;
import org.apache.calcite.config.CalciteConnectionConfig;
import org.apache.calcite.plan.RelTraitDef;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.Statistic;
import org.apache.calcite.schema.Statistics;
import org.apache.calcite.schema.Table;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.tools.FrameworkConfig;
import org.apache.calcite.tools.Frameworks;
import org.apache.calcite.tools.Programs;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.junit.Test;
import org.opensearch.sql.ast.Node;
import org.opensearch.sql.ast.statement.Query;
import org.opensearch.sql.ast.tree.UnresolvedPlan;
import org.opensearch.sql.calcite.sqlnode.PplToSqlNode;
import org.opensearch.sql.calcite.sqlnode.SqlNodePlanner;
import org.opensearch.sql.ppl.antlr.PPLSyntaxParser;
import org.opensearch.sql.ppl.parser.AstBuilder;
import org.opensearch.sql.ppl.parser.AstStatementBuilder;

/**
 * Tests for PPL pipes that operate on array columns (expand/unnest). Uses a custom in-memory schema
 * since SCOTT doesn't include array columns.
 */
public class CalcitePPLSqlNodeArrayTest {

  /** Schema with an array column for expand testing. */
  public static class TableWithArray implements Table {
    @Override
    public RelDataType getRowType(RelDataTypeFactory factory) {
      return factory
          .builder()
          .add("DEPTNO", SqlTypeName.INTEGER)
          .add("EMPNOS", factory.createArrayType(factory.createSqlType(SqlTypeName.INTEGER), -1))
          .build();
    }

    @Override
    public Statistic getStatistic() {
      return Statistics.of(0d, ImmutableList.of(), RelCollations.createSingleton(0));
    }

    @Override
    public Schema.TableType getJdbcTableType() {
      return Schema.TableType.TABLE;
    }

    @Override
    public boolean isRolledUp(String column) {
      return false;
    }

    @Override
    public boolean rolledUpColumnValidInsideAgg(
        String column,
        SqlCall call,
        @Nullable SqlNode parent,
        @Nullable CalciteConnectionConfig c) {
      return false;
    }
  }

  private final FrameworkConfig config = buildConfig();
  private final PPLSyntaxParser pplParser = new PPLSyntaxParser();

  private FrameworkConfig buildConfig() {
    SchemaPlus root = Frameworks.createRootSchema(true);
    root.add("DEPT", new TableWithArray());
    return Frameworks.newConfigBuilder()
        .parserConfig(SqlParser.Config.DEFAULT)
        .defaultSchema(root)
        .traitDefs((List<RelTraitDef>) null)
        .programs(Programs.heuristicJoinOrder(Programs.RULE_SET, true, 2))
        .build();
  }

  private RelNode runViaSqlNode(String ppl) {
    UnresolvedPlan plan = parse(ppl);
    SqlNode sqlNode = new PplToSqlNode().visit(plan);
    return new SqlNodePlanner(config).plan(sqlNode);
  }

  private UnresolvedPlan parse(String ppl) {
    AstStatementBuilder builder =
        new AstStatementBuilder(
            new AstBuilder(ppl, /* settings */ null),
            AstStatementBuilder.StatementBuilderContext.builder().build());
    Node node = builder.visit(pplParser.parse(ppl));
    return ((Query) node).getPlan();
  }

  @Test
  public void expand_unrolls_array() {
    RelNode root = runViaSqlNode("source=DEPT | expand EMPNOS");
    String tree = root.explain();
    // Expect a Correlate (lateral cross-join) with an Uncollect under it — same shape as the
    // existing CalciteRelNodeVisitor path produces.
    assertThat("plan contains LogicalCorrelate", tree.contains("LogicalCorrelate"), is(true));
    assertThat("plan contains Uncollect", tree.contains("Uncollect"), is(true));
  }
}
