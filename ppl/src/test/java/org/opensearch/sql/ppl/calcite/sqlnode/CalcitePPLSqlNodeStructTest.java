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
 * Tests for PPL pipes that need to introspect input row types — flatten, rename — exercised via the
 * row-type oracle wired between {@link PplToSqlNode} and {@link SqlNodePlanner}.
 */
public class CalcitePPLSqlNodeStructTest {

  /** Schema with struct-style flattened columns ("EMP", "EMP.EMPNO", "EMP.EMPNAME"). */
  public static class TableWithStruct implements Table {
    @Override
    public RelDataType getRowType(RelDataTypeFactory factory) {
      RelDataType structType =
          factory
              .builder()
              .add("EMPNO", SqlTypeName.INTEGER)
              .add("EMPNAME", SqlTypeName.VARCHAR)
              .build();
      return factory
          .builder()
          .add("DEPTNO", SqlTypeName.INTEGER)
          .add("EMP", structType)
          .add("EMP.EMPNAME", SqlTypeName.VARCHAR)
          .add("EMP.EMPNO", SqlTypeName.INTEGER)
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
    root.add("DEPT", new TableWithStruct());
    return Frameworks.newConfigBuilder()
        .parserConfig(SqlParser.Config.DEFAULT)
        .defaultSchema(root)
        .traitDefs((List<RelTraitDef>) null)
        .programs(Programs.heuristicJoinOrder(Programs.RULE_SET, true, 2))
        .build();
  }

  private RelNode runViaSqlNode(String ppl) {
    UnresolvedPlan plan = parse(ppl);
    SqlNodePlanner planner = new SqlNodePlanner(config);
    SqlNode sqlNode = new PplToSqlNode(planner.rowTypeOracle()).visit(plan);
    return planner.plan(sqlNode);
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
  public void flatten_expands_struct_subfields() {
    RelNode root = runViaSqlNode("source=DEPT | flatten EMP");
    String tree = root.explain();
    // Expect the original DEPTNO, EMP, plus aliased EMPNAME and EMPNO from the struct.
    assertThat("DEPTNO is preserved", tree.contains("DEPTNO=["), is(true));
    assertThat("EMP is preserved", tree.contains("EMP=["), is(true));
    assertThat("EMPNAME alias is added", tree.contains("EMPNAME=["), is(true));
    assertThat("EMPNO alias is added", tree.contains("EMPNO=["), is(true));
  }

  @Test
  public void flatten_with_explicit_aliases() {
    RelNode root = runViaSqlNode("source=DEPT | flatten EMP as name, number");
    String tree = root.explain();
    assertThat("explicit alias 'name'", tree.contains("name=["), is(true));
    assertThat("explicit alias 'number'", tree.contains("number=["), is(true));
  }
}
