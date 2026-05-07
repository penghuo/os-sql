/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.ppl.calcite.sqlnode;

import static org.apache.calcite.test.Matchers.hasTree;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.opensearch.sql.executor.QueryType.PPL;

import java.util.List;
import org.apache.calcite.plan.Contexts;
import org.apache.calcite.plan.RelTraitDef;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.rel2sql.RelToSqlConverter;
import org.apache.calcite.rel.rel2sql.SqlImplementor;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.test.CalciteAssert;
import org.apache.calcite.tools.FrameworkConfig;
import org.apache.calcite.tools.Frameworks;
import org.apache.calcite.tools.Programs;
import org.apache.calcite.tools.RelBuilder;
import org.junit.Before;
import org.junit.Test;
import org.opensearch.sql.ast.Node;
import org.opensearch.sql.ast.statement.Query;
import org.opensearch.sql.ast.tree.UnresolvedPlan;
import org.opensearch.sql.calcite.CalcitePlanContext;
import org.opensearch.sql.calcite.CalciteRelNodeVisitor;
import org.opensearch.sql.calcite.SysLimit;
import org.opensearch.sql.calcite.sqlnode.PplToSqlNode;
import org.opensearch.sql.calcite.sqlnode.SqlNodePlanner;
import org.opensearch.sql.common.setting.Settings;
import org.opensearch.sql.datasource.DataSourceService;
import org.opensearch.sql.ppl.antlr.PPLSyntaxParser;
import org.opensearch.sql.ppl.calcite.OpenSearchSparkSqlDialect;
import org.opensearch.sql.ppl.parser.AstBuilder;
import org.opensearch.sql.ppl.parser.AstStatementBuilder;

/**
 * Parity check: for a curated set of PPL queries, verify that the new SqlNode path produces a
 * RelNode whose Spark SQL projection is identical to the existing {@code CalciteRelNodeVisitor}
 * path's. RelNode trees can differ structurally (e.g. validator inserts CASTs that the RelBuilder
 * path skips, or merges Project layers differently); the externally visible SQL is what users care
 * about, so that's the parity bar.
 *
 * <p>Where the two paths legitimately diverge (e.g. type coercion casts emitted as explicit CAST by
 * the validator), document the divergence in the test rather than mask it.
 */
public class CalcitePPLSqlNodeParityTest {

  private final FrameworkConfig oldPathConfig = buildConfig();
  private final FrameworkConfig newPathConfig = buildConfig();
  private final PPLSyntaxParser pplParser = new PPLSyntaxParser();
  private final RelToSqlConverter relToSql =
      new RelToSqlConverter(OpenSearchSparkSqlDialect.DEFAULT);

  private Settings settings;
  private DataSourceService dataSourceService;
  private CalciteRelNodeVisitor oldVisitor;

  private FrameworkConfig buildConfig() {
    SchemaPlus rootSchema = Frameworks.createRootSchema(true);
    SchemaPlus schema =
        CalciteAssert.addSchema(rootSchema, CalciteAssert.SchemaSpec.SCOTT_WITH_TEMPORAL);
    return Frameworks.newConfigBuilder()
        .parserConfig(SqlParser.Config.DEFAULT)
        .defaultSchema(schema)
        .traitDefs((List<RelTraitDef>) null)
        .programs(Programs.heuristicJoinOrder(Programs.RULE_SET, true, 2))
        .context(Contexts.of(RelBuilder.Config.DEFAULT))
        .build();
  }

  @Before
  public void init() {
    settings = mock(Settings.class);
    dataSourceService = mock(DataSourceService.class);
    doReturn(true).when(settings).getSettingValue(Settings.Key.CALCITE_ENGINE_ENABLED);
    doReturn(true).when(settings).getSettingValue(Settings.Key.CALCITE_SUPPORT_ALL_JOIN_TYPES);
    doReturn(true).when(settings).getSettingValue(Settings.Key.PPL_SYNTAX_LEGACY_PREFERRED);
    doReturn(-1).when(settings).getSettingValue(Settings.Key.PPL_JOIN_SUBSEARCH_MAXOUT);
    doReturn(-1).when(settings).getSettingValue(Settings.Key.PPL_SUBSEARCH_MAXOUT);
    doReturn(false).when(dataSourceService).dataSourceExists(any());
    oldVisitor = new CalciteRelNodeVisitor(dataSourceService);
  }

  // -- Parity assertions ----------------------------------------------------

  /** Drive {@code ppl} through the existing CalciteRelNodeVisitor path. */
  private RelNode runViaOldPath(String ppl) {
    UnresolvedPlan plan = parse(ppl);
    CalcitePlanContext context =
        CalcitePlanContext.create(oldPathConfig, SysLimit.fromSettings(settings), PPL);
    oldVisitor.analyze(plan, context);
    return context.relBuilder.build();
  }

  /** Drive {@code ppl} through the new SqlNode path. */
  private RelNode runViaNewPath(String ppl) {
    UnresolvedPlan plan = parse(ppl);
    SqlNode sqlNode = new PplToSqlNode().visit(plan);
    return new SqlNodePlanner(newPathConfig).plan(sqlNode);
  }

  private UnresolvedPlan parse(String ppl) {
    AstStatementBuilder b =
        new AstStatementBuilder(
            new AstBuilder(ppl, settings),
            AstStatementBuilder.StatementBuilderContext.builder().build());
    Node node = b.visit(pplParser.parse(ppl));
    return ((Query) node).getPlan();
  }

  private String toSparkSql(RelNode rel) {
    SqlImplementor.Result r = relToSql.visitRoot(rel);
    return r.asStatement().toSqlString(OpenSearchSparkSqlDialect.DEFAULT).getSql();
  }

  /** Asserts both paths emit the same Spark SQL projection of the resulting plan. */
  private void assertSparkSqlMatches(String ppl) {
    String oldSql = toSparkSql(runViaOldPath(ppl));
    String newSql = toSparkSql(runViaNewPath(ppl));
    assertThat("Spark SQL differs for PPL: " + ppl, newSql, is(oldSql));
  }

  // -- Tests ----------------------------------------------------------------

  @Test
  public void source_only_parity() {
    assertSparkSqlMatches("source=EMP | fields ENAME, DEPTNO");
  }

  /**
   * Divergence: validator emits an explicit {@code CAST(DEPTNO AS INTEGER)} when DEPTNO is TINYINT
   * and the literal is INTEGER. This is correct SQL semantics and is exactly the coercion behavior
   * the SqlNode path is designed to enable. The pre-existing RelBuilder path silently skipped the
   * cast.
   */
  @Test
  public void where_filter_emits_explicit_cast_under_validator() {
    String ppl = "source=EMP | where DEPTNO > 20 | fields ENAME, DEPTNO";
    String oldSql = toSparkSql(runViaOldPath(ppl));
    String newSql = toSparkSql(runViaNewPath(ppl));
    assertThat("old path no cast", oldSql.contains("CAST"), is(false));
    assertThat("new path inserts CAST", newSql.contains("CAST(`DEPTNO` AS INTEGER)"), is(true));
  }

  @Test
  public void eval_then_fields_parity() {
    assertSparkSqlMatches("source=EMP | eval bonus = SAL * 2 | fields ENAME, bonus");
  }

  @Test
  public void stats_count_parity() {
    assertSparkSqlMatches("source=EMP | stats count()");
  }

  /**
   * Divergence: column order in the projection. The existing path emits aggregate-then-key ({@code
   * SELECT AVG(SAL), DEPTNO}); the new path emits key-then-aggregate ({@code SELECT DEPTNO,
   * AVG(SAL)}). Both are semantically equivalent and both are valid SQL. A user-visible column
   * order is the only difference.
   */
  @Test
  public void stats_avg_by_dept_column_order_differs() {
    String ppl = "source=EMP | stats avg(SAL) by DEPTNO";
    String oldSql = toSparkSql(runViaOldPath(ppl));
    String newSql = toSparkSql(runViaNewPath(ppl));
    assertThat(
        oldSql,
        is(
            "SELECT AVG(`SAL`) `avg(SAL)`, `DEPTNO`\n"
                + "FROM `scott`.`EMP`\n"
                + "GROUP BY `DEPTNO`"));
    assertThat(
        newSql,
        is(
            "SELECT `DEPTNO`, AVG(`SAL`) `avg(SAL)`\n"
                + "FROM `scott`.`EMP`\n"
                + "GROUP BY `DEPTNO`"));
  }

  @Test
  public void sort_at_pipeline_end_parity() {
    assertSparkSqlMatches("source=EMP | fields ENAME, SAL | sort SAL");
  }

  /**
   * Demonstrates the documented divergence: when sort comes mid-pipeline, the SqlNode path drops
   * the sort under SQL semantics, so the emitted SQL differs from the existing path. This is the
   * tracked design issue, captured here as an explicit non-parity test so a future fix flips it to
   * a parity assertion.
   */
  @Test
  public void sort_then_fields_known_divergence() {
    String ppl = "source=EMP | sort SAL | fields ENAME, SAL";
    String oldSql = toSparkSql(runViaOldPath(ppl));
    String newSql = toSparkSql(runViaNewPath(ppl));
    assertThat("old path keeps the ORDER BY", oldSql.toUpperCase().contains("ORDER BY"), is(true));
    assertThat(
        "new path drops the ORDER BY (known limitation)",
        newSql.toUpperCase().contains("ORDER BY"),
        is(false));
  }

  /**
   * Divergence: the validator emits explicit {@code CAST(DEPTNO AS INTEGER)} inside CASE branches
   * (same root cause as the WHERE test). Both paths produce the same {@code CASE} structure
   * otherwise.
   */
  @Test
  public void case_when_else_emits_explicit_cast_under_validator() {
    String ppl =
        "source=EMP | eval a = case(DEPTNO >= 20 AND DEPTNO < 30, 'A', DEPTNO >= 30 AND DEPTNO <"
            + " 40, 'B' else 'C') | fields a";
    String oldSql = toSparkSql(runViaOldPath(ppl));
    String newSql = toSparkSql(runViaNewPath(ppl));
    assertThat("old path no cast", oldSql.contains("CAST"), is(false));
    assertThat(
        "new path casts inside CASE", newSql.contains("CAST(`DEPTNO` AS INTEGER)"), is(true));
  }

  /** Sanity hook: a tree-shape probe to make refactors visible. */
  @Test
  public void source_only_old_tree_shape() {
    assertThat(
        runViaOldPath("source=EMP | fields ENAME, DEPTNO"),
        hasTree(
            "LogicalProject(ENAME=[$1], DEPTNO=[$7])\n"
                + "  LogicalTableScan(table=[[scott, EMP]])\n"));
  }
}
