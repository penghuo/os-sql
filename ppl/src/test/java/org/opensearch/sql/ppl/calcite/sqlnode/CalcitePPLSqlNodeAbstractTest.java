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

import java.util.List;
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
import org.junit.Before;
import org.opensearch.sql.ast.Node;
import org.opensearch.sql.ast.statement.Query;
import org.opensearch.sql.ast.tree.UnresolvedPlan;
import org.opensearch.sql.calcite.sqlnode.PplToSqlNode;
import org.opensearch.sql.calcite.sqlnode.SqlNodePlanner;
import org.opensearch.sql.common.setting.Settings;
import org.opensearch.sql.datasource.DataSourceService;
import org.opensearch.sql.ppl.antlr.PPLSyntaxParser;
import org.opensearch.sql.ppl.calcite.OpenSearchSparkSqlDialect;
import org.opensearch.sql.ppl.parser.AstBuilder;
import org.opensearch.sql.ppl.parser.AstStatementBuilder;

/**
 * Mirrors {@code CalcitePPLAbstractTest}: drives PPL queries through the new {@code
 * PPL→SqlNode→SqlValidator→SqlToRelConverter} pipeline.
 *
 * <p>One sqlnode test class per v2 test class proves command-by-command coverage. Tests share the
 * same PPL inputs as the v2 fixture; assertions can either match the v2 expected logical plan
 * exactly (when the validator-emitted shape is identical) or assert structural properties when the
 * paths legitimately diverge.
 */
public abstract class CalcitePPLSqlNodeAbstractTest {

  protected final PPLSyntaxParser pplParser = new PPLSyntaxParser();
  protected final RelToSqlConverter relToSql =
      new RelToSqlConverter(OpenSearchSparkSqlDialect.DEFAULT);
  protected Settings settings;
  protected DataSourceService dataSourceService;
  protected FrameworkConfig config;

  protected CalcitePPLSqlNodeAbstractTest(CalciteAssert.SchemaSpec... schemaSpecs) {
    this.config = buildConfig(schemaSpecs);
  }

  @Before
  public void initSettings() {
    settings = mock(Settings.class);
    dataSourceService = mock(DataSourceService.class);
    doReturn(true).when(settings).getSettingValue(Settings.Key.CALCITE_ENGINE_ENABLED);
    doReturn(true).when(settings).getSettingValue(Settings.Key.CALCITE_SUPPORT_ALL_JOIN_TYPES);
    doReturn(true).when(settings).getSettingValue(Settings.Key.PPL_SYNTAX_LEGACY_PREFERRED);
    doReturn(-1).when(settings).getSettingValue(Settings.Key.PPL_JOIN_SUBSEARCH_MAXOUT);
    doReturn(-1).when(settings).getSettingValue(Settings.Key.PPL_SUBSEARCH_MAXOUT);
    doReturn(10).when(settings).getSettingValue(Settings.Key.PPL_REX_MAX_MATCH_LIMIT);
    doReturn("simple_pattern").when(settings).getSettingValue(Settings.Key.PATTERN_METHOD);
    doReturn("LABEL").when(settings).getSettingValue(Settings.Key.PATTERN_MODE);
    doReturn(10).when(settings).getSettingValue(Settings.Key.PATTERN_MAX_SAMPLE_COUNT);
    doReturn(false).when(dataSourceService).dataSourceExists(any());
  }

  private FrameworkConfig buildConfig(CalciteAssert.SchemaSpec... schemaSpecs) {
    SchemaPlus rootSchema = Frameworks.createRootSchema(true);
    SchemaPlus schema = CalciteAssert.addSchema(rootSchema, schemaSpecs);
    return Frameworks.newConfigBuilder()
        .parserConfig(SqlParser.Config.DEFAULT)
        .defaultSchema(schema)
        .traitDefs((List<RelTraitDef>) null)
        .programs(Programs.heuristicJoinOrder(Programs.RULE_SET, true, 2))
        .build();
  }

  /** Drive a PPL query through the SqlNode path and return the resulting RelNode. */
  protected RelNode getRelNode(String ppl) {
    SqlNodePlanner planner = new SqlNodePlanner(config);
    UnresolvedPlan plan = parse(ppl);
    SqlNode sqlNode = new PplToSqlNode(planner.rowTypeOracle()).visit(plan);
    return planner.plan(sqlNode);
  }

  protected UnresolvedPlan parse(String ppl) {
    AstStatementBuilder builder =
        new AstStatementBuilder(
            new AstBuilder(ppl, settings),
            AstStatementBuilder.StatementBuilderContext.builder().build());
    Node node = builder.visit(pplParser.parse(ppl));
    return ((Query) node).getPlan();
  }

  protected void verifyLogical(RelNode rel, String expectedLogical) {
    assertThat(rel, hasTree(expectedLogical));
  }

  protected void verifyPPLToSparkSQL(RelNode rel, String expected) {
    String normalized = expected.replace("\n", System.lineSeparator());
    SqlImplementor.Result result = relToSql.visitRoot(rel);
    final SqlNode sqlNode = result.asStatement();
    final String sql = sqlNode.toSqlString(OpenSearchSparkSqlDialect.DEFAULT).getSql();
    assertThat(sql, is(normalized));
  }

  /**
   * Assert the query translates to a non-null RelNode without throwing. Used for "this PPL command
   * works through the SqlNode path" coverage tests where the exact tree shape is checked separately
   * (or matches v2 closely enough to share assertions).
   */
  protected void assertTranslates(String ppl) {
    RelNode root = getRelNode(ppl);
    assertThat("RelNode produced for: " + ppl, root != null, is(true));
  }
}
