/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.ppl.calcite.sqlnode;

import static org.apache.calcite.test.Matchers.hasTree;
import static org.hamcrest.MatcherAssert.assertThat;

import java.util.List;
import org.apache.calcite.plan.RelTraitDef;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.test.CalciteAssert;
import org.apache.calcite.tools.FrameworkConfig;
import org.apache.calcite.tools.Frameworks;
import org.apache.calcite.tools.Programs;
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
 * Spike test: drive a PPL query end-to-end through the new path:
 *
 * <pre>
 *   PPL text -> AstBuilder -> UnresolvedPlan -> PplToSqlNode -> SqlNode
 *            -> SqlValidator -> SqlToRelConverter -> RelNode
 * </pre>
 *
 * <p>The validated RelNode must match the expected logical shape (same one we'd get from the
 * existing {@code CalciteRelNodeVisitor} path), proving that PPL semantics survive the
 * Calcite-standard validation/type-coercion pipeline.
 */
public class CalcitePPLSqlNodePathTest {

  private final FrameworkConfig config = buildConfig();
  private final PPLSyntaxParser pplParser = new PPLSyntaxParser();

  private FrameworkConfig buildConfig() {
    SchemaPlus rootSchema = Frameworks.createRootSchema(true);
    SchemaPlus schema =
        CalciteAssert.addSchema(rootSchema, CalciteAssert.SchemaSpec.SCOTT_WITH_TEMPORAL);
    return Frameworks.newConfigBuilder()
        .parserConfig(SqlParser.Config.DEFAULT)
        .defaultSchema(schema)
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
  public void source_only() {
    RelNode root = runViaSqlNode("source=EMP | fields ENAME, DEPTNO");
    String expected =
        "LogicalProject(ENAME=[$1], DEPTNO=[$7])\n" + "  LogicalTableScan(table=[[scott, EMP]])\n";
    assertThat(root, hasTree(expected));
  }

  @Test
  public void where_filters_rows() {
    RelNode root = runViaSqlNode("source=EMP | where DEPTNO > 20 | fields ENAME, DEPTNO");
    // Validator inserts CAST($7):INTEGER because EMP.DEPTNO is TINYINT in the SCOTT schema; this
    // implicit numeric promotion is exactly the type-coercion the SqlNode path is here to enable.
    // The pre-existing RelBuilder path skipped this step.
    String expected =
        "LogicalProject(ENAME=[$1], DEPTNO=[$7])\n"
            + "  LogicalFilter(condition=[>(CAST($7):INTEGER, 20)])\n"
            + "    LogicalTableScan(table=[[scott, EMP]])\n";
    assertThat(root, hasTree(expected));
  }

  @Test
  public void sort_at_pipeline_end_is_preserved() {
    // SORT as the final pipe maps cleanly to a top-level ORDER BY.
    RelNode root = runViaSqlNode("source=EMP | fields ENAME, SAL | sort SAL");
    String expected =
        "LogicalSort(sort0=[$1], dir0=[ASC-nulls-first])\n"
            + "  LogicalProject(ENAME=[$1], SAL=[$5])\n"
            + "    LogicalTableScan(table=[[scott, EMP]])\n";
    assertThat(root, hasTree(expected));
  }

  /**
   * Documents a known POC limitation: PPL `sort | fields` carries the order through to the final
   * result, but SQL semantics treat ORDER BY in a subquery as informational (the validator drops
   * it). A real cutover will need to either lift ORDER BY to the outermost level or post-attach
   * collation from RelRoot. Tracked for the design phase.
   */
  @Test
  public void sort_then_fields_loses_sort_in_pure_sql_translation() {
    RelNode root = runViaSqlNode("source=EMP | sort SAL | fields ENAME, SAL");
    String expectedNoSort =
        "LogicalProject(ENAME=[$1], SAL=[$5])\n" + "  LogicalTableScan(table=[[scott, EMP]])\n";
    assertThat(root, hasTree(expectedNoSort));
  }

  @Test
  public void head_n_caps_rows() {
    RelNode root = runViaSqlNode("source=EMP | head 5 | fields ENAME");
    // The inner SELECT * yields a redundant LogicalProject(*) — harmless; FieldTrimmer or
    // RelOptUtil collapse passes drop it during optimization. The Sort(fetch=5) is preserved.
    String expected =
        "LogicalProject(ENAME=[$1])\n"
            + "  LogicalSort(fetch=[5])\n"
            + "    LogicalProject(EMPNO=[$0], ENAME=[$1], JOB=[$2], MGR=[$3], HIREDATE=[$4],"
            + " SAL=[$5], COMM=[$6], DEPTNO=[$7])\n"
            + "      LogicalTableScan(table=[[scott, EMP]])\n";
    assertThat(root, hasTree(expected));
  }

  @Test
  public void stats_count_no_groupby() {
    RelNode root = runViaSqlNode("source=EMP | stats count()");
    String expected =
        "LogicalAggregate(group=[{}], count()=[COUNT()])\n"
            + "  LogicalTableScan(table=[[scott, EMP]])\n";
    assertThat(root, hasTree(expected));
  }

  @Test
  public void stats_avg_by_dept() {
    RelNode root = runViaSqlNode("source=EMP | stats avg(SAL) by DEPTNO");
    // Standard SQL aggregate produces a clean Aggregate(group=[{0}]) over the trim-projected
    // input; no post-project needed since we're at the top of the tree.
    String expected =
        "LogicalAggregate(group=[{0}], avg(SAL)=[AVG($1)])\n"
            + "  LogicalProject(DEPTNO=[$7], SAL=[$5])\n"
            + "    LogicalTableScan(table=[[scott, EMP]])\n";
    assertThat(root, hasTree(expected));
  }

  @Test
  public void case_when_with_else() {
    // PPL `case(cond, val, ..., else val)` translates to Calcite's SqlCase. The validator inserts
    // CAST($7):INTEGER for DEPTNO (TINYINT in SCOTT) when comparing to integer literals — the same
    // implicit promotion the SqlNode path is designed to enable.
    RelNode root =
        runViaSqlNode(
            "source=EMP | eval a = case(DEPTNO >= 20 AND DEPTNO < 30, 'A', DEPTNO >= 30 AND"
                + " DEPTNO < 40, 'B' else 'C') | fields a");
    String expected =
        "LogicalProject(a=[CASE(SEARCH(CAST($7):INTEGER, Sarg[[20..30)]), 'A', SEARCH(CAST($7"
            + "):INTEGER, Sarg[[30..40)]), 'B', 'C')])\n"
            + "  LogicalTableScan(table=[[scott, EMP]])\n";
    assertThat(root, hasTree(expected));
  }

  @Test
  public void case_when_in_filter() {
    // Note: the existing CalciteRelNodeVisitor path rewrites "not true = X" into "IS NOT TRUE(X)"
    // and inverts the AND/OR structure as a custom PPL simplification. The standards-conformant
    // SqlNode path leaves that as a literal NOT(= true, CASE ...) — semantically equivalent but
    // structurally different. The optimizer can still simplify it; we'd add the same peephole
    // rewrite later if we want exact parity.
    RelNode root =
        runViaSqlNode(
            "source=EMP | where not true = case(DEPTNO in (20, 21), true, DEPTNO in (30, 31),"
                + " false else false)");
    String expected =
        "LogicalProject(EMPNO=[$0], ENAME=[$1], JOB=[$2], MGR=[$3], HIREDATE=[$4], SAL=[$5],"
            + " COMM=[$6], DEPTNO=[$7])\n"
            + "  LogicalFilter(condition=[NOT(=(true, CASE(OR(=(CAST($7):INTEGER, 20),"
            + " =(CAST($7):INTEGER, 21)), true, OR(=(CAST($7):INTEGER, 30), =(CAST($7):INTEGER,"
            + " 31)), false, false)))])\n"
            + "    LogicalTableScan(table=[[scott, EMP]])\n";
    assertThat(root, hasTree(expected));
  }

  @Test
  public void eval_then_fields() {
    RelNode root = runViaSqlNode("source=EMP | eval bonus = SAL * 2 | fields ENAME, bonus");
    // Validator + SqlToRelConverter should resolve SAL, type-coerce the literal 2,
    // and produce a Project carrying the new computed column.
    String expected =
        "LogicalProject(ENAME=[$1], bonus=[*($5, 2)])\n"
            + "  LogicalTableScan(table=[[scott, EMP]])\n";
    assertThat(root, hasTree(expected));
  }
}
