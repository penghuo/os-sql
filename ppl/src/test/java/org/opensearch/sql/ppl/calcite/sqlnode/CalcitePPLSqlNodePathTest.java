/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.ppl.calcite.sqlnode;

import static org.apache.calcite.test.Matchers.hasTree;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

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
  public void multi_source_unions_tables() {
    // Use EMP twice (same row type) so SQL UNION ALL row-type unification trivially succeeds.
    // PPL's existing path widens schemas across tables — that broader unification is an open
    // gap for the SqlNode path.
    RelNode root = runViaSqlNode("source=EMP, EMP | fields ENAME");
    String tree = root.explain();
    assertThat(
        "plan contains LogicalUnion(all=[true])",
        tree.contains("LogicalUnion(all=[true])"),
        is(true));
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
   * PPL `sort | fields` keeps the sort through downstream pipes. We achieve this by storing
   * SORT/HEAD/LIMIT effects at the pipeline level (not the inner select) and applying them as the
   * outermost SqlOrderBy. The validator no longer treats them as informational subquery sorts.
   */
  @Test
  public void sort_then_fields_preserves_sort() {
    RelNode root = runViaSqlNode("source=EMP | sort SAL | fields ENAME, SAL");
    String expected =
        "LogicalSort(sort0=[$1], dir0=[ASC-nulls-first])\n"
            + "  LogicalProject(ENAME=[$1], SAL=[$5])\n"
            + "    LogicalTableScan(table=[[scott, EMP]])\n";
    assertThat(root, hasTree(expected));
  }

  @Test
  public void head_n_caps_rows() {
    RelNode root = runViaSqlNode("source=EMP | head 5 | fields ENAME");
    // FETCH is lifted to the outermost SqlOrderBy so it sits above the projection in the
    // resulting RelNode — semantically correct and produces a tight tree with no redundant
    // intermediate Project(*).
    String expected =
        "LogicalSort(fetch=[5])\n"
            + "  LogicalProject(ENAME=[$1])\n"
            + "    LogicalTableScan(table=[[scott, EMP]])\n";
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
  public void function_resolved_by_validator() {
    // `upper` is not in our hand-rolled operator table — it must be resolved by the validator
    // via the chained SqlOperatorTable (PPLBuiltinOperators + SqlStdOperatorTable).
    RelNode root = runViaSqlNode("source=EMP | eval n = upper(ENAME) | fields n");
    String expected =
        "LogicalProject(n=[UPPER($1)])\n" + "  LogicalTableScan(table=[[scott, EMP]])\n";
    assertThat(root, hasTree(expected));
  }

  @Test
  public void lookup_append_left_joins_lookup_table() {
    // PPL's `append` strategy keeps all input columns and appends the lookup-side outputs.
    // (`output` in PPL semantics maps to OutputStrategy.REPLACE, which needs schema enumeration
    // and is currently unsupported in the SqlNode POC — the visitor throws and QueryService
    // falls back to the legacy path.)
    RelNode root = runViaSqlNode("source=EMP | lookup DEPT DEPTNO append LOC");
    String tree = root.explain();
    // Expected shape: LogicalJoin(condition=[=($7, $9)], joinType=[left]) over EMP and a
    // projected DEPT side (LOC, DEPTNO).
    assertThat("plan contains LogicalJoin", tree.contains("LogicalJoin"), is(true));
    assertThat("plan contains joinType=[left]", tree.contains("joinType=[left]"), is(true));
    assertThat("plan reads scott.EMP", tree.contains("[scott, EMP]"), is(true));
    assertThat("plan reads scott.DEPT", tree.contains("[scott, DEPT]"), is(true));
    assertThat("LOC is selected from lookup side", tree.contains("LOC=["), is(true));
  }

  @Test
  public void parse_regex_named_groups() {
    // Mirrors CalcitePPLParseTest.testParse — extract a named group via PARSE+ITEM.
    RelNode root =
        runViaSqlNode(
            "source=EMP | parse DATE_FORMAT(HIREDATE, '%Y-%m-%d')"
                + " '(?<year>\\d{4})-\\d{2}-\\d{2}' | fields JOB, year");
    // Existing path emits explicit `:VARCHAR` type suffixes on string literals (via makeLiteral
    // with the type set). The SqlNode path lets the validator infer the literal type, producing
    // bare 'foo' (without the suffix) — semantically identical.
    String expected =
        "LogicalProject(JOB=[$2], year=[ITEM(PARSE(DATE_FORMAT($4, '%Y-%m-%d'),"
            + " '(?<year>\\d{4})-\\d{2}-\\d{2}', 'regex'), 'year')])\n"
            + "  LogicalTableScan(table=[[scott, EMP]])\n";
    assertThat(root, hasTree(expected));
  }

  @Test
  public void streamstats_no_partition_excluding_current() {
    // Mirrors CalcitePPLStreamstatsTest.testStreamstatsCurrent — running max excluding current row.
    RelNode root = runViaSqlNode("source=EMP | streamstats current = false max(SAL)");
    String tree = root.explain();
    assertThat(
        "MAX over an unbounded-preceding-to-1-preceding ROWS frame is present",
        tree.contains("MAX($5) OVER (ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING)"),
        is(true));
  }

  @Test
  public void dedup_one_field() {
    RelNode root = runViaSqlNode("source=EMP | dedup 1 DEPTNO");
    String tree = root.explain();
    // Expect ROW_NUMBER OVER (PARTITION BY DEPTNO), an IS NOT NULL filter, and a <= 1 cap.
    assertThat(
        "ROW_NUMBER over partition is present",
        tree.contains("ROW_NUMBER() OVER (PARTITION BY"),
        is(true));
    assertThat("IS NOT NULL filter for non-keepempty", tree.contains("IS NOT NULL"), is(true));
    assertThat(
        "row-cap filter present", tree.contains("LogicalFilter") && tree.contains("<=("), is(true));
  }

  @Test
  public void eventstats_count() {
    RelNode root = runViaSqlNode("source=EMP | eventstats count()");
    // The validator inserts an explicit RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED
    // FOLLOWING (semantically the default empty-window for an unbounded aggregate; the existing
    // path abbreviates as `OVER ()`). It also injects `ORDER BY $0` because OVER without ORDER
    // BY is non-deterministic in standard SQL — this matches Calcite's standard window
    // validation. Both behaviors are exactly what the existing CalcitePPLEventstatsTest's
    // verifyPPLToSparkSQL expects.
    String expected =
        "LogicalProject(EMPNO=[$0], ENAME=[$1], JOB=[$2], MGR=[$3], HIREDATE=[$4], SAL=[$5],"
            + " COMM=[$6], DEPTNO=[$7], count()=[COUNT() OVER (ORDER BY $0 RANGE BETWEEN"
            + " UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)])\n"
            + "  LogicalTableScan(table=[[scott, EMP]])\n";
    assertThat(root, hasTree(expected));
  }

  @Test
  public void eventstats_by() {
    RelNode root = runViaSqlNode("source=EMP | eventstats max(SAL) by DEPTNO");
    String expected =
        "LogicalProject(EMPNO=[$0], ENAME=[$1], JOB=[$2], MGR=[$3], HIREDATE=[$4], SAL=[$5],"
            + " COMM=[$6], DEPTNO=[$7], max(SAL)=[MAX($5) OVER (PARTITION BY $7 ORDER BY $0"
            + " RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)])\n"
            + "  LogicalTableScan(table=[[scott, EMP]])\n";
    assertThat(root, hasTree(expected));
  }

  @Test
  public void where_in_subquery() {
    // PPL `where x in [source=T | ...]` mirrors SQL `WHERE x IN (SELECT ...)`.
    RelNode root =
        runViaSqlNode(
            "source=EMP | where DEPTNO in [source=EMP | eval new_deptno = case(DEPTNO in (20,"
                + " 21), 20, DEPTNO in (30, 31), 30 else 100) | fields new_deptno] | fields"
                + " DEPTNO");
    // The inner subquery becomes a LogicalProject(case(...)) over the EMP scan; the outer
    // becomes a LogicalFilter(IN(DEPTNO, subquery)) over EMP.
    String prefix = "LogicalProject(DEPTNO=[$7])\n";
    String tree = root.explain();
    assertThat(tree.startsWith(prefix), is(true));
    assertThat(tree.contains("LogicalFilter"), is(true));
    assertThat(tree.contains("IN"), is(true));
    assertThat(tree.contains("CASE"), is(true));
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
