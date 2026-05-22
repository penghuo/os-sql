/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.ppl.calcite.sqlnode;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

import org.apache.calcite.test.CalciteAssert;
import org.junit.Test;

/**
 * Sweep test: covers every PPL command supported through the SqlNode path with a representative
 * query drawn from the v2 {@code CalcitePPL*Test} suite. Each test asserts the command translates
 * without throwing; deeper structural checks live in the per-command test classes.
 *
 * <p>Commands intentionally not yet supported (throwing {@link UnsupportedOperationException} so
 * QueryService falls back to v2): {@code addtotals}, {@code transpose}, {@code bin}, {@code
 * patterns}, {@code appendcol}, {@code graphlookup}, {@code chart}, {@code timechart}, {@code
 * trendline} (WMA only), {@code rex} (sed mode), {@code ad}/{@code ml}/{@code kmeans}.
 */
public class CalcitePPLSqlNodeCommandsTest extends CalcitePPLSqlNodeAbstractTest {

  public CalcitePPLSqlNodeCommandsTest() {
    super(CalciteAssert.SchemaSpec.SCOTT_WITH_TEMPORAL);
  }

  // -- Core data-flow ------------------------------------------------------

  @Test
  public void source_only() {
    assertTranslates("source=EMP");
  }

  @Test
  public void where_filter() {
    assertTranslates("source=EMP | where DEPTNO > 20");
  }

  @Test
  public void fields_select() {
    assertTranslates("source=EMP | fields ENAME, SAL");
  }

  @Test
  public void fields_exclude() {
    assertTranslates("source=EMP | fields - SAL, COMM");
  }

  @Test
  public void eval_extends_row() {
    assertTranslates("source=EMP | eval bonus = SAL * 2 | fields ENAME, bonus");
  }

  @Test
  public void rename_alias() {
    assertTranslates("source=EMP | rename ENAME as employee_name | fields employee_name");
  }

  @Test
  public void sort_then_head() {
    assertTranslates("source=EMP | sort SAL | head 5");
  }

  @Test
  public void limit_caps_rows() {
    assertTranslates("source=EMP | head 3");
  }

  @Test
  public void reverse_default_sort() {
    assertTranslates("source=EMP | sort SAL | reverse | fields ENAME, SAL");
  }

  // -- Aggregation ---------------------------------------------------------

  @Test
  public void stats_count() {
    assertTranslates("source=EMP | stats count()");
  }

  @Test
  public void stats_with_groupby() {
    assertTranslates("source=EMP | stats avg(SAL), max(SAL) by DEPTNO");
  }

  @Test
  public void stats_distinct_count() {
    assertTranslates("source=EMP | stats distinct_count(JOB)");
  }

  @Test
  public void stats_with_span() {
    assertTranslates("source=EMP | stats count() by span(SAL, 1000)");
  }

  // -- Window functions ----------------------------------------------------

  @Test
  public void eventstats_global() {
    assertTranslates("source=EMP | eventstats max(SAL)");
  }

  @Test
  public void eventstats_by_dept() {
    assertTranslates("source=EMP | eventstats max(SAL) by DEPTNO");
  }

  @Test
  public void streamstats_global() {
    assertTranslates("source=EMP | streamstats sum(SAL)");
  }

  @Test
  public void streamstats_partition() {
    assertTranslates("source=EMP | streamstats max(SAL) by DEPTNO");
  }

  @Test
  public void streamstats_window() {
    assertTranslates("source=EMP | streamstats window=3 sum(SAL)");
  }

  @Test
  public void streamstats_current_false() {
    assertTranslates("source=EMP | streamstats current=false max(SAL)");
  }

  // -- Dedup ---------------------------------------------------------------

  @Test
  public void dedup_one_field() {
    assertTranslates("source=EMP | dedup 1 DEPTNO");
  }

  @Test
  public void dedup_n_keepempty() {
    assertTranslates("source=EMP | dedup 1 DEPTNO, JOB keepempty=true");
  }

  @Test
  public void dedup_consecutive() {
    assertTranslates("source=EMP | dedup DEPTNO consecutive=true");
  }

  // -- Schema reshape ------------------------------------------------------

  @Test
  public void rename_multi() {
    assertTranslates("source=EMP | rename ENAME as nm, SAL as salary | fields nm, salary");
  }

  @Test
  public void fillnull_specific_field() {
    assertTranslates("source=EMP | fillnull with 0 in COMM");
  }

  @Test
  public void fillnull_multiple() {
    assertTranslates("source=EMP | fillnull with 0 in COMM, MGR");
  }

  // -- Lookup --------------------------------------------------------------

  @Test
  public void lookup_replace() {
    assertTranslates("source=EMP | lookup DEPT DEPTNO replace LOC");
  }

  @Test
  public void lookup_append() {
    assertTranslates("source=EMP | lookup DEPT DEPTNO append LOC");
  }

  // -- Multi-source --------------------------------------------------------

  @Test
  public void multi_source_comma() {
    // PPL `source=A, B` is OpenSearch-storage-engine-resolved (multi-index/wildcard expansion).
    // The SCOTT schema doesn't carry that resolution, so the validator can't find a table named
    // "EMP,EMP". This isn't a translation gap — the comma-joined identifier flows through as a
    // single scan, which the SqlNode emits correctly. We sanity-check the unparsed SQL contains
    // the joined name rather than executing through the validator.
    org.apache.calcite.sql.SqlNode sqlNode =
        new org.opensearch.sql.calcite.sqlnode.PplToSqlNode().visit(parse("source=EMP, EMP"));
    String unparsed = sqlNode.toString().toUpperCase().replace("\"", "");
    assertThat("joined name appears", unparsed.contains("EMP,EMP"), is(true));
  }

  @Test
  public void union_two_sources() {
    assertTranslates("source=EMP | fields ENAME | union [source=EMP | fields ENAME]");
  }

  @Test
  public void appendpipe_filter() {
    assertTranslates("source=EMP | appendpipe [where DEPTNO = 20]");
  }

  @Test
  public void multisearch_two_branches() {
    assertTranslates("| multisearch [source=EMP | fields ENAME] [source=EMP | fields ENAME]");
  }

  @Test
  public void join_on_condition() {
    assertTranslates("source=EMP | join on EMP.DEPTNO = DEPT.DEPTNO DEPT");
  }

  @Test
  public void left_join_on_condition() {
    assertTranslates("source=EMP | left join on EMP.DEPTNO = DEPT.DEPTNO DEPT");
  }

  // -- Text/regex extraction ----------------------------------------------

  @Test
  public void parse_named_groups() {
    assertTranslates(
        "source=EMP | parse DATE_FORMAT(HIREDATE, '%Y-%m-%d') '(?<year>\\d{4})-\\d{2}-\\d{2}'");
  }

  @Test
  public void rex_extract_named_groups() {
    assertTranslates(
        "source=EMP | rex field=ENAME '(?<first>[A-Z])(?<rest>.*)' | fields first, rest");
  }

  @Test
  public void regex_filter() {
    assertTranslates("source=EMP | regex ENAME='^A.*'");
  }

  @Test
  public void regex_negated_filter() {
    assertTranslates("source=EMP | regex ENAME!='^A.*'");
  }

  // -- Convert -------------------------------------------------------------

  @Test
  public void convert_num_replaces_field() {
    assertTranslates("source=EMP | convert num(SAL)");
  }

  @Test
  public void convert_with_alias() {
    assertTranslates("source=EMP | convert auto(SAL) AS salary_num");
  }

  @Test
  public void convert_multiple() {
    assertTranslates("source=EMP | convert auto(SAL), num(COMM)");
  }

  // -- Search/relevance ---------------------------------------------------

  @Test
  public void search_query_string() {
    // PPL `search source=X cond=val` => `query_string(MAP('query', '<combined>'))` filter. The
    // SqlNode path emits this through PermissiveRelevanceFunctions.QUERY_STRING (a variadic-MAP
    // wrapper) so the validator accepts the call without invoking PPLBuiltinOperators' strict
    // 14-MAP composite checker.
    assertTranslates("search source=EMP DEPTNO=20");
  }

  // -- Replace -------------------------------------------------------------

  @Test
  public void replace_simple() {
    assertTranslates("source=EMP | replace \"CLERK\" WITH \"EMPLOYEE\" IN JOB");
  }

  @Test
  public void replace_multi_field() {
    assertTranslates("source=EMP | replace \"7839\" WITH \"CEO\" IN MGR, EMPNO");
  }

  // -- Multi-value --------------------------------------------------------

  @Test
  public void mvexpand_field() {
    // mvexpand requires an array/multiset field. The SCOTT EMP table has only scalars; the
    // CalcitePPLSqlNodeArrayTest fixture exercises the actual UNNEST path against an array
    // column. Here we only assert the SqlNode emission shape (the validator rejects scalar
    // UNNEST with a clear error).
    org.apache.calcite.sql.SqlNode sqlNode =
        new org.opensearch.sql.calcite.sqlnode.PplToSqlNode()
            .visit(parse("source=EMP | mvexpand ENAME"));
    String unparsed = sqlNode.toString().toUpperCase();
    assertThat("UNNEST appears", unparsed.contains("UNNEST"), is(true));
  }

  @Test
  public void mvcombine_field() {
    assertTranslates("source=EMP | mvcombine ENAME");
  }

  @Test
  public void nomv_field() {
    // nomv rewrites to Eval that wraps with array_compact for arrays. The SCOTT EMP table has
    // scalars; rewriteAsEval still emits the array UDF call which the validator can't resolve
    // here. Sanity-check translation produces a plan node (no exception) by emitting only.
    org.apache.calcite.sql.SqlNode sqlNode =
        new org.opensearch.sql.calcite.sqlnode.PplToSqlNode()
            .visit(parse("source=EMP | nomv ENAME"));
    assertThat("nomv produces a SqlNode", sqlNode != null, is(true));
  }

  // -- Subqueries ---------------------------------------------------------

  @Test
  public void where_in_subquery() {
    assertTranslates("source=EMP | where DEPTNO in [source=EMP | fields DEPTNO] | fields DEPTNO");
  }

  // -- Rare/Top -----------------------------------------------------------

  @Test
  public void top_n_by_dept() {
    assertTranslates("source=EMP | top 3 DEPTNO");
  }

  @Test
  public void rare_n_by_dept() {
    assertTranslates("source=EMP | rare 3 DEPTNO");
  }

  // -- Trendline ----------------------------------------------------------

  @Test
  public void trendline_sma() {
    assertTranslates("source=EMP | trendline sma(3, SAL) AS sma_3");
  }

  // -- Bin -----------------------------------------------------------------

  @Test
  public void bin_span() {
    assertTranslates("source=EMP | bin SAL span=1000");
  }

  @Test
  public void bin_count() {
    assertTranslates("source=EMP | bin SAL bins=10");
  }

  @Test
  public void bin_minspan() {
    assertTranslates("source=EMP | bin SAL minspan=100");
  }

  @Test
  public void bin_range() {
    assertTranslates("source=EMP | bin SAL start=1000 end=5000");
  }

  // -- Patterns ------------------------------------------------------------

  @Test
  public void patterns_simple() {
    assertTranslates("source=EMP | patterns ENAME | fields ENAME, patterns_field");
  }

  // -- Replace / addtotals / addcoltotals ---------------------------------

  @Test
  public void addtotals_row_sum() {
    assertTranslates("source=EMP | addtotals SAL");
  }

  // -- Streamstats reset --------------------------------------------------

  @Test
  public void streamstats_reset_before() {
    assertTranslates("source=EMP | streamstats reset_before=SAL>1000 sum(SAL)");
  }

  @Test
  public void streamstats_reset_after() {
    assertTranslates("source=EMP | streamstats reset_after=SAL<500 sum(SAL)");
  }

  // -- Trendline WMA -------------------------------------------------------

  @Test
  public void trendline_wma() {
    assertTranslates("source=EMP | trendline wma(3, SAL) AS wma_3");
  }

  // -- Rex sed ------------------------------------------------------------

  @Test
  public void rex_sed_substitution() {
    assertTranslates("source=EMP | rex field=ENAME mode=sed 's/A/X/g'");
  }

  // -- Chart --------------------------------------------------------------

  @Test
  public void chart_avg_by() {
    assertTranslates("source=EMP | chart avg(SAL) by DEPTNO");
  }

  // -- Transpose ----------------------------------------------------------

  @Test
  public void transpose_default() {
    assertTranslates("source=EMP | fields ENAME, JOB | transpose 3");
  }

  // -- AppendCol ----------------------------------------------------------

  @Test
  public void appendcol_simple() {
    assertTranslates("source=EMP | appendcol [where DEPTNO = 20]");
  }

  // -- AddTotals col=true (summary row) ----------------------------------

  @Test
  public void addtotals_col_summary_row() {
    assertTranslates("source=EMP | fields DEPTNO, SAL | addtotals SAL col=true");
  }

  @Test
  public void addtotals_col_with_label() {
    assertTranslates(
        "source=EMP | fields DEPTNO, SAL, JOB | addtotals SAL labelfield='JOB' col=true");
  }

  // -- Chart 2D pivot ----------------------------------------------------

  @Test
  public void chart_two_dim_pivot() {
    // chart agg over X by Y — 2D pivot (long format). Top-N + OTHER bucketing not yet enforced
    // but the GROUP BY (X, Y) with NULL labeling works.
    assertTranslates("source=EMP | chart avg(SAL) over DEPTNO by JOB");
  }

  // -- Patterns BRAIN method ---------------------------------------------

  @Test
  public void patterns_brain_method() {
    assertTranslates("source=EMP | patterns ENAME method=BRAIN | fields ENAME, patterns_field");
  }

  @Test
  public void patterns_brain_with_partition() {
    assertTranslates(
        "source=EMP | patterns ENAME by DEPTNO method=BRAIN | fields ENAME, DEPTNO,"
            + " patterns_field");
  }

  // -- Patterns mode=aggregation -----------------------------------------

  @Test
  public void patterns_aggregation_mode() {
    assertTranslates("source=EMP | patterns ENAME mode=aggregation");
  }

  // -- Flatten/Expand -----------------------------------------------------
  //   (covered by CalcitePPLSqlNodeStructTest / ArrayTest with TableWithStruct fixtures)
}
