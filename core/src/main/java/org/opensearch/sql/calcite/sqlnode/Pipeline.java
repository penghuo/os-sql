/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.sqlnode;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlOrderBy;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParserPos;

/**
 * Mutable in-flight {@link SqlSelect} state being assembled by the PPL→SqlNode walk.
 *
 * <p>{@code PplToSqlNode}'s visitors mutate a {@code Pipeline} instance as each pipe is processed,
 * collapsing consecutive pipes into a single SELECT until structural constraints (alias visibility,
 * row-cap ordering, etc.) require a subquery wrap. {@link #wrap()} closes the in-flight SELECT and
 * starts a new one whose FROM is the just-closed select; {@link #toFinalSqlNode()} produces the
 * final SqlNode tree once the pipeline is complete.
 */
final class Pipeline {

  private static final SqlParserPos POS = SqlParserPos.ZERO;

  SqlNode from;
  SqlNode where;

  /** {@code null} means "SELECT *" (un-modified projection). */
  List<SqlNode> projection;

  List<SqlNode> groupBy;
  List<SqlNode> orderBy;
  SqlNode fetch;
  SqlNode offset;

  /**
   * Set when the plan tree contains any StreamWindow. Causes visitRelation to wrap the source in a
   * SELECT that adds a {@code __stream_seq__} ROW_NUMBER() column, so multi-streamstats share a
   * stable global ordering.
   */
  boolean injectStreamSeq;

  /**
   * PPL preserves SORT/HEAD effects through downstream pipes. SQL ORDER BY in a subquery is
   * informational only — so we accumulate them at the pipeline level and apply as the outermost
   * {@link SqlOrderBy} once the entire pipeline has been walked.
   */
  List<SqlNode> outerOrderBy;

  SqlNode outerFetch;
  SqlNode outerOffset;

  /**
   * Most recent sort keys seen anywhere in the pipeline (outer or flushed inner). Survives wrap so
   * a downstream {@code reverse} can recover the order even after a fields-projection narrowed
   * columns and forced an inner-flush wrap. Cleared by aggregations that destroy row-level
   * collation.
   */
  List<SqlNode> lastOrderBy;

  /** True when an Eval pipe added alias columns to the projection. */
  boolean evalExtended;

  /** True when a Project pipe replaced the projection list. */
  boolean projectionReplaced;

  /**
   * Names introduced by Eval/Bin/etc. that live in the current SELECT list. SQL doesn't allow
   * SELECT-list aliases to be referenced in the same SELECT; PPL allows it across pipes (and within
   * the same eval's left-to-right list). We track these so a downstream Eval/Where that references
   * an earlier name knows to wrap into a subquery first.
   */
  final Set<String> evalAliasNames = new HashSet<>();

  /**
   * Track the table alias set by a {@code source = X as <name>} SubqueryAlias so Stats / Sort /
   * Eval that wrap state.from can re-attach the alias on the wrapping SELECT — letting downstream
   * agg/sort expressions like {@code <name>.<col>} keep resolving.
   */
  String subqueryAliasName;

  /**
   * Most-recent JOIN's left-side alias, if any. PPL semantics: a bare column reference like {@code
   * name} after a join binds to the LEFT side. Calcite's validator throws "Column 'name' is
   * ambiguous" instead. Set by visitJoin so visitProject can qualify bare references with the left
   * alias when the same column also exists on the right side.
   */
  String joinLeftAlias;

  /** Right-side alias for the same scope. */
  String joinRightAlias;

  void setFrom(SqlNode f) {
    from = f;
  }

  void addWhere(SqlNode cond) {
    where =
        where == null ? cond : new SqlBasicCall(SqlStdOperatorTable.AND, List.of(where, cond), POS);
  }

  void addEvalAlias(SqlNode expr, String alias) {
    if (projection == null) {
      projection = new ArrayList<>();
      projection.add(SqlIdentifier.star(POS));
    }
    projection.add(asAlias(expr, alias));
    evalExtended = true;
    evalAliasNames.add(alias);
  }

  void setProjection(List<SqlNode> list) {
    projection = list;
    projectionReplaced = true;
  }

  void setGroupBy(List<SqlNode> keys) {
    groupBy = keys;
  }

  void setOrderBy(List<SqlNode> keys) {
    orderBy = keys;
    lastOrderBy = keys;
  }

  void setFetch(SqlNode f) {
    fetch = f;
  }

  void setOffset(SqlNode o) {
    offset = o;
  }

  void setOuterOrderBy(List<SqlNode> keys) {
    outerOrderBy = keys;
    lastOrderBy = keys;
  }

  void setOuterFetch(SqlNode f) {
    outerFetch = f;
  }

  /** Close the current SqlSelect and start a new one whose FROM is the just-closed select. */
  void wrap() {
    from = toSqlNode();
    // Re-attach the SubqueryAlias name so downstream `<alias>.<col>` references in stats/sort
    // expressions still resolve. Without this, Stats's wrap-then-emit-agg-expr path emits
    // `(SELECT * FROM X AS i WHERE ...) (no outer alias)` and `max(i.uid)` raises
    // "Table 'i' not found".
    //
    // When no SubqueryAlias name is in play, attach a default `__pipe_in_` alias so the
    // validator's fullyQualify expansion uses that stable name instead of an auto-assigned
    // EXPR$N. Auto-aliases are derived from the validator's per-context counter and the
    // resulting mutation can fail to resolve under a different validation context (e.g. when
    // the same wrapped subquery participates in multiple union operands or correlation
    // scopes). Skip when the FROM is already AS-wrapped (avoid double-aliasing).
    if (subqueryAliasName != null) {
      from =
          new SqlBasicCall(
              SqlStdOperatorTable.AS,
              List.of(from, new SqlIdentifier(subqueryAliasName, POS)),
              POS);
    } else if (!(from instanceof SqlBasicCall sbc && sbc.getOperator() == SqlStdOperatorTable.AS)) {
      from =
          new SqlBasicCall(
              SqlStdOperatorTable.AS, List.of(from, new SqlIdentifier("__pipe_in_", POS)), POS);
    }
    where = null;
    projection = null;
    groupBy = null;
    orderBy = null;
    fetch = null;
    offset = null;
    evalExtended = false;
    projectionReplaced = false;
    // After wrap, prior eval-aliases become regular columns of the new FROM; they are no
    // longer SELECT-list-only and thus referenceable normally.
    evalAliasNames.clear();
    // outerOrderBy/outerFetch are deliberately preserved across wraps — they apply at the
    // outermost level of the final SqlNode tree, regardless of pipe nesting.
  }

  /**
   * Drop all in-flight pipeline state, including the FROM. Used by commands that build a brand-new
   * source (Union, Append, Join) where the existing pipeline has already been compiled into a
   * subquery and what remains is a different FROM expression.
   */
  void reset() {
    from = null;
    where = null;
    projection = null;
    groupBy = null;
    orderBy = null;
    fetch = null;
    offset = null;
    evalExtended = false;
    projectionReplaced = false;
    evalAliasNames.clear();
    // outerOrderBy/outerFetch are deliberately preserved.
  }

  /**
   * Build the final SqlNode tree for the whole pipeline: take the in-flight select and wrap it in a
   * top-level {@link SqlOrderBy} carrying any pending outer sort/fetch from upstream
   * SORT/HEAD/LIMIT pipes that need to survive subsequent pipes.
   */
  SqlNode toFinalSqlNode() {
    // The validator needs a query expression (SELECT ...) at the top level, never a bare
    // table identifier. Force SELECT * FROM <table> when nothing else is set so a bare
    // `source=test` query reaches validation as a complete SELECT.
    SqlNode body;
    if (where == null
        && projection == null
        && groupBy == null
        && orderBy == null
        && fetch == null
        && from instanceof SqlIdentifier) {
      SqlNodeList selectList = new SqlNodeList(POS);
      selectList.add(SqlIdentifier.star(POS));
      body =
          new SqlSelect(
              POS,
              /* keywordList */ null,
              selectList,
              from,
              /* where */ null,
              /* group */ null,
              /* having */ null,
              /* windowList */ null,
              /* qualify */ null,
              /* orderBy */ null,
              /* offset */ null,
              /* fetch */ null,
              /* hints */ null);
    } else {
      body = toSqlNode();
    }
    if (outerOrderBy != null || outerFetch != null || outerOffset != null) {
      SqlNodeList ord = new SqlNodeList(POS);
      if (outerOrderBy != null) {
        for (SqlNode n : outerOrderBy) {
          ord.add(n);
        }
      }
      return new SqlOrderBy(POS, body, ord, /* offset */ outerOffset, outerFetch);
    }
    return body;
  }

  SqlNode toSqlNode() {
    // Nothing populated besides a bare table reference — return identifier directly.
    if (where == null
        && projection == null
        && groupBy == null
        && orderBy == null
        && fetch == null
        && offset == null
        && from instanceof SqlIdentifier) {
      return from;
    }
    SqlNodeList selectList = new SqlNodeList(POS);
    if (projection == null) {
      selectList.add(SqlIdentifier.star(POS));
    } else {
      for (SqlNode n : projection) {
        selectList.add(n);
      }
    }
    SqlNodeList groupList = null;
    if (groupBy != null && !groupBy.isEmpty()) {
      groupList = new SqlNodeList(POS);
      for (SqlNode n : groupBy) {
        groupList.add(n);
      }
    }
    // Build a plain SELECT ... FROM ... [WHERE ...] [GROUP BY ...]; ORDER BY / FETCH go into a
    // wrapping SqlOrderBy. Putting them on the SqlSelect directly trips Calcite's
    // precedence-driven subquery-wrap path during unparse and during validation, dropping the
    // order on the outermost select.
    SqlSelect select =
        new SqlSelect(
            POS,
            /* keywordList */ null,
            selectList,
            from,
            where,
            groupList,
            /* having */ null,
            /* windowList */ null,
            /* qualify */ null,
            /* orderBy */ null,
            /* offset */ null,
            /* fetch */ null,
            /* hints */ null);
    if (orderBy != null || fetch != null || offset != null) {
      SqlNodeList ord = new SqlNodeList(POS);
      if (orderBy != null) {
        for (SqlNode n : orderBy) {
          ord.add(n);
        }
      }
      return new SqlOrderBy(POS, select, ord, offset, fetch);
    }
    return select;
  }

  /**
   * Build {@code expr AS alias} where {@code alias} is emitted as a quoted identifier. Quoting
   * bypasses the validator's niladic-function lookup (USER, CURRENT_USER, LOCALTIME, ...) which
   * would otherwise shadow alias names matching those function names.
   */
  private static SqlNode asAlias(SqlNode expr, String alias) {
    SqlIdentifier quotedAlias =
        new SqlIdentifier(
            java.util.Collections.singletonList(alias),
            null,
            POS,
            List.of(SqlParserPos.ZERO.withQuoting(true)));
    return new SqlBasicCall(SqlStdOperatorTable.AS, List.of(expr, quotedAlias), POS);
  }
}
