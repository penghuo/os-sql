/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.sqlnode;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.function.Function;
import org.apache.calcite.sql.JoinType;
import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.SqlSelectKeyword;
import org.apache.calcite.sql.SqlWindow;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.opensearch.sql.ast.AbstractNodeVisitor;
import org.opensearch.sql.ast.expression.AggregateFunction;
import org.opensearch.sql.ast.expression.Alias;
import org.opensearch.sql.ast.expression.AllFields;
import org.opensearch.sql.ast.expression.AllFieldsExcludeMeta;
import org.opensearch.sql.ast.expression.And;
import org.opensearch.sql.ast.expression.Argument;
import org.opensearch.sql.ast.expression.Cast;
import org.opensearch.sql.ast.expression.Compare;
import org.opensearch.sql.ast.expression.DataType;
import org.opensearch.sql.ast.expression.Field;
import org.opensearch.sql.ast.expression.Let;
import org.opensearch.sql.ast.expression.Literal;
import org.opensearch.sql.ast.expression.Not;
import org.opensearch.sql.ast.expression.Or;
import org.opensearch.sql.ast.expression.QualifiedName;
import org.opensearch.sql.ast.expression.Span;
import org.opensearch.sql.ast.expression.SpanUnit;
import org.opensearch.sql.ast.expression.UnresolvedExpression;
import org.opensearch.sql.ast.tree.Aggregation;
import org.opensearch.sql.ast.tree.Dedupe;
import org.opensearch.sql.ast.tree.Eval;
import org.opensearch.sql.ast.tree.Filter;
import org.opensearch.sql.ast.tree.Head;
import org.opensearch.sql.ast.tree.Join;
import org.opensearch.sql.ast.tree.Limit;
import org.opensearch.sql.ast.tree.Parse;
import org.opensearch.sql.ast.tree.Project;
import org.opensearch.sql.ast.tree.RareTopN;
import org.opensearch.sql.ast.tree.Relation;
import org.opensearch.sql.ast.tree.Rename;
import org.opensearch.sql.ast.tree.Reverse;
import org.opensearch.sql.ast.tree.Rex;
import org.opensearch.sql.ast.tree.Sort;
import org.opensearch.sql.ast.tree.SubqueryAlias;
import org.opensearch.sql.ast.tree.UnresolvedPlan;
import org.opensearch.sql.calcite.plan.OpenSearchConstants;
import org.opensearch.sql.calcite.utils.WildcardUtils;

/**
 * PPL AST → Calcite SqlNode translator. Compositional design: each visitor returns the SqlNode for
 * its subtree, and the parent visitor composes children's SqlNodes. Schema lookup goes through a
 * row-type provider keyed by table qualified name — no validator probe.
 *
 * <p>Status: POC, currently handles a subset of PPL commands needed for {@code
 * CalciteFieldsCommandIT} and the no-filter/no-eval/no-stats subset of {@code CalcitePPLJoinIT}.
 * Other commands throw {@code UnsupportedOperationException} until implemented. The legacy {@link
 * PplToSqlNode} remains in the source tree but is no longer wired into {@link
 * org.opensearch.sql.executor.QueryService} once this visitor takes over.
 */
public class PPLToSqlNodeVisitor extends AbstractNodeVisitor<SqlNode, PPLToSqlNodeVisitor.Frame> {

  private static final SqlParserPos POS = SqlParserPos.ZERO;

  /** Per-translation state: the visible field list and any active join-disambiguation hints. */
  static final class Frame {
    List<String> currentFields;

    /**
     * Alias-name synonyms: when AstBuilder injects {@code SubqueryAlias("outer", SubqueryAlias(
     * "inner", X))} for a side that has both an inner {@code as inner} AND an explicit {@code
     * left=outer} or {@code right=outer} join arg, the outer name overrides for the validator while
     * {@code inner} is recorded here so a downstream {@code | fields inner.col} still resolves
     * under {@code outer.col}. Empty when no synonyms are active.
     */
    java.util.Map<String, String> aliasSynonyms = new java.util.LinkedHashMap<>();

    /**
     * Set by an upstream {@code visitJoin} when the explicit-ON path leaves alias scope live for
     * the next pipe (so a downstream {@code | fields t1.col} can resolve). Cleared when a wrap
     * seals scope. {@code null} when no join hint applies. Stored as a single record so the
     * three-field invariant (all-or-none) is enforced by type, not by setter convention.
     */
    JoinHints joinHints;

    /**
     * Most-recent sort keys seen anywhere in the pipeline. Used by {@code visitReverse} to flip the
     * active ordering. Set by {@link SqlBuilder.SelectBuilder#orderBy} via {@code wrap}. Survives a
     * wrap (the keys themselves remain semantically valid even when the SqlSelect changes scope) —
     * only cleared by visitors that destroy row-level collation (e.g. visitAggregation).
     */
    List<SqlNode> lastOrderBy;

    /**
     * Return the most-recent sort keys with each direction flipped (ASC ↔ DESC, NULLS_FIRST ↔
     * NULLS_LAST). Returns {@code null} if no prior sort exists. Used by {@code visitReverse}.
     */
    List<SqlNode> reversedLastOrderBy() {
      return reverseSortKeys(lastOrderBy);
    }
  }

  /** Bind-bare-to-LEFT semantics state for the current join scope. */
  record JoinHints(String leftAlias, String rightAlias, Set<String> ambiguousColumns) {}

  /** Resolves a table qualified name (e.g. {@code ["my_index"]}) to its column names. */
  private final Function<List<String>, List<String>> tableFields;

  /**
   * The Frame currently in scope at expression-translation time. Set by {@link #translate} and
   * swapped by {@link #visitJoin} during the right walk so {@link #expr} and {@link #toIdentifier}
   * can apply alias synonyms recorded by upstream {@code visitSubqueryAlias} chain collapse or
   * {@code applyExplicitAlias} alias displacement. Threading the Frame through every {@code expr()}
   * call would cascade through many helpers; a transient pointer is simpler and the visit is
   * single-threaded.
   */
  private Frame exprFrame;

  public PPLToSqlNodeVisitor(Function<List<String>, List<String>> tableFields) {
    this.tableFields = tableFields;
  }

  /** Public entry point. */
  public SqlNode translate(UnresolvedPlan plan) {
    Frame frame = new Frame();
    this.exprFrame = frame;
    return stripImplicitMetaProjects(plan).accept(this, frame);
  }

  /**
   * Pre-pass: remove every {@code Project([AllFieldsExcludeMeta])} wrapper that AstBuilder inserted
   * around join sides, the top-level query, and subsearches. These wrappers exist only as a
   * v2-engine marker meaning "drop OpenSearch metadata fields from this subtree's output".
   *
   * <p>Materializing them as {@code SELECT cols FROM X} would hide the table identifier {@code X}
   * from the outer scope, breaking downstream {@code X.col} references. We strip them here and
   * apply the metadata filter post-conversion in {@link
   * org.opensearch.sql.executor.QueryService#analyze} via a RelNode-level shuttle.
   *
   * <p>User-written {@code | fields *} parses as {@code Project([AllFields])} (the parent class) so
   * it is NOT touched by this pass — only the AstBuilder-injected {@link AllFieldsExcludeMeta}
   * subclass marker is.
   */
  private static UnresolvedPlan stripImplicitMetaProjects(UnresolvedPlan plan) {
    if (plan instanceof Project p
        && !p.isExcluded()
        && p.getProjectList().size() == 1
        && p.getProjectList().getFirst() instanceof AllFieldsExcludeMeta
        && !p.getChild().isEmpty()) {
      return stripImplicitMetaProjects(p.getChild().get(0));
    }
    if (plan instanceof SubqueryAlias sa && !sa.getChild().isEmpty()) {
      UnresolvedPlan rewritten = stripImplicitMetaProjects(sa.getChild().get(0));
      if (rewritten != sa.getChild().get(0)) {
        return new SubqueryAlias(sa.getAlias(), rewritten);
      }
      return sa;
    }
    if (plan instanceof Join j) {
      UnresolvedPlan newLeft = stripImplicitMetaProjects(j.getChildren().get(0));
      UnresolvedPlan newRight = stripImplicitMetaProjects(j.getRight());
      if (newLeft != j.getChildren().get(0) || newRight != j.getRight()) {
        Join rebuilt =
            new Join(
                newRight,
                j.getLeftAlias(),
                j.getRightAlias(),
                j.getJoinType(),
                j.getJoinCondition(),
                j.getJoinHint(),
                j.getJoinFields(),
                j.getArgumentMap());
        rebuilt.attach(newLeft);
        return rebuilt;
      }
      return j;
    }
    if (plan instanceof Project p && !p.getChild().isEmpty()) {
      UnresolvedPlan rewritten = stripImplicitMetaProjects(p.getChild().get(0));
      if (rewritten != p.getChild().get(0)) {
        Project rebuilt = new Project(p.getProjectList(), p.getArgExprList());
        rebuilt.attach(rewritten);
        return rebuilt;
      }
      return p;
    }
    // Generic unary descent: any other plan with a single child (Aggregation, Sort, Filter,
    // Eval, Rename, Limit, Reverse, Head, ...) re-attaches its rewritten child. Required because
    // AstBuilder injects `Project(AllFieldsExcludeMeta, ...)` markers around join sides and
    // subsearches at any depth, including under non-Project parents like `Aggregation` (which
    // appears at the top of `... | join ... | stats ...` queries).
    if (plan.getChild() != null && plan.getChild().size() == 1) {
      UnresolvedPlan child = (UnresolvedPlan) plan.getChild().get(0);
      UnresolvedPlan rewritten = stripImplicitMetaProjects(child);
      if (rewritten != child) {
        return (UnresolvedPlan) plan.attach(rewritten);
      }
    }
    return plan;
  }

  @Override
  public SqlNode visitRelation(Relation node, Frame frame) {
    List<String> parts = node.getTableQualifiedName().getParts();
    return SqlBuilder.relation(parts, lookupTableFields(parts), frame);
  }

  @Override
  public SqlNode visitSubqueryAlias(SubqueryAlias node, Frame frame) {
    // Collapse `SubqueryAlias("outer", SubqueryAlias("inner", X))` chains. AstBuilder produces
    // this shape when both an explicit `left=outer` (or `right=outer`) join arg AND an inner
    // `as inner` are given on the same side. SQL only accepts one alias per relation; the outer
    // join-arg alias wins, and each inner name is recorded as a synonym so a downstream
    // `| fields inner.col` still resolves.
    UnresolvedPlan child = node.getChild().get(0);
    while (child instanceof SubqueryAlias innerSa) {
      frame.aliasSynonyms.put(innerSa.getAlias(), node.getAlias());
      child = innerSa.getChild().get(0);
    }
    SqlNode inner = child.accept(this, frame);
    // PPL allows `source = X as i` and `[ source = X ] as i`; both produce a SubqueryAlias around
    // either a bare Relation or a sub-pipeline. Calcite needs the alias attached as a SqlNode
    // AS-call so qualified refs `i.col` resolve.
    return SqlBuilder.aliasAs(inner, node.getAlias());
  }

  @Override
  public SqlNode visitProject(Project node, Frame frame) {
    UnresolvedPlan childPlan = node.getChild().get(0);
    SqlNode from = childPlan.accept(this, frame);

    // Short-circuit: a `Project([AllFields])` (the implicit-final `| fields *` injected by
    // AstStatementBuilder, or a user-written `| fields *`) has no projection effect when the
    // child is already a top-level-valid SELECT shape. Returning the child avoids nesting an
    // outer SELECT that seals projection columns inside the FROM subquery — e.g. an aggregation
    // `b.country AS "b.country"` becomes invisible to an outer `SELECT b.country FROM (...)`.
    // Bare SqlJoin / AS-wrapped children still need wrapping so the validator sees a top-level
    // SELECT.
    if (!node.isExcluded()
        && node.getProjectList().size() == 1
        && node.getProjectList().getFirst() instanceof AllFields
        && (from instanceof SqlSelect || from instanceof org.apache.calcite.sql.SqlOrderBy)) {
      return from;
    }

    List<String> selected = resolveSelectedFields(node, frame.currentFields);

    // Build the SELECT list. Two responsibilities here:
    //
    //   1. Emit multi-part SqlIdentifiers for dotted user names (`t1.name` → `[t1, name]`).
    //      The validator resolves the parts itself — alias-qualified column vs STRUCT-field
    //      access is its job, not ours.
    //
    //   2. Disambiguate header labels for dotted refs over a join scope. Calcite labels a
    //      multi-part ref `t1.name` as just `name`; that's fine when the leaf is unique. Quote
    //      with `<dotted> AS "<dotted>"` (preserving the dot in the row-type field name) when:
    //        a. the leaf was already projected by an earlier item (avoid Calcite's `name0` dedup),
    //        b. the dotted prefix matches the RIGHT join alias AND the leaf appears on BOTH sides
    //           (PPL's bind-bare-to-LEFT means `b.col` would otherwise render identically to a
    //           left-side bare ref).
    //      Mirrors the legacy visitor's `joinScope`-aware projection rule.
    SqlNodeList selectList = new SqlNodeList(POS);
    Set<String> seenLeaves = new LinkedHashSet<>();
    JoinHints hints = frame.joinHints;
    for (String name : selected) {
      SqlIdentifier ref = qualifyIfAmbiguous(name, frame);
      String leaf = name.substring(name.lastIndexOf('.') + 1);
      boolean dotted = name.indexOf('.') >= 0;
      String prefix = dotted ? name.substring(0, name.indexOf('.')) : null;
      boolean leafSeen = seenLeaves.contains(leaf);
      boolean rightAliasOverlap =
          dotted
              && hints != null
              && hints.rightAlias() != null
              && hints.rightAlias().equals(prefix)
              && hints.ambiguousColumns().contains(leaf);
      if (dotted && (leafSeen || rightAliasOverlap)) {
        SqlIdentifier alias =
            new SqlIdentifier(
                java.util.Collections.singletonList(name),
                null,
                POS,
                List.of(SqlParserPos.ZERO.withQuoting(true)));
        selectList.add(new SqlBasicCall(SqlStdOperatorTable.AS, List.of(ref, alias), POS));
      } else {
        selectList.add(ref);
      }
      seenLeaves.add(leaf);
    }

    // Peephole: if the child is a SELECT * with no projection of its own (visitSort/visitFilter/
    // visitHead style), pull the user-projection into THAT SELECT instead of nesting a new outer
    // SELECT. Nesting would seal join aliases inside a subquery — `| sort a.age | fields a.name`
    // produces `SELECT a.name FROM (SELECT * FROM <join> ORDER BY a.age)` where the outer SELECT
    // can no longer see `a` from the inner FROM. Rewriting the inner select list keeps the join
    // aliases live in the same SELECT scope.
    SqlNode rewritten = projectIntoChildSelectStar(from, selectList);
    if (rewritten != null) {
      frame.currentFields = stripAliasPrefix(selected);
      return rewritten;
    }
    return SqlBuilder.select(selectList)
        .from(from)
        .withFields(stripAliasPrefix(selected))
        .wrap(frame);
  }

  /**
   * If {@code from} is a {@code SELECT * FROM <inner>} (optionally wrapped in a SqlOrderBy or
   * carrying ORDER BY/FETCH/OFFSET) and no GROUP BY / HAVING, swap its select list for {@code
   * newList} in place and return the rewritten node. Returns null when the shape doesn't match —
   * caller falls back to normal wrapping.
   */
  private static SqlNode projectIntoChildSelectStar(SqlNode from, SqlNodeList newList) {
    SqlSelect select = null;
    if (from instanceof SqlSelect s) {
      select = s;
    } else if (from instanceof org.apache.calcite.sql.SqlOrderBy ob
        && ob.query instanceof SqlSelect s) {
      select = s;
    }
    if (select == null) return null;
    SqlNodeList items = select.getSelectList();
    if (items == null || items.size() != 1) return null;
    SqlNode lone = items.get(0);
    if (!(lone instanceof SqlIdentifier id) || !id.isStar()) return null;
    if (select.getGroup() != null && !select.getGroup().getList().isEmpty()) return null;
    if (select.getHaving() != null) return null;
    select.setSelectList(newList);
    return from;
  }

  @Override
  public SqlNode visitHead(Head node, Frame frame) {
    SqlNode from = node.getChild().get(0).accept(this, frame);
    SqlLiteral fetch = SqlLiteral.createExactNumeric(node.getSize().toString(), POS);
    SqlBuilder.SelectBuilder b = SqlBuilder.select(starList()).from(from).fetch(fetch);
    Integer fromOffset = node.getFrom();
    if (fromOffset != null && fromOffset > 0) {
      b.offset(SqlLiteral.createExactNumeric(fromOffset.toString(), POS));
    }
    return b.wrap(frame);
  }

  @Override
  public SqlNode visitFilter(Filter node, Frame frame) {
    SqlNode from = node.getChild().get(0).accept(this, frame);
    SqlNode where = expr(node.getCondition());
    return SqlBuilder.select(starList()).from(from).where(where).wrap(frame);
  }

  @Override
  public SqlNode visitEval(Eval node, Frame frame) {
    SqlNode from = node.getChild().get(0).accept(this, frame);
    // PPL eval extends the row with N new columns: `SELECT *, e1 AS a1, e2 AS a2, ... FROM
    // <child>`.
    // Within a single eval, later lets may reference earlier aliases (PPL's left-to-right
    // semantics); SQL doesn't allow SELECT-list aliases inside the same SELECT. We honour that by
    // wrapping when a let references a name introduced earlier in this same eval.
    List<String> visible =
        new ArrayList<>(frame.currentFields == null ? List.of() : frame.currentFields);
    Set<String> existingNames = new LinkedHashSet<>(visible);
    SqlNodeList items = new SqlNodeList(POS);
    items.add(SqlIdentifier.star(POS));
    Set<String> aliasesInThisSelect = new LinkedHashSet<>();
    for (Let let : node.getExpressionList()) {
      String alias = let.getVar().getField().toString();
      // If this let's RHS references an alias introduced earlier in this same eval, the previous
      // SELECT can't expose that alias to its own list — wrap and start a new SELECT.
      if (referencesAny(let.getExpression(), aliasesInThisSelect)) {
        from = SqlBuilder.select(items).from(from).withFields(visible).wrap(frame);
        items = new SqlNodeList(POS);
        items.add(SqlIdentifier.star(POS));
        aliasesInThisSelect = new LinkedHashSet<>();
      }
      SqlNode rhs = expr(let.getExpression());
      items.add(asAliased(rhs, alias));
      aliasesInThisSelect.add(alias);
      if (existingNames.add(alias)) {
        visible.add(alias);
      }
    }
    // Peephole: when child is a SELECT * (no GROUP BY / HAVING), append our extra eval items to
    // its select list and reuse it. Avoids nesting `SELECT *, e1 AS a1 FROM (SELECT * FROM <X> AS
    // a JOIN ...)` which seals join aliases inside the FROM subquery, breaking subsequent pipes
    // that reference `a.col`. The `<child>` SELECT keeps its FROM scope live so `a.col` in our
    // appended `e_i` items resolves.
    SqlNode peep = appendEvalToChildSelectStar(from, items, visible, frame);
    if (peep != null) return peep;
    return SqlBuilder.select(items).from(from).withFields(visible).wrap(frame);
  }

  /**
   * If {@code from} is a {@code SELECT *[, ...prior eval items] FROM <inner> [WHERE ...]} (no GROUP
   * BY / HAVING / ORDER BY / FETCH / OFFSET, and the first select-list item is {@code *}), append
   * our extra eval items ({@code [e_i AS a_i, ...]}) to its select list in place and return the
   * rewritten node. Returns null when the shape doesn't match — caller falls back to normal
   * wrapping. The {@code *} as first item keeps the prior pipe's columns visible; chained evals
   * just keep appending.
   */
  private static SqlNode appendEvalToChildSelectStar(
      SqlNode from, SqlNodeList newList, List<String> visible, Frame frame) {
    if (!(from instanceof SqlSelect select)) return null;
    SqlNodeList existing = select.getSelectList();
    if (existing == null || existing.size() < 1) return null;
    SqlNode first = existing.get(0);
    if (!(first instanceof SqlIdentifier id) || !id.isStar()) return null;
    if (select.getGroup() != null && !select.getGroup().getList().isEmpty()) return null;
    if (select.getHaving() != null) return null;
    if (select.getOrderList() != null && !select.getOrderList().getList().isEmpty()) return null;
    if (select.getFetch() != null || select.getOffset() != null) return null;
    // newList starts with `*` then our eval items. Skip its leading `*` (already in `existing`)
    // and append the rest.
    for (int i = 1; i < newList.size(); i++) {
      existing.add(newList.get(i));
    }
    frame.currentFields = visible;
    frame.joinHints = null;
    return select;
  }

  @Override
  public SqlNode visitRename(Rename node, Frame frame) {
    SqlNode from = node.getChild().get(0).accept(this, frame);
    // PPL `rename old1 as new1, old2 as new2` produces a new SELECT list with each renamed
    // column as `<old> AS <new>` and all other columns passed through. Build the list explicitly
    // so the row type's field names reflect the renames; downstream pipes see the new names.
    java.util.Map<String, String> renames = new java.util.LinkedHashMap<>();
    for (org.opensearch.sql.ast.expression.Map m : node.getRenameList()) {
      String oldName = ((Field) m.getOrigin()).getField().toString();
      String newName = ((Field) m.getTarget()).getField().toString();
      renames.put(oldName, newName);
    }
    List<String> incoming = frame.currentFields == null ? List.of() : frame.currentFields;
    List<String> newVisible = new ArrayList<>(incoming.size());
    SqlNodeList items = new SqlNodeList(POS);
    for (String name : incoming) {
      String mapped = renames.get(name);
      if (mapped != null) {
        items.add(asAliased(toIdentifier(name), mapped));
        newVisible.add(mapped);
      } else {
        items.add(toIdentifier(name));
        newVisible.add(name);
      }
    }
    return SqlBuilder.select(items).from(from).withFields(newVisible).wrap(frame);
  }

  @Override
  public SqlNode visitAggregation(Aggregation node, Frame frame) {
    SqlNode from = node.getChild().get(0).accept(this, frame);
    // Detect upstream join AFTER walking the child — only then has visitJoin's joinHints been
    // populated on the frame. (Before recursion the frame still reflects the calling pipe.)
    boolean joinUpstream = isJoinUpstream(frame);
    // SELECT list = aggregates (aliased) + group keys (aliased). FROM = child. GROUP BY = group
    // key expressions (without aliases). The visible field list for the next pipe is the ordered
    // list of agg-output names (PPL convention: aggregates first, span (if any), then by-fields).
    SqlNodeList items = new SqlNodeList(POS);
    List<SqlNode> groupKeys = new ArrayList<>();
    List<String> visible = new ArrayList<>();
    String spanAlias = null;

    for (UnresolvedExpression aggExpr : node.getAggExprList()) {
      String alias = aggLabel(aggExpr);
      UnresolvedExpression core = (aggExpr instanceof Alias a) ? a.getDelegated() : aggExpr;
      SqlNode call = aggCall(core);
      items.add(asAliased(call, alias));
      visible.add(alias);
    }
    // span is a separate field on Aggregation (not in groupExprList). Emit it first in the group
    // list to match PPL's `<aggs> by span(...), <by_fields>` row layout.
    if (node.getSpan() != null) {
      UnresolvedExpression spanExpr = node.getSpan();
      spanAlias = (spanExpr instanceof Alias a) ? a.getName() : spanAutoLabel(spanExpr);
      UnresolvedExpression spanCore = (spanExpr instanceof Alias a) ? a.getDelegated() : spanExpr;
      SqlNode keyExpr = expr(spanCore);
      groupKeys.add(keyExpr);
      items.add(asAliased(keyExpr, spanAlias));
      visible.add(spanAlias);
    }
    for (UnresolvedExpression groupExpr : node.getGroupExprList()) {
      String alias = aggLabel(groupExpr);
      UnresolvedExpression core = (groupExpr instanceof Alias a) ? a.getDelegated() : groupExpr;
      SqlNode keyExpr = expr(core);
      groupKeys.add(keyExpr);
      items.add(asAliased(keyExpr, alias));
      visible.add(alias);
    }
    // Aggregations destroy row-level collation; clear the lastOrderBy hint.
    frame.lastOrderBy = null;

    SqlBuilder.SelectBuilder b =
        SqlBuilder.select(items).from(from).groupBy(groupKeys).withFields(visible);
    // Implicit ORDER BY on the span column when the upstream contains a JOIN. v2's RelBuilder
    // pipeline preserves the OpenSearch terms-aggregation collation from the post-join scan; the
    // SqlNode pipeline emits a plain SQL Aggregate which Calcite is free to render without
    // ordering. Adding an explicit ORDER BY on the span gives deterministic group-output order
    // for `... | join ... | stats ... by span(...)`. Skip when the upstream is unjoined — v2's
    // RelBuilder produces an unordered aggregate too, and adding our own would create cosmetic
    // plan-shape diffs in CalciteExplainIT/Big5.
    if (joinUpstream && spanAlias != null) {
      SqlNode sortKey =
          new SqlBasicCall(
              SqlStdOperatorTable.NULLS_FIRST, List.of(new SqlIdentifier(spanAlias, POS)), POS);
      b.orderBy(java.util.List.of(sortKey));
    }
    return b.wrap(frame);
  }

  /**
   * True when the visitor is operating on output from an upstream JOIN — either the recent join
   * left {@link Frame#joinHints} live on the frame, or the FROM tree of in-flight wraps contains a
   * SqlJoin. Used by {@link #visitAggregation} to add an implicit deterministic ORDER BY on
   * span/group keys (matching v2's RelBuilder collation from terms-aggregation pushdown).
   */
  private static boolean isJoinUpstream(Frame frame) {
    return frame.joinHints != null;
  }

  @Override
  public SqlNode visitRareTopN(RareTopN node, Frame frame) {
    SqlNode from = node.getChild().get(0).accept(this, frame);
    // PPL `rare F by G [usenull=false] [showcount=true]`:
    //   1. Aggregate: `SELECT G, F, COUNT(*) AS count FROM <child> [WHERE G IS NOT NULL AND F IS
    //      NOT NULL] GROUP BY G, F`.
    //   2. Per-G partition cap: ROW_NUMBER() OVER (PARTITION BY G ORDER BY count [ASC for rare,
    //      DESC for top]) <= N.
    //   3. Final projection: `[G..., F..., count?]` with the helper column dropped, and the count
    //      column also dropped when showcount=false.
    org.opensearch.sql.ast.expression.Argument.ArgumentMap argMap =
        org.opensearch.sql.ast.expression.Argument.ArgumentMap.of(node.getArguments());
    String countFieldName = (String) argMap.get(RareTopN.Option.countField.name()).getValue();
    boolean showCount = (Boolean) argMap.get(RareTopN.Option.showCount.name()).getValue();
    boolean useNull = (Boolean) argMap.get(RareTopN.Option.useNull.name()).getValue();
    boolean isTop = node.getCommandType() == RareTopN.CommandType.TOP;

    // Step 1 — collect group keys (G followed by F in PPL row order) and the IS NOT NULL filter
    // when useNull=false.
    List<SqlNode> groupKeys = new ArrayList<>();
    List<SqlNode> partitionKeys = new ArrayList<>();
    List<String> groupNames = new ArrayList<>();
    List<String> fieldNames = new ArrayList<>();
    for (UnresolvedExpression g : node.getGroupExprList()) {
      groupKeys.add(expr(g));
      partitionKeys.add(expr(g));
      groupNames.add(rareGroupName(g));
    }
    for (Field f : node.getFields()) {
      groupKeys.add(expr(f.getField()));
      fieldNames.add(f.getField().toString());
    }

    SqlNode aggFrom = from;
    if (!useNull && !groupKeys.isEmpty()) {
      // Wrap with a WHERE that drops NULL-keyed rows. SELECT * FROM <child> WHERE g IS NOT NULL
      // AND f IS NOT NULL ...
      SqlNode acc = null;
      for (SqlNode k : groupKeys) {
        SqlNode notNull = new SqlBasicCall(SqlStdOperatorTable.IS_NOT_NULL, List.of(k), POS);
        acc =
            (acc == null)
                ? notNull
                : new SqlBasicCall(SqlStdOperatorTable.AND, List.of(acc, notNull), POS);
      }
      SqlNodeList items = new SqlNodeList(POS);
      items.add(SqlIdentifier.star(POS));
      aggFrom =
          new SqlSelect(POS, null, items, from, acc, null, null, null, null, null, null, null);
    }

    // Step 2 — aggregate: SELECT <group-keys>, COUNT(*) AS count FROM <aggFrom> GROUP BY ...
    SqlNodeList aggItems = new SqlNodeList(POS);
    for (int i = 0; i < node.getGroupExprList().size(); i++) {
      aggItems.add(asAliased(groupKeys.get(i), groupNames.get(i)));
    }
    int fieldOffset = node.getGroupExprList().size();
    for (int i = 0; i < node.getFields().size(); i++) {
      aggItems.add(asAliased(groupKeys.get(fieldOffset + i), fieldNames.get(i)));
    }
    SqlNode countCall =
        new SqlBasicCall(SqlStdOperatorTable.COUNT, List.of(SqlIdentifier.star(POS)), POS);
    aggItems.add(asAliased(countCall, countFieldName));
    SqlNodeList groupByList = new SqlNodeList(POS);
    for (SqlNode k : groupKeys) {
      groupByList.add(k);
    }
    SqlNode aggSelect =
        new SqlSelect(
            POS, null, aggItems, aggFrom, null, groupByList, null, null, null, null, null, null);

    // Step 3 — ROW_NUMBER() over (PARTITION BY <G> ORDER BY count [DESC for TOP, ASC for rare])
    // and filter <= N. Partition keys reference the count-aliased columns (so dotted refs become
    // flat names after the wrap).
    SqlNodeList partitionBy = new SqlNodeList(POS);
    for (int i = 0; i < node.getGroupExprList().size(); i++) {
      partitionBy.add(new SqlIdentifier(groupNames.get(i), POS));
    }
    SqlNodeList rnOrderBy = new SqlNodeList(POS);
    SqlNode countRef = new SqlIdentifier(countFieldName, POS);
    rnOrderBy.add(
        isTop ? new SqlBasicCall(SqlStdOperatorTable.DESC, List.of(countRef), POS) : countRef);
    SqlNode rowNumWindow =
        SqlWindow.create(
            null,
            null,
            partitionBy,
            rnOrderBy,
            SqlLiteral.createBoolean(false, POS),
            null,
            null,
            null,
            POS);
    SqlNode rowNum =
        new SqlBasicCall(
            SqlStdOperatorTable.OVER,
            List.of(
                new SqlBasicCall(
                    SqlStdOperatorTable.ROW_NUMBER, java.util.Collections.emptyList(), POS),
                rowNumWindow),
            POS);
    SqlNodeList wrappedItems = new SqlNodeList(POS);
    wrappedItems.add(SqlIdentifier.star(POS));
    wrappedItems.add(asAliased(rowNum, "__rn_rare__"));
    SqlNode windowSelect =
        new SqlSelect(
            POS, null, wrappedItems, aggSelect, null, null, null, null, null, null, null, null);

    SqlNode rnCheck =
        new SqlBasicCall(
            SqlStdOperatorTable.LESS_THAN_OR_EQUAL,
            List.of(
                new SqlIdentifier("__rn_rare__", POS),
                SqlLiteral.createExactNumeric(node.getNoOfResults().toString(), POS)),
            POS);

    // Step 4 — final projection: [G..., F..., (count if showCount)] dropping the helper column.
    SqlNodeList finalItems = new SqlNodeList(POS);
    List<String> finalNames = new ArrayList<>();
    for (String gn : groupNames) {
      finalItems.add(new SqlIdentifier(gn, POS));
      finalNames.add(gn);
    }
    for (String fn : fieldNames) {
      finalItems.add(new SqlIdentifier(fn, POS));
      finalNames.add(fn);
    }
    if (showCount) {
      finalItems.add(countRef);
      finalNames.add(countFieldName);
    }
    frame.currentFields = finalNames;
    frame.lastOrderBy = null;
    frame.joinHints = null;
    return new SqlSelect(
        POS, null, finalItems, windowSelect, rnCheck, null, null, null, null, null, null, null);
  }

  /**
   * Extract a user-facing column label from a {@code rare ... by G} grouping expression. PPL
   * accepts both {@link QualifiedName} (bare column refs) and {@link Field} wrappers; both render
   * to a column name like {@code gender} or {@code addr.city}, not the AST's {@code toString()}.
   */
  private static String rareGroupName(UnresolvedExpression g) {
    if (g instanceof QualifiedName qn) return qn.toString();
    if (g instanceof Field f && f.getField() instanceof QualifiedName qn) return qn.toString();
    return g.toString();
  }

  @Override
  public SqlNode visitRex(Rex node, Frame frame) {
    SqlNode from = node.getChild().get(0).accept(this, frame);
    // PPL `rex field=<f> "<pattern>" [max_match=N] [offset_field=<o>]` extracts named groups via
    // REX_EXTRACT (or REX_EXTRACT_MULTI when max_match > 1). SED mode rewrites the source field
    // and is deferred. Each named group + the optional offset field becomes a new column on the
    // row, replacing colliding existing columns.
    if (node.getMode() == Rex.RexMode.SED) {
      throw new UnsupportedOperationException(
          "rex SED mode not yet supported in PPLToSqlNodeVisitor");
    }
    String patternStr = (String) node.getPattern().getValue();
    List<String> namedGroups =
        org.opensearch.sql.expression.parse.RegexCommonUtils.getNamedGroupCandidates(patternStr);
    if (namedGroups.isEmpty()) {
      throw new IllegalArgumentException(
          "Rex pattern must contain at least one named capture group");
    }
    SqlNode source = expr(node.getField());
    SqlNode patternLit = SqlLiteral.createCharString(patternStr, POS);

    java.util.Set<String> newColumns = new java.util.LinkedHashSet<>(namedGroups);
    node.getOffsetField().ifPresent(newColumns::add);
    List<String> visible =
        frame.currentFields == null ? new ArrayList<>() : new ArrayList<>(frame.currentFields);
    SqlNodeList items = new SqlNodeList(POS);
    if (frame.currentFields == null) {
      items.add(SqlIdentifier.star(POS));
    } else {
      List<String> retained = new ArrayList<>();
      for (String name : visible) {
        if (newColumns.contains(name)) continue;
        items.add(toIdentifier(name));
        retained.add(name);
      }
      visible = retained;
    }
    boolean multi = node.getMaxMatch().isPresent() && node.getMaxMatch().get() > 1;
    for (String group : namedGroups) {
      SqlNode call;
      if (multi) {
        call =
            new SqlBasicCall(
                new org.apache.calcite.sql.SqlUnresolvedFunction(
                    new SqlIdentifier("REX_EXTRACT_MULTI", POS),
                    null,
                    null,
                    null,
                    null,
                    org.apache.calcite.sql.SqlFunctionCategory.USER_DEFINED_FUNCTION),
                List.of(
                    source,
                    patternLit,
                    SqlLiteral.createCharString(group, POS),
                    SqlLiteral.createExactNumeric(node.getMaxMatch().get().toString(), POS)),
                POS);
      } else {
        call =
            new SqlBasicCall(
                new org.apache.calcite.sql.SqlUnresolvedFunction(
                    new SqlIdentifier("REX_EXTRACT", POS),
                    null,
                    null,
                    null,
                    null,
                    org.apache.calcite.sql.SqlFunctionCategory.USER_DEFINED_FUNCTION),
                List.of(source, patternLit, SqlLiteral.createCharString(group, POS)),
                POS);
      }
      items.add(asAliased(call, group));
      visible.add(group);
    }
    if (node.getOffsetField().isPresent()) {
      String offset = node.getOffsetField().get();
      SqlNode offsetCall =
          new SqlBasicCall(
              new org.apache.calcite.sql.SqlUnresolvedFunction(
                  new SqlIdentifier("REX_OFFSET", POS),
                  null,
                  null,
                  null,
                  null,
                  org.apache.calcite.sql.SqlFunctionCategory.USER_DEFINED_FUNCTION),
              List.of(source, patternLit),
              POS);
      items.add(asAliased(offsetCall, offset));
      visible.add(offset);
    }
    return SqlBuilder.select(items).from(from).withFields(visible).wrap(frame);
  }

  @Override
  public SqlNode visitParse(Parse node, Frame frame) {
    SqlNode from = node.getChild().get(0).accept(this, frame);
    // PPL `parse <field> '<pattern>' [...]` extracts named groups from the regex match. Each
    // named group becomes an extra column on the row. The group name may collide with the source
    // field (e.g. `parse email '.+@(?<email>.+)'` overrides email); handle that by stripping the
    // colliding name from the visible field list before appending.
    org.opensearch.sql.ast.expression.ParseMethod method = node.getParseMethod();
    if (method == org.opensearch.sql.ast.expression.ParseMethod.PATTERNS) {
      throw new UnsupportedOperationException(
          "patterns parse method not yet supported in PPLToSqlNodeVisitor");
    }
    String patternValue = (String) node.getPattern().getValue();
    List<String> groups =
        org.opensearch.sql.utils.ParseUtils.getNamedGroupCandidates(
            method, patternValue, node.getArguments());
    if (groups.isEmpty()) {
      return from;
    }
    SqlNode source = expr(node.getSourceField());
    SqlNode patternLit = SqlLiteral.createCharString(patternValue, POS);
    SqlNode methodLit = SqlLiteral.createCharString(method.getName(), POS);

    // Build the new SELECT list: first emit existing columns (or `*` if currentFields is null),
    // dropping any names that collide with a parse group, then append `PARSE(...)['group'] AS
    // <group>` for each group. Result: parse extends the row, replacing colliding columns.
    java.util.Set<String> groupSet = new java.util.LinkedHashSet<>(groups);
    List<String> visible =
        frame.currentFields == null ? new ArrayList<>() : new ArrayList<>(frame.currentFields);
    SqlNodeList items = new SqlNodeList(POS);
    if (frame.currentFields == null) {
      items.add(SqlIdentifier.star(POS));
    } else {
      List<String> retained = new ArrayList<>();
      for (String name : visible) {
        if (groupSet.contains(name)) continue;
        items.add(toIdentifier(name));
        retained.add(name);
      }
      visible = retained;
    }
    for (String group : groups) {
      SqlNode parseCall =
          new SqlBasicCall(
              new org.apache.calcite.sql.SqlUnresolvedFunction(
                  new SqlIdentifier("PARSE", POS),
                  null,
                  null,
                  null,
                  null,
                  org.apache.calcite.sql.SqlFunctionCategory.USER_DEFINED_FUNCTION),
              List.of(source, patternLit, methodLit),
              POS);
      SqlNode itemCall =
          new SqlBasicCall(
              SqlStdOperatorTable.ITEM,
              List.of(parseCall, SqlLiteral.createCharString(group, POS)),
              POS);
      items.add(asAliased(itemCall, group));
      visible.add(group);
    }
    return SqlBuilder.select(items).from(from).withFields(visible).wrap(frame);
  }

  @Override
  public SqlNode visitDedupe(Dedupe node, Frame frame) {
    SqlNode from = node.getChild().get(0).accept(this, frame);
    // PPL `dedup [N] F1, F2, ... [keepempty=true]` keeps at most N rows per <F1,F2,...> partition.
    // Default N=1. SQL form via ROW_NUMBER:
    //   SELECT * FROM (SELECT *, ROW_NUMBER() OVER (PARTITION BY F1, F2 ORDER BY <upstream>) AS
    //   __rn FROM <child>) WHERE __rn <= N
    // With keepempty=true, NULL groups bypass the cap (each NULL row stays); with the default
    // keepempty=false, also drop rows where any dedup field IS NULL.
    // Consecutive dedup falls back to v2 (testConsecutiveDedup wraps in withFallbackEnabled).
    int allowedDup = 1;
    boolean keepEmpty = false;
    boolean consecutive = false;
    for (Argument arg : node.getOptions()) {
      switch (arg.getArgName()) {
        case "number" -> allowedDup = (Integer) arg.getValue().getValue();
        case "keepempty" -> keepEmpty = (Boolean) arg.getValue().getValue();
        case "consecutive" -> consecutive = (Boolean) arg.getValue().getValue();
        default -> {}
      }
    }
    if (allowedDup <= 0) {
      throw new IllegalArgumentException("Number of duplicate events must be greater than 0");
    }
    if (consecutive) {
      throw new UnsupportedOperationException(
          "consecutive=true dedup is not yet supported in PPLToSqlNodeVisitor");
    }
    List<SqlNode> partitionFields = new ArrayList<>();
    for (Field f : node.getFields()) {
      partitionFields.add(expr(f.getField()));
    }
    SqlNodeList partitionBy = new SqlNodeList(POS);
    for (SqlNode pf : partitionFields) {
      partitionBy.add(pf);
    }
    SqlNodeList windowOrderBy = new SqlNodeList(POS);
    if (frame.lastOrderBy != null) {
      for (SqlNode k : frame.lastOrderBy) {
        windowOrderBy.add(k);
      }
    }
    SqlNode rowNumWindow =
        SqlWindow.create(
            null,
            null,
            partitionBy,
            windowOrderBy,
            SqlLiteral.createBoolean(false, POS),
            null,
            null,
            null,
            POS);
    SqlNode rowNum =
        new SqlBasicCall(
            SqlStdOperatorTable.OVER,
            List.of(
                new SqlBasicCall(
                    SqlStdOperatorTable.ROW_NUMBER, java.util.Collections.emptyList(), POS),
                rowNumWindow),
            POS);
    SqlNodeList innerSelects = new SqlNodeList(POS);
    innerSelects.add(SqlIdentifier.star(POS));
    innerSelects.add(asAliased(rowNum, "__rn_dedup__"));
    SqlNode innerSelect =
        new SqlSelect(
            POS, null, innerSelects, from, null, null, null, null, null, null, null, null);
    SqlNode boundCheck =
        new SqlBasicCall(
            SqlStdOperatorTable.LESS_THAN_OR_EQUAL,
            List.of(
                new SqlIdentifier("__rn_dedup__", POS),
                SqlLiteral.createExactNumeric(Integer.toString(allowedDup), POS)),
            POS);
    SqlNode whereCond;
    if (keepEmpty) {
      // (F1 IS NULL) OR (F2 IS NULL) OR ... OR (__rn_dedup__ <= N)
      SqlNode acc = boundCheck;
      for (SqlNode pf : partitionFields) {
        acc =
            new SqlBasicCall(
                SqlStdOperatorTable.OR,
                List.of(new SqlBasicCall(SqlStdOperatorTable.IS_NULL, List.of(pf), POS), acc),
                POS);
      }
      whereCond = acc;
    } else {
      // (F1 IS NOT NULL) AND ... AND (__rn_dedup__ <= N)
      SqlNode acc = boundCheck;
      for (SqlNode pf : partitionFields) {
        acc =
            new SqlBasicCall(
                SqlStdOperatorTable.AND,
                List.of(new SqlBasicCall(SqlStdOperatorTable.IS_NOT_NULL, List.of(pf), POS), acc),
                POS);
      }
      whereCond = acc;
    }
    SqlNodeList outerSelects = new SqlNodeList(POS);
    outerSelects.add(SqlIdentifier.star(POS));
    // Wrap returns SELECT * FROM (<inner>) WHERE <cond>. Downstream `| fields` projects away the
    // helper `__rn_dedup__` column. Without an explicit projection the helper leaks into the
    // user-facing row type — matches legacy behaviour for the `oracle == null` path.
    return new SqlSelect(
        POS, null, outerSelects, innerSelect, whereCond, null, null, null, null, null, null, null);
  }

  /**
   * Auto-generate the column label for a {@link Span} when the user didn't supply an explicit
   * alias. Mirrors v2's display name: {@code span(<field>,<value>[,<unit>])}.
   */
  private static String spanAutoLabel(UnresolvedExpression e) {
    if (!(e instanceof Span sp)) return e.toString();
    StringBuilder sb = new StringBuilder("span(");
    sb.append(sp.getField()).append(",").append(sp.getValue());
    if (sp.getUnit() != null && sp.getUnit() != SpanUnit.NONE && sp.getUnit() != SpanUnit.UNKNOWN) {
      sb.append(",").append(sp.getUnit().getName());
    }
    sb.append(")");
    return sb.toString();
  }

  /**
   * Build a Calcite aggregate call from a PPL {@link AggregateFunction}. Currently supports
   * count/sum/avg/min/max plus the {@code distinct_count} family ({@code dc}/{@code
   * distinct_count}). count() with {@link AllFields} arg becomes {@code COUNT(*)}.
   */
  private SqlNode aggCall(UnresolvedExpression e) {
    if (!(e instanceof AggregateFunction af)) {
      throw new UnsupportedOperationException(
          "stats aggregator must be an AggregateFunction, got: " + e.getClass().getSimpleName());
    }
    String fnLower = af.getFuncName().toLowerCase(java.util.Locale.ROOT);
    boolean distinct = Boolean.TRUE.equals(af.getDistinct());
    if (fnLower.equals("dc") || fnLower.equals("distinct_count")) {
      fnLower = "count";
      distinct = true;
    } else if (fnLower.equals("c")) {
      fnLower = "count";
    }
    org.apache.calcite.sql.SqlAggFunction op =
        switch (fnLower) {
          case "count" -> SqlStdOperatorTable.COUNT;
          case "sum" -> SqlStdOperatorTable.SUM;
          case "avg" -> SqlStdOperatorTable.AVG;
          case "min" -> SqlStdOperatorTable.MIN;
          case "max" -> SqlStdOperatorTable.MAX;
          default ->
              throw new UnsupportedOperationException(
                  "Aggregate function not yet supported in PPLToSqlNodeVisitor: " + fnLower);
        };
    UnresolvedExpression argExpr = af.getField();
    SqlNode arg;
    if (argExpr instanceof AllFields) {
      arg = SqlIdentifier.star(POS);
    } else {
      arg = expr(argExpr);
    }
    org.apache.calcite.sql.SqlLiteral quantifier =
        distinct ? SqlSelectKeyword.DISTINCT.symbol(POS) : null;
    return new SqlBasicCall(op, List.of(arg), POS, quantifier);
  }

  /**
   * Compute the user-facing label for a stats agg/group expression. {@code Alias("name", expr)}
   * carries the user's explicit `as name`; otherwise the AST's toString() (e.g. {@code "avg(age)"})
   * is used.
   */
  private static String aggLabel(UnresolvedExpression e) {
    if (e instanceof Alias a) return a.getName();
    return e.toString();
  }

  /** Build {@code <expr> AS "<alias>"} with the alias quoted (preserves dots in the label). */
  private static SqlNode asAliased(SqlNode rhs, String alias) {
    SqlIdentifier id =
        new SqlIdentifier(
            java.util.Collections.singletonList(alias),
            null,
            POS,
            List.of(SqlParserPos.ZERO.withQuoting(true)));
    return new SqlBasicCall(SqlStdOperatorTable.AS, List.of(rhs, id), POS);
  }

  /** True when {@code e} references any name in {@code names} (recursive walk). */
  private static boolean referencesAny(UnresolvedExpression e, Set<String> names) {
    if (names == null || names.isEmpty()) return false;
    if (e instanceof QualifiedName qn && names.contains(qn.toString())) return true;
    if (e instanceof Field f
        && f.getField() instanceof QualifiedName qn
        && names.contains(qn.toString())) return true;
    for (Object child : e.getChild()) {
      if (child instanceof UnresolvedExpression ce && referencesAny(ce, names)) return true;
    }
    return false;
  }

  @Override
  public SqlNode visitSort(Sort node, Frame frame) {
    SqlNode from = node.getChild().get(0).accept(this, frame);
    List<SqlNode> orderKeys = buildSortKeys(node.getSortList());
    SqlLiteral fetch =
        node.getCount() != null && node.getCount() != 0
            ? SqlLiteral.createExactNumeric(node.getCount().toString(), POS)
            : null;
    // Peephole: when child is a SELECT * already (e.g. SEMI/ANTI's `SELECT * FROM <X AS a> WHERE
    // EXISTS(...)`), wrap with SqlOrderBy directly instead of building a new SELECT around it.
    // The new SELECT would seal the alias `a` inside its FROM subquery, breaking `ORDER BY a.col`.
    SqlNode peep = orderByOnChildSelectStar(from, orderKeys, null, fetch, frame);
    if (peep != null) return peep;
    SqlBuilder.SelectBuilder b = SqlBuilder.select(starList()).from(from).orderBy(orderKeys);
    if (fetch != null) {
      b.fetch(fetch);
    }
    return b.wrap(frame);
  }

  @Override
  public SqlNode visitLimit(Limit node, Frame frame) {
    SqlNode from = node.getChild().get(0).accept(this, frame);
    SqlLiteral fetch = SqlLiteral.createExactNumeric(node.getLimit().toString(), POS);
    SqlLiteral offset =
        node.getOffset() != null && node.getOffset() > 0
            ? SqlLiteral.createExactNumeric(node.getOffset().toString(), POS)
            : null;
    SqlNode peep =
        orderByOnChildSelectStar(from, java.util.Collections.emptyList(), offset, fetch, frame);
    if (peep != null) return peep;
    SqlBuilder.SelectBuilder b = SqlBuilder.select(starList()).from(from).fetch(fetch);
    if (offset != null) {
      b.offset(offset);
    }
    return b.wrap(frame);
  }

  /**
   * Install ORDER BY / FETCH / OFFSET on a child {@code SELECT *} without nesting a new outer
   * SELECT. When the child is a {@code SELECT * FROM <inner> [WHERE ...]} (no GROUP BY / HAVING /
   * existing ORDER BY / FETCH / OFFSET, and the SELECT list is just {@code *}), wrap it with a
   * {@link org.apache.calcite.sql.SqlOrderBy} that operates on the same SELECT's namespace —
   * keeping the inner FROM's aliases visible to the new ORDER BY expressions. Returns null when the
   * shape doesn't match; caller falls back to the standard {@link SqlBuilder} wrap.
   */
  private static SqlNode orderByOnChildSelectStar(
      SqlNode from, List<SqlNode> orderKeys, SqlLiteral offset, SqlLiteral fetch, Frame frame) {
    if (!(from instanceof SqlSelect select)) return null;
    SqlNodeList items = select.getSelectList();
    if (items == null || items.size() != 1) return null;
    SqlNode lone = items.get(0);
    if (!(lone instanceof SqlIdentifier id) || !id.isStar()) return null;
    if (select.getGroup() != null && !select.getGroup().getList().isEmpty()) return null;
    if (select.getHaving() != null) return null;
    if (select.getOrderList() != null && !select.getOrderList().getList().isEmpty()) return null;
    if (select.getFetch() != null || select.getOffset() != null) return null;
    SqlNodeList ord = new SqlNodeList(POS);
    if (orderKeys != null) {
      for (SqlNode k : orderKeys) {
        ord.add(k);
      }
    }
    if (orderKeys != null && !orderKeys.isEmpty()) {
      frame.lastOrderBy = orderKeys;
    }
    frame.joinHints = null;
    return new org.apache.calcite.sql.SqlOrderBy(POS, select, ord, offset, fetch);
  }

  @Override
  public SqlNode visitReverse(Reverse node, Frame frame) {
    SqlNode from = node.getChild().get(0).accept(this, frame);
    // PPL `reverse` flips whatever ordering is currently in effect. We only handle the
    // explicit-prior-sort case here; the implicit-`__stream_seq__` / @timestamp fallback used by
    // streamstats is deferred until those visitors land.
    List<SqlNode> reversed = frame.reversedLastOrderBy();
    if (reversed == null) {
      throw new UnsupportedOperationException(
          "reverse without a prior sort is not yet supported in PPLToSqlNodeVisitor");
    }
    return SqlBuilder.select(starList()).from(from).orderBy(reversed).wrap(frame);
  }

  /** Build SqlNode ORDER BY keys from PPL sort fields. */
  private List<SqlNode> buildSortKeys(List<Field> sortList) {
    List<SqlNode> keys = new ArrayList<>(sortList.size());
    for (Field f : sortList) {
      SqlNode key = expr(f.getField());
      Sort.SortOption opt = analyzeSortOption(f.getFieldArgs());
      if (opt.getSortOrder() == Sort.SortOrder.DESC) {
        key = new SqlBasicCall(SqlStdOperatorTable.DESC, List.of(key), POS);
      }
      org.apache.calcite.sql.SqlOperator nullsOp =
          opt.getNullOrder() == Sort.NullOrder.NULL_LAST
              ? SqlStdOperatorTable.NULLS_LAST
              : SqlStdOperatorTable.NULLS_FIRST;
      keys.add(new SqlBasicCall(nullsOp, List.of(key), POS));
    }
    return keys;
  }

  private static Sort.SortOption analyzeSortOption(List<Argument> args) {
    boolean desc = false;
    for (Argument a : args) {
      if ("asc".equalsIgnoreCase(a.getArgName())) {
        desc = !((Boolean) a.getValue().getValue());
      }
    }
    return desc ? Sort.SortOption.DEFAULT_DESC : Sort.SortOption.DEFAULT_ASC;
  }

  /**
   * Flip each sort key (ASC ↔ DESC, NULLS_FIRST ↔ NULLS_LAST). Recognises the {@code
   * NULLS_FIRST/LAST(DESC?(expr))} shape produced by {@link #buildSortKeys}.
   */
  private static List<SqlNode> reverseSortKeys(List<SqlNode> keys) {
    if (keys == null || keys.isEmpty()) return null;
    List<SqlNode> out = new ArrayList<>(keys.size());
    for (SqlNode k : keys) {
      if (k instanceof SqlBasicCall outer
          && (outer.getOperator() == SqlStdOperatorTable.NULLS_FIRST
              || outer.getOperator() == SqlStdOperatorTable.NULLS_LAST)) {
        org.apache.calcite.sql.SqlOperator flippedNulls =
            outer.getOperator() == SqlStdOperatorTable.NULLS_FIRST
                ? SqlStdOperatorTable.NULLS_LAST
                : SqlStdOperatorTable.NULLS_FIRST;
        SqlNode inner = outer.operand(0);
        SqlNode flippedInner;
        if (inner instanceof SqlBasicCall innerCall
            && innerCall.getOperator() == SqlStdOperatorTable.DESC) {
          flippedInner = innerCall.operand(0);
        } else {
          flippedInner = new SqlBasicCall(SqlStdOperatorTable.DESC, List.of(inner), POS);
        }
        out.add(new SqlBasicCall(flippedNulls, List.of(flippedInner), POS));
      } else {
        out.add(k);
      }
    }
    return out;
  }

  @Override
  public SqlNode visitJoin(Join node, Frame frame) {
    // Compose: walk the left side into the parent frame (so currentFields reflects the LEFT side
    // post-walk), then walk the right side in a fresh frame so its independent table/column scope
    // doesn't leak back. Build the SqlJoin from both sides plus the ON condition.
    SqlNode leftSide = node.getChildren().get(0).accept(this, frame);
    Frame rightFrame = new Frame();
    Frame savedExprFrame = this.exprFrame;
    this.exprFrame = rightFrame;
    SqlNode rightSide = node.getRight().accept(this, rightFrame);
    this.exprFrame = savedExprFrame;
    // Promote synonyms recorded during the right walk into the parent frame: they're scoped to
    // this join's outer pipeline (e.g. `[Y as tt] right=t2 | ... | fields tt.col` resolves through
    // the join's lifetime, not just inside the right subsearch).
    if (!rightFrame.aliasSynonyms.isEmpty()) {
      frame.aliasSynonyms.putAll(rightFrame.aliasSynonyms);
    }

    // Apply max=N per-partition cap on the right side BEFORE attaching the join-arg alias so the
    // cap is invisible to the outer scope. The cluster-wide subsearch_maxout cap is applied
    // post-RelNode (in QueryService) instead — wrapping a bare relation here would hide the table
    // identifier from the JOIN's outer scope.
    rightSide = applyMaxPerPartition(node, rightSide);

    // PPL allows referencing a side via either an explicit `left=l right=r` arg, or — when the
    // side is a SubqueryAlias — by the alias name. The visitor for SubqueryAlias already wrapped
    // the inner in `AS <alias>`. For the bare-table-without-explicit-alias case, attach a
    // default `__l` / `__r` only when we need the alias for synthetic ON conditions; otherwise
    // leave the bare table in scope (PPL `JOIN ON X.col = Y.col` references the raw table name).
    String leftAlias = node.getLeftAlias().orElse(null);
    String rightAlias = node.getRightAlias().orElse(null);

    // Settle the left/right alias if explicit. When the side is already AS-wrapped under a
    // different name (e.g. user wrote `source=X as tt | JOIN left=t1 ...`), the explicit join-arg
    // alias OVERRIDES the inner SubqueryAlias name — the outer scope sees only the new alias.
    // Record the displaced inner alias as a synonym so a downstream `| fields tt.col` resolves.
    leftSide = applyExplicitAlias(leftSide, leftAlias, frame);
    rightSide = applyExplicitAlias(rightSide, rightAlias, frame);

    // SEMI/ANTI joins aren't supported in Calcite's SqlNode dialect (raises "Dialect does not
    // support feature"). Rewrite as `<left> WHERE [NOT] EXISTS (SELECT * FROM <right> WHERE
    // <on-cond>)`. Returning a SqlSelect (rather than a SqlJoin) means `frame.currentFields`
    // becomes the LEFT-side fields only — SEMI/ANTI don't add right-side columns to the output.
    if (node.getJoinType() == Join.JoinType.SEMI || node.getJoinType() == Join.JoinType.ANTI) {
      return semiAntiAsExists(node, leftSide, rightSide, leftAlias, rightAlias, frame);
    }

    JoinType jt = mapJoinType(node.getJoinType());

    // CROSS + condition: PPL allows `cross join ON ...`; SQL forbids it. Fall back to INNER.
    if (jt == JoinType.CROSS
        && (node.getJoinCondition().isPresent()
            || (node.getJoinFields().isPresent() && !node.getJoinFields().get().isEmpty()))) {
      jt = JoinType.INNER;
    }

    SqlNode condition;
    if (node.getJoinCondition().isPresent()) {
      condition = expr(node.getJoinCondition().get());
    } else if (node.getJoinFields().isPresent() && !node.getJoinFields().get().isEmpty()) {
      // PPL `join F1, F2 ...` — expand to `ON l.F = r.F AND ...` so qualified refs survive
      // (USING auto-dedupes columns and prevents qualified access; we want the explicit shape).
      String l = leftAliasOrDefault(leftAlias);
      String r = rightAliasOrDefault(rightAlias);
      // Need synthetic aliases on both sides to attach ON.l/r refs; force-alias if not yet.
      leftSide = ensureAliased(leftSide, leftAlias, "__l");
      rightSide = ensureAliased(rightSide, rightAlias, "__r");
      SqlNode acc = null;
      for (Field f : node.getJoinFields().get()) {
        String name = f.getField().toString();
        SqlNode lref = new SqlIdentifier(Arrays.asList(l, name), POS);
        SqlNode rref = new SqlIdentifier(Arrays.asList(r, name), POS);
        SqlNode eq = new SqlBasicCall(SqlStdOperatorTable.EQUALS, List.of(lref, rref), POS);
        acc = (acc == null) ? eq : new SqlBasicCall(SqlStdOperatorTable.AND, List.of(acc, eq), POS);
      }
      condition = acc;
    } else if (jt != JoinType.CROSS) {
      // No ON, no field list, non-cross: PPL falls back to ON-equality on duplicate column names.
      List<String> shared = sharedFields(frame.currentFields, rightFrame.currentFields);
      if (!shared.isEmpty()) {
        String l = leftAliasOrDefault(leftAlias);
        String r = rightAliasOrDefault(rightAlias);
        leftSide = ensureAliased(leftSide, leftAlias, "__l");
        rightSide = ensureAliased(rightSide, rightAlias, "__r");
        SqlNode acc = null;
        for (String c : shared) {
          SqlNode lref = new SqlIdentifier(Arrays.asList(l, c), POS);
          SqlNode rref = new SqlIdentifier(Arrays.asList(r, c), POS);
          SqlNode eq = new SqlBasicCall(SqlStdOperatorTable.EQUALS, List.of(lref, rref), POS);
          acc =
              (acc == null) ? eq : new SqlBasicCall(SqlStdOperatorTable.AND, List.of(acc, eq), POS);
        }
        condition = acc;
      } else {
        condition = SqlLiteral.createBoolean(true, POS);
      }
    } else {
      condition = null;
    }

    // Compute the post-join field list. For `JOIN F1, F2 ...` (and the auto-equality fallback)
    // PPL drops duplicates from one side (default overwrite=true → drop LEFT dups; keep RIGHT).
    boolean usesFieldList =
        node.getJoinFields().isPresent()
            && !node.getJoinFields().get().isEmpty()
            && !node.getJoinCondition().isPresent();
    boolean usesAutoEquality =
        !node.getJoinCondition().isPresent()
            && !(node.getJoinFields().isPresent() && !node.getJoinFields().get().isEmpty())
            && jt != JoinType.CROSS;

    List<String> leftCols =
        frame.currentFields == null ? List.of() : stripMeta(frame.currentFields);
    List<String> rightCols =
        rightFrame.currentFields == null ? List.of() : stripMeta(rightFrame.currentFields);

    if (usesFieldList || usesAutoEquality) {
      // Field-list / auto-equality: wrap the SqlJoin in a SELECT that materialises the deduped
      // projection. A `| fields F1, F2` pipe after this join sees the deduped field list.
      // joinHints stays null on the frame — the explicit projection has already disambiguated.
      DedupedProjection dp = dedupedProjection(node, leftCols, rightCols, leftAlias, rightAlias);
      SqlNode join =
          SqlBuilder.join().left(leftSide).right(rightSide).type(jt).on(condition).build(frame);
      return SqlBuilder.select(dp.selectList).from(join).withFields(dp.names).wrap(frame);
    }

    // Explicit ON clause path. Expose the union of both sides' columns (so `t1.name`, `t2.name`
    // resolve in a downstream `| fields ...`) and leave PPL's bind-bare-to-LEFT hints live on
    // the frame so the next pipe can qualify ambiguous bare refs. Cleared by the next wrap.
    return SqlBuilder.join()
        .left(leftSide)
        .right(rightSide)
        .type(jt)
        .on(condition)
        .withFields(unionFieldsWithAliasPrefixes(leftCols, rightCols, leftAlias, rightAlias))
        .joinHints(leftAlias, rightAlias, new LinkedHashSet<>(sharedFields(leftCols, rightCols)))
        .build(frame);
  }

  /** Build the SELECT-list and column-name list for the field-list / auto-equality dedup path. */
  private DedupedProjection dedupedProjection(
      Join node,
      List<String> leftCols,
      List<String> rightCols,
      String leftAlias,
      String rightAlias) {
    boolean overwrite = true;
    if (node.getArgumentMap() != null && node.getArgumentMap().get("overwrite") != null) {
      Object v = node.getArgumentMap().get("overwrite").getValue();
      if (v instanceof Boolean b) overwrite = b;
    }
    String l = leftAliasOrDefault(leftAlias);
    String r = rightAliasOrDefault(rightAlias);
    Set<String> rightSet = new LinkedHashSet<>(rightCols);
    Set<String> leftSet = new LinkedHashSet<>(leftCols);
    List<String> dedupedNames = new ArrayList<>();
    SqlNodeList selectList = new SqlNodeList(POS);
    if (overwrite) {
      for (String c : leftCols) {
        if (rightSet.contains(c)) continue;
        selectList.add(new SqlIdentifier(Arrays.asList(l, c), POS));
        dedupedNames.add(c);
      }
      for (String c : rightCols) {
        selectList.add(new SqlIdentifier(Arrays.asList(r, c), POS));
        // RIGHT-side duplicates keep the bare name; LEFT-only references also keep the bare
        // name; user-facing column count is the union.
        dedupedNames.add(c);
      }
    } else {
      for (String c : leftCols) {
        selectList.add(new SqlIdentifier(Arrays.asList(l, c), POS));
        dedupedNames.add(c);
      }
      for (String c : rightCols) {
        if (leftSet.contains(c)) continue;
        selectList.add(new SqlIdentifier(Arrays.asList(r, c), POS));
        dedupedNames.add(c);
      }
    }
    return new DedupedProjection(selectList, dedupedNames);
  }

  private record DedupedProjection(SqlNodeList selectList, List<String> names) {}

  /**
   * Rewrite {@code <left> [LEFT] SEMI/ANTI JOIN <right> ON cond} as {@code <left> WHERE [NOT]
   * EXISTS (SELECT * FROM <right> WHERE cond)}. Calcite's SqlNode dialect doesn't accept
   * LEFT_SEMI/LEFT_ANTI directly (raises "Dialect does not support feature"); the EXISTS rewrite is
   * semantically equivalent and works through the standard validator/SqlToRelConverter path.
   *
   * <p>Both sides are force-aliased (explicit join-arg or default {@code __l}/{@code __r}) so the
   * correlated subquery's {@code <left-alias>.<col> = <right-alias>.<col>} ON-clause can resolve.
   * The wrapping SELECT keeps the LEFT side's row type intact — SEMI/ANTI don't add right-side
   * columns to the output.
   */
  private SqlNode semiAntiAsExists(
      Join node,
      SqlNode leftSide,
      SqlNode rightSide,
      String leftAlias,
      String rightAlias,
      Frame frame) {
    boolean isAnti = node.getJoinType() == Join.JoinType.ANTI;
    String lAlias = leftAliasOrDefault(leftAlias);
    String rAlias = rightAliasOrDefault(rightAlias);
    SqlNode aliasedLeft = ensureAliased(leftSide, leftAlias, "__l");
    SqlNode aliasedRight = ensureAliased(rightSide, rightAlias, "__r");

    SqlNode subCond;
    if (node.getJoinCondition().isPresent()) {
      subCond = expr(node.getJoinCondition().get());
    } else if (node.getJoinFields().isPresent() && !node.getJoinFields().get().isEmpty()) {
      SqlNode acc = null;
      for (Field f : node.getJoinFields().get()) {
        String name = f.getField().toString();
        SqlNode l = new SqlIdentifier(Arrays.asList(lAlias, name), POS);
        SqlNode r = new SqlIdentifier(Arrays.asList(rAlias, name), POS);
        SqlNode eq = new SqlBasicCall(SqlStdOperatorTable.EQUALS, List.of(l, r), POS);
        acc = (acc == null) ? eq : new SqlBasicCall(SqlStdOperatorTable.AND, List.of(acc, eq), POS);
      }
      subCond = acc;
    } else {
      subCond = SqlLiteral.createBoolean(true, POS);
    }

    SqlNodeList subSelectList = new SqlNodeList(POS);
    subSelectList.add(SqlIdentifier.star(POS));
    SqlNode subQuery =
        new SqlSelect(
            POS,
            null,
            subSelectList,
            aliasedRight,
            subCond,
            null,
            null,
            null,
            null,
            null,
            null,
            null);
    SqlNode existsCall = new SqlBasicCall(SqlStdOperatorTable.EXISTS, List.of(subQuery), POS);
    SqlNode whereCond =
        isAnti ? new SqlBasicCall(SqlStdOperatorTable.NOT, List.of(existsCall), POS) : existsCall;

    // SEMI/ANTI keeps only the LEFT side's columns — frame.currentFields stays as-is (already
    // populated by the left walk). Clear joinHints; SEMI/ANTI doesn't expose the right alias.
    frame.joinHints = null;

    SqlNodeList topSelectList = new SqlNodeList(POS);
    topSelectList.add(SqlIdentifier.star(POS));
    return new SqlSelect(
        POS, null, topSelectList, aliasedLeft, whereCond, null, null, null, null, null, null, null);
  }

  /**
   * Cap the right side of a JOIN to N rows per partition when the user wrote {@code max=N}. PPL
   * semantics: deduplicate rows so each {@code <partition-cols>} group keeps at most N rows. SQL
   * shape: {@code SELECT * FROM (SELECT *, ROW_NUMBER() OVER (PARTITION BY <cols>) AS __rn FROM
   * <right>) WHERE __rn <= N}. Partition cols come from the {@code join F1 F2 ...} field list, or
   * from the right-side equi-join columns when the user wrote {@code on l.X = r.Y}.
   */
  private SqlNode applyMaxPerPartition(Join node, SqlNode rightSide) {
    Integer maxArg = null;
    if (node.getArgumentMap() != null && node.getArgumentMap().get("max") != null) {
      Object v = node.getArgumentMap().get("max").getValue();
      if (v instanceof Integer i) maxArg = i;
    }
    if (maxArg == null || maxArg <= 0) return rightSide;
    List<String> partitionCols = null;
    if (node.getJoinFields().isPresent() && !node.getJoinFields().get().isEmpty()) {
      partitionCols = new ArrayList<>();
      for (Field f : node.getJoinFields().get()) {
        partitionCols.add(f.getField().toString());
      }
    } else if (node.getJoinCondition().isPresent() && node.getRightAlias().isPresent()) {
      partitionCols =
          extractRightEquiJoinCols(node.getJoinCondition().get(), node.getRightAlias().get());
    }
    if (partitionCols == null || partitionCols.isEmpty()) return rightSide;
    SqlNodeList partitionBy = new SqlNodeList(POS);
    for (String col : partitionCols) {
      partitionBy.add(new SqlIdentifier(col, POS));
    }
    SqlNode rowNumWindow =
        SqlWindow.create(
            null,
            null,
            partitionBy,
            new SqlNodeList(POS),
            SqlLiteral.createBoolean(false, POS),
            null,
            null,
            null,
            POS);
    SqlNode rowNum =
        new SqlBasicCall(
            SqlStdOperatorTable.OVER,
            List.of(
                new SqlBasicCall(
                    SqlStdOperatorTable.ROW_NUMBER, java.util.Collections.emptyList(), POS),
                rowNumWindow),
            POS);
    SqlNodeList innerSelects = new SqlNodeList(POS);
    innerSelects.add(SqlIdentifier.star(POS));
    innerSelects.add(asAliased(rowNum, "__join_max_rn__"));
    SqlNode innerSelect =
        new SqlSelect(
            POS, null, innerSelects, rightSide, null, null, null, null, null, null, null, null);
    SqlNode innerAliased =
        new SqlBasicCall(
            SqlStdOperatorTable.AS,
            List.of(innerSelect, new SqlIdentifier("__max_inner__", POS)),
            POS);
    SqlNodeList outerSelects = new SqlNodeList(POS);
    outerSelects.add(SqlIdentifier.star(POS));
    SqlNode outerWhere =
        new SqlBasicCall(
            SqlStdOperatorTable.LESS_THAN_OR_EQUAL,
            List.of(
                new SqlIdentifier("__join_max_rn__", POS),
                SqlLiteral.createExactNumeric(Integer.toString(maxArg), POS)),
            POS);
    return new SqlSelect(
        POS,
        null,
        outerSelects,
        innerAliased,
        outerWhere,
        null,
        null,
        null,
        null,
        null,
        null,
        null);
  }

  /**
   * Walk a JOIN ON expression collecting right-side column names from each conjunct of equi-join
   * shape. {@code l.X = r.Y} contributes {@code Y}; non-equi or non-conjunctive shapes return null
   * so the caller falls back to "no max-cap partition cols available".
   */
  private static List<String> extractRightEquiJoinCols(
      UnresolvedExpression condition, String rightAlias) {
    List<String> out = new ArrayList<>();
    if (!collectRightEquiJoinCols(condition, rightAlias, out)) return null;
    return out;
  }

  private static boolean collectRightEquiJoinCols(
      UnresolvedExpression e, String rightAlias, List<String> out) {
    if (e instanceof And and) {
      return collectRightEquiJoinCols(and.getLeft(), rightAlias, out)
          && collectRightEquiJoinCols(and.getRight(), rightAlias, out);
    }
    if (e instanceof Compare c && "=".equals(c.getOperator())) {
      String l = qualifiedRightCol(c.getLeft(), rightAlias);
      String r = qualifiedRightCol(c.getRight(), rightAlias);
      if (l != null) {
        out.add(l);
        return true;
      }
      if (r != null) {
        out.add(r);
        return true;
      }
    }
    return false;
  }

  private static String qualifiedRightCol(UnresolvedExpression e, String rightAlias) {
    QualifiedName qn = null;
    if (e instanceof QualifiedName q) qn = q;
    else if (e instanceof Field f && f.getField() instanceof QualifiedName q) qn = q;
    if (qn == null) return null;
    List<String> parts = qn.getParts();
    if (parts.size() == 2 && rightAlias.equalsIgnoreCase(parts.get(0))) {
      return parts.get(1);
    }
    return null;
  }

  // ---------- Helpers ----------

  private static SqlNodeList starList() {
    SqlNodeList list = new SqlNodeList(POS);
    list.add(SqlIdentifier.star(POS));
    return list;
  }

  private List<String> lookupTableFields(List<String> tableParts) {
    List<String> fields = tableFields.apply(tableParts);
    if (fields == null) {
      throw new IllegalStateException("Table not found in catalog: " + tableParts);
    }
    return fields;
  }

  /**
   * Resolve the explicit SELECT-list field names this Project should emit, given the Project AST
   * and the field list visible at this pipe.
   *
   * <p>Behavior:
   *
   * <ul>
   *   <li>If the projection is a single {@link AllFields}, return all non-metadata fields (the
   *       implicit final {@code | fields *} every PPL query carries).
   *   <li>Otherwise, expand wildcards and dedup.
   *   <li>If {@link Project#isExcluded()}, return {@code nonMeta - requested}; otherwise return
   *       {@code requested}.
   * </ul>
   */
  private List<String> resolveSelectedFields(Project node, List<String> incomingFields) {
    if (incomingFields == null) {
      // Post-join with explicit ON: the join layer didn't produce a deduped field list because
      // the user is expected to project explicitly. Tolerate by treating "all visible" as empty
      // for AllFields/wildcard purposes — explicit names will pass through.
      incomingFields = List.of();
    }
    List<String> nonMeta =
        incomingFields.stream()
            .filter(f -> !OpenSearchConstants.METADATAFIELD_TYPE_MAP.containsKey(f))
            .toList();

    if (node.getProjectList().size() == 1
        && node.getProjectList().getFirst() instanceof AllFields) {
      return nonMeta;
    }

    List<String> requested = new ArrayList<>();
    Set<String> requestedSet = new LinkedHashSet<>();
    String firstWildcardWithNoMatch = null;
    for (UnresolvedExpression expr : node.getProjectList()) {
      String name = projectionFieldName(expr);
      if (WildcardUtils.containsWildcard(name)) {
        List<String> matches = WildcardUtils.expandWildcardPattern(name, nonMeta);
        if (matches.isEmpty() && firstWildcardWithNoMatch == null) {
          firstWildcardWithNoMatch = name;
        }
        for (String m : matches) {
          if (requestedSet.add(m)) {
            requested.add(m);
          }
        }
      } else if (requestedSet.add(name)) {
        requested.add(name);
      }
    }

    if (node.isExcluded()) {
      Set<String> exclude = new LinkedHashSet<>(requested);
      List<String> kept = nonMeta.stream().filter(f -> !exclude.contains(f)).toList();
      if (kept.isEmpty()) {
        throw new IllegalArgumentException(
            "Invalid field exclusion: operation would exclude all fields from the result set");
      }
      return kept;
    }

    if (requested.isEmpty() && firstWildcardWithNoMatch != null) {
      throw new IllegalArgumentException(
          String.format("wildcard pattern [%s] matches no fields", firstWildcardWithNoMatch));
    }
    return requested;
  }

  private static String projectionFieldName(UnresolvedExpression expr) {
    if (expr instanceof Field field) {
      return field.getField().toString();
    }
    if (expr instanceof QualifiedName qn) {
      return qn.toString();
    }
    throw new UnsupportedOperationException(
        "Project supports only plain Field/QualifiedName projections at this stage, got "
            + expr.getClass().getSimpleName());
  }

  /**
   * Translate a PPL UnresolvedExpression to a Calcite SqlNode. Grows incrementally as visitors land
   * — current cases cover what visitJoin/visitFilter/visitEval exercise.
   */
  private SqlNode expr(UnresolvedExpression e) {
    if (e instanceof Literal lit) {
      return literalToSqlNode(lit);
    }
    if (e instanceof QualifiedName qn) {
      List<String> parts = qn.getParts();
      if (exprFrame != null
          && !exprFrame.aliasSynonyms.isEmpty()
          && parts.size() >= 2
          && exprFrame.aliasSynonyms.containsKey(parts.get(0))) {
        List<String> rewritten = new ArrayList<>(parts);
        rewritten.set(0, exprFrame.aliasSynonyms.get(parts.get(0)));
        return new SqlIdentifier(rewritten, POS);
      }
      return new SqlIdentifier(parts, POS);
    }
    if (e instanceof Field f) {
      return expr(f.getField());
    }
    if (e instanceof Compare c) {
      SqlNode l = expr(c.getLeft());
      SqlNode r = expr(c.getRight());
      return new SqlBasicCall(comparisonOperator(c.getOperator()), List.of(l, r), POS);
    }
    if (e instanceof And a) {
      return new SqlBasicCall(
          SqlStdOperatorTable.AND, List.of(expr(a.getLeft()), expr(a.getRight())), POS);
    }
    if (e instanceof Or o) {
      return new SqlBasicCall(
          SqlStdOperatorTable.OR, List.of(expr(o.getLeft()), expr(o.getRight())), POS);
    }
    if (e instanceof Not n) {
      return new SqlBasicCall(SqlStdOperatorTable.NOT, List.of(expr(n.getExpression())), POS);
    }
    if (e instanceof Cast c) {
      return castExpr(c);
    }
    if (e instanceof org.opensearch.sql.ast.expression.Function fn) {
      return funcExpr(fn);
    }
    if (e instanceof Span sp) {
      return spanExpr(sp);
    }
    throw new UnsupportedOperationException(
        "Expression not yet supported in PPLToSqlNodeVisitor: " + e.getClass().getSimpleName());
  }

  /**
   * Translate {@link Span} (PPL {@code span(field, value [, unit])}) to a call on the SPAN
   * built-in. {@code SpanFunction} dispatches numeric vs time bucket via the unit operand: a NULL
   * cast to NULL type for the numeric branch, a string literal for time units.
   */
  private SqlNode spanExpr(Span sp) {
    SqlNode field = expr(sp.getField());
    SqlNode value = expr(sp.getValue());
    SpanUnit unit = sp.getUnit();
    SqlNode unitNode;
    if (unit == SpanUnit.NONE || unit == SpanUnit.UNKNOWN) {
      // SqlLiteral.createNull() types as ANY by default; SpanFunction's dispatcher relies on
      // SqlTypeUtil.isNull. Cast to NULL to land on the numeric-bucket branch.
      org.apache.calcite.sql.SqlDataTypeSpec nullSpec =
          new org.apache.calcite.sql.SqlDataTypeSpec(
              new org.apache.calcite.sql.SqlBasicTypeNameSpec(
                  org.apache.calcite.sql.type.SqlTypeName.NULL, POS),
              POS);
      unitNode =
          new SqlBasicCall(
              org.apache.calcite.sql.fun.SqlLibraryOperators.SAFE_CAST,
              List.of(SqlLiteral.createNull(POS), nullSpec),
              POS);
    } else {
      unitNode = SqlLiteral.createCharString(unit.getName(), POS);
    }
    return new SqlBasicCall(
        org.opensearch.sql.expression.function.PPLBuiltinOperators.SPAN,
        List.of(field, value, unitNode),
        POS);
  }

  /**
   * Translate a {@link Function} call. Arithmetic operators (PPL parses {@code +}/{@code -}/etc. as
   * Functions) bind to the corresponding Calcite operator; PPL's {@code +} between strings desugars
   * to {@code CONCAT}. Other named functions go through {@link
   * org.apache.calcite.sql.SqlUnresolvedFunction} so the validator's name lookup resolves them.
   */
  private SqlNode funcExpr(org.opensearch.sql.ast.expression.Function fn) {
    List<SqlNode> args = new ArrayList<>(fn.getFuncArgs().size());
    for (UnresolvedExpression a : fn.getFuncArgs()) {
      args.add(expr(a));
    }
    org.apache.calcite.sql.SqlOperator op = arithmeticOperator(fn.getFuncName());
    if (op != null) {
      // PPL overloads `+` as both numeric addition and string concatenation. When any operand is
      // statically a string (literal, CAST(... AS STRING), or a `+` chain ending in one), emit
      // CONCAT so the validator picks the string-concat overload.
      if (op == SqlStdOperatorTable.PLUS && hasStringOperand(fn.getFuncArgs())) {
        return new SqlBasicCall(SqlStdOperatorTable.CONCAT, args, POS);
      }
      return new SqlBasicCall(op, args, POS);
    }
    return new SqlBasicCall(
        new org.apache.calcite.sql.SqlUnresolvedFunction(
            new SqlIdentifier(fn.getFuncName(), POS),
            null,
            null,
            null,
            null,
            org.apache.calcite.sql.SqlFunctionCategory.USER_DEFINED_FUNCTION),
        args,
        POS);
  }

  /**
   * Translate {@code CAST(expr AS type)}. Currently only STRING/numeric/boolean — date/IP UDT casts
   * stay deferred until those visitors land.
   */
  private SqlNode castExpr(Cast c) {
    SqlNode value = expr(c.getExpression());
    org.apache.calcite.sql.type.SqlTypeName tn = pplTypeToSqlType(c.getDataType());
    org.apache.calcite.sql.SqlDataTypeSpec spec =
        new org.apache.calcite.sql.SqlDataTypeSpec(
            new org.apache.calcite.sql.SqlBasicTypeNameSpec(tn, POS), POS);
    return new SqlBasicCall(
        org.apache.calcite.sql.fun.SqlLibraryOperators.SAFE_CAST, List.of(value, spec), POS);
  }

  private static org.apache.calcite.sql.type.SqlTypeName pplTypeToSqlType(DataType t) {
    return switch (t) {
      case BOOLEAN -> org.apache.calcite.sql.type.SqlTypeName.BOOLEAN;
      case SHORT -> org.apache.calcite.sql.type.SqlTypeName.SMALLINT;
      case INTEGER -> org.apache.calcite.sql.type.SqlTypeName.INTEGER;
      case LONG -> org.apache.calcite.sql.type.SqlTypeName.BIGINT;
      case FLOAT -> org.apache.calcite.sql.type.SqlTypeName.FLOAT;
      case DOUBLE -> org.apache.calcite.sql.type.SqlTypeName.DOUBLE;
      case DECIMAL -> org.apache.calcite.sql.type.SqlTypeName.DECIMAL;
      case STRING -> org.apache.calcite.sql.type.SqlTypeName.VARCHAR;
      default ->
          throw new UnsupportedOperationException(
              "Cast target type not yet supported in PPLToSqlNodeVisitor: " + t);
    };
  }

  private static org.apache.calcite.sql.SqlOperator arithmeticOperator(String name) {
    if (name == null) return null;
    return switch (name) {
      case "+" -> SqlStdOperatorTable.PLUS;
      case "-" -> SqlStdOperatorTable.MINUS;
      case "*" -> SqlStdOperatorTable.MULTIPLY;
      case "/" -> SqlStdOperatorTable.DIVIDE;
      case "%" -> SqlStdOperatorTable.MOD;
      default -> null;
    };
  }

  /**
   * True if any operand of a {@code +} expression is statically a string — used to pick CONCAT over
   * PLUS when PPL's {@code +} acts as string concatenation.
   */
  private static boolean hasStringOperand(List<UnresolvedExpression> args) {
    for (UnresolvedExpression a : args) {
      if (isStringExpr(a)) return true;
    }
    return false;
  }

  private static boolean isStringExpr(UnresolvedExpression e) {
    if (e instanceof Literal lit) {
      return lit.getType() == DataType.STRING;
    }
    if (e instanceof Cast c) {
      return c.getDataType() == DataType.STRING;
    }
    if (e instanceof org.opensearch.sql.ast.expression.Function fn
        && "+".equals(fn.getFuncName())) {
      return hasStringOperand(fn.getFuncArgs());
    }
    return false;
  }

  private static org.apache.calcite.sql.SqlOperator comparisonOperator(String op) {
    return switch (op) {
      case "=" -> SqlStdOperatorTable.EQUALS;
      case "!=", "<>" -> SqlStdOperatorTable.NOT_EQUALS;
      case "<" -> SqlStdOperatorTable.LESS_THAN;
      case "<=" -> SqlStdOperatorTable.LESS_THAN_OR_EQUAL;
      case ">" -> SqlStdOperatorTable.GREATER_THAN;
      case ">=" -> SqlStdOperatorTable.GREATER_THAN_OR_EQUAL;
      default ->
          throw new UnsupportedOperationException("Comparison operator not supported: " + op);
    };
  }

  private static SqlNode literalToSqlNode(Literal lit) {
    Object v = lit.getValue();
    if (v == null) return SqlLiteral.createNull(POS);
    return switch (lit.getType()) {
      case BOOLEAN -> SqlLiteral.createBoolean((Boolean) v, POS);
      case INTEGER, LONG, SHORT -> SqlLiteral.createExactNumeric(v.toString(), POS);
      case STRING -> SqlLiteral.createCharString(v.toString(), POS);
      default ->
          throw new UnsupportedOperationException(
              "Literal type not yet supported in ON-clause: " + lit.getType());
    };
  }

  private static JoinType mapJoinType(Join.JoinType jt) {
    return switch (jt) {
      case INNER -> JoinType.INNER;
      case LEFT -> JoinType.LEFT;
      case RIGHT -> JoinType.RIGHT;
      case FULL -> JoinType.FULL;
      case CROSS -> JoinType.CROSS;
      case SEMI, ANTI ->
          throw new UnsupportedOperationException(
              "SEMI/ANTI join not yet supported in PPLToSqlNodeVisitor");
    };
  }

  /** True when the SqlNode is already wrapped as `<inner> AS <alias>`. */
  private static boolean isAlreadyAliased(SqlNode side) {
    return side instanceof SqlBasicCall sbc && sbc.getOperator() == SqlStdOperatorTable.AS;
  }

  /** Wrap {@code side} as {@code <side> AS <name>} when no AS-wrapper already exists. */
  private static SqlNode ensureAliased(SqlNode side, String explicitAlias, String defaultAlias) {
    if (isAlreadyAliased(side)) return side;
    String name = explicitAlias != null ? explicitAlias : defaultAlias;
    return new SqlBasicCall(
        SqlStdOperatorTable.AS, List.of(side, new SqlIdentifier(name, POS)), POS);
  }

  private static String leftAliasOrDefault(String explicitAlias) {
    return explicitAlias != null ? explicitAlias : "__l";
  }

  private static String rightAliasOrDefault(String explicitAlias) {
    return explicitAlias != null ? explicitAlias : "__r";
  }

  /** Drop OpenSearch metadata fields from a column list. */
  private static List<String> stripMeta(List<String> fields) {
    return fields.stream()
        .filter(f -> !OpenSearchConstants.METADATAFIELD_TYPE_MAP.containsKey(f))
        .toList();
  }

  /** Intersection of two non-meta column lists, preserving left-side order. */
  private static List<String> sharedFields(List<String> left, List<String> right) {
    if (left == null || right == null) return List.of();
    Set<String> rightSet =
        new LinkedHashSet<>(
            right.stream()
                .filter(f -> !OpenSearchConstants.METADATAFIELD_TYPE_MAP.containsKey(f))
                .toList());
    List<String> shared = new ArrayList<>();
    for (String c : left) {
      if (OpenSearchConstants.METADATAFIELD_TYPE_MAP.containsKey(c)) continue;
      if (rightSet.contains(c)) shared.add(c);
    }
    return shared;
  }

  /**
   * Build the post-join visible field list for the explicit-ON path. Returns the union of left and
   * right column names; for columns that exist on both sides we ALSO add alias-qualified entries
   * ({@code l.col}, {@code r.col}) so a downstream `| fields t1.name, t2.name` can resolve them.
   * Order: left bare cols, right bare cols (deduped), alias-qualified cols.
   */
  private static List<String> unionFieldsWithAliasPrefixes(
      List<String> left, List<String> right, String leftAlias, String rightAlias) {
    Set<String> seen = new LinkedHashSet<>();
    List<String> out = new ArrayList<>();
    for (String c : left) {
      if (seen.add(c)) out.add(c);
    }
    for (String c : right) {
      if (seen.add(c)) out.add(c);
    }
    // Alias-qualified: for any column name present in either list, expose `<alias>.<col>` under
    // its explicit alias. This is for the `| fields t1.name, t2.name` shape; the validator's
    // multi-part identifier resolution handles the actual binding so we just need to list the
    // names so the project resolver doesn't reject them as unknown.
    if (leftAlias != null) {
      for (String c : left) {
        String q = leftAlias + "." + c;
        if (seen.add(q)) out.add(q);
      }
    }
    if (rightAlias != null) {
      for (String c : right) {
        String q = rightAlias + "." + c;
        if (seen.add(q)) out.add(q);
      }
    }
    return out;
  }

  /**
   * Strip the {@code <alias>.} prefix from each element so frame.currentFields stays in
   * "user-facing column name" form for downstream pipes. Names without a dot pass through; names
   * with a dot keep only the suffix.
   */
  private static List<String> stripAliasPrefix(List<String> names) {
    List<String> out = new ArrayList<>(names.size());
    Set<String> seen = new LinkedHashSet<>();
    for (String n : names) {
      int dot = n.indexOf('.');
      String stripped = dot < 0 ? n : n.substring(dot + 1);
      if (seen.add(stripped)) out.add(stripped);
    }
    return out;
  }

  /**
   * Convert a possibly-dotted field name into a {@link SqlIdentifier}. Applies any active alias
   * synonyms (recorded by {@code visitSubqueryAlias} chain collapse or {@code applyExplicitAlias}
   * displacement) so a downstream {@code | fields tt.col} on a `[Y as tt] right=t2` join still
   * resolves under {@code t2.col} for the validator.
   */
  private SqlIdentifier toIdentifier(String name) {
    List<String> parts = name.indexOf('.') < 0 ? List.of(name) : Arrays.asList(name.split("\\."));
    if (exprFrame != null
        && !exprFrame.aliasSynonyms.isEmpty()
        && parts.size() >= 2
        && exprFrame.aliasSynonyms.containsKey(parts.get(0))) {
      List<String> rewritten = new ArrayList<>(parts);
      rewritten.set(0, exprFrame.aliasSynonyms.get(parts.get(0)));
      return new SqlIdentifier(rewritten, POS);
    }
    return new SqlIdentifier(parts, POS);
  }

  /**
   * If {@code name} is a bare column that exists on BOTH sides of a recent join, prefix it with the
   * left alias so Calcite resolves it without raising "Column 'x' is ambiguous". PPL binds bare
   * references to the LEFT side. Names that are already qualified ({@code a.col}) pass through
   * unchanged.
   */
  private SqlIdentifier qualifyIfAmbiguous(String name, Frame frame) {
    JoinHints hints = frame.joinHints;
    if (name.indexOf('.') < 0
        && hints != null
        && hints.leftAlias() != null
        && hints.ambiguousColumns().contains(name)) {
      return new SqlIdentifier(Arrays.asList(hints.leftAlias(), name), POS);
    }
    return toIdentifier(name);
  }

  /**
   * Apply an explicit join-arg alias to a join side. When {@code explicitAlias} is null, the side
   * keeps whatever alias (or none) it already carries. When {@code explicitAlias} is non-null, the
   * side is wrapped as {@code <inner> AS <alias>} — replacing any existing AS-wrapper. PPL
   * semantics: an explicit {@code left=t1}/{@code right=t2} on the JOIN command overrides any inner
   * SubqueryAlias for the join's outer scope.
   */
  private static SqlNode applyExplicitAlias(SqlNode side, String explicitAlias, Frame frame) {
    if (explicitAlias == null) return side;
    SqlNode inner = side;
    if (side instanceof SqlBasicCall sbc && sbc.getOperator() == SqlStdOperatorTable.AS) {
      inner = sbc.operand(0);
      // Inner alias is being displaced; remember the mapping so qualified refs `<displaced>.col`
      // still resolve in downstream pipes via the same scope's synonym map.
      SqlNode displacedAlias = sbc.operand(1);
      if (displacedAlias instanceof SqlIdentifier id && !id.names.isEmpty()) {
        String displaced = id.names.get(0);
        if (!displaced.equals(explicitAlias)) {
          frame.aliasSynonyms.put(displaced, explicitAlias);
        }
      }
    }
    return new SqlBasicCall(
        SqlStdOperatorTable.AS, List.of(inner, new SqlIdentifier(explicitAlias, POS)), POS);
  }
}
