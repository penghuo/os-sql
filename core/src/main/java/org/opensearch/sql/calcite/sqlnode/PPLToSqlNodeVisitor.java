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
import org.opensearch.sql.ast.tree.AddColTotals;
import org.opensearch.sql.ast.tree.AddTotals;
import org.opensearch.sql.ast.tree.Aggregation;
import org.opensearch.sql.ast.tree.Append;
import org.opensearch.sql.ast.tree.AppendCol;
import org.opensearch.sql.ast.tree.AppendPipe;
import org.opensearch.sql.ast.tree.Bin;
import org.opensearch.sql.ast.tree.Chart;
import org.opensearch.sql.ast.tree.Convert;
import org.opensearch.sql.ast.tree.Dedupe;
import org.opensearch.sql.ast.tree.Eval;
import org.opensearch.sql.ast.tree.FillNull;
import org.opensearch.sql.ast.tree.Filter;
import org.opensearch.sql.ast.tree.Head;
import org.opensearch.sql.ast.tree.Join;
import org.opensearch.sql.ast.tree.Limit;
import org.opensearch.sql.ast.tree.Multisearch;
import org.opensearch.sql.ast.tree.Parse;
import org.opensearch.sql.ast.tree.Project;
import org.opensearch.sql.ast.tree.RareTopN;
import org.opensearch.sql.ast.tree.Regex;
import org.opensearch.sql.ast.tree.Relation;
import org.opensearch.sql.ast.tree.Rename;
import org.opensearch.sql.ast.tree.Replace;
import org.opensearch.sql.ast.tree.ReplacePair;
import org.opensearch.sql.ast.tree.Reverse;
import org.opensearch.sql.ast.tree.Rex;
import org.opensearch.sql.ast.tree.SPath;
import org.opensearch.sql.ast.tree.Sort;
import org.opensearch.sql.ast.tree.SpanBin;
import org.opensearch.sql.ast.tree.StreamWindow;
import org.opensearch.sql.ast.tree.SubqueryAlias;
import org.opensearch.sql.ast.tree.Transpose;
import org.opensearch.sql.ast.tree.Trendline;
import org.opensearch.sql.ast.tree.Union;
import org.opensearch.sql.ast.tree.UnresolvedPlan;
import org.opensearch.sql.ast.tree.Window;
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
  public SqlNode visitSearch(org.opensearch.sql.ast.tree.Search node, Frame frame) {
    // PPL `source=T <searchExprs>` desugars to a query_string filter against T. Mirrors the legacy
    // visitor: build MAP('query', VARCHAR(text)) and pass through PermissiveRelevanceFunctions's
    // QUERY_STRING wrapper (the real operator's operand-checker rejects single-MAP arity).
    SqlNode from = node.getChild().get(0).accept(this, frame);
    SqlNode queryArg =
        new SqlBasicCall(
            SqlStdOperatorTable.MAP_VALUE_CONSTRUCTOR,
            List.of(
                SqlLiteral.createCharString("query", POS),
                new SqlBasicCall(
                    org.apache.calcite.sql.fun.SqlLibraryOperators.SAFE_CAST,
                    List.of(
                        SqlLiteral.createCharString(node.getQueryString(), POS),
                        new org.apache.calcite.sql.SqlDataTypeSpec(
                            new org.apache.calcite.sql.SqlBasicTypeNameSpec(
                                org.apache.calcite.sql.type.SqlTypeName.VARCHAR, POS),
                            POS)),
                    POS)),
            POS);
    SqlNode where =
        new SqlBasicCall(
            org.opensearch.sql.calcite.sqlnode.PermissiveRelevanceFunctions.QUERY_STRING,
            List.of(queryArg),
            POS);
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
    // PPL semantics for `eval x = expr` when `x` already exists: replace the column. SQL's
    // `SELECT *, expr AS x` would emit BOTH the original `x` (from `*`) and the new aliased one,
    // producing duplicate-named columns. When ANY let rebinds an existing visible name, expand
    // `*` into an explicit list of currentFields with the rebound names dropped — the eval's
    // aliased projections then reintroduce them. Falls back to `*` when no rebind happens.
    Set<String> rebound = new java.util.HashSet<>();
    for (Let let : node.getExpressionList()) {
      String name = let.getVar().getField().toString();
      if (existingNames.contains(name)) {
        rebound.add(name);
      }
    }
    SqlNodeList items = new SqlNodeList(POS);
    if (rebound.isEmpty() || frame.currentFields == null) {
      items.add(SqlIdentifier.star(POS));
    } else {
      for (String c : frame.currentFields) {
        if (!rebound.contains(c)) {
          items.add(toIdentifier(c));
        }
      }
    }
    Set<String> aliasesInThisSelect = new LinkedHashSet<>();
    boolean wrappedMidEval = false;
    for (Let let : node.getExpressionList()) {
      String alias = let.getVar().getField().toString();
      // If this let's RHS references an alias introduced earlier in this same eval, the previous
      // SELECT can't expose that alias to its own list — wrap and start a new SELECT.
      if (referencesAny(let.getExpression(), aliasesInThisSelect)) {
        from = SqlBuilder.select(items).from(from).withFields(visible).wrap(frame);
        items = new SqlNodeList(POS);
        items.add(SqlIdentifier.star(POS));
        aliasesInThisSelect = new LinkedHashSet<>();
        wrappedMidEval = true;
      }
      SqlNode rhs = expr(let.getExpression());
      // Calcite types string literals as CHAR(N) where N is the literal length. When two branches
      // of a downstream UNION (multisearch / append / chart pivot) bind the same alias to string
      // literals of different lengths, the union widens the result to CHAR(max) and right-pads
      // shorter values with spaces. PPL expects VARCHAR semantics, so cast a bare STRING literal
      // RHS to VARCHAR.
      if (let.getExpression() instanceof Literal stringLit
          && stringLit.getType() == DataType.STRING) {
        org.apache.calcite.sql.SqlDataTypeSpec varcharSpec =
            new org.apache.calcite.sql.SqlDataTypeSpec(
                new org.apache.calcite.sql.SqlBasicTypeNameSpec(
                    org.apache.calcite.sql.type.SqlTypeName.VARCHAR, POS),
                POS);
        rhs =
            new SqlBasicCall(
                org.apache.calcite.sql.fun.SqlLibraryOperators.SAFE_CAST,
                List.of(rhs, varcharSpec),
                POS);
      }
      // PPL `fieldformat alias = "prefix" . expr . "suffix"` parses into a Let with optional
      // concatPrefix/concatSuffix literals. Rewrite to NULL-preserving SQL concat (`||`) — the
      // CONCAT operator preserves NULLs (matches v2's emission shape; CONCAT_FUNCTION coerces
      // NULL → empty string).
      if (let.getConcatPrefix() != null || let.getConcatSuffix() != null) {
        SqlNode strRhs = rhs;
        // Cast non-string rhs to VARCHAR so `||` produces a string column, not a coerced numeric.
        strRhs =
            new SqlBasicCall(
                org.apache.calcite.sql.fun.SqlLibraryOperators.SAFE_CAST,
                List.of(
                    strRhs,
                    new org.apache.calcite.sql.SqlDataTypeSpec(
                        new org.apache.calcite.sql.SqlBasicTypeNameSpec(
                            org.apache.calcite.sql.type.SqlTypeName.VARCHAR, POS),
                        POS)),
                POS);
        if (let.getConcatPrefix() != null) {
          strRhs =
              new SqlBasicCall(
                  SqlStdOperatorTable.CONCAT,
                  List.of(literalToSqlNode(let.getConcatPrefix()), strRhs),
                  POS);
        }
        if (let.getConcatSuffix() != null) {
          strRhs =
              new SqlBasicCall(
                  SqlStdOperatorTable.CONCAT,
                  List.of(strRhs, literalToSqlNode(let.getConcatSuffix())),
                  POS);
        }
        rhs = strRhs;
      }
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
    //
    // Skip the peephole when the eval forced a mid-eval wrap (because a later let references an
    // earlier alias). The `from` we just built is the wrapped child; if we append our remaining
    // items to its select list, we end up with `SELECT *, prev_alias AS x, x AS y FROM ...` —
    // SQL doesn't let SELECT-list aliases reference each other within the same SELECT.
    if (!wrappedMidEval) {
      SqlNode peep = appendEvalToChildSelectStar(from, items, visible, frame);
      if (peep != null) return peep;
    }
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
    // Bail out when any of our new items references an alias already introduced by the existing
    // SELECT list. SQL doesn't let SELECT-list aliases reference each other within the same
    // SELECT — appending here would emit `SELECT *, prev AS x, f(x) AS y FROM ...` which the
    // validator rejects with "Field [x] not found".
    Set<String> existingAliases = new java.util.LinkedHashSet<>();
    for (int i = 1; i < existing.size(); i++) {
      SqlNode item = existing.get(i);
      if (item instanceof SqlBasicCall call
          && call.getOperator() == SqlStdOperatorTable.AS
          && call.operandCount() == 2
          && call.operand(1) instanceof SqlIdentifier aliasId
          && !aliasId.isStar()) {
        existingAliases.add(aliasId.getSimple());
      }
    }
    if (!existingAliases.isEmpty()) {
      for (int i = 1; i < newList.size(); i++) {
        if (refsAnyName(newList.get(i), existingAliases)) {
          return null;
        }
      }
    }
    // newList starts with `*` then our eval items. Skip its leading `*` (already in `existing`)
    // and append the rest.
    for (int i = 1; i < newList.size(); i++) {
      existing.add(newList.get(i));
    }
    frame.currentFields = visible;
    frame.joinHints = null;
    return select;
  }

  /**
   * Recursive identifier scan: does any descendant of {@code node} refer to an unqualified column
   * name in {@code names}? Multi-part identifiers (e.g. {@code a.col}) are skipped — those resolve
   * through their alias scope, not through SELECT-list aliases.
   */
  private static boolean refsAnyName(SqlNode node, Set<String> names) {
    if (node == null) return false;
    if (node instanceof SqlIdentifier id) {
      if (id.isStar()) return false;
      if (!id.isSimple()) return false;
      return names.contains(id.getSimple());
    }
    if (node instanceof SqlBasicCall call) {
      for (SqlNode operand : call.getOperandList()) {
        if (refsAnyName(operand, names)) return true;
      }
    }
    if (node instanceof SqlNodeList list) {
      for (SqlNode child : list.getList()) {
        if (refsAnyName(child, names)) return true;
      }
    }
    return false;
  }

  @Override
  public SqlNode visitRename(Rename node, Frame frame) {
    SqlNode from = node.getChild().get(0).accept(this, frame);
    // PPL `rename old1 as new1, old2 as new2` rewrites named columns and passes the rest through.
    // PPL also supports chained renames within a single command (`A as B, B as C` collapses to
    // A→C) and wildcard patterns (`*ame as *AME` captures the wildcard substring and substitutes
    // it into the replacement). When a rename target collides with an existing column, the
    // existing column is dropped.
    List<String> incoming = frame.currentFields == null ? List.of() : frame.currentFields;
    List<String> cols =
        incoming.stream()
            .filter(c -> !OpenSearchConstants.METADATAFIELD_TYPE_MAP.containsKey(c))
            .toList();
    java.util.Map<String, String> currentName = new java.util.LinkedHashMap<>();
    for (String c : cols) currentName.put(c, c);
    for (org.opensearch.sql.ast.expression.Map m : node.getRenameList()) {
      String origin = ((Field) m.getOrigin()).getField().toString();
      String target = ((Field) m.getTarget()).getField().toString();
      boolean wildcard = origin.contains("*") || target.contains("*");
      if (wildcard) {
        long originStars = origin.chars().filter(c -> c == '*').count();
        long targetStars = target.chars().filter(c -> c == '*').count();
        if (originStars != targetStars) {
          throw new IllegalArgumentException(
              "Source and target patterns have different wildcard counts: "
                  + origin
                  + " has "
                  + originStars
                  + ", "
                  + target
                  + " has "
                  + targetStars);
        }
        java.util.regex.Pattern re =
            java.util.regex.Pattern.compile("^" + origin.replace("*", "(.*)") + "$");
        for (java.util.Map.Entry<String, String> e : currentName.entrySet()) {
          java.util.regex.Matcher m2 = re.matcher(e.getValue());
          if (m2.matches()) {
            StringBuilder out = new StringBuilder();
            int captureIdx = 1;
            for (int i = 0; i < target.length(); i++) {
              char ch = target.charAt(i);
              if (ch == '*' && captureIdx <= m2.groupCount()) {
                out.append(m2.group(captureIdx++));
              } else {
                out.append(ch);
              }
            }
            e.setValue(out.toString());
          }
        }
      } else {
        for (java.util.Map.Entry<String, String> e : currentName.entrySet()) {
          if (origin.equals(e.getValue())) {
            e.setValue(target);
            break;
          }
        }
      }
    }
    java.util.Set<String> renamedTargets = new java.util.HashSet<>();
    for (org.opensearch.sql.ast.expression.Map m : node.getRenameList()) {
      String origin = ((Field) m.getOrigin()).getField().toString();
      String target = ((Field) m.getTarget()).getField().toString();
      if (!target.contains("*") && !origin.equals(target)) {
        renamedTargets.add(target);
      }
    }
    List<String> newVisible = new ArrayList<>(cols.size());
    SqlNodeList items = new SqlNodeList(POS);
    for (String c : cols) {
      String t = currentName.get(c);
      if (!t.equals(c)) {
        items.add(asAliased(toIdentifier(c), t));
        newVisible.add(t);
      } else if (renamedTargets.contains(c)) {
        // Original column dropped because another rename targets this name.
        continue;
      } else {
        items.add(toIdentifier(c));
        newVisible.add(c);
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
    List<SqlNode> nonSpanGroupExprs = new ArrayList<>();
    for (UnresolvedExpression groupExpr : node.getGroupExprList()) {
      String alias = aggLabel(groupExpr);
      UnresolvedExpression core = (groupExpr instanceof Alias a) ? a.getDelegated() : groupExpr;
      SqlNode keyExpr = expr(core);
      groupKeys.add(keyExpr);
      nonSpanGroupExprs.add(keyExpr);
      items.add(asAliased(keyExpr, alias));
      visible.add(alias);
    }
    // Aggregations destroy row-level collation; clear the lastOrderBy hint.
    frame.lastOrderBy = null;

    // PPL `stats bucket_nullable=false ... by X, Y` excludes rows where any group key is NULL.
    // Default true keeps NULL buckets. The argExprList carries the option literal.
    boolean bucketNullable = true;
    if (node.getArgExprList() != null) {
      for (Argument arg : node.getArgExprList()) {
        if (Argument.BUCKET_NULLABLE.equals(arg.getArgName())
            && arg.getValue() != null
            && Boolean.FALSE.equals(arg.getValue().getValue())) {
          bucketNullable = false;
        }
      }
    }
    SqlNode where = null;
    if (!bucketNullable) {
      // For span-based group keys, filter on the source field (not the SPAN call) so the inner
      // SELECT exposes the bare column. Mirrors v2's emission shape.
      List<SqlNode> filterKeys = new ArrayList<>();
      if (node.getSpan() != null) {
        UnresolvedExpression spanCore =
            (node.getSpan() instanceof Alias al) ? al.getDelegated() : node.getSpan();
        if (spanCore instanceof Span sp) {
          filterKeys.add(expr(sp.getField()));
        }
      }
      filterKeys.addAll(nonSpanGroupExprs);
      for (SqlNode k : filterKeys) {
        SqlNode notNull = new SqlBasicCall(SqlStdOperatorTable.IS_NOT_NULL, List.of(k), POS);
        where =
            (where == null)
                ? notNull
                : new SqlBasicCall(SqlStdOperatorTable.AND, List.of(where, notNull), POS);
      }
    } else if (node.getSpan() != null && isTimeSpanGroupKey(node.getSpan())) {
      // Time-unit Span always filters NULL bucket regardless of bucket_nullable. PPL semantics:
      // a NULL-keyed time bucket from rows where the span field is NULL is hidden in the output.
      // Mirrors v2's visitAggregation behaviour. Filter on the FIELD (not the wrapped SPAN call)
      // so the OpenSearch pushdown emits a term-not-exists query rather than a script.
      UnresolvedExpression spanCore =
          (node.getSpan() instanceof Alias al) ? al.getDelegated() : node.getSpan();
      if (spanCore instanceof Span sp) {
        where =
            new SqlBasicCall(SqlStdOperatorTable.IS_NOT_NULL, List.of(expr(sp.getField())), POS);
      }
    }

    SqlBuilder.SelectBuilder b =
        SqlBuilder.select(items).from(from).groupBy(groupKeys).withFields(visible);
    if (where != null) {
      b.where(where);
    }
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
  public SqlNode visitFillNull(FillNull node, Frame frame) {
    SqlNode from = node.getChild().get(0).accept(this, frame);
    // PPL `fillnull with <repl> in <f1>, <f2>, ...` (or `fillnull value=<repl> [<f1>...]`)
    // replaces NULLs with the given value via COALESCE. The new visitor lacks a row-type
    // oracle, so the "all fields" form (replacementForAll without explicit field list) walks
    // frame.currentFields. Per-field replacements walk the same list and only rewrite the
    // listed fields.
    java.util.Map<String, UnresolvedExpression> perField = new java.util.LinkedHashMap<>();
    for (org.apache.commons.lang3.tuple.Pair<Field, UnresolvedExpression> p :
        node.getReplacementPairs()) {
      perField.put(p.getLeft().getField().toString(), p.getRight());
    }
    UnresolvedExpression forAll =
        node.getReplacementForAll().isPresent() ? node.getReplacementForAll().get() : null;
    if (frame.currentFields == null) {
      throw new UnsupportedOperationException(
          "fillnull requires a known column list — call after a `| fields ...` pipe");
    }
    SqlNodeList items = new SqlNodeList(POS);
    for (String c : frame.currentFields) {
      SqlNode fieldRef = toIdentifier(c);
      UnresolvedExpression repl = perField.get(c);
      if (repl == null && forAll != null && perField.isEmpty()) {
        repl = forAll;
      }
      if (repl != null) {
        SqlNode replExpr = expr(repl);
        SqlNode coalesce =
            new SqlBasicCall(SqlStdOperatorTable.COALESCE, List.of(fieldRef, replExpr), POS);
        items.add(asAliased(coalesce, c));
      } else {
        items.add(fieldRef);
      }
    }
    return SqlBuilder.select(items)
        .from(from)
        .withFields(new ArrayList<>(frame.currentFields))
        .wrap(frame);
  }

  @Override
  public SqlNode visitBin(Bin node, Frame frame) {
    SqlNode from = node.getChild().get(0).accept(this, frame);
    UnresolvedExpression rawFieldExpr = node.getField();
    if (rawFieldExpr instanceof Field f) {
      rawFieldExpr = f.getField();
    }
    String fieldName =
        (rawFieldExpr instanceof QualifiedName qn) ? qn.toString() : rawFieldExpr.toString();
    String alias = node.getAlias() != null ? node.getAlias() : fieldName;
    SqlNode fieldRef = expr(node.getField());
    SqlNode bucketCall;
    if (node instanceof SpanBin sb) {
      UnresolvedExpression spanExpr = sb.getSpan();
      boolean numericSpan =
          spanExpr instanceof Literal lit
              && (lit.getType() == DataType.INTEGER
                  || lit.getType() == DataType.LONG
                  || lit.getType() == DataType.SHORT
                  || lit.getType() == DataType.DOUBLE
                  || lit.getType() == DataType.FLOAT
                  || lit.getType() == DataType.DECIMAL);
      if (numericSpan) {
        bucketCall =
            new SqlBasicCall(
                org.opensearch.sql.expression.function.PPLBuiltinOperators.SPAN_BUCKET,
                List.of(fieldRef, expr(spanExpr)),
                POS);
      } else if (spanExpr instanceof Literal stringLit
          && stringLit.getType() == DataType.STRING
          && stringLit.getValue() != null) {
        SqlNode timeSpan =
            tryTimeSpanCall(fieldRef, stringLit.getValue().toString(), sb.getAligntime());
        SqlNode logSpan =
            timeSpan == null ? tryLogSpanCall(fieldRef, stringLit.getValue().toString()) : null;
        if (timeSpan != null) {
          bucketCall = timeSpan;
        } else if (logSpan != null) {
          bucketCall = logSpan;
        } else {
          throw new UnsupportedOperationException(
              "bin span variant "
                  + stringLit.getValue()
                  + " not yet supported in PPLToSqlNodeVisitor");
        }
      } else {
        throw new UnsupportedOperationException(
            "bin span variant " + spanExpr + " not yet supported in PPLToSqlNodeVisitor");
      }
    } else if (node instanceof org.opensearch.sql.ast.tree.CountBin cb) {
      // WIDTH_BUCKET(field, num_bins, data_range=max-min, max_value=max).
      SqlNode minVal = minOver(fieldRef);
      SqlNode maxVal = maxOver(fieldRef);
      SqlNode dataRange = new SqlBasicCall(SqlStdOperatorTable.MINUS, List.of(maxVal, minVal), POS);
      bucketCall =
          new SqlBasicCall(
              org.opensearch.sql.expression.function.PPLBuiltinOperators.WIDTH_BUCKET,
              List.of(
                  fieldRef,
                  SqlLiteral.createExactNumeric(cb.getBins().toString(), POS),
                  dataRange,
                  maxVal),
              POS);
    } else if (node instanceof org.opensearch.sql.ast.tree.MinSpanBin msb) {
      // MINSPAN_BUCKET(field, min_span, data_range=max-min, max_value=max).
      SqlNode minVal = minOver(fieldRef);
      SqlNode maxVal = maxOver(fieldRef);
      SqlNode dataRange = new SqlBasicCall(SqlStdOperatorTable.MINUS, List.of(maxVal, minVal), POS);
      bucketCall =
          new SqlBasicCall(
              org.opensearch.sql.expression.function.PPLBuiltinOperators.MINSPAN_BUCKET,
              List.of(fieldRef, expr(msb.getMinspan()), dataRange, maxVal),
              POS);
    } else if (node instanceof org.opensearch.sql.ast.tree.RangeBin rb) {
      // RANGE_BUCKET(field, MIN OVER, MAX OVER, start, end).
      SqlNode start = rb.getStart() != null ? expr(rb.getStart()) : minOver(fieldRef);
      SqlNode end = rb.getEnd() != null ? expr(rb.getEnd()) : maxOver(fieldRef);
      bucketCall =
          new SqlBasicCall(
              org.opensearch.sql.expression.function.PPLBuiltinOperators.RANGE_BUCKET,
              List.of(fieldRef, minOver(fieldRef), maxOver(fieldRef), start, end),
              POS);
    } else if (node instanceof org.opensearch.sql.ast.tree.DefaultBin) {
      bucketCall =
          new SqlBasicCall(
              org.opensearch.sql.expression.function.PPLBuiltinOperators.RANGE_BUCKET,
              List.of(
                  fieldRef,
                  minOver(fieldRef),
                  maxOver(fieldRef),
                  minOver(fieldRef),
                  maxOver(fieldRef)),
              POS);
    } else {
      throw new UnsupportedOperationException(
          "bin command subtype " + node.getClass().getSimpleName() + " not yet supported");
    }
    // Project: emit non-bin columns in original order, then append the bin column at the end
    // (mirrors v2's emission shape). Without a row-type oracle, walk frame.currentFields.
    if (frame.currentFields == null) {
      throw new UnsupportedOperationException(
          "bin requires a known column list — call after a `| fields ...` pipe");
    }
    SqlNodeList items = new SqlNodeList(POS);
    List<String> visible = new ArrayList<>();
    for (String c : frame.currentFields) {
      if (c.equals(alias)) continue;
      items.add(toIdentifier(c));
      visible.add(c);
    }
    items.add(asAliased(bucketCall, alias));
    visible.add(alias);
    return SqlBuilder.select(items).from(from).withFields(visible).wrap(frame);
  }

  @Override
  public SqlNode visitAppend(Append node, Frame frame) {
    // PPL `<main> | append [<subsearch>]` is UNION ALL of the main pipeline with the subsearch.
    // PPL pads missing columns with NULL on each side so heterogeneous schemas can union. Use
    // each side's frame.currentFields as the column-set oracle (visitProject / visitAggregation
    // / visitFields keep this list authoritative).
    //
    // Apply v2's EmptySourcePropagateVisitor BEFORE walking. Empty-source subsearches like
    // `append [ ]` or `append [ | where ... | append [ ] ]` collapse to (or contain) Values([])
    // that the visitor would otherwise fail to handle. The propagator's visitAppend always
    // returns a new Append even when both sides became EMPTY; collapse that here too.
    UnresolvedPlan prunedSubSearch =
        node.getSubSearch().accept(new org.opensearch.sql.ast.EmptySourcePropagateVisitor(), null);
    if (isEmptyValues(prunedSubSearch) || isFullyEmptyAppend(prunedSubSearch)) {
      // Empty subsearch is a no-op for append: return main unchanged.
      SqlNode mainBody = node.getChild().get(0).accept(this, frame);
      return mainBody;
    }
    SqlNode mainBody = node.getChild().get(0).accept(this, frame);
    if (frame.currentFields == null) {
      throw new UnsupportedOperationException(
          "append main side requires a known column list — call after a pipe that sets it");
    }
    List<String> mainCols = new ArrayList<>(frame.currentFields);

    Frame subFrame = new Frame();
    Frame savedExpr = this.exprFrame;
    this.exprFrame = subFrame;
    SqlNode sub;
    try {
      sub = stripImplicitMetaProjects(prunedSubSearch).accept(this, subFrame);
    } finally {
      this.exprFrame = savedExpr;
    }
    if (subFrame.currentFields == null) {
      throw new UnsupportedOperationException(
          "append subsearch requires a known column list — call after a pipe that sets it");
    }
    List<String> subCols = new ArrayList<>(subFrame.currentFields);

    // Unified column order: main's order first, then sub's columns absent on main (preserving
    // sub's order). Mirrors v2 emission shape so explain plans line up.
    List<String> unified = new ArrayList<>(mainCols);
    for (String c : subCols) {
      if (!unified.contains(c)) {
        unified.add(c);
      }
    }
    SqlNode mainPadded = padToUnifiedSchema(mainBody, mainCols, unified);
    SqlNode subPadded = padToUnifiedSchema(sub, subCols, unified);
    SqlNode unioned =
        new SqlBasicCall(SqlStdOperatorTable.UNION_ALL, List.of(mainPadded, subPadded), POS);
    SqlNodeList wrapItems = new SqlNodeList(POS);
    wrapItems.add(SqlIdentifier.star(POS));
    SqlNode wrapped =
        new SqlSelect(
            POS, null, wrapItems, unioned, null, null, null, null, null, null, null, null);
    frame.currentFields = unified;
    frame.joinHints = null;
    frame.lastOrderBy = null;
    return wrapped;
  }

  /**
   * Wrap {@code body} with a SELECT that lists every column in {@code unified}, taking each from
   * {@code body}'s {@code present} columns when available and emitting {@code NULL AS <col>} for
   * columns absent from this side. Pads heterogeneous-schema unions so PPL's NULL-pad semantics is
   * preserved.
   */
  /**
   * True when {@code plan} is the empty-source sentinel produced by EmptySourcePropagateVisitor.
   */
  private static boolean isEmptyValues(UnresolvedPlan plan) {
    return plan instanceof org.opensearch.sql.ast.tree.Values v
        && (v.getValues() == null || v.getValues().isEmpty());
  }

  /**
   * True when {@code plan} is an Append whose both sides resolved (after propagation) to empty —
   * EmptySourcePropagateVisitor's visitAppend always rebuilds an Append even when both children
   * became empty, so we collapse it ourselves.
   */
  private static boolean isFullyEmptyAppend(UnresolvedPlan plan) {
    if (!(plan instanceof Append app)) return false;
    UnresolvedPlan sub = app.getSubSearch();
    UnresolvedPlan child = app.getChild().isEmpty() ? null : (UnresolvedPlan) app.getChild().get(0);
    boolean subEmpty = sub != null && (isEmptyValues(sub) || isFullyEmptyAppend(sub));
    boolean childEmpty = child != null && (isEmptyValues(child) || isFullyEmptyAppend(child));
    return subEmpty && childEmpty;
  }

  /** Lowercase function name from a WindowFunction's inner expression (Aggregate or Function). */
  private static String wfFuncNameLower(UnresolvedExpression fn) {
    if (fn instanceof AggregateFunction af) {
      return af.getFuncName().toLowerCase(java.util.Locale.ROOT);
    }
    if (fn instanceof org.opensearch.sql.ast.expression.Function f) {
      return f.getFuncName().toLowerCase(java.util.Locale.ROOT);
    }
    return "";
  }

  /** First positional arg (the field) from a WindowFunction's inner expression. */
  private static UnresolvedExpression wfFirstArg(UnresolvedExpression fn) {
    if (fn instanceof AggregateFunction af) {
      return af.getField();
    }
    if (fn instanceof org.opensearch.sql.ast.expression.Function f && !f.getFuncArgs().isEmpty()) {
      return f.getFuncArgs().get(0);
    }
    return null;
  }

  /** {@code CASE WHEN <cond> THEN 1 ELSE 0 END} — used for streamstats reset flags. */
  private SqlNode caseFlagOneZero(SqlNode cond) {
    SqlNodeList whens = new SqlNodeList(POS);
    whens.add(cond);
    SqlNodeList thens = new SqlNodeList(POS);
    thens.add(SqlLiteral.createExactNumeric("1", POS));
    return new org.apache.calcite.sql.fun.SqlCase(
        POS, null, whens, thens, SqlLiteral.createExactNumeric("0", POS));
  }

  private SqlNode padToUnifiedSchema(SqlNode body, List<String> present, List<String> unified) {
    Set<String> presentSet = new java.util.LinkedHashSet<>(present);
    SqlNodeList items = new SqlNodeList(POS);
    for (String c : unified) {
      if (presentSet.contains(c)) {
        items.add(toIdentifier(c));
      } else {
        items.add(asAliased(SqlLiteral.createNull(POS), c));
      }
    }
    return new SqlSelect(POS, null, items, body, null, null, null, null, null, null, null, null);
  }

  @Override
  public SqlNode visitAppendPipe(AppendPipe node, Frame frame) {
    // PPL `<main> | appendpipe [<sub>]` ≡ `(<main>) UNION ALL (<main> | <sub>)`. The subquery
    // is appended to the current pipeline. Walk the subquery from its root down to the
    // placeholder leaf (Values) and attach our main child there, then visit it as a normal
    // pipeline.
    UnresolvedPlan mainChild = node.getChild().get(0);
    Frame mainFrame = new Frame();
    Frame savedExprMain = this.exprFrame;
    this.exprFrame = mainFrame;
    SqlNode mainBody;
    try {
      mainBody = stripImplicitMetaProjects(mainChild).accept(this, mainFrame);
    } finally {
      this.exprFrame = savedExprMain;
    }
    if (mainFrame.currentFields == null) {
      throw new UnsupportedOperationException("appendpipe main side requires a known column list");
    }
    List<String> mainCols = new ArrayList<>(mainFrame.currentFields);

    // Walk subquery to its leaf Values placeholder (or to the deepest single-child node) and
    // attach main as its source.
    UnresolvedPlan subqueryPlan = node.getSubQuery();
    UnresolvedPlan subTail = subqueryPlan;
    while (subTail.getChild() != null
        && !subTail.getChild().isEmpty()
        && !(subTail.getChild().get(0) instanceof org.opensearch.sql.ast.tree.Values)) {
      if (subTail.getChild().size() > 1) {
        throw new RuntimeException("AppendPipe doesn't support multiply children subquery.");
      }
      subTail = (UnresolvedPlan) subTail.getChild().get(0);
    }
    subTail.attach(mainChild);

    Frame subFrame = new Frame();
    Frame savedExprSub = this.exprFrame;
    this.exprFrame = subFrame;
    SqlNode subBody;
    try {
      subBody = stripImplicitMetaProjects(subqueryPlan).accept(this, subFrame);
    } finally {
      this.exprFrame = savedExprSub;
    }
    if (subFrame.currentFields == null) {
      throw new UnsupportedOperationException("appendpipe subquery requires a known column list");
    }
    List<String> subCols = new ArrayList<>(subFrame.currentFields);

    List<String> unified = new ArrayList<>(mainCols);
    for (String c : subCols) {
      if (!unified.contains(c)) {
        unified.add(c);
      }
    }
    SqlNode mainPadded = padToUnifiedSchema(mainBody, mainCols, unified);
    SqlNode subPadded = padToUnifiedSchema(subBody, subCols, unified);
    SqlNode unioned =
        new SqlBasicCall(SqlStdOperatorTable.UNION_ALL, List.of(mainPadded, subPadded), POS);
    SqlNodeList wrapItems = new SqlNodeList(POS);
    wrapItems.add(SqlIdentifier.star(POS));
    SqlNode wrapped =
        new SqlSelect(
            POS, null, wrapItems, unioned, null, null, null, null, null, null, null, null);
    frame.currentFields = unified;
    frame.joinHints = null;
    frame.lastOrderBy = null;
    return wrapped;
  }

  @Override
  public SqlNode visitAppendCol(AppendCol node, Frame frame) {
    // PPL `<main> | appendcol [<sub>]` horizontally merges main and sub by row order. Both
    // sides get a synthetic ROW_NUMBER(); LEFT JOIN on _rn_main_ = _rn_sub_; project the
    // chosen columns. Mirrors v2's emission shape.
    UnresolvedPlan mainChild = (UnresolvedPlan) node.getChild().get(0);
    Frame mainFrame = new Frame();
    Frame savedExprMain = this.exprFrame;
    this.exprFrame = mainFrame;
    SqlNode mainBody;
    try {
      mainBody = stripImplicitMetaProjects(mainChild).accept(this, mainFrame);
    } finally {
      this.exprFrame = savedExprMain;
    }
    if (mainFrame.currentFields == null) {
      throw new UnsupportedOperationException("appendcol main side requires a known column list");
    }
    List<String> mainCols = new ArrayList<>(mainFrame.currentFields);

    // Walk subquery to its leaf Values placeholder and attach main's source as the subquery's
    // own source — same shape as visitAppendPipe.
    UnresolvedPlan subqueryPlan = node.getSubSearch();
    UnresolvedPlan subTail = subqueryPlan;
    while (subTail.getChild() != null
        && !subTail.getChild().isEmpty()
        && !(subTail.getChild().get(0) instanceof org.opensearch.sql.ast.tree.Values)) {
      if (subTail.getChild().size() > 1) {
        throw new RuntimeException("AppendCol doesn't support multiply children subquery.");
      }
      subTail = (UnresolvedPlan) subTail.getChild().get(0);
    }
    subTail.attach(mainChild);

    Frame subFrame = new Frame();
    Frame savedExprSub = this.exprFrame;
    this.exprFrame = subFrame;
    SqlNode subBody;
    try {
      subBody = stripImplicitMetaProjects(subqueryPlan).accept(this, subFrame);
    } finally {
      this.exprFrame = savedExprSub;
    }
    if (subFrame.currentFields == null) {
      throw new UnsupportedOperationException("appendcol subquery requires a known column list");
    }
    List<String> subCols = new ArrayList<>(subFrame.currentFields);

    // Wrap each side with ROW_NUMBER OVER () AS _rn_*_.
    SqlNode mainWithRn = withRowNumber(mainBody, "_rn_main_");
    SqlNode subWithRn = withRowNumber(subBody, "_rn_sub_");
    SqlNode aliasedMain =
        new SqlBasicCall(
            SqlStdOperatorTable.AS, List.of(mainWithRn, new SqlIdentifier("_main_", POS)), POS);
    SqlNode aliasedSub =
        new SqlBasicCall(
            SqlStdOperatorTable.AS, List.of(subWithRn, new SqlIdentifier("_sub_", POS)), POS);
    SqlNode joinCond =
        new SqlBasicCall(
            SqlStdOperatorTable.EQUALS,
            List.of(
                new SqlIdentifier(java.util.Arrays.asList("_main_", "_rn_main_"), POS),
                new SqlIdentifier(java.util.Arrays.asList("_sub_", "_rn_sub_"), POS)),
            POS);
    org.apache.calcite.sql.SqlJoin join =
        new org.apache.calcite.sql.SqlJoin(
            POS,
            aliasedMain,
            SqlLiteral.createBoolean(false, POS),
            JoinType.LEFT.symbol(POS),
            aliasedSub,
            org.apache.calcite.sql.JoinConditionType.ON.symbol(POS),
            joinCond);

    // Build projection using frame-derived column lists.
    java.util.Set<String> mainSet = new java.util.HashSet<>(mainCols);
    java.util.Set<String> subSet = new java.util.HashSet<>(subCols);
    SqlNodeList projection = new SqlNodeList(POS);
    List<String> visible = new ArrayList<>();
    if (node.isOverride()) {
      SqlNode caseCond = joinCond;
      for (String c : mainCols) {
        if (subSet.contains(c)) {
          SqlNodeList whens = new SqlNodeList(POS);
          whens.add(caseCond);
          SqlNodeList thens = new SqlNodeList(POS);
          thens.add(new SqlIdentifier(java.util.Arrays.asList("_sub_", c), POS));
          SqlNode caseExpr =
              new org.apache.calcite.sql.fun.SqlCase(
                  POS,
                  null,
                  whens,
                  thens,
                  new SqlIdentifier(java.util.Arrays.asList("_main_", c), POS));
          projection.add(asAliased(caseExpr, c));
        } else {
          projection.add(
              asAliased(new SqlIdentifier(java.util.Arrays.asList("_main_", c), POS), c));
        }
        visible.add(c);
      }
      for (String c : subCols) {
        if (!mainSet.contains(c)) {
          projection.add(asAliased(new SqlIdentifier(java.util.Arrays.asList("_sub_", c), POS), c));
          visible.add(c);
        }
      }
    } else {
      for (String c : mainCols) {
        projection.add(asAliased(new SqlIdentifier(java.util.Arrays.asList("_main_", c), POS), c));
        visible.add(c);
      }
      for (String c : subCols) {
        if (!mainSet.contains(c)) {
          projection.add(asAliased(new SqlIdentifier(java.util.Arrays.asList("_sub_", c), POS), c));
          visible.add(c);
        }
      }
    }
    SqlNode wrapped =
        new SqlSelect(POS, null, projection, join, null, null, null, null, null, null, null, null);
    frame.currentFields = visible;
    frame.joinHints = null;
    frame.lastOrderBy = null;
    return wrapped;
  }

  /** Wrap {@code inner} as {@code SELECT *, ROW_NUMBER() OVER () AS <alias> FROM (inner)}. */
  private SqlNode withRowNumber(SqlNode inner, String alias) {
    SqlNode rowNum =
        new SqlBasicCall(
            SqlStdOperatorTable.OVER,
            List.of(
                new SqlBasicCall(SqlStdOperatorTable.ROW_NUMBER, List.of(), POS),
                SqlWindow.create(
                    null,
                    null,
                    new SqlNodeList(POS),
                    new SqlNodeList(POS),
                    SqlLiteral.createBoolean(false, POS),
                    null,
                    null,
                    null,
                    POS)),
            POS);
    SqlNodeList items = new SqlNodeList(POS);
    items.add(SqlIdentifier.star(POS));
    items.add(asAliased(rowNum, alias));
    return new SqlSelect(POS, null, items, inner, null, null, null, null, null, null, null, null);
  }

  @Override
  public SqlNode visitMultisearch(Multisearch node, Frame frame) {
    // PPL `| multisearch [<sub1>] [<sub2>] ...` is N-way UNION ALL with NULL-padding for
    // missing columns on each branch. Resolve each branch in a fresh frame so we know its
    // column-set, then pad to the unified schema (preserving each branch's left-to-right
    // column order).
    if (node.getSubsearches() == null || node.getSubsearches().isEmpty()) {
      throw new IllegalArgumentException("Multisearch requires at least one subsearch");
    }
    List<SqlNode> branchNodes = new ArrayList<>();
    List<List<String>> branchCols = new ArrayList<>();
    List<String> unified = new ArrayList<>();
    for (UnresolvedPlan sub : node.getSubsearches()) {
      // Apply EmptySourcePropagateVisitor — drop branches that collapse to Values([]).
      UnresolvedPlan pruned =
          sub.accept(new org.opensearch.sql.ast.EmptySourcePropagateVisitor(), null);
      if (isEmptyValues(pruned)) {
        continue;
      }
      Frame subFrame = new Frame();
      Frame savedExpr = this.exprFrame;
      this.exprFrame = subFrame;
      try {
        branchNodes.add(stripImplicitMetaProjects(pruned).accept(this, subFrame));
      } finally {
        this.exprFrame = savedExpr;
      }
      if (subFrame.currentFields == null) {
        throw new UnsupportedOperationException(
            "multisearch subsearch requires a known column list");
      }
      branchCols.add(new ArrayList<>(subFrame.currentFields));
      for (String c : subFrame.currentFields) {
        if (!unified.contains(c)) {
          unified.add(c);
        }
      }
    }
    if (branchNodes.isEmpty()) {
      throw new IllegalArgumentException("Multisearch requires at least one non-empty subsearch");
    }
    SqlNode union = padToUnifiedSchema(branchNodes.get(0), branchCols.get(0), unified);
    for (int i = 1; i < branchNodes.size(); i++) {
      SqlNode padded = padToUnifiedSchema(branchNodes.get(i), branchCols.get(i), unified);
      union = new SqlBasicCall(SqlStdOperatorTable.UNION_ALL, List.of(union, padded), POS);
    }
    SqlNodeList wrapItems = new SqlNodeList(POS);
    wrapItems.add(SqlIdentifier.star(POS));
    SqlNode wrapped =
        new SqlSelect(POS, null, wrapItems, union, null, null, null, null, null, null, null, null);
    // PPL multisearch interleaves the union by @timestamp DESC when the column is present in
    // the unified row. Without it, multisearch is just plain UNION ALL (subsearch order).
    if (unified.contains(OpenSearchConstants.IMPLICIT_FIELD_TIMESTAMP)) {
      SqlNodeList ord = new SqlNodeList(POS);
      ord.add(
          new SqlBasicCall(
              SqlStdOperatorTable.DESC,
              List.of(new SqlIdentifier(OpenSearchConstants.IMPLICIT_FIELD_TIMESTAMP, POS)),
              POS));
      wrapped = new org.apache.calcite.sql.SqlOrderBy(POS, wrapped, ord, null, null);
    }
    frame.currentFields = unified;
    frame.joinHints = null;
    frame.lastOrderBy = null;
    return wrapped;
  }

  @Override
  public SqlNode visitStreamWindow(StreamWindow node, Frame frame) {
    SqlNode from = node.getChild().get(0).accept(this, frame);
    // PPL `streamstats` is the cumulative variant of eventstats: each row gets the running
    // aggregate of all rows up to and including itself, ordered by an implicit row-sequence.
    //   <agg> OVER (PARTITION BY <by>? ORDER BY <seq> ROWS UNBOUNDED PRECEDING)
    //
    // Defer reset, global=true with by, distinct_count, current=false, window=N (uses
    // narrower frames). Those need a synthesised __stream_seq__ column or RANGE frames and
    // are tracked as a follow-up phase.
    if (frame.currentFields == null) {
      throw new UnsupportedOperationException(
          "streamstats requires a known column list — call after a `| fields ...` pipe");
    }
    boolean hasReset = node.getResetBefore() != null || node.getResetAfter() != null;
    // ROWS frame:
    //   current=true,  window=N>0 : lower = (N-1) PRECEDING, upper = CURRENT ROW (N rows total)
    //   current=false, window=N>0 : lower = N     PRECEDING, upper = 1 PRECEDING (N rows excl)
    //   window=0 (unbounded)      : lower = UNBOUNDED PRECEDING, upper = CURRENT ROW
    int win = node.getWindow();
    int lowerOffset = node.isCurrent() ? win - 1 : win;
    SqlNode frameLower =
        win > 0
            ? new SqlBasicCall(
                SqlWindow.PRECEDING_OPERATOR,
                List.of(SqlLiteral.createExactNumeric(Integer.toString(lowerOffset), POS)),
                POS)
            : SqlWindow.createUnboundedPreceding(POS);
    SqlNode frameUpper =
        node.isCurrent()
            ? SqlWindow.createCurrentRow(POS)
            : new SqlBasicCall(
                SqlWindow.PRECEDING_OPERATOR,
                List.of(SqlLiteral.createExactNumeric("1", POS)),
                POS);
    SqlNodeList partitionBy = new SqlNodeList(POS);
    List<SqlNode> partitionExprs = new ArrayList<>();
    for (UnresolvedExpression p : node.getGroupList()) {
      UnresolvedExpression core = (p instanceof Alias a) ? a.getDelegated() : p;
      SqlNode key = expr(core);
      partitionBy.add(key);
      partitionExprs.add(key);
    }
    boolean hasGroup = !partitionExprs.isEmpty();
    // global=true with a by-partition switches the frame from ROWS to RANGE on the global seq
    // column. Bounds then reflect global-row distance (not partition-local row count). Reset
    // also uses RANGE on the seq so the cumulative aggregate restarts at segment boundaries.
    boolean useRange = (node.isGlobal() && hasGroup && win > 0) || hasReset;
    // ORDER BY uses upstream sort keys when present; otherwise reuse an upstream
    // __stream_seq__ column (from a prior streamstats) when in scope, else synthesise one.
    // Calcite forbids OVER clauses inside another window's ORDER BY, so a freshly synthesised
    // seq has to be materialised in a wrapping SELECT before this window references it.
    SqlNodeList orderBy = new SqlNodeList(POS);
    SqlNode wrappedFrom = from;
    List<String> postSeqFields = frame.currentFields;
    if (frame.currentFields.contains("__stream_seq__")) {
      // Upstream streamstats already provides the seq — reuse it for stable ordering. Don't
      // re-synthesise.
      orderBy.add(new SqlIdentifier("__stream_seq__", POS));
    } else if (!useRange && frame.lastOrderBy != null && !frame.lastOrderBy.isEmpty()) {
      // useRange needs a numeric seq column for RANGE bounds. Skip the lastOrderBy path so the
      // synth branch below materialises __stream_seq__.
      for (SqlNode k : frame.lastOrderBy) orderBy.add(k);
    } else {
      SqlNode rowNum =
          new SqlBasicCall(
              SqlStdOperatorTable.OVER,
              List.of(
                  new SqlBasicCall(SqlStdOperatorTable.ROW_NUMBER, List.of(), POS),
                  SqlWindow.create(
                      null,
                      null,
                      new SqlNodeList(POS),
                      new SqlNodeList(POS),
                      SqlLiteral.createBoolean(false, POS),
                      null,
                      null,
                      null,
                      POS)),
              POS);
      SqlNodeList seqSelect = new SqlNodeList(POS);
      seqSelect.add(SqlIdentifier.star(POS));
      seqSelect.add(asAliased(rowNum, "__stream_seq__"));
      wrappedFrom =
          new SqlSelect(
              POS, null, seqSelect, from, null, null, null, null, null, null, null, null, null);
      postSeqFields = new ArrayList<>(frame.currentFields);
      postSeqFields.add("__stream_seq__");
      orderBy.add(new SqlIdentifier("__stream_seq__", POS));
    }
    // Reset support: each row's reset flag bumps a __seg_id__; cumulative aggregate restarts
    // when segment id changes. Compute __seg_id__ in a wrapping SELECT and prepend it to the
    // partition-by so the cumulative window restarts on segment boundaries.
    if (hasReset) {
      SqlNode beforeFlag =
          node.getResetBefore() != null
              ? caseFlagOneZero(expr(node.getResetBefore()))
              : SqlLiteral.createExactNumeric("0", POS);
      SqlNode afterFlag =
          node.getResetAfter() != null
              ? caseFlagOneZero(expr(node.getResetAfter()))
              : SqlLiteral.createExactNumeric("0", POS);
      SqlNodeList orderBySeq = new SqlNodeList(POS);
      orderBySeq.add(new SqlIdentifier("__stream_seq__", POS));
      SqlNode beforeSumWindow =
          SqlWindow.create(
              null,
              null,
              new SqlNodeList(POS),
              orderBySeq,
              SqlLiteral.createBoolean(true, POS),
              SqlWindow.createUnboundedPreceding(POS),
              SqlWindow.createCurrentRow(POS),
              null,
              POS);
      SqlNode afterSumWindow =
          SqlWindow.create(
              null,
              null,
              new SqlNodeList(POS),
              orderBySeq,
              SqlLiteral.createBoolean(true, POS),
              SqlWindow.createUnboundedPreceding(POS),
              new SqlBasicCall(
                  SqlWindow.PRECEDING_OPERATOR,
                  List.of(SqlLiteral.createExactNumeric("1", POS)),
                  POS),
              null,
              POS);
      SqlNode beforeSum =
          new SqlBasicCall(
              SqlStdOperatorTable.OVER,
              List.of(
                  new SqlBasicCall(SqlStdOperatorTable.SUM, List.of(beforeFlag), POS),
                  beforeSumWindow),
              POS);
      SqlNode afterSum =
          new SqlBasicCall(
              SqlStdOperatorTable.OVER,
              List.of(
                  new SqlBasicCall(SqlStdOperatorTable.SUM, List.of(afterFlag), POS),
                  afterSumWindow),
              POS);
      SqlNode afterSumZero =
          new SqlBasicCall(
              SqlStdOperatorTable.COALESCE,
              List.of(afterSum, SqlLiteral.createExactNumeric("0", POS)),
              POS);
      SqlNode segId =
          new SqlBasicCall(SqlStdOperatorTable.PLUS, List.of(beforeSum, afterSumZero), POS);
      SqlNodeList segWrap = new SqlNodeList(POS);
      segWrap.add(SqlIdentifier.star(POS));
      segWrap.add(asAliased(segId, "__seg_id__"));
      wrappedFrom =
          new SqlSelect(
              POS,
              null,
              segWrap,
              wrappedFrom,
              null,
              null,
              null,
              null,
              null,
              null,
              null,
              null,
              null);
      SqlNodeList newPartitionBy = new SqlNodeList(POS);
      newPartitionBy.add(new SqlIdentifier("__seg_id__", POS));
      for (SqlNode p : partitionBy.getList()) newPartitionBy.add(p);
      partitionBy = newPartitionBy;
    }
    SqlNode partitionNotNullCheck = null;
    if (!node.isBucketNullable() && !partitionExprs.isEmpty()) {
      for (SqlNode pk : partitionExprs) {
        SqlNode isNotNull = new SqlBasicCall(SqlStdOperatorTable.IS_NOT_NULL, List.of(pk), POS);
        partitionNotNullCheck =
            partitionNotNullCheck == null
                ? isNotNull
                : new SqlBasicCall(
                    SqlStdOperatorTable.AND, List.of(partitionNotNullCheck, isNotNull), POS);
      }
    }

    // Streamstats dc(x) prep — Calcite forbids COUNT(DISTINCT) OVER and the partition-wide
    // dense_rank trick is wrong for cumulative semantics. Emit `is_first_<i> = CASE WHEN x IS
    // NULL THEN 0 WHEN ROW_NUMBER() OVER (PARTITION BY [g, ] x ORDER BY seq) = 1 THEN 1 ELSE 0
    // END` as a wrapping SELECT, then in the agg loop swap dc(x) for SUM(is_first_<i>) OVER
    // (... ROWS UNBOUNDED PRECEDING). Splitting into two SELECTs avoids the "Aggregate
    // expressions cannot be nested" error from putting ROW_NUMBER OVER inside SUM OVER.
    java.util.Map<Integer, String> dcIsFirstColForFnIdx = new java.util.HashMap<>();
    List<UnresolvedExpression> wfns = node.getWindowFunctionList();
    if (!orderBy.getList().isEmpty()) {
      SqlNodeList dcWrapList = null;
      for (int i = 0; i < wfns.size(); i++) {
        Alias al = (Alias) wfns.get(i);
        org.opensearch.sql.ast.expression.WindowFunction wf =
            (org.opensearch.sql.ast.expression.WindowFunction) al.getDelegated();
        String fnLowerI = wfFuncNameLower(wf.getFunction());
        if (!"dc".equals(fnLowerI)
            && !"distinct_count".equals(fnLowerI)
            && !"distinct_count_approx".equals(fnLowerI)) {
          continue;
        }
        UnresolvedExpression argExpr = wfFirstArg(wf.getFunction());
        if (argExpr == null || argExpr instanceof AllFields) continue;
        SqlNodeList rnPart = new SqlNodeList(POS);
        for (SqlNode p : partitionBy.getList()) rnPart.add(p);
        rnPart.add(expr(argExpr));
        SqlNode rnWindow =
            SqlWindow.create(
                null,
                null,
                rnPart,
                orderBy,
                SqlLiteral.createBoolean(false, POS),
                null,
                null,
                null,
                POS);
        SqlNode rn =
            new SqlBasicCall(
                SqlStdOperatorTable.OVER,
                List.of(new SqlBasicCall(SqlStdOperatorTable.ROW_NUMBER, List.of(), POS), rnWindow),
                POS);
        SqlNode isNullArg =
            new SqlBasicCall(SqlStdOperatorTable.IS_NULL, List.of(expr(argExpr)), POS);
        SqlNode rnEqOne =
            new SqlBasicCall(
                SqlStdOperatorTable.EQUALS,
                List.of(rn, SqlLiteral.createExactNumeric("1", POS)),
                POS);
        SqlNodeList isFirstWhens = new SqlNodeList(POS);
        isFirstWhens.add(isNullArg);
        isFirstWhens.add(rnEqOne);
        SqlNodeList isFirstThens = new SqlNodeList(POS);
        isFirstThens.add(SqlLiteral.createExactNumeric("0", POS));
        isFirstThens.add(SqlLiteral.createExactNumeric("1", POS));
        SqlNode isFirst =
            new org.apache.calcite.sql.fun.SqlCase(
                POS, null, isFirstWhens, isFirstThens, SqlLiteral.createExactNumeric("0", POS));
        String colName = "_dc_is_first_" + i + "_";
        if (dcWrapList == null) {
          dcWrapList = new SqlNodeList(POS);
          dcWrapList.add(SqlIdentifier.star(POS));
        }
        dcWrapList.add(asAliased(isFirst, colName));
        dcIsFirstColForFnIdx.put(i, colName);
      }
      if (dcWrapList != null) {
        wrappedFrom =
            new SqlSelect(
                POS,
                null,
                dcWrapList,
                wrappedFrom,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null);
      }
    }

    SqlNodeList items = new SqlNodeList(POS);
    List<String> visible = new ArrayList<>();
    // Pass through every column from postSeqFields (which includes the just-synthesised or
    // upstream __stream_seq__) so a downstream streamstats can reuse the same row ordering.
    // SqlNodePlanner.stripSyntheticSeqColumns drops __stream_seq__ from the user-facing
    // top-level row type after RelNode conversion.
    for (String c : postSeqFields) {
      items.add(toIdentifier(c));
      visible.add(c);
    }
    for (int i = 0; i < wfns.size(); i++) {
      UnresolvedExpression item = wfns.get(i);
      Alias al = (Alias) item;
      org.opensearch.sql.ast.expression.WindowFunction wf =
          (org.opensearch.sql.ast.expression.WindowFunction) al.getDelegated();
      UnresolvedExpression aggInput = wf.getFunction();
      if (aggInput instanceof org.opensearch.sql.ast.expression.Function f) {
        UnresolvedExpression argExpr =
            f.getFuncArgs().isEmpty() ? AllFields.of() : f.getFuncArgs().get(0);
        java.util.List<UnresolvedExpression> rest =
            f.getFuncArgs().size() > 1
                ? f.getFuncArgs().subList(1, f.getFuncArgs().size())
                : java.util.List.of();
        aggInput = new AggregateFunction(f.getFuncName(), argExpr, rest);
      }
      String fnLower =
          aggInput instanceof AggregateFunction afn
              ? afn.getFuncName().toLowerCase(java.util.Locale.ROOT)
              : "";
      if (fnLower.equals("percentile")
          || fnLower.equals("percentile_approx")
          || fnLower.equals("median")) {
        throw new UnsupportedOperationException(
            "Unexpected window function: " + fnLower.toUpperCase(java.util.Locale.ROOT));
      }
      SqlNode aggNode;
      if (dcIsFirstColForFnIdx.containsKey(i)) {
        // dc(x) → SUM(is_first_<i>) OVER (PARTITION BY [g] ORDER BY seq ROWS UNBOUNDED
        // PRECEDING). Always cumulative regardless of streamstats `window=N` setting —
        // distinct count is running over the full preceding stream by PPL semantics.
        aggNode =
            new SqlBasicCall(
                SqlStdOperatorTable.SUM,
                List.of(new SqlIdentifier(dcIsFirstColForFnIdx.get(i), POS)),
                POS);
      } else if (fnLower.equals("dc")
          || fnLower.equals("distinct_count")
          || fnLower.equals("distinct_count_approx")) {
        // dc with no ORDER BY — fall back to the legacy error.
        throw new UnsupportedOperationException(
            "streamstats distinct_count requires a stable order (no upstream sort and no"
                + " synthesised __stream_seq__)");
      } else {
        aggNode = aggCall(aggInput, /* windowed */ true);
      }
      // ROWS frame derived from current/window above (current=true window=0 → cumulative;
      // current=true window=N → last N rows; current=false window=N → previous N rows).
      // useRange=true switches to RANGE frame on __stream_seq__ — bounds reflect global
      // row distance (for global=true with by-partition).
      // dc(x) emulation: SUM(is_first_<i>) is always cumulative regardless of window=N.
      boolean cumulativeForDc = dcIsFirstColForFnIdx.containsKey(i);
      SqlNode dcLower = SqlWindow.createUnboundedPreceding(POS);
      SqlNode dcUpper = SqlWindow.createCurrentRow(POS);
      SqlNode window =
          SqlWindow.create(
              null,
              null,
              partitionBy,
              orderBy,
              SqlLiteral.createBoolean(cumulativeForDc || !useRange, POS),
              cumulativeForDc ? dcLower : frameLower,
              cumulativeForDc ? dcUpper : frameUpper,
              null,
              POS);
      SqlNode over = new SqlBasicCall(SqlStdOperatorTable.OVER, List.of(aggNode, window), POS);
      // Same NULL-guard as visitWindow: AVG/VAR/STDDEV over empty/all-NULL return NULL.
      if (aggNode instanceof SqlBasicCall bc && bc.getOperandList().size() == 1) {
        org.apache.calcite.sql.SqlOperator op = bc.getOperator();
        boolean needsZeroGuard =
            op == SqlStdOperatorTable.AVG
                || op == SqlStdOperatorTable.VAR_POP
                || op == SqlStdOperatorTable.STDDEV_POP;
        boolean needsOneGuard =
            op == SqlStdOperatorTable.VAR_SAMP || op == SqlStdOperatorTable.STDDEV_SAMP;
        if (needsZeroGuard || needsOneGuard) {
          int minCount = needsOneGuard ? 1 : 0;
          SqlNode countCall =
              new SqlBasicCall(SqlStdOperatorTable.COUNT, List.of(bc.getOperandList().get(0)), POS);
          SqlNode countOver =
              new SqlBasicCall(SqlStdOperatorTable.OVER, List.of(countCall, window), POS);
          SqlNode countCondition =
              new SqlBasicCall(
                  SqlStdOperatorTable.GREATER_THAN,
                  List.of(
                      countOver, SqlLiteral.createExactNumeric(Integer.toString(minCount), POS)),
                  POS);
          SqlNodeList whens = new SqlNodeList(POS);
          whens.add(countCondition);
          SqlNodeList thens = new SqlNodeList(POS);
          thens.add(over);
          over =
              new org.apache.calcite.sql.fun.SqlCase(
                  POS, null, whens, thens, SqlLiteral.createNull(POS));
        }
      }
      if (partitionNotNullCheck != null) {
        SqlNodeList nnWhens = new SqlNodeList(POS);
        nnWhens.add(partitionNotNullCheck);
        SqlNodeList nnThens = new SqlNodeList(POS);
        nnThens.add(over);
        over =
            new org.apache.calcite.sql.fun.SqlCase(
                POS, null, nnWhens, nnThens, SqlLiteral.createNull(POS));
      }
      items.add(asAliased(over, al.getName()));
      visible.add(al.getName());
    }
    return SqlBuilder.select(items).from(wrappedFrom).withFields(visible).wrap(frame);
  }

  @Override
  public SqlNode visitWindow(Window node, Frame frame) {
    SqlNode from = node.getChild().get(0).accept(this, frame);
    // PPL `eventstats <agg1>[ as a1], <agg2>[ as a2], ... [by g1, g2, ...]` appends one column
    // per agg to every input row using a window function: `<agg> OVER (PARTITION BY <gs>)`.
    // The result preserves the input row count (in contrast to `stats` which collapses to one
    // row per group).
    if (frame.currentFields == null) {
      throw new UnsupportedOperationException(
          "eventstats requires a known column list — call after a `| fields ...` pipe");
    }
    SqlNodeList partitionBy = new SqlNodeList(POS);
    List<SqlNode> partitionExprs = new ArrayList<>();
    for (UnresolvedExpression p : node.getGroupList()) {
      UnresolvedExpression core = (p instanceof Alias a) ? a.getDelegated() : p;
      SqlNode key = expr(core);
      partitionBy.add(key);
      partitionExprs.add(key);
    }
    // PPL `eventstats bucket_nullable=false ... by X, Y` excludes rows where any partition key
    // is NULL from the aggregate output. Wrap each agg with `CASE WHEN <pk1> IS NOT NULL [AND
    // ...] THEN <agg> ELSE NULL END`. Without partitions, bucket_nullable has no effect.
    SqlNode partitionNotNullCheck = null;
    if (!node.isBucketNullable() && !partitionExprs.isEmpty()) {
      for (SqlNode pk : partitionExprs) {
        SqlNode isNotNull = new SqlBasicCall(SqlStdOperatorTable.IS_NOT_NULL, List.of(pk), POS);
        partitionNotNullCheck =
            partitionNotNullCheck == null
                ? isNotNull
                : new SqlBasicCall(
                    SqlStdOperatorTable.AND, List.of(partitionNotNullCheck, isNotNull), POS);
      }
    }
    SqlNodeList items = new SqlNodeList(POS);
    List<String> visible = new ArrayList<>();
    for (String c : frame.currentFields) {
      items.add(toIdentifier(c));
      visible.add(c);
    }
    for (UnresolvedExpression item : node.getWindowFunctionList()) {
      Alias al = (Alias) item;
      org.opensearch.sql.ast.expression.WindowFunction wf =
          (org.opensearch.sql.ast.expression.WindowFunction) al.getDelegated();
      // PPL eventstats wraps aggs as `WindowFunction(Function(name, args))` (parser path), while
      // stats wraps them as `AggregateFunction`. Normalize the Function shape into
      // AggregateFunction so aggCall's dispatch sees a uniform input.
      UnresolvedExpression aggInput = wf.getFunction();
      if (aggInput instanceof org.opensearch.sql.ast.expression.Function f) {
        // count() parses as Function("count", []). Default to AllFields so aggCall emits
        // COUNT(*); other zero-arg aggs are not valid in eventstats and would surface a
        // validator error downstream.
        UnresolvedExpression argExpr =
            f.getFuncArgs().isEmpty() ? AllFields.of() : f.getFuncArgs().get(0);
        java.util.List<UnresolvedExpression> rest =
            f.getFuncArgs().size() > 1
                ? f.getFuncArgs().subList(1, f.getFuncArgs().size())
                : java.util.List.of();
        aggInput = new AggregateFunction(f.getFuncName(), argExpr, rest);
      }
      String fnLower =
          aggInput instanceof AggregateFunction afn
              ? afn.getFuncName().toLowerCase(java.util.Locale.ROOT)
              : "";
      if (fnLower.equals("dc")
          || fnLower.equals("distinct_count")
          || fnLower.equals("distinct_count_approx")) {
        // Calcite forbids COUNT(DISTINCT x) inside OVER. Emulate via two dense_rank windows:
        //   forward = dense_rank() OVER (PARTITION BY p ORDER BY x ASC)
        //   reverse = dense_rank() OVER (PARTITION BY p ORDER BY x DESC)
        //   distinct_count = forward + reverse - 1 - (1 if any NULL else 0)
        // forward + reverse - 1 counts distinct values including NULL; subtracting the
        // any-NULL flag gives PPL's NULL-ignoring distinct_count.
        AggregateFunction af = (AggregateFunction) aggInput;
        UnresolvedExpression argExpr = af.getField();
        if (argExpr == null || argExpr instanceof AllFields) {
          throw new UnsupportedOperationException("distinct_count requires a field argument");
        }
        SqlNode argRef = expr(argExpr);
        SqlNodeList orderAsc = new SqlNodeList(POS);
        orderAsc.add(expr(argExpr));
        SqlNodeList orderDesc = new SqlNodeList(POS);
        orderDesc.add(new SqlBasicCall(SqlStdOperatorTable.DESC, List.of(expr(argExpr)), POS));
        SqlNode wAsc =
            SqlWindow.create(
                null,
                null,
                partitionBy,
                orderAsc,
                SqlLiteral.createBoolean(false, POS),
                null,
                null,
                null,
                POS);
        SqlNode wDesc =
            SqlWindow.create(
                null,
                null,
                partitionBy,
                orderDesc,
                SqlLiteral.createBoolean(false, POS),
                null,
                null,
                null,
                POS);
        SqlNode rankAsc =
            new SqlBasicCall(
                SqlStdOperatorTable.OVER,
                List.of(new SqlBasicCall(SqlStdOperatorTable.DENSE_RANK, List.of(), POS), wAsc),
                POS);
        SqlNode rankDesc =
            new SqlBasicCall(
                SqlStdOperatorTable.OVER,
                List.of(new SqlBasicCall(SqlStdOperatorTable.DENSE_RANK, List.of(), POS), wDesc),
                POS);
        SqlNode sum = new SqlBasicCall(SqlStdOperatorTable.PLUS, List.of(rankAsc, rankDesc), POS);
        SqlNode countIncludingNull =
            new SqlBasicCall(
                SqlStdOperatorTable.MINUS,
                List.of(sum, SqlLiteral.createExactNumeric("1", POS)),
                POS);
        // Any-NULL flag: MAX(CASE WHEN x IS NULL THEN 1 ELSE 0 END) OVER (PARTITION BY p).
        SqlNode wPartOnly =
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
        SqlNodeList nullWhens = new SqlNodeList(POS);
        nullWhens.add(new SqlBasicCall(SqlStdOperatorTable.IS_NULL, List.of(argRef), POS));
        SqlNodeList nullThens = new SqlNodeList(POS);
        nullThens.add(SqlLiteral.createExactNumeric("1", POS));
        SqlNode nullFlag =
            new org.apache.calcite.sql.fun.SqlCase(
                POS, null, nullWhens, nullThens, SqlLiteral.createExactNumeric("0", POS));
        SqlNode anyNullInPart =
            new SqlBasicCall(
                SqlStdOperatorTable.OVER,
                List.of(
                    new SqlBasicCall(SqlStdOperatorTable.MAX, List.of(nullFlag), POS), wPartOnly),
                POS);
        SqlNode dcOver =
            new SqlBasicCall(
                SqlStdOperatorTable.MINUS, List.of(countIncludingNull, anyNullInPart), POS);
        if (partitionNotNullCheck != null) {
          SqlNodeList nnWhens = new SqlNodeList(POS);
          nnWhens.add(partitionNotNullCheck);
          SqlNodeList nnThens = new SqlNodeList(POS);
          nnThens.add(dcOver);
          dcOver =
              new org.apache.calcite.sql.fun.SqlCase(
                  POS, null, nnWhens, nnThens, SqlLiteral.createNull(POS));
        }
        items.add(asAliased(dcOver, al.getName()));
        visible.add(al.getName());
        continue;
      }
      if (fnLower.equals("percentile")
          || fnLower.equals("percentile_approx")
          || fnLower.equals("median")) {
        // Match legacy error message; CalcitePPLEventstatsIT.testUnsupportedWindowFunctions
        // asserts on the substring "Unexpected window function: <NAME>".
        throw new UnsupportedOperationException(
            "Unexpected window function: " + fnLower.toUpperCase(java.util.Locale.ROOT));
      }
      SqlNode aggNode = aggCall(aggInput, /* windowed */ true);
      SqlNode window =
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
      SqlNode over = new SqlBasicCall(SqlStdOperatorTable.OVER, List.of(aggNode, window), POS);
      // Calcite's standard AVG over a partition with all-NULL values returns 0 (the SUM/COUNT
      // convertlet yields 0/0 in the enumerable runtime). PPL semantics: NULL when no non-NULL
      // rows contributed. Wrap AVG with `CASE WHEN COUNT(field) > 0 THEN <avg> ELSE NULL END`.
      // VAR_SAMP/STDDEV_SAMP need n>1 for Bessel's correction; VAR_POP/STDDEV_POP need n>0.
      if (aggNode instanceof SqlBasicCall bc && bc.getOperandList().size() == 1) {
        org.apache.calcite.sql.SqlOperator op = bc.getOperator();
        boolean needsZeroGuard =
            op == SqlStdOperatorTable.AVG
                || op == SqlStdOperatorTable.VAR_POP
                || op == SqlStdOperatorTable.STDDEV_POP;
        boolean needsOneGuard =
            op == SqlStdOperatorTable.VAR_SAMP || op == SqlStdOperatorTable.STDDEV_SAMP;
        if (needsZeroGuard || needsOneGuard) {
          int minCount = needsOneGuard ? 1 : 0;
          SqlNode countCall =
              new SqlBasicCall(SqlStdOperatorTable.COUNT, List.of(bc.getOperandList().get(0)), POS);
          SqlNode countOver =
              new SqlBasicCall(SqlStdOperatorTable.OVER, List.of(countCall, window), POS);
          SqlNode countCondition =
              new SqlBasicCall(
                  SqlStdOperatorTable.GREATER_THAN,
                  List.of(
                      countOver, SqlLiteral.createExactNumeric(Integer.toString(minCount), POS)),
                  POS);
          SqlNodeList whens = new SqlNodeList(POS);
          whens.add(countCondition);
          SqlNodeList thens = new SqlNodeList(POS);
          thens.add(over);
          over =
              new org.apache.calcite.sql.fun.SqlCase(
                  POS, null, whens, thens, SqlLiteral.createNull(POS));
        }
      }
      if (partitionNotNullCheck != null) {
        SqlNodeList nnWhens = new SqlNodeList(POS);
        nnWhens.add(partitionNotNullCheck);
        SqlNodeList nnThens = new SqlNodeList(POS);
        nnThens.add(over);
        over =
            new org.apache.calcite.sql.fun.SqlCase(
                POS, null, nnWhens, nnThens, SqlLiteral.createNull(POS));
      }
      items.add(asAliased(over, al.getName()));
      visible.add(al.getName());
    }
    return SqlBuilder.select(items).from(from).withFields(visible).wrap(frame);
  }

  @Override
  public SqlNode visitTrendline(Trendline node, Frame frame) {
    SqlNode from = node.getChild().get(0).accept(this, frame);
    // PPL `trendline [sort F] sma(N, fld) [as alias] ...`:
    //   1. Pre-filter NULL values for each computation's data field — rows with NULL in fld
    //      don't contribute to the rolling window (SQL convention; v2 enforces by WHERE
    //      fld IS NOT NULL).
    //   2. Window expression per computation: ROWS N-1 PRECEDING, ORDER BY <sortField if any>.
    //      SMA → AVG(fld) OVER (window). WMA → Σ(i * NTH_VALUE(fld, i)) / (N*(N+1)/2) over the
    //      same window.
    //   3. CASE WHEN COUNT(*) OVER (window) > N-1 THEN <agg> ELSE NULL END — first N-1 rows
    //      yield NULL while the window isn't full.
    //   4. Project: when alias collides with an existing field name, override that column
    //      in place; otherwise append the trendline column at the end.
    if (frame.currentFields == null) {
      throw new UnsupportedOperationException(
          "trendline requires a known column list — call after a `| fields ...` pipe");
    }

    // Step 1: NULL pre-filter for each computation's data field. Combine into a single WHERE.
    SqlNode whereCond = null;
    for (Trendline.TrendlineComputation c : node.getComputations()) {
      SqlNode fieldRef = expr(c.getDataField().getField());
      SqlNode notNull = new SqlBasicCall(SqlStdOperatorTable.IS_NOT_NULL, List.of(fieldRef), POS);
      whereCond =
          (whereCond == null)
              ? notNull
              : new SqlBasicCall(SqlStdOperatorTable.AND, List.of(whereCond, notNull), POS);
    }

    // Sort embedded in the OVER clause's ORDER BY. PPL `trendline sort <fld> sma(...)` puts the
    // ordering inside the window; an outer ORDER BY on the same key is also retained so the
    // result preserves the requested sort.
    SqlNodeList windowOrderBy = new SqlNodeList(POS);
    List<SqlNode> outerOrderBy = null;
    if (node.getSortByField().isPresent()) {
      Field sortField = node.getSortByField().get();
      Sort.SortOption opt = analyzeSortOption(sortField.getFieldArgs());
      SqlNode key = expr(sortField.getField());
      if (opt.getSortOrder() == Sort.SortOrder.DESC) {
        key = new SqlBasicCall(SqlStdOperatorTable.DESC, List.of(key), POS);
      }
      windowOrderBy.add(key);
      outerOrderBy = List.of(key);
    }

    // Wrap the child with the NULL pre-filter so the trendline columns and pre-existing columns
    // come from the same SELECT scope.
    SqlNode filtered =
        new SqlSelect(
            POS,
            null,
            new SqlNodeList(List.of(SqlIdentifier.star(POS)), POS),
            from,
            whereCond,
            null,
            null,
            null,
            null,
            null,
            null,
            null);

    // Step 2-3: build CASE expressions per computation.
    java.util.Map<String, SqlNode> aliasOverrides = new java.util.LinkedHashMap<>();
    List<SqlNode> appended = new ArrayList<>();
    List<String> appendedNames = new ArrayList<>();
    for (Trendline.TrendlineComputation c : node.getComputations()) {
      int n = c.getNumberOfDataPoints();
      SqlNode lower =
          new SqlBasicCall(
              SqlWindow.PRECEDING_OPERATOR,
              List.of(SqlLiteral.createExactNumeric(Integer.toString(n - 1), POS)),
              POS);
      SqlNode upper = SqlWindow.createCurrentRow(POS);
      SqlNode window =
          SqlWindow.create(
              null,
              null,
              new SqlNodeList(POS),
              windowOrderBy,
              SqlLiteral.createBoolean(true, POS),
              lower,
              upper,
              null,
              POS);
      SqlNode fieldRef = expr(c.getDataField().getField());
      SqlNode countCall =
          new SqlBasicCall(SqlStdOperatorTable.COUNT, List.of(SqlIdentifier.star(POS)), POS);
      SqlNode countOver =
          new SqlBasicCall(SqlStdOperatorTable.OVER, List.of(countCall, window), POS);
      SqlNode countCondition =
          new SqlBasicCall(
              SqlStdOperatorTable.GREATER_THAN,
              List.of(countOver, SqlLiteral.createExactNumeric(Integer.toString(n - 1), POS)),
              POS);
      SqlNode windowedAgg;
      switch (c.getComputationType()) {
        case SMA -> {
          SqlNode agg = new SqlBasicCall(SqlStdOperatorTable.AVG, List.of(fieldRef), POS);
          windowedAgg = new SqlBasicCall(SqlStdOperatorTable.OVER, List.of(agg, window), POS);
        }
        case WMA -> {
          // WMA = Σ(i * NTH_VALUE(field, i)) / (N*(N+1)/2). Cast the divisor to DOUBLE so the
          // result is double-precision (matches v2's type promotion).
          SqlNode divider = null;
          for (int i = 1; i <= n; i++) {
            SqlNode nth =
                new SqlBasicCall(
                    SqlStdOperatorTable.NTH_VALUE,
                    List.of(fieldRef, SqlLiteral.createExactNumeric(Integer.toString(i), POS)),
                    POS);
            SqlNode nthOver = new SqlBasicCall(SqlStdOperatorTable.OVER, List.of(nth, window), POS);
            SqlNode weighted =
                new SqlBasicCall(
                    SqlStdOperatorTable.MULTIPLY,
                    List.of(SqlLiteral.createExactNumeric(Integer.toString(i), POS), nthOver),
                    POS);
            divider =
                divider == null
                    ? weighted
                    : new SqlBasicCall(SqlStdOperatorTable.PLUS, List.of(divider, weighted), POS);
          }
          org.apache.calcite.sql.SqlDataTypeSpec doubleSpec =
              new org.apache.calcite.sql.SqlDataTypeSpec(
                  new org.apache.calcite.sql.SqlBasicTypeNameSpec(
                      org.apache.calcite.sql.type.SqlTypeName.DOUBLE, POS),
                  POS);
          SqlNode divisor =
              new SqlBasicCall(
                  org.apache.calcite.sql.fun.SqlLibraryOperators.SAFE_CAST,
                  List.of(
                      SqlLiteral.createExactNumeric(Integer.toString(n * (n + 1) / 2), POS),
                      doubleSpec),
                  POS);
          windowedAgg =
              new SqlBasicCall(SqlStdOperatorTable.DIVIDE, List.of(divider, divisor), POS);
        }
        default ->
            throw new UnsupportedOperationException(
                "Unsupported trendline type: " + c.getComputationType());
      }
      SqlNodeList whens = new SqlNodeList(POS);
      whens.add(countCondition);
      SqlNodeList thens = new SqlNodeList(POS);
      thens.add(windowedAgg);
      SqlNode caseExpr =
          new org.apache.calcite.sql.fun.SqlCase(
              POS, null, whens, thens, SqlLiteral.createNull(POS));
      String alias = c.getAlias();
      if (frame.currentFields.contains(alias)) {
        aliasOverrides.put(alias, caseExpr);
      } else {
        appended.add(caseExpr);
        appendedNames.add(alias);
      }
    }

    // Step 4: build SELECT list — original columns (overridden where alias collides), then
    // appended trendline columns.
    SqlNodeList items = new SqlNodeList(POS);
    List<String> visible = new ArrayList<>();
    for (String c : frame.currentFields) {
      if (aliasOverrides.containsKey(c)) {
        items.add(asAliased(aliasOverrides.get(c), c));
      } else {
        items.add(toIdentifier(c));
      }
      visible.add(c);
    }
    for (int i = 0; i < appended.size(); i++) {
      items.add(asAliased(appended.get(i), appendedNames.get(i)));
      visible.add(appendedNames.get(i));
    }

    SqlBuilder.SelectBuilder b = SqlBuilder.select(items).from(filtered).withFields(visible);
    if (outerOrderBy != null) {
      b.orderBy(outerOrderBy);
    }
    return b.wrap(frame);
  }

  @Override
  public SqlNode visitUnion(Union node, Frame frame) {
    // PPL `| union [<plan1>, <plan2>, ...]` is UNION ALL of N datasets. The new visitor lacks an
    // oracle so we don't pad mismatched schemas — branches must already align (or the validator
    // raises a column-mismatch error). For matching-schema unions this is a transparent
    // emission. Mismatched-schema padding requires schema introspection; defer to a follow-up.
    if (node.getDatasets() == null || node.getDatasets().size() < 2) {
      throw new IllegalArgumentException(
          "Union command requires at least two datasets. Provided: "
              + (node.getDatasets() == null ? 0 : node.getDatasets().size()));
    }
    List<SqlNode> branches = new ArrayList<>();
    for (UnresolvedPlan ds : node.getDatasets()) {
      Frame branchFrame = new Frame();
      Frame savedExpr = this.exprFrame;
      this.exprFrame = branchFrame;
      branches.add(stripImplicitMetaProjects(ds).accept(this, branchFrame));
      this.exprFrame = savedExpr;
    }
    SqlNode union = branches.get(0);
    for (int i = 1; i < branches.size(); i++) {
      union = new SqlBasicCall(SqlStdOperatorTable.UNION_ALL, List.of(union, branches.get(i)), POS);
    }
    SqlNodeList items = new SqlNodeList(POS);
    items.add(SqlIdentifier.star(POS));
    SqlNode wrapper =
        new SqlSelect(
            POS,
            null,
            items,
            union,
            null,
            null,
            null,
            null,
            null,
            null,
            node.getMaxout() != null && node.getMaxout() > 0
                ? SqlLiteral.createExactNumeric(node.getMaxout().toString(), POS)
                : null,
            null);
    frame.joinHints = null;
    frame.lastOrderBy = null;
    return wrapper;
  }

  @Override
  public SqlNode visitAddColTotals(AddColTotals node, Frame frame) {
    SqlNode from = node.getChild().get(0).accept(this, frame);
    if (frame.currentFields == null) {
      throw new UnsupportedOperationException(
          "addcoltotals requires a known column list — call after a `| fields ...` pipe");
    }
    java.util.Map<String, Literal> options = node.getOptions();
    String labelField = null;
    if (options != null && options.containsKey("labelfield")) {
      labelField = options.get("labelfield").getValue().toString();
    }
    String label = "Total";
    if (options != null && options.containsKey("label")) {
      label = options.get("label").getValue().toString();
    }
    java.util.Set<String> aggFieldNames = new java.util.LinkedHashSet<>();
    if (node.getFieldList() != null) {
      for (Field f : node.getFieldList()) {
        aggFieldNames.add(f.getField().toString());
      }
    }
    // Without a row-type oracle, treat every visible column as eligible for SUM unless an
    // explicit field list is given. Non-numeric columns will surface as a runtime cast error
    // (matches PPL behaviour: addcoltotals on non-numeric throws).
    boolean explicitFields = !aggFieldNames.isEmpty();
    boolean appendLabelField = labelField != null && !frame.currentFields.contains(labelField);

    // Wrap the main pipeline as a subquery so we can UNION ALL it with the summary row.
    SqlNodeList mainProj = new SqlNodeList(POS);
    for (String c : frame.currentFields) {
      mainProj.add(toIdentifier(c));
    }
    if (appendLabelField) {
      // Pad the data side with an empty-VARCHAR placeholder so the UNION's row-type derivation
      // resolves to VARCHAR (not BIGINT-from-NULL). Match label length so the union doesn't
      // widen to a longer CHAR(N).
      org.apache.calcite.sql.SqlDataTypeSpec varcharSpec =
          new org.apache.calcite.sql.SqlDataTypeSpec(
              new org.apache.calcite.sql.SqlBasicTypeNameSpec(
                  org.apache.calcite.sql.type.SqlTypeName.VARCHAR, label.length(), POS),
              POS);
      SqlNode pad =
          new SqlBasicCall(
              org.apache.calcite.sql.fun.SqlLibraryOperators.SAFE_CAST,
              List.of(SqlLiteral.createCharString(" ".repeat(label.length()), POS), varcharSpec),
              POS);
      mainProj.add(asAliased(pad, labelField));
    }
    SqlSelect mainProjected =
        new SqlSelect(POS, null, mainProj, from, null, null, null, null, null, null, null, null);

    // Summary row: SELECT SUM(c) AS c (or NULL/label) FROM (mainProjected).
    SqlNodeList summaryProj = new SqlNodeList(POS);
    for (String c : frame.currentFields) {
      boolean shouldAgg = !explicitFields || aggFieldNames.contains(c);
      if (shouldAgg) {
        SqlNode sum =
            new SqlBasicCall(SqlStdOperatorTable.SUM, List.of(new SqlIdentifier(c, POS)), POS);
        summaryProj.add(asAliased(sum, c));
      } else if (labelField != null && c.equals(labelField)) {
        summaryProj.add(asAliased(SqlLiteral.createCharString(label, POS), c));
      } else {
        summaryProj.add(asAliased(SqlLiteral.createNull(POS), c));
      }
    }
    if (appendLabelField) {
      org.apache.calcite.sql.SqlDataTypeSpec varcharSpec =
          new org.apache.calcite.sql.SqlDataTypeSpec(
              new org.apache.calcite.sql.SqlBasicTypeNameSpec(
                  org.apache.calcite.sql.type.SqlTypeName.VARCHAR, label.length(), POS),
              POS);
      SqlNode lab =
          new SqlBasicCall(
              org.apache.calcite.sql.fun.SqlLibraryOperators.SAFE_CAST,
              List.of(SqlLiteral.createCharString(label, POS), varcharSpec),
              POS);
      summaryProj.add(asAliased(lab, labelField));
    }
    SqlNode aggFrom =
        new SqlBasicCall(
            SqlStdOperatorTable.AS,
            List.of(mainProjected, new SqlIdentifier("_addct_main_", POS)),
            POS);
    SqlSelect summarySelect =
        new SqlSelect(
            POS, null, summaryProj, aggFrom, null, null, null, null, null, null, null, null);
    SqlNode union =
        new SqlBasicCall(SqlStdOperatorTable.UNION_ALL, List.of(mainProjected, summarySelect), POS);
    SqlNodeList wrapItems = new SqlNodeList(POS);
    wrapItems.add(SqlIdentifier.star(POS));
    SqlNode wrapped =
        new SqlSelect(POS, null, wrapItems, union, null, null, null, null, null, null, null, null);
    List<String> visible = new ArrayList<>(frame.currentFields);
    if (appendLabelField) visible.add(labelField);
    frame.currentFields = visible;
    frame.joinHints = null;
    frame.lastOrderBy = null;
    return wrapped;
  }

  @Override
  public SqlNode visitAddTotals(AddTotals node, Frame frame) {
    SqlNode from = node.getChild().get(0).accept(this, frame);
    if (frame.currentFields == null) {
      throw new UnsupportedOperationException(
          "addtotals requires a known column list — call after a `| fields ...` pipe");
    }
    java.util.Map<String, Literal> options = node.getOptions();
    boolean addRow = options == null || getBoolOption(options, "row", true);
    boolean addCol = options != null && getBoolOption(options, "col", false);
    if (!addRow && !addCol) {
      // Both flags off: pass through.
      return from;
    }
    String alias = "Total";
    if (options != null && options.containsKey("fieldname")) {
      alias = options.get("fieldname").getValue().toString();
    }
    String labelField = null;
    if (options != null && options.containsKey("labelfield")) {
      labelField = options.get("labelfield").getValue().toString();
    }
    String label = "Total";
    if (options != null && options.containsKey("label")) {
      label = options.get("label").getValue().toString();
    }

    // Step 1: row=true → append per-row sum column.
    if (addRow) {
      List<Field> fields = node.getFieldList();
      List<String> sumNames;
      if (fields == null || fields.isEmpty()) {
        sumNames = new ArrayList<>(frame.currentFields);
      } else {
        sumNames = new ArrayList<>();
        for (Field f : fields) {
          sumNames.add(f.getField().toString());
        }
      }
      if (sumNames.isEmpty()) {
        throw new UnsupportedOperationException("addtotals row=true needs at least one field");
      }
      SqlNode sum = toIdentifier(sumNames.get(0));
      for (int i = 1; i < sumNames.size(); i++) {
        sum =
            new SqlBasicCall(
                SqlStdOperatorTable.PLUS, List.of(sum, toIdentifier(sumNames.get(i))), POS);
      }
      SqlNodeList items = new SqlNodeList(POS);
      List<String> visible = new ArrayList<>();
      for (String c : frame.currentFields) {
        if (c.equals(alias)) continue;
        items.add(toIdentifier(c));
        visible.add(c);
      }
      items.add(asAliased(sum, alias));
      visible.add(alias);
      from = SqlBuilder.select(items).from(from).withFields(visible).wrap(frame);
    }

    // Step 2: col=true → UNION ALL the main pipeline with a summary row containing SUM(f) for
    // each listed (or all) numeric column. Same shape as visitAddColTotals.
    if (addCol) {
      java.util.Set<String> aggFieldNames = new java.util.LinkedHashSet<>();
      if (node.getFieldList() != null) {
        for (Field f : node.getFieldList()) {
          aggFieldNames.add(f.getField().toString());
        }
      }
      boolean explicitFields = !aggFieldNames.isEmpty();
      boolean appendLabelField = labelField != null && !frame.currentFields.contains(labelField);

      SqlNodeList mainProj = new SqlNodeList(POS);
      for (String c : frame.currentFields) {
        mainProj.add(toIdentifier(c));
      }
      if (appendLabelField) {
        org.apache.calcite.sql.SqlDataTypeSpec varcharSpec =
            new org.apache.calcite.sql.SqlDataTypeSpec(
                new org.apache.calcite.sql.SqlBasicTypeNameSpec(
                    org.apache.calcite.sql.type.SqlTypeName.VARCHAR, label.length(), POS),
                POS);
        SqlNode pad =
            new SqlBasicCall(
                org.apache.calcite.sql.fun.SqlLibraryOperators.SAFE_CAST,
                List.of(SqlLiteral.createCharString(" ".repeat(label.length()), POS), varcharSpec),
                POS);
        mainProj.add(asAliased(pad, labelField));
      }
      SqlSelect mainProjected =
          new SqlSelect(POS, null, mainProj, from, null, null, null, null, null, null, null, null);

      SqlNodeList summaryProj = new SqlNodeList(POS);
      for (String c : frame.currentFields) {
        boolean shouldAgg = !explicitFields || aggFieldNames.contains(c);
        if (shouldAgg) {
          SqlNode sum =
              new SqlBasicCall(SqlStdOperatorTable.SUM, List.of(new SqlIdentifier(c, POS)), POS);
          summaryProj.add(asAliased(sum, c));
        } else if (labelField != null && c.equals(labelField)) {
          summaryProj.add(asAliased(SqlLiteral.createCharString(label, POS), c));
        } else {
          summaryProj.add(asAliased(SqlLiteral.createNull(POS), c));
        }
      }
      if (appendLabelField) {
        org.apache.calcite.sql.SqlDataTypeSpec varcharSpec =
            new org.apache.calcite.sql.SqlDataTypeSpec(
                new org.apache.calcite.sql.SqlBasicTypeNameSpec(
                    org.apache.calcite.sql.type.SqlTypeName.VARCHAR, label.length(), POS),
                POS);
        SqlNode lab =
            new SqlBasicCall(
                org.apache.calcite.sql.fun.SqlLibraryOperators.SAFE_CAST,
                List.of(SqlLiteral.createCharString(label, POS), varcharSpec),
                POS);
        summaryProj.add(asAliased(lab, labelField));
      }
      SqlNode aggFrom =
          new SqlBasicCall(
              SqlStdOperatorTable.AS,
              List.of(mainProjected, new SqlIdentifier("_addt_main_", POS)),
              POS);
      SqlSelect summarySelect =
          new SqlSelect(
              POS, null, summaryProj, aggFrom, null, null, null, null, null, null, null, null);
      SqlNode union =
          new SqlBasicCall(
              SqlStdOperatorTable.UNION_ALL, List.of(mainProjected, summarySelect), POS);
      SqlNodeList wrapItems = new SqlNodeList(POS);
      wrapItems.add(SqlIdentifier.star(POS));
      from =
          new SqlSelect(
              POS, null, wrapItems, union, null, null, null, null, null, null, null, null);
      List<String> visible2 = new ArrayList<>(frame.currentFields);
      if (appendLabelField) visible2.add(labelField);
      frame.currentFields = visible2;
      frame.joinHints = null;
      frame.lastOrderBy = null;
    }
    return from;
  }

  /**
   * Parse a time-span literal like {@code "1d"}, {@code "4h"}, {@code "45minute"} into a {@link
   * SqlBasicCall} on the SPAN UDF. Returns null when the literal isn't a recognised time-span. Unit
   * aliases match Rounding.DateTimeUnit (the OpenSearch pushdown's runtime resolver):
   *
   * <ul>
   *   <li>ms / millisecond / milliseconds
   *   <li>s / sec / second / seconds
   *   <li>m / min / minute / minutes (lowercase \"m\" → minute; uppercase \"M\" → month)
   *   <li>h / hr / hour / hours
   *   <li>d / day / days
   *   <li>w / week / weeks
   *   <li>mon / month / months / "M"
   *   <li>q / quarter / quarters
   *   <li>y / yr / year / years
   * </ul>
   */
  private SqlNode tryTimeSpanCall(
      SqlNode fieldRef, String spanStr, UnresolvedExpression aligntimeExpr) {
    spanStr = spanStr.replace("'", "").replace("\"", "").trim();
    int splitAt = -1;
    for (int i = 0; i < spanStr.length(); i++) {
      if (!Character.isDigit(spanStr.charAt(i))) {
        splitAt = i;
        break;
      }
    }
    if (splitAt <= 0) return null;
    int value;
    try {
      value = Integer.parseInt(spanStr.substring(0, splitAt));
    } catch (NumberFormatException ignored) {
      return null;
    }
    String rawUnitOriginal = spanStr.substring(splitAt);
    String rawUnit = rawUnitOriginal.toLowerCase(java.util.Locale.ROOT);
    String unit;
    if ("M".equals(rawUnitOriginal)) {
      unit = "M";
    } else if ("us".equals(rawUnitOriginal)
        || "cs".equals(rawUnitOriginal)
        || "ds".equals(rawUnitOriginal)) {
      // Subsecond units are case-sensitive — keep the original casing as the unit name.
      unit = rawUnitOriginal;
    } else {
      unit =
          switch (rawUnit) {
            case "ms", "millisecond", "milliseconds" -> "ms";
            case "s", "sec", "secs", "second", "seconds" -> "s";
            case "m", "min", "mins", "minute", "minutes" -> "m";
            case "h", "hr", "hrs", "hour", "hours" -> "h";
            case "d", "day", "days" -> "d";
            case "w", "week", "weeks" -> "w";
            case "mon", "month", "months" -> "M";
            case "q", "quarter", "quarters" -> "q";
            case "y", "yr", "year", "years" -> "y";
            default -> null;
          };
    }
    if (unit == null) return null;
    // Monthly span returns a "YYYY-MM" string (PPL's MonthSpanHandler shape) rather than the
    // SPAN UDF's timestamp output. Mirror v2's emission so the verifySchema's `string` type
    // assertion passes.
    if ("M".equals(unit)) {
      return buildMonthlySpan(fieldRef, value);
    }
    // Aligntime support (h/m/s units only — sub-second alignment isn't supported by v2 either).
    boolean isAlignableUnit =
        "ms".equals(unit)
            || "s".equals(unit)
            || "m".equals(unit)
            || "h".equals(unit)
            || "us".equals(unit)
            || "cs".equals(unit)
            || "ds".equals(unit);
    if (aligntimeExpr != null && isAlignableUnit) {
      Long alignmentOffsetSeconds = parseAlignTimeOffsetSeconds(aligntimeExpr);
      if (alignmentOffsetSeconds != null) {
        long intervalSeconds = unitToSeconds(unit, value);
        if (intervalSeconds > 0) {
          return buildAlignedTimeSpan(fieldRef, intervalSeconds, alignmentOffsetSeconds);
        }
      }
    }
    // Sub-second binning: SPAN UDF doesn't accept us/cs/ds; emit the v2 StandardTimeSpanHandler
    // shape using FROM_UNIXTIME(FLOOR(unix*scale/interval)*interval/scale).
    if ("us".equals(unit) || "cs".equals(unit) || "ds".equals(unit) || "ms".equals(unit)) {
      return buildSubsecondSpan(fieldRef, value, unit);
    }
    return new SqlBasicCall(
        org.opensearch.sql.expression.function.PPLBuiltinOperators.SPAN,
        List.of(
            fieldRef,
            SqlLiteral.createExactNumeric(Integer.toString(value), POS),
            SqlLiteral.createCharString(unit, POS)),
        POS);
  }

  private long unitToSeconds(String unit, int value) {
    return switch (unit) {
      case "s" -> (long) value;
      case "m" -> (long) value * 60L;
      case "h" -> (long) value * 3600L;
      default -> -1;
    };
  }

  private Long parseAlignTimeOffsetSeconds(UnresolvedExpression aligntimeExpr) {
    if (!(aligntimeExpr instanceof Literal lit) || lit.getValue() == null) return null;
    String s = lit.getValue().toString().replace("'", "").replace("\"", "").trim();
    try {
      return Long.parseLong(s);
    } catch (NumberFormatException ignored) {
      // fall through
    }
    if (s.startsWith("@d")) {
      String tail = s.substring(2);
      if (tail.isEmpty()) return 0L;
      java.util.regex.Matcher m =
          java.util.regex.Pattern.compile("^([+-])(\\d+)([smh])$").matcher(tail);
      if (!m.matches()) return null;
      long sign = "+".equals(m.group(1)) ? 1L : -1L;
      long n = Long.parseLong(m.group(2));
      String u = m.group(3);
      long perUnit = "s".equals(u) ? 1L : "m".equals(u) ? 60L : 3600L;
      return sign * n * perUnit;
    }
    return null;
  }

  private SqlNode buildAlignedTimeSpan(
      SqlNode fieldRef, long intervalSeconds, long alignmentOffsetSeconds) {
    SqlNode unixSeconds =
        new SqlBasicCall(
            org.opensearch.sql.expression.function.PPLBuiltinOperators.UNIX_TIMESTAMP,
            List.of(fieldRef),
            POS);
    SqlNode shifted =
        alignmentOffsetSeconds == 0
            ? unixSeconds
            : new SqlBasicCall(
                SqlStdOperatorTable.MINUS,
                List.of(
                    unixSeconds,
                    SqlLiteral.createExactNumeric(Long.toString(alignmentOffsetSeconds), POS)),
                POS);
    SqlLiteral intervalLit = SqlLiteral.createExactNumeric(Long.toString(intervalSeconds), POS);
    SqlNode divided =
        new SqlBasicCall(SqlStdOperatorTable.DIVIDE, List.of(shifted, intervalLit), POS);
    SqlNode floored = new SqlBasicCall(SqlStdOperatorTable.FLOOR, List.of(divided), POS);
    SqlNode multiplied =
        new SqlBasicCall(SqlStdOperatorTable.MULTIPLY, List.of(floored, intervalLit), POS);
    SqlNode binSeconds =
        alignmentOffsetSeconds == 0
            ? multiplied
            : new SqlBasicCall(
                SqlStdOperatorTable.PLUS,
                List.of(
                    multiplied,
                    SqlLiteral.createExactNumeric(Long.toString(alignmentOffsetSeconds), POS)),
                POS);
    return new SqlBasicCall(
        org.opensearch.sql.expression.function.PPLBuiltinOperators.FROM_UNIXTIME,
        List.of(binSeconds),
        POS);
  }

  private SqlNode buildSubsecondSpan(SqlNode fieldRef, int intervalValue, String unit) {
    long scale =
        switch (unit) {
          case "us" -> 1_000_000L;
          case "ms" -> 1_000L;
          case "cs" -> 100L;
          case "ds" -> 10L;
          default -> 1L;
        };
    SqlNode unixSeconds =
        new SqlBasicCall(
            org.opensearch.sql.expression.function.PPLBuiltinOperators.UNIX_TIMESTAMP,
            List.of(fieldRef),
            POS);
    SqlNode scaledUp =
        new SqlBasicCall(
            SqlStdOperatorTable.MULTIPLY,
            List.of(unixSeconds, SqlLiteral.createExactNumeric(Long.toString(scale), POS)),
            POS);
    SqlNode divided =
        new SqlBasicCall(
            SqlStdOperatorTable.DIVIDE,
            List.of(scaledUp, SqlLiteral.createExactNumeric(Integer.toString(intervalValue), POS)),
            POS);
    SqlNode floored = new SqlBasicCall(SqlStdOperatorTable.FLOOR, List.of(divided), POS);
    SqlNode multiplied =
        new SqlBasicCall(
            SqlStdOperatorTable.MULTIPLY,
            List.of(floored, SqlLiteral.createExactNumeric(Integer.toString(intervalValue), POS)),
            POS);
    SqlNode binSeconds =
        new SqlBasicCall(
            SqlStdOperatorTable.DIVIDE,
            List.of(multiplied, SqlLiteral.createExactNumeric(Long.toString(scale), POS)),
            POS);
    return new SqlBasicCall(
        org.opensearch.sql.expression.function.PPLBuiltinOperators.FROM_UNIXTIME,
        List.of(binSeconds),
        POS);
  }

  /**
   * Build "YYYY-MM" monthly bin label using the v2 emission shape: DATE_FORMAT(MAKEDATE(yearOfBin,
   * dayOfYear), '%Y-%m') over a months-since-epoch bin start.
   */
  private SqlNode buildMonthlySpan(SqlNode fieldRef, int intervalMonths) {
    SqlNode yearCall =
        new SqlBasicCall(
            org.opensearch.sql.expression.function.PPLBuiltinOperators.YEAR,
            List.of(fieldRef),
            POS);
    SqlNode monthCall =
        new SqlBasicCall(
            org.opensearch.sql.expression.function.PPLBuiltinOperators.MONTH,
            List.of(fieldRef),
            POS);
    SqlLiteral i1970 = SqlLiteral.createExactNumeric("1970", POS);
    SqlLiteral i12 = SqlLiteral.createExactNumeric("12", POS);
    SqlLiteral i1 = SqlLiteral.createExactNumeric("1", POS);
    SqlLiteral i31 = SqlLiteral.createExactNumeric("31", POS);
    SqlLiteral interval = SqlLiteral.createExactNumeric(Integer.toString(intervalMonths), POS);
    SqlNode yearsSinceEpoch =
        new SqlBasicCall(SqlStdOperatorTable.MINUS, List.of(yearCall, i1970), POS);
    SqlNode monthsFromYears =
        new SqlBasicCall(SqlStdOperatorTable.MULTIPLY, List.of(yearsSinceEpoch, i12), POS);
    SqlNode monthMinus1 = new SqlBasicCall(SqlStdOperatorTable.MINUS, List.of(monthCall, i1), POS);
    SqlNode monthsSinceEpoch =
        new SqlBasicCall(SqlStdOperatorTable.PLUS, List.of(monthsFromYears, monthMinus1), POS);
    SqlNode positionInCycle =
        new SqlBasicCall(SqlStdOperatorTable.MOD, List.of(monthsSinceEpoch, interval), POS);
    SqlNode binStartMonths =
        new SqlBasicCall(
            SqlStdOperatorTable.MINUS, List.of(monthsSinceEpoch, positionInCycle), POS);
    SqlNode binStartYear =
        new SqlBasicCall(
            SqlStdOperatorTable.PLUS,
            List.of(
                i1970,
                new SqlBasicCall(SqlStdOperatorTable.DIVIDE, List.of(binStartMonths, i12), POS)),
            POS);
    SqlNode binStartMonth =
        new SqlBasicCall(
            SqlStdOperatorTable.PLUS,
            List.of(
                new SqlBasicCall(SqlStdOperatorTable.MOD, List.of(binStartMonths, i12), POS), i1),
            POS);
    SqlNode dayOfYear =
        new SqlBasicCall(
            SqlStdOperatorTable.PLUS,
            List.of(
                new SqlBasicCall(
                    SqlStdOperatorTable.MULTIPLY,
                    List.of(
                        new SqlBasicCall(
                            SqlStdOperatorTable.MINUS, List.of(binStartMonth, i1), POS),
                        i31),
                    POS),
                i1),
            POS);
    SqlNode tempDate =
        new SqlBasicCall(
            org.opensearch.sql.expression.function.PPLBuiltinOperators.MAKEDATE,
            List.of(binStartYear, dayOfYear),
            POS);
    return new SqlBasicCall(
        org.opensearch.sql.expression.function.PPLBuiltinOperators.DATE_FORMAT,
        List.of(tempDate, SqlLiteral.createCharString("%Y-%m", POS)),
        POS);
  }

  /**
   * Parse a log-span literal like {@code "log10"}, {@code "1.5log10"}, {@code "log2"}, {@code
   * "loge"} into a CASE expression that emits {@code "<lower>-<upper>"} for a positive field value
   * (and {@code "Invalid"} for non-positive). Mirrors v2's tryLogSpanCall:
   *
   * <pre>
   *   coef = optional leading number (default 1.0)
   *   base = required digits after "log"
   *   bin  = floor(ln(field/coef) / ln(base))
   *   range = coef*base^bin .. coef*base^(bin+1)
   * </pre>
   *
   * Returns null when the literal isn't a recognised log-span pattern.
   */
  private SqlNode tryLogSpanCall(SqlNode fieldRef, String spanStr) {
    String lowered =
        spanStr.replace("'", "").replace("\"", "").trim().toLowerCase(java.util.Locale.ROOT);
    double base;
    double coefficient = 1.0;
    if ("log10".equals(lowered)) {
      base = 10.0;
    } else if ("log2".equals(lowered)) {
      base = 2.0;
    } else if ("loge".equals(lowered) || "ln".equals(lowered)) {
      base = Math.E;
    } else {
      java.util.regex.Matcher m =
          java.util.regex.Pattern.compile("^(\\d*\\.?\\d*)?log(\\d+\\.?\\d*)$").matcher(lowered);
      if (!m.matches()) return null;
      String coeffStr = m.group(1);
      String baseStr = m.group(2);
      coefficient = (coeffStr == null || coeffStr.isEmpty()) ? 1.0 : Double.parseDouble(coeffStr);
      base = Double.parseDouble(baseStr);
      if (base <= 1.0 || coefficient <= 0.0) return null;
    }
    SqlNode coefLit = SqlLiteral.createApproxNumeric(coefficient + "E0", POS);
    SqlNode baseLit = SqlLiteral.createApproxNumeric(base + "E0", POS);
    SqlNode lnBaseLit = SqlLiteral.createApproxNumeric(Math.log(base) + "E0", POS);
    SqlNode adjustedField =
        coefficient == 1.0
            ? fieldRef
            : new SqlBasicCall(SqlStdOperatorTable.DIVIDE, List.of(fieldRef, coefLit), POS);
    SqlNode lnField = new SqlBasicCall(SqlStdOperatorTable.LN, List.of(adjustedField), POS);
    SqlNode logValue =
        new SqlBasicCall(SqlStdOperatorTable.DIVIDE, List.of(lnField, lnBaseLit), POS);
    SqlNode binNumber = new SqlBasicCall(SqlStdOperatorTable.FLOOR, List.of(logValue), POS);
    SqlNode basePowerBin =
        new SqlBasicCall(SqlStdOperatorTable.POWER, List.of(baseLit, binNumber), POS);
    SqlNode lowerBound =
        new SqlBasicCall(SqlStdOperatorTable.MULTIPLY, List.of(coefLit, basePowerBin), POS);
    SqlNode binPlusOne =
        new SqlBasicCall(
            SqlStdOperatorTable.PLUS,
            List.of(binNumber, SqlLiteral.createApproxNumeric("1.0E0", POS)),
            POS);
    SqlNode basePowerBinPlusOne =
        new SqlBasicCall(SqlStdOperatorTable.POWER, List.of(baseLit, binPlusOne), POS);
    SqlNode upperBound =
        new SqlBasicCall(SqlStdOperatorTable.MULTIPLY, List.of(coefLit, basePowerBinPlusOne), POS);
    org.apache.calcite.sql.SqlDataTypeSpec varcharSpec =
        new org.apache.calcite.sql.SqlDataTypeSpec(
            new org.apache.calcite.sql.SqlBasicTypeNameSpec(
                org.apache.calcite.sql.type.SqlTypeName.VARCHAR, POS),
            POS);
    SqlNode lowerStr =
        new SqlBasicCall(
            org.apache.calcite.sql.fun.SqlLibraryOperators.SAFE_CAST,
            List.of(lowerBound, varcharSpec),
            POS);
    SqlNode upperStr =
        new SqlBasicCall(
            org.apache.calcite.sql.fun.SqlLibraryOperators.SAFE_CAST,
            List.of(upperBound, varcharSpec),
            POS);
    SqlNode firstConcat =
        new SqlBasicCall(
            SqlStdOperatorTable.CONCAT,
            List.of(lowerStr, SqlLiteral.createCharString("-", POS)),
            POS);
    SqlNode rangeStr =
        new SqlBasicCall(SqlStdOperatorTable.CONCAT, List.of(firstConcat, upperStr), POS);
    SqlNode positiveCheck =
        new SqlBasicCall(
            SqlStdOperatorTable.GREATER_THAN,
            List.of(fieldRef, SqlLiteral.createApproxNumeric("0.0E0", POS)),
            POS);
    SqlNodeList whens = new SqlNodeList(POS);
    whens.add(positiveCheck);
    SqlNodeList thens = new SqlNodeList(POS);
    thens.add(rangeStr);
    return new org.apache.calcite.sql.fun.SqlCase(
        POS, null, whens, thens, SqlLiteral.createCharString("Invalid", POS));
  }

  /** {@code MIN(field) OVER ()} — used as a default range bound for CountBin/RangeBin. */
  private SqlNode minOver(SqlNode field) {
    return aggOver(SqlStdOperatorTable.MIN, field);
  }

  private SqlNode maxOver(SqlNode field) {
    return aggOver(SqlStdOperatorTable.MAX, field);
  }

  private SqlNode aggOver(org.apache.calcite.sql.SqlOperator agg, SqlNode field) {
    SqlNode window =
        SqlWindow.create(
            null,
            null,
            new SqlNodeList(POS),
            new SqlNodeList(POS),
            SqlLiteral.createBoolean(false, POS),
            null,
            null,
            null,
            POS);
    return new SqlBasicCall(
        SqlStdOperatorTable.OVER, List.of(new SqlBasicCall(agg, List.of(field), POS), window), POS);
  }

  private static boolean getBoolOption(
      java.util.Map<String, Literal> options, String key, boolean defaultVal) {
    Literal v = options.get(key);
    if (v == null) return defaultVal;
    Object raw = v.getValue();
    if (raw instanceof Boolean b) return b;
    if (raw instanceof String s) {
      return "true".equalsIgnoreCase(s) || "t".equalsIgnoreCase(s) || "1".equals(s);
    }
    return defaultVal;
  }

  @Override
  public SqlNode visitChart(Chart node, Frame frame) {
    SqlNode from = node.getChild().get(0).accept(this, frame);
    if (node.getAggregationFunction() == null) {
      throw new UnsupportedOperationException("chart requires an aggregation function");
    }
    // 1D case: `chart agg by X` or `chart agg over X` — equivalent to GROUP BY X with NULL-key
    // filtering and an implicit ORDER BY on the split key (so a downstream `reverse` has a sort
    // to flip). The 2D `over X by Y` pivot case requires schema-aware re-aggregation and is
    // deferred — falls through to the unsupported branch.
    if (node.getRowSplit() != null && node.getColumnSplit() != null) {
      return visitChart2D(node, frame, from);
    }
    UnresolvedExpression splitKey =
        node.getRowSplit() != null ? node.getRowSplit() : node.getColumnSplit();
    if (splitKey == null) {
      throw new UnsupportedOperationException("chart requires a split key (`by` or `over`)");
    }
    // Strip the alias around the split key (if any) and translate the inner expression. The
    // `agg first, group key last` row layout matches v2's chart output.
    String gkAlias = null;
    UnresolvedExpression gkCore = splitKey;
    if (splitKey instanceof Alias a) {
      gkAlias = a.getName();
      gkCore = a.getDelegated();
    } else if (splitKey instanceof QualifiedName qn) {
      gkAlias = qn.toString();
    } else if (splitKey instanceof Field f && f.getField() instanceof QualifiedName qn) {
      gkAlias = qn.toString();
    }
    SqlNode keyExpr = expr(gkCore);

    // Build the agg call, capturing alias.
    UnresolvedExpression aggExpr = node.getAggregationFunction();
    String aggAlias = aggLabel(aggExpr);
    UnresolvedExpression aggCore = (aggExpr instanceof Alias a) ? a.getDelegated() : aggExpr;
    SqlNode aggCallNode = aggCall(aggCore);

    // PPL chart drops rows where the split key is NULL (mirroring v2's nonNullGroupMask).
    SqlNode nullFilter = new SqlBasicCall(SqlStdOperatorTable.IS_NOT_NULL, List.of(keyExpr), POS);
    SqlNodeList items = new SqlNodeList(POS);
    items.add(asAliased(keyExpr, gkAlias != null ? gkAlias : "_split"));
    items.add(asAliased(aggCallNode, aggAlias));
    List<String> visible = new ArrayList<>();
    visible.add(gkAlias != null ? gkAlias : "_split");
    visible.add(aggAlias);

    // Order by the split key NULLS_LAST (matches v2's chart output ordering).
    SqlNode orderRef = gkAlias != null ? new SqlIdentifier(gkAlias, POS) : keyExpr;
    SqlNode ordered = new SqlBasicCall(SqlStdOperatorTable.NULLS_LAST, List.of(orderRef), POS);

    frame.lastOrderBy = null;
    frame.joinHints = null;

    return SqlBuilder.select(items)
        .from(from)
        .where(nullFilter)
        .groupBy(List.of(keyExpr))
        .orderBy(List.of(ordered))
        .withFields(visible)
        .wrap(frame);
  }

  /**
   * 2D chart pivot: {@code chart agg over X by Y}. Pivots Y values as columns within X groups, with
   * TOP-N filtering on the per-Y aggregate. Emitted as a five-step nested pipeline (no row-type
   * oracle needed):
   *
   * <ol>
   *   <li>Inner GROUP BY (X, Y) producing the metric.
   *   <li>Per-Y total via window SUM partitioned by Y.
   *   <li>DENSE_RANK over (perYTotal DESC|ASC NULLS LAST, Y NULLS LAST) for top-N selection.
   *   <li>Optional rank-cap filter when useOther=false.
   *   <li>CASE-label non-top Y as OTHER (or filter), NULL as nullLabel; outer re-aggregate.
   * </ol>
   */
  private SqlNode visitChart2D(Chart node, Frame frame, SqlNode from) {
    org.opensearch.sql.ast.expression.Argument.ArgumentMap argMap =
        org.opensearch.sql.ast.expression.Argument.ArgumentMap.of(node.getArguments());
    int limit = argMap.get("limit") != null ? (Integer) argMap.get("limit").getValue() : 10;
    boolean useNull = argMap.get("usenull") == null || (Boolean) argMap.get("usenull").getValue();
    boolean useOther =
        argMap.get("useother") == null || (Boolean) argMap.get("useother").getValue();
    boolean top = argMap.get("top") == null || (Boolean) argMap.get("top").getValue();
    String otherLabel =
        argMap.get("otherstr") != null ? argMap.get("otherstr").getValue().toString() : "OTHER";
    String nullLabel =
        argMap.get("nullstr") != null ? argMap.get("nullstr").getValue().toString() : "NULL";

    UnresolvedExpression rowKey = node.getRowSplit();
    UnresolvedExpression colKey = node.getColumnSplit();
    UnresolvedExpression aggExpr = node.getAggregationFunction();
    String aggAlias = (aggExpr instanceof Alias al) ? al.getName() : "agg";
    UnresolvedExpression aggCore = aggExpr instanceof Alias al ? al.getDelegated() : aggExpr;
    String overName = chartKeyName(rowKey, "_over_");
    String byName = chartKeyName(colKey, "_by_");

    // Step 1: inner GROUP BY (X, Y) — emit (overName, byName, aggAlias). Cast Y to VARCHAR so
    // OTHER/NULL substitutes can sit in the same column. Drop NULL aggregate-input rows when the
    // aggregate has a specific field to null-check (matches v2 includeAggFieldsInNullFilter=true).
    SqlNode innerRowExpr = expr(stripChartAlias(rowKey));
    org.apache.calcite.sql.SqlDataTypeSpec varcharSpec =
        new org.apache.calcite.sql.SqlDataTypeSpec(
            new org.apache.calcite.sql.SqlBasicTypeNameSpec(
                org.apache.calcite.sql.type.SqlTypeName.VARCHAR, POS),
            POS);
    SqlNode innerColExpr =
        new SqlBasicCall(
            org.apache.calcite.sql.fun.SqlLibraryOperators.SAFE_CAST,
            List.of(expr(stripChartAlias(colKey)), varcharSpec),
            POS);
    SqlNodeList innerItems = new SqlNodeList(POS);
    innerItems.add(asAliased(innerRowExpr, overName));
    innerItems.add(asAliased(innerColExpr, byName));
    innerItems.add(asAliased(aggCall(aggCore), aggAlias));

    SqlNode innerWhere = null;
    if (aggCore instanceof AggregateFunction af
        && af.getField() != null
        && !(af.getField() instanceof Literal)
        && !(af.getField() instanceof AllFields)) {
      innerWhere =
          new SqlBasicCall(SqlStdOperatorTable.IS_NOT_NULL, List.of(expr(af.getField())), POS);
    }
    Frame innerFrame = new Frame();
    SqlBuilder.SelectBuilder innerB =
        SqlBuilder.select(innerItems)
            .from(from)
            .groupBy(List.of(expr(stripChartAlias(rowKey)), expr(stripChartAlias(colKey))));
    if (innerWhere != null) {
      innerB.where(innerWhere);
    }
    innerB.withFields(List.of(overName, byName, aggAlias));
    SqlNode step1 = innerB.wrap(innerFrame);

    // Step 2: extend with per-Y total. Drop X NULLs (always); drop Y NULLs only when !useNull.
    SqlNode yTotalWindow =
        SqlWindow.create(
            null,
            null,
            new SqlNodeList(List.of(new SqlIdentifier(byName, POS)), POS),
            new SqlNodeList(POS),
            SqlLiteral.createBoolean(false, POS),
            null,
            null,
            null,
            POS);
    SqlNode yTotalOver =
        new SqlBasicCall(
            SqlStdOperatorTable.OVER,
            List.of(
                new SqlBasicCall(
                    SqlStdOperatorTable.SUM, List.of(new SqlIdentifier(aggAlias, POS)), POS),
                yTotalWindow),
            POS);
    SqlNodeList step2Items = new SqlNodeList(POS);
    step2Items.add(new SqlIdentifier(overName, POS));
    step2Items.add(new SqlIdentifier(byName, POS));
    step2Items.add(new SqlIdentifier(aggAlias, POS));
    step2Items.add(asAliased(yTotalOver, "__chart_y_total__"));
    SqlNode step2Where =
        new SqlBasicCall(
            SqlStdOperatorTable.IS_NOT_NULL, List.of(new SqlIdentifier(overName, POS)), POS);
    if (!useNull) {
      step2Where =
          new SqlBasicCall(
              SqlStdOperatorTable.AND,
              List.of(
                  step2Where,
                  new SqlBasicCall(
                      SqlStdOperatorTable.IS_NOT_NULL,
                      List.of(new SqlIdentifier(byName, POS)),
                      POS)),
              POS);
    }
    Frame f2 = new Frame();
    SqlNode step2 =
        SqlBuilder.select(step2Items)
            .from(step1)
            .where(step2Where)
            .withFields(List.of(overName, byName, aggAlias, "__chart_y_total__"))
            .wrap(f2);

    // Step 3: DENSE_RANK over (yTotal DESC|ASC NULLS LAST, byName NULLS LAST). Secondary key
    // breaks ties so each distinct Y receives a unique rank — required for correct top-N.
    SqlNode yTotalRef = new SqlIdentifier("__chart_y_total__", POS);
    SqlNode rankPrimary =
        top ? new SqlBasicCall(SqlStdOperatorTable.DESC, List.of(yTotalRef), POS) : yTotalRef;
    rankPrimary = new SqlBasicCall(SqlStdOperatorTable.NULLS_LAST, List.of(rankPrimary), POS);
    SqlNode rankSecondary =
        new SqlBasicCall(
            SqlStdOperatorTable.NULLS_LAST, List.of(new SqlIdentifier(byName, POS)), POS);
    SqlNodeList rankOrder = new SqlNodeList(List.of(rankPrimary, rankSecondary), POS);
    SqlNode rankWindow =
        SqlWindow.create(
            null,
            null,
            new SqlNodeList(POS),
            rankOrder,
            SqlLiteral.createBoolean(false, POS),
            null,
            null,
            null,
            POS);
    SqlNode rankOver =
        new SqlBasicCall(
            SqlStdOperatorTable.OVER,
            List.of(
                new SqlBasicCall(
                    SqlStdOperatorTable.DENSE_RANK, java.util.Collections.emptyList(), POS),
                rankWindow),
            POS);
    SqlNodeList step3Items = new SqlNodeList(POS);
    step3Items.add(new SqlIdentifier(overName, POS));
    step3Items.add(new SqlIdentifier(byName, POS));
    step3Items.add(new SqlIdentifier(aggAlias, POS));
    step3Items.add(asAliased(rankOver, "__chart_rn__"));
    Frame f3 = new Frame();
    SqlNode step3 =
        SqlBuilder.select(step3Items)
            .from(step2)
            .withFields(List.of(overName, byName, aggAlias, "__chart_rn__"))
            .wrap(f3);

    // Step 4 (optional): drop non-top rows when useOther=false.
    SqlNode rankFiltered = step3;
    if (!useOther && limit > 0) {
      Frame f4 = new Frame();
      SqlNode rnLE =
          new SqlBasicCall(
              SqlStdOperatorTable.LESS_THAN_OR_EQUAL,
              List.of(
                  new SqlIdentifier("__chart_rn__", POS),
                  SqlLiteral.createExactNumeric(Integer.toString(limit), POS)),
              POS);
      SqlNodeList passThrough = new SqlNodeList(POS);
      passThrough.add(new SqlIdentifier(overName, POS));
      passThrough.add(new SqlIdentifier(byName, POS));
      passThrough.add(new SqlIdentifier(aggAlias, POS));
      passThrough.add(new SqlIdentifier("__chart_rn__", POS));
      rankFiltered =
          SqlBuilder.select(passThrough)
              .from(step3)
              .where(rnLE)
              .withFields(List.of(overName, byName, aggAlias, "__chart_rn__"))
              .wrap(f4);
    }

    // Step 5a: relabel — WHEN Y IS NULL THEN <nullLabel|NULL>; WHEN rn<=limit (useOther=true) THEN
    // Y; ELSE <otherLabel|Y>. Materialize the labeled column before the outer GROUP BY.
    SqlNode yRef = new SqlIdentifier(byName, POS);
    SqlNode rnRef = new SqlIdentifier("__chart_rn__", POS);
    SqlNodeList caseWhens = new SqlNodeList(POS);
    SqlNodeList caseThens = new SqlNodeList(POS);
    caseWhens.add(new SqlBasicCall(SqlStdOperatorTable.IS_NULL, List.of(yRef), POS));
    caseThens.add(
        useNull ? SqlLiteral.createCharString(nullLabel, POS) : SqlLiteral.createNull(POS));
    if (useOther && limit > 0) {
      caseWhens.add(
          new SqlBasicCall(
              SqlStdOperatorTable.LESS_THAN_OR_EQUAL,
              List.of(rnRef, SqlLiteral.createExactNumeric(Integer.toString(limit), POS)),
              POS));
      caseThens.add(yRef);
    }
    SqlNode caseElse = useOther && limit > 0 ? SqlLiteral.createCharString(otherLabel, POS) : yRef;
    SqlNode labeledY =
        new org.apache.calcite.sql.fun.SqlCase(POS, null, caseWhens, caseThens, caseElse);
    SqlNodeList step5aItems = new SqlNodeList(POS);
    step5aItems.add(new SqlIdentifier(overName, POS));
    step5aItems.add(asAliased(labeledY, byName));
    step5aItems.add(new SqlIdentifier(aggAlias, POS));
    Frame f5a = new Frame();
    SqlNode step5a =
        SqlBuilder.select(step5aItems)
            .from(rankFiltered)
            .withFields(List.of(overName, byName, aggAlias))
            .wrap(f5a);

    // Step 5b: outer re-aggregate by (overName, byName). MIN/EARLIEST→MIN, MAX/LATEST→MAX,
    // AVG→AVG, others→SUM (matches v2 buildAggCall mapping).
    String aggFnName = "sum";
    if (aggCore instanceof AggregateFunction af) {
      aggFnName = af.getFuncName().toLowerCase(java.util.Locale.ROOT);
    } else if (aggCore instanceof org.opensearch.sql.ast.expression.Function fn0) {
      aggFnName = fn0.getFuncName().toLowerCase(java.util.Locale.ROOT);
    }
    org.apache.calcite.sql.SqlOperator outerAgg =
        switch (aggFnName) {
          case "min", "earliest" -> SqlStdOperatorTable.MIN;
          case "max", "latest" -> SqlStdOperatorTable.MAX;
          case "avg" -> SqlStdOperatorTable.AVG;
          default -> SqlStdOperatorTable.SUM;
        };
    SqlNodeList outerItems = new SqlNodeList(POS);
    outerItems.add(new SqlIdentifier(overName, POS));
    outerItems.add(new SqlIdentifier(byName, POS));
    outerItems.add(
        asAliased(
            new SqlBasicCall(outerAgg, List.of(new SqlIdentifier(aggAlias, POS)), POS), aggAlias));
    SqlNode orderRow =
        new SqlBasicCall(
            SqlStdOperatorTable.NULLS_LAST, List.of(new SqlIdentifier(overName, POS)), POS);
    SqlNode orderCol =
        new SqlBasicCall(
            SqlStdOperatorTable.NULLS_LAST, List.of(new SqlIdentifier(byName, POS)), POS);
    return SqlBuilder.select(outerItems)
        .from(step5a)
        .groupBy(List.of(new SqlIdentifier(overName, POS), new SqlIdentifier(byName, POS)))
        .orderBy(List.of(orderRow, orderCol))
        .withFields(List.of(overName, byName, aggAlias))
        .wrap(frame);
  }

  private static UnresolvedExpression stripChartAlias(UnresolvedExpression e) {
    return e instanceof Alias a ? a.getDelegated() : e;
  }

  private static String chartKeyName(UnresolvedExpression e, String fallback) {
    if (e instanceof Alias a) return a.getName();
    if (e instanceof Field f && f.getField() instanceof QualifiedName qn) return qn.toString();
    if (e instanceof QualifiedName qn) return qn.toString();
    return fallback;
  }

  @Override
  public SqlNode visitSpath(SPath node, Frame frame) {
    // SPath is a JSON-extract expressed as eval. The AST node carries the rewriteAsEval()
    // factory; delegate to visitEval against the rewritten Eval node — no custom translation
    // needed.
    return visitEval(node.rewriteAsEval(), frame);
  }

  @Override
  public SqlNode visitConvert(Convert node, Frame frame) {
    SqlNode from = node.getChild().get(0).accept(this, frame);
    // PPL `convert [timeformat="<fmt>"] fn(field) [AS alias] [, ...]`. When alias == source,
    // replace the source column in-place; with an alias, append a new column.
    // ctime/mktime accept a custom timeformat as a second arg when supplied.
    List<Let> conversions = node.getConversions();
    if (conversions == null || conversions.isEmpty()) {
      return from;
    }
    java.util.Map<String, SqlNode> replacements = new java.util.LinkedHashMap<>();
    java.util.LinkedHashMap<String, SqlNode> additions = new java.util.LinkedHashMap<>();
    String timeFormat = node.getTimeFormat();
    for (Let conv : conversions) {
      String target = conv.getVar().getField().toString();
      UnresolvedExpression rhs = conv.getExpression();
      if (rhs instanceof Field srcField) {
        // `convert none(field) AS alias` — copy/rename only, no conversion.
        String source = srcField.getField().toString();
        if (!target.equals(source)) {
          additions.put(target, expr(srcField));
        }
        continue;
      }
      if (!(rhs instanceof org.opensearch.sql.ast.expression.Function fn)) {
        throw new IllegalArgumentException("Convert command requires function call expressions");
      }
      if (fn.getFuncArgs().size() != 1 || !(fn.getFuncArgs().get(0) instanceof Field srcField)) {
        throw new IllegalArgumentException("Convert function must have exactly one field argument");
      }
      String source = srcField.getField().toString();
      SqlNode call;
      String fnName = fn.getFuncName().toLowerCase(java.util.Locale.ROOT);
      if (timeFormat != null && (fnName.equals("ctime") || fnName.equals("mktime"))) {
        call =
            new SqlBasicCall(
                new org.apache.calcite.sql.SqlUnresolvedFunction(
                    new SqlIdentifier(fn.getFuncName(), POS),
                    null,
                    null,
                    null,
                    null,
                    org.apache.calcite.sql.SqlFunctionCategory.USER_DEFINED_FUNCTION),
                List.of(expr(srcField), SqlLiteral.createCharString(timeFormat, POS)),
                POS);
      } else {
        call = expr(fn);
      }
      if (target.equals(source)) {
        replacements.put(source, call);
      } else {
        additions.put(target, call);
      }
    }
    // Build the final projection list. When frame.currentFields is null (bare relation), fall
    // back to `*` plus the appended columns; explicit replacements need the full column list.
    List<String> visible = frame.currentFields == null ? List.of() : frame.currentFields;
    SqlNodeList items = new SqlNodeList(POS);
    List<String> newVisible = new ArrayList<>();
    if (!visible.isEmpty()) {
      for (String c : visible) {
        if (replacements.containsKey(c)) {
          items.add(asAliased(replacements.get(c), c));
        } else {
          items.add(toIdentifier(c));
        }
        newVisible.add(c);
      }
    } else {
      // No oracle: emit `*` and trust replacements via additional aliased projections. Will not
      // override an existing column, but works for the common case of new aliases.
      items.add(SqlIdentifier.star(POS));
    }
    for (java.util.Map.Entry<String, SqlNode> e : additions.entrySet()) {
      items.add(asAliased(e.getValue(), e.getKey()));
      newVisible.add(e.getKey());
    }
    return SqlBuilder.select(items)
        .from(from)
        .withFields(newVisible.isEmpty() ? null : newVisible)
        .wrap(frame);
  }

  @Override
  public SqlNode visitRegex(Regex node, Frame frame) {
    SqlNode from = node.getChild().get(0).accept(this, frame);
    // PPL `regex field = "pattern"` / `regex field != "pattern"` filters rows where the field
    // matches (or doesn't match) the regex. Pattern cast to VARCHAR explicitly to match v2's
    // REGEXP_CONTAINS($0, 'pat':VARCHAR) emission shape.
    SqlNode field = expr(node.getField());
    SqlNode pattern = castStringToVarchar(node.getPattern().getValue().toString());
    SqlNode call =
        new SqlBasicCall(
            org.apache.calcite.sql.fun.SqlLibraryOperators.REGEXP_CONTAINS,
            List.of(field, pattern),
            POS);
    if (node.isNegated()) {
      call = new SqlBasicCall(SqlStdOperatorTable.NOT, List.of(call), POS);
    }
    return SqlBuilder.select(starList()).from(from).where(call).wrap(frame);
  }

  @Override
  public SqlNode visitReplace(Replace node, Frame frame) {
    SqlNode from = node.getChild().get(0).accept(this, frame);
    // PPL `replace "p1" WITH "r1" [, "p2" WITH "r2", ...] IN field1, field2, ...` rewrites each
    // listed field's value with chained REPLACE calls (one per pair). Other columns pass through.
    // Patterns containing `*` are wildcard patterns that desugar to REGEXP_REPLACE; literal
    // patterns use plain REPLACE. The visitor builds an explicit projection that retains all
    // visible columns, swapping target fields for their REPLACE-chain.
    java.util.Set<String> targets = new java.util.LinkedHashSet<>();
    for (Field f : node.getFieldList()) {
      targets.add(f.getField().toString());
    }
    List<String> visible = frame.currentFields == null ? List.of() : frame.currentFields;
    java.util.Set<String> visibleSet = new java.util.LinkedHashSet<>(visible);
    for (String t : targets) {
      if (!visibleSet.contains(t)) {
        throw new IllegalArgumentException(
            "field [" + t + "] not found; input fields are: " + visible);
      }
    }
    SqlNodeList items = new SqlNodeList(POS);
    List<String> newVisible = new ArrayList<>(visible.size());
    for (String c : visible) {
      SqlNode fieldRef = toIdentifier(c);
      if (targets.contains(c)) {
        items.add(asAliased(buildReplaceChain(fieldRef, node), c));
      } else {
        items.add(fieldRef);
      }
      newVisible.add(c);
    }
    return SqlBuilder.select(items).from(from).withFields(newVisible).wrap(frame);
  }

  /**
   * Apply each {@link ReplacePair} of a {@link Replace} command to {@code value}, returning the
   * chained call. Wildcard patterns (containing {@code *}) desugar to REGEXP_REPLACE_3 with
   * symmetry validation; literal patterns use plain REPLACE. Operands are cast to VARCHAR to avoid
   * Calcite widening unequal-length CHAR literals (which would right-pad shorter values).
   */
  private SqlNode buildReplaceChain(SqlNode value, Replace node) {
    for (ReplacePair pair : node.getReplacePairs()) {
      String patternStr = pair.getPattern().getValue().toString();
      String replacementStr = pair.getReplacement().getValue().toString();
      if (patternStr.contains("*")) {
        org.opensearch.sql.calcite.utils.WildcardUtils.validateWildcardSymmetry(
            patternStr, replacementStr);
        String regexPattern =
            org.opensearch.sql.calcite.utils.WildcardUtils.convertWildcardPatternToRegex(
                patternStr);
        String regexReplacement =
            org.opensearch.sql.calcite.utils.WildcardUtils.convertWildcardReplacementToRegex(
                replacementStr);
        value =
            new SqlBasicCall(
                org.apache.calcite.sql.fun.SqlLibraryOperators.REGEXP_REPLACE_3,
                List.of(
                    value,
                    castStringToVarchar(regexPattern),
                    castStringToVarchar(regexReplacement)),
                POS);
      } else {
        value =
            new SqlBasicCall(
                SqlStdOperatorTable.REPLACE,
                List.of(
                    value, castStringToVarchar(patternStr), castStringToVarchar(replacementStr)),
                POS);
      }
    }
    return value;
  }

  /** Cast a string literal to VARCHAR. */
  private static SqlNode castStringToVarchar(String s) {
    org.apache.calcite.sql.SqlDataTypeSpec spec =
        new org.apache.calcite.sql.SqlDataTypeSpec(
            new org.apache.calcite.sql.SqlBasicTypeNameSpec(
                org.apache.calcite.sql.type.SqlTypeName.VARCHAR, POS),
            POS);
    return new SqlBasicCall(
        org.apache.calcite.sql.fun.SqlLibraryOperators.SAFE_CAST,
        List.of(SqlLiteral.createCharString(s, POS), spec),
        POS);
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
      return visitDedupeConsecutive(node, frame, from, allowedDup, keepEmpty);
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
    // Outer SELECT projects only the user-visible columns so the helper `__rn_dedup__` column
    // doesn't leak into the row type (PPL's implicit final `| fields *` should not surface it).
    // Falls back to SELECT * when we don't know the visible fields (no row-type oracle).
    SqlNodeList outerSelects = new SqlNodeList(POS);
    List<String> visible = frame.currentFields;
    if (visible != null && !visible.isEmpty()) {
      for (String c : visible) {
        outerSelects.add(toIdentifier(c));
      }
    } else {
      outerSelects.add(SqlIdentifier.star(POS));
    }
    return new SqlSelect(
        POS, null, outerSelects, innerSelect, whereCond, null, null, null, null, null, null, null);
  }

  /**
   * `dedup consecutive=true` keeps a row if it differs from the previous row in any of the dedup
   * fields. For {@code allowedDup} N, keeps up to N consecutive rows of the same key. With
   * keepEmpty=false, NULL fields are dropped.
   *
   * <p>Strategy (3 wraps):
   *
   * <ol>
   *   <li>Add ROW_NUMBER() OVER () AS _consec_rn_ and LAG(field) OVER () AS _consec_lag_<i>_ for
   *       each dedup field.
   *   <li>Compute "is run start" = (_consec_rn_=1 OR any field IS DISTINCT FROM its lag), then
   *       cumulative SUM(run-start) → _consec_run_id_.
   *   <li>Per-run ROW_NUMBER() OVER (PARTITION BY _consec_run_id_ ORDER BY _consec_rn_) → keep rows
   *       with position ≤ allowedDup.
   * </ol>
   */
  private SqlNode visitDedupeConsecutive(
      Dedupe node, Frame frame, SqlNode from, int allowedDup, boolean keepEmpty) {
    List<SqlNode> fieldNodes = new ArrayList<>(node.getFields().size());
    for (Field f : node.getFields()) {
      fieldNodes.add(expr(f.getField()));
    }
    SqlNodeList emptyPart = new SqlNodeList(POS);
    SqlNodeList emptyOrder = new SqlNodeList(POS);

    // Step 1: SELECT *, ROW_NUMBER() OVER () AS _consec_rn_, LAG(F0) OVER () AS _consec_lag_0_, ...
    // FROM (from). When keepEmpty=false, prepend WHERE F_i IS NOT NULL conjuncts.
    SqlNodeList step1Items = new SqlNodeList(POS);
    step1Items.add(SqlIdentifier.star(POS));
    SqlNode rnOver =
        new SqlBasicCall(
            SqlStdOperatorTable.OVER,
            List.of(
                new SqlBasicCall(
                    SqlStdOperatorTable.ROW_NUMBER, java.util.Collections.emptyList(), POS),
                SqlWindow.create(
                    null,
                    null,
                    emptyPart,
                    emptyOrder,
                    SqlLiteral.createBoolean(false, POS),
                    null,
                    null,
                    null,
                    POS)),
            POS);
    step1Items.add(asAliased(rnOver, "_consec_rn_"));
    for (int i = 0; i < fieldNodes.size(); i++) {
      SqlNode lagOver =
          new SqlBasicCall(
              SqlStdOperatorTable.OVER,
              List.of(
                  new SqlBasicCall(SqlStdOperatorTable.LAG, List.of(fieldNodes.get(i)), POS),
                  SqlWindow.create(
                      null,
                      null,
                      emptyPart,
                      emptyOrder,
                      SqlLiteral.createBoolean(false, POS),
                      null,
                      null,
                      null,
                      POS)),
              POS);
      step1Items.add(asAliased(lagOver, "_consec_lag_" + i));
    }
    SqlNode step1Where = null;
    if (!keepEmpty) {
      for (SqlNode field : fieldNodes) {
        SqlNode notNull = new SqlBasicCall(SqlStdOperatorTable.IS_NOT_NULL, List.of(field), POS);
        step1Where =
            (step1Where == null)
                ? notNull
                : new SqlBasicCall(SqlStdOperatorTable.AND, List.of(step1Where, notNull), POS);
      }
    }
    SqlNode step1 =
        new SqlSelect(
            POS, null, step1Items, from, step1Where, null, null, null, null, null, null, null);

    // Step 2: SELECT *, SUM(runFlag) OVER (ORDER BY _consec_rn_) AS _consec_run_id_ FROM (step1).
    // runFlag = CASE WHEN _consec_rn_=1 OR any (F_i IS DISTINCT FROM _consec_lag_i_) THEN 1 ELSE 0
    SqlNode firstRow =
        new SqlBasicCall(
            SqlStdOperatorTable.EQUALS,
            List.of(new SqlIdentifier("_consec_rn_", POS), SqlLiteral.createExactNumeric("1", POS)),
            POS);
    SqlNode isRunStart = firstRow;
    for (int i = 0; i < fieldNodes.size(); i++) {
      SqlNode field = fieldNodes.get(i);
      SqlNode lag = new SqlIdentifier("_consec_lag_" + i, POS);
      SqlNode distinct =
          new SqlBasicCall(SqlStdOperatorTable.IS_DISTINCT_FROM, List.of(field, lag), POS);
      isRunStart = new SqlBasicCall(SqlStdOperatorTable.OR, List.of(isRunStart, distinct), POS);
    }
    SqlNodeList runFlagWhens = new SqlNodeList(POS);
    runFlagWhens.add(isRunStart);
    SqlNodeList runFlagThens = new SqlNodeList(POS);
    runFlagThens.add(SqlLiteral.createExactNumeric("1", POS));
    SqlNode runFlag =
        new org.apache.calcite.sql.fun.SqlCase(
            POS, null, runFlagWhens, runFlagThens, SqlLiteral.createExactNumeric("0", POS));
    SqlNode runIdOver =
        new SqlBasicCall(
            SqlStdOperatorTable.OVER,
            List.of(
                new SqlBasicCall(SqlStdOperatorTable.SUM, List.of(runFlag), POS),
                SqlWindow.create(
                    null,
                    null,
                    emptyPart,
                    new SqlNodeList(List.of(new SqlIdentifier("_consec_rn_", POS)), POS),
                    SqlLiteral.createBoolean(true, POS),
                    SqlWindow.createUnboundedPreceding(POS),
                    SqlWindow.createCurrentRow(POS),
                    null,
                    POS)),
            POS);
    SqlNodeList step2Items = new SqlNodeList(POS);
    step2Items.add(SqlIdentifier.star(POS));
    step2Items.add(asAliased(runIdOver, "_consec_run_id_"));
    SqlNode step2 =
        new SqlSelect(POS, null, step2Items, step1, null, null, null, null, null, null, null, null);

    // Step 3: SELECT *, ROW_NUMBER() OVER (PARTITION BY _consec_run_id_ ORDER BY _consec_rn_) AS
    // _consec_pos_ FROM (step2). Then outer wrap filters _consec_pos_ <= allowedDup and projects
    // user-visible columns only (drop helper cols).
    SqlNode posOver =
        new SqlBasicCall(
            SqlStdOperatorTable.OVER,
            List.of(
                new SqlBasicCall(
                    SqlStdOperatorTable.ROW_NUMBER, java.util.Collections.emptyList(), POS),
                SqlWindow.create(
                    null,
                    null,
                    new SqlNodeList(List.of(new SqlIdentifier("_consec_run_id_", POS)), POS),
                    new SqlNodeList(List.of(new SqlIdentifier("_consec_rn_", POS)), POS),
                    SqlLiteral.createBoolean(false, POS),
                    null,
                    null,
                    null,
                    POS)),
            POS);
    SqlNodeList step3Items = new SqlNodeList(POS);
    step3Items.add(SqlIdentifier.star(POS));
    step3Items.add(asAliased(posOver, "_consec_pos_"));
    SqlNode step3 =
        new SqlSelect(POS, null, step3Items, step2, null, null, null, null, null, null, null, null);

    // Outer: filter _consec_pos_ <= N, project user-visible columns only.
    SqlNode boundCheck =
        new SqlBasicCall(
            SqlStdOperatorTable.LESS_THAN_OR_EQUAL,
            List.of(
                new SqlIdentifier("_consec_pos_", POS),
                SqlLiteral.createExactNumeric(Integer.toString(allowedDup), POS)),
            POS);
    SqlNodeList outerSelects = new SqlNodeList(POS);
    List<String> visible = frame.currentFields;
    if (visible != null && !visible.isEmpty()) {
      for (String c : visible) {
        outerSelects.add(toIdentifier(c));
      }
    } else {
      outerSelects.add(SqlIdentifier.star(POS));
    }
    return new SqlSelect(
        POS, null, outerSelects, step3, boundCheck, null, null, null, null, null, null, null);
  }

  /**
   * PPL `transpose [N] [columnName]` pivots N data rows into N columns. The input row order becomes
   * the new "row 1..N" columns, and the input column names become the new pivot key column.
   * Two-stage emission:
   *
   * <ol>
   *   <li>UNION ALL of (column_name_lit AS column, col_value AS _value_, ROW_NUMBER() OVER () AS
   *       _rn_) for each visible input column.
   *   <li>GROUP BY column with MAX(_value_) FILTER (WHERE _rn_ = N) AS "row N" for N=1..maxRows.
   * </ol>
   *
   * Without a row-type oracle we use {@code frame.currentFields} as the column list. Mirrors v2
   * legacy SqlNode visitor's {@code visitTranspose}.
   */
  @Override
  public SqlNode visitTranspose(Transpose node, Frame frame) {
    SqlNode from = node.getChild().get(0).accept(this, frame);
    if (frame.currentFields == null || frame.currentFields.isEmpty()) {
      throw new UnsupportedOperationException(
          "transpose requires a known column list — call after a `| fields ...` pipe");
    }
    List<String> cols =
        frame.currentFields.stream()
            .filter(c -> !OpenSearchConstants.METADATAFIELD_TYPE_MAP.containsKey(c))
            .toList();
    int maxRows = node.getMaxRows();
    String columnAlias = node.getColumnName() != null ? node.getColumnName() : "column";

    // Step 1: UNION ALL of per-column branches.
    org.apache.calcite.sql.SqlDataTypeSpec varcharSpec =
        new org.apache.calcite.sql.SqlDataTypeSpec(
            new org.apache.calcite.sql.SqlBasicTypeNameSpec(
                org.apache.calcite.sql.type.SqlTypeName.VARCHAR, POS),
            POS);
    SqlNode unioned = null;
    for (String c : cols) {
      SqlNodeList sl = new SqlNodeList(POS);
      // CAST the literal to VARCHAR — UNION ALL of CHAR literals would CHAR-pad to the
      // longest branch's width, leaving trailing spaces in shorter column names.
      SqlNode colNameLit =
          new SqlBasicCall(
              org.apache.calcite.sql.fun.SqlLibraryOperators.SAFE_CAST,
              List.of(SqlLiteral.createCharString(c, POS), varcharSpec),
              POS);
      sl.add(asAliased(colNameLit, columnAlias));
      sl.add(
          asAliased(
              new SqlBasicCall(
                  org.apache.calcite.sql.fun.SqlLibraryOperators.SAFE_CAST,
                  List.of(toIdentifier(c), varcharSpec),
                  POS),
              "_value_"));
      SqlNode rnOver =
          new SqlBasicCall(
              SqlStdOperatorTable.OVER,
              List.of(
                  new SqlBasicCall(
                      SqlStdOperatorTable.ROW_NUMBER, java.util.Collections.emptyList(), POS),
                  SqlWindow.create(
                      null,
                      null,
                      new SqlNodeList(POS),
                      new SqlNodeList(POS),
                      SqlLiteral.createBoolean(false, POS),
                      null,
                      null,
                      null,
                      POS)),
              POS);
      sl.add(asAliased(rnOver, "_rn_"));
      SqlNode branch =
          new SqlSelect(POS, null, sl, from, null, null, null, null, null, null, null, null);
      unioned =
          (unioned == null)
              ? branch
              : new SqlBasicCall(SqlStdOperatorTable.UNION_ALL, List.of(unioned, branch), POS);
    }

    // Step 2: GROUP BY column, MAX(_value_) FILTER (WHERE _rn_=N) AS "row N".
    SqlNodeList outerItems = new SqlNodeList(POS);
    outerItems.add(new SqlIdentifier(columnAlias, POS));
    List<String> visible = new ArrayList<>();
    visible.add(columnAlias);
    for (int n = 1; n <= maxRows; n++) {
      SqlNode filterCond =
          new SqlBasicCall(
              SqlStdOperatorTable.EQUALS,
              List.of(
                  new SqlIdentifier("_rn_", POS),
                  SqlLiteral.createExactNumeric(Integer.toString(n), POS)),
              POS);
      SqlNode maxCall =
          new SqlBasicCall(
              SqlStdOperatorTable.MAX, List.of(new SqlIdentifier("_value_", POS)), POS);
      SqlNode filtered =
          new SqlBasicCall(SqlStdOperatorTable.FILTER, List.of(maxCall, filterCond), POS);
      String rowAlias = "row " + n;
      outerItems.add(asAliased(filtered, rowAlias));
      visible.add(rowAlias);
    }
    return SqlBuilder.select(outerItems)
        .from(unioned)
        .groupBy(List.of(new SqlIdentifier(columnAlias, POS)))
        .withFields(visible)
        .wrap(frame);
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
    return aggCall(e, false);
  }

  /**
   * Build a Calcite aggregate call. When {@code windowed} is true, the call will be used inside an
   * OVER clause: PPL's nullable AVG/VAR/STDDEV variants have no window-context enumerable
   * implementation, so fall back to the standard SQL operators (and pair with a CASE-WHEN-COUNT
   * guard at the call site for the empty-group → NULL semantics).
   */
  private SqlNode aggCall(UnresolvedExpression e, boolean windowed) {
    if (!(e instanceof AggregateFunction af)) {
      throw new UnsupportedOperationException(
          "stats aggregator must be an AggregateFunction, got: " + e.getClass().getSimpleName());
    }
    String fnLower = af.getFuncName().toLowerCase(java.util.Locale.ROOT);
    boolean distinct = Boolean.TRUE.equals(af.getDistinct());
    if (fnLower.equals("dc")
        || fnLower.equals("distinct_count")
        || fnLower.equals("distinct_count_approx")) {
      // distinct_count_approx is HLL-style in v2 PPL but the legacy SqlNode visitor maps it to
      // plain COUNT(DISTINCT) — match that behaviour.
      fnLower = "count";
      distinct = true;
    } else if (fnLower.equals("c")) {
      fnLower = "count";
    }
    // earliest/latest dispatch to Calcite's ARG_MIN/ARG_MAX with a (value, time-field) signature.
    // The time-field defaults to @timestamp when not supplied (matches v2's resolveTimeField).
    if (fnLower.equals("earliest") || fnLower.equals("latest")) {
      List<SqlNode> args = new ArrayList<>();
      UnresolvedExpression argExpr0 = af.getField();
      args.add(argExpr0 instanceof AllFields ? SqlIdentifier.star(POS) : expr(argExpr0));
      if (af.getArgList() != null) {
        for (UnresolvedExpression extra : af.getArgList()) {
          if (extra instanceof org.opensearch.sql.ast.expression.UnresolvedArgument ua) {
            args.add(expr(ua.getValue()));
          } else {
            args.add(expr(extra));
          }
        }
      }
      if (args.size() == 1) {
        args.add(new SqlIdentifier(OpenSearchConstants.IMPLICIT_FIELD_TIMESTAMP, POS));
      }
      org.apache.calcite.sql.SqlOperator op0 =
          fnLower.equals("earliest") ? SqlStdOperatorTable.ARG_MIN : SqlStdOperatorTable.ARG_MAX;
      return new SqlBasicCall(op0, args, POS);
    }
    org.apache.calcite.sql.SqlAggFunction op =
        switch (fnLower) {
          case "count" -> SqlStdOperatorTable.COUNT;
          case "sum" -> SqlStdOperatorTable.SUM;
          // PPL stats AVG over an empty/all-NULL group should return NULL — Calcite's standard
          // AVG return type is NOT NULL and trips a "Cannot convert null to double" runtime error
          // when the group is empty. PPLBuiltinOperators.AVG_NULLABLE is a nullable wrapper that
          // returns NULL in that case. In window context, the nullable variant has no enumerable
          // implementor, so we use standard AVG and pair with a CASE-WHEN-COUNT guard at the
          // visitWindow call site.
          case "avg" ->
              windowed
                  ? SqlStdOperatorTable.AVG
                  : org.opensearch.sql.expression.function.PPLBuiltinOperators.AVG_NULLABLE;
          case "min" -> SqlStdOperatorTable.MIN;
          case "max" -> SqlStdOperatorTable.MAX;
          // PPL `stddev` is sample-stddev (Bessel's correction), `stddev_pop` is population
          // form. Same nullable-vs-windowed dispatch as AVG.
          case "stddev", "stddev_samp" ->
              windowed
                  ? SqlStdOperatorTable.STDDEV_SAMP
                  : org.opensearch.sql.expression.function.PPLBuiltinOperators.STDDEV_SAMP_NULLABLE;
          case "stddev_pop" ->
              windowed
                  ? SqlStdOperatorTable.STDDEV_POP
                  : org.opensearch.sql.expression.function.PPLBuiltinOperators.STDDEV_POP_NULLABLE;
          case "variance", "var_samp" ->
              windowed
                  ? SqlStdOperatorTable.VAR_SAMP
                  : org.opensearch.sql.expression.function.PPLBuiltinOperators.VAR_SAMP_NULLABLE;
          case "var_pop" ->
              windowed
                  ? SqlStdOperatorTable.VAR_POP
                  : org.opensearch.sql.expression.function.PPLBuiltinOperators.VAR_POP_NULLABLE;
          // percentile/percentile_approx/median dispatch to the OpenSearch T-Digest UDAF.
          // median(x) is shorthand for percentile_approx(x, 50).
          case "percentile", "percentile_approx", "median" ->
              org.opensearch.sql.expression.function.PPLBuiltinOperators.PERCENTILE_APPROX;
          // Multi-value aggregates: list/values collect group rows into a string array;
          // first/last return the first/last value in the group's natural order; take returns
          // the first N values (the N is passed as a second positional arg).
          case "list" -> org.opensearch.sql.expression.function.PPLBuiltinOperators.LIST;
          case "values" -> org.opensearch.sql.expression.function.PPLBuiltinOperators.VALUES;
          case "first" -> org.opensearch.sql.expression.function.PPLBuiltinOperators.FIRST;
          case "last" -> org.opensearch.sql.expression.function.PPLBuiltinOperators.LAST;
          case "take" -> org.opensearch.sql.expression.function.PPLBuiltinOperators.TAKE;
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
    // percentile/percentile_approx (extra <percent> arg, with median defaulting to 50),
    // first/last/take (optional <N>) all carry positional args that PPL stores in
    // AggregateFunction.argList. Pass them through verbatim.
    boolean takesExtraArgs =
        op == org.opensearch.sql.expression.function.PPLBuiltinOperators.PERCENTILE_APPROX
            || op == org.opensearch.sql.expression.function.PPLBuiltinOperators.FIRST
            || op == org.opensearch.sql.expression.function.PPLBuiltinOperators.LAST
            || op == org.opensearch.sql.expression.function.PPLBuiltinOperators.TAKE
            // VALUES carries an optional UnresolvedArgument("limit", n) injected by the parser
            // when plugins.ppl.values.max.limit is configured. The runtime VALUES UDAF reads
            // the limit from values[1].
            || op == org.opensearch.sql.expression.function.PPLBuiltinOperators.VALUES;
    if (takesExtraArgs) {
      List<SqlNode> args = new ArrayList<>();
      args.add(arg);
      if (af.getArgList() != null) {
        for (UnresolvedExpression extra : af.getArgList()) {
          // PPL passes extra args as named UnresolvedArgument(name, value); unwrap the value.
          if (extra instanceof org.opensearch.sql.ast.expression.UnresolvedArgument ua) {
            args.add(expr(ua.getValue()));
          } else {
            args.add(expr(extra));
          }
        }
      }
      if (fnLower.equals("median") && args.size() == 1) {
        args.add(SqlLiteral.createExactNumeric("50", POS));
      }
      return new SqlBasicCall(op, args, POS, quantifier);
    }
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
    // PPL `reverse` flips whatever ordering is currently in effect:
    //   1. Explicit prior sort: emit ORDER BY <reversed-keys>.
    //   2. No prior sort but @timestamp visible in scope: ORDER BY @timestamp DESC (matches
    //      v2's reverse semantic for time-series indices like TIME_TEST_DATA).
    //   3. Otherwise: pass through (e.g. after aggregation, bare source, data-dependent input).
    //
    // Reverse-after-streamstats is intentionally a no-op without an explicit prior sort —
    // PPL semantics: `__stream_seq__` is internal scaffolding, not a user-visible collation,
    // so a downstream `reverse` cannot pick it up. Tests testStreamstatsWithReverse and
    // testStreamstatsWindowWithReverse codify this.
    List<SqlNode> reversed = frame.reversedLastOrderBy();
    if (reversed != null) {
      return SqlBuilder.select(starList()).from(from).orderBy(reversed).wrap(frame);
    }
    if (frame.currentFields != null
        && frame.currentFields.contains(OpenSearchConstants.IMPLICIT_FIELD_TIMESTAMP)) {
      SqlNode tsKey =
          new SqlBasicCall(
              SqlStdOperatorTable.DESC,
              List.of(new SqlIdentifier(OpenSearchConstants.IMPLICIT_FIELD_TIMESTAMP, POS)),
              POS);
      return SqlBuilder.select(starList()).from(from).orderBy(List.of(tsKey)).wrap(frame);
    }
    return from;
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
    if (e instanceof org.opensearch.sql.ast.expression.In in) {
      return inExpr(in);
    }
    if (e instanceof org.opensearch.sql.ast.expression.Case caseExpr) {
      return caseExpr(caseExpr);
    }
    if (e instanceof org.opensearch.sql.ast.expression.Interval interval) {
      return intervalExpr(interval);
    }
    if (e instanceof org.opensearch.sql.ast.expression.UnresolvedArgument ua) {
      return unresolvedArgExpr(ua);
    }
    if (e instanceof org.opensearch.sql.ast.expression.RelevanceFieldList rfl) {
      return relevanceFieldListExpr(rfl);
    }
    if (e instanceof org.opensearch.sql.ast.expression.LambdaFunction lf) {
      // PPL lambdas (forall/exists/filter/transform/reduce/...) emit as Calcite SqlLambda whose
      // parameter list is the PPL argument identifiers and whose body is the translated body.
      // SqlToRelConverter turns this into a RexLambda that the PPL array UDFs invoke directly.
      SqlNodeList paramList = new SqlNodeList(POS);
      for (QualifiedName qn : lf.getFuncArgs()) {
        paramList.add(new SqlIdentifier(qn.toString(), POS));
      }
      SqlNode body = expr(lf.getFunction());
      return new org.apache.calcite.sql.SqlLambda(POS, paramList, body);
    }
    if (e instanceof org.opensearch.sql.ast.expression.subquery.InSubquery is) {
      return inSubqueryExpr(is);
    }
    if (e instanceof org.opensearch.sql.ast.expression.Between b) {
      return new SqlBasicCall(
          SqlStdOperatorTable.BETWEEN,
          List.of(expr(b.getValue()), expr(b.getLowerBound()), expr(b.getUpperBound())),
          POS);
    }
    if (e instanceof org.opensearch.sql.ast.expression.Xor xor) {
      // a XOR b ≡ (a OR b) AND NOT (a AND b)
      SqlNode l = expr(xor.getLeft());
      SqlNode r = expr(xor.getRight());
      SqlNode or = new SqlBasicCall(SqlStdOperatorTable.OR, List.of(l, r), POS);
      SqlNode and = new SqlBasicCall(SqlStdOperatorTable.AND, List.of(l, r), POS);
      SqlNode notAnd = new SqlBasicCall(SqlStdOperatorTable.NOT, List.of(and), POS);
      return new SqlBasicCall(SqlStdOperatorTable.AND, List.of(or, notAnd), POS);
    }
    if (e instanceof org.opensearch.sql.ast.expression.subquery.ExistsSubquery es) {
      return existsSubqueryExpr(es);
    }
    if (e instanceof org.opensearch.sql.ast.expression.subquery.ScalarSubquery ss) {
      return scalarSubqueryExpr(ss);
    }
    throw new UnsupportedOperationException(
        "Expression not yet supported in PPLToSqlNodeVisitor: " + e.getClass().getSimpleName());
  }

  /**
   * {@code EXISTS (<subquery>)} — outer-scope columns referenced from the subquery resolve via
   * Calcite's correlated-subquery binding.
   */
  private SqlNode existsSubqueryExpr(org.opensearch.sql.ast.expression.subquery.ExistsSubquery es) {
    Frame subFrame = new Frame();
    Frame savedExpr = this.exprFrame;
    this.exprFrame = subFrame;
    SqlNode subQuery;
    try {
      subQuery = stripImplicitMetaProjects(es.getQuery()).accept(this, subFrame);
    } finally {
      this.exprFrame = savedExpr;
    }
    return new SqlBasicCall(SqlStdOperatorTable.EXISTS, List.of(subQuery), POS);
  }

  /**
   * Scalar subquery: emit the subquery's SqlSelect — Calcite treats it as a scalar expression when
   * used in a comparable context (e.g. {@code col = (subquery)}).
   */
  private SqlNode scalarSubqueryExpr(org.opensearch.sql.ast.expression.subquery.ScalarSubquery ss) {
    Frame subFrame = new Frame();
    Frame savedExpr = this.exprFrame;
    this.exprFrame = subFrame;
    try {
      return stripImplicitMetaProjects(ss.getQuery()).accept(this, subFrame);
    } finally {
      this.exprFrame = savedExpr;
    }
  }

  /**
   * PPL {@code where col in [<subquery>]} — emit {@code col IN (subSqlNode)}. The subquery is
   * walked in a fresh Frame so its own scope doesn't leak into the outer expression's frame.
   * Multi-column IN wraps the LHS as a {@code ROW(...)} so Calcite's IN operator binds against a
   * row-typed subquery output.
   */
  private SqlNode inSubqueryExpr(org.opensearch.sql.ast.expression.subquery.InSubquery is) {
    Frame subFrame = new Frame();
    Frame savedExpr = this.exprFrame;
    this.exprFrame = subFrame;
    SqlNode subQuery;
    try {
      subQuery = stripImplicitMetaProjects(is.getQuery()).accept(this, subFrame);
    } finally {
      this.exprFrame = savedExpr;
    }
    SqlNode left;
    if (is.getValue().size() == 1) {
      left = expr(is.getValue().get(0));
    } else {
      List<SqlNode> rowOperands = new ArrayList<>(is.getValue().size());
      for (UnresolvedExpression v : is.getValue()) {
        rowOperands.add(expr(v));
      }
      left = new SqlBasicCall(SqlStdOperatorTable.ROW, rowOperands, POS);
    }
    return new SqlBasicCall(SqlStdOperatorTable.IN, List.of(left, subQuery), POS);
  }

  /**
   * Translate PPL {@code multi_match(["fld1"^1.5, "fld2"^2], query="x")}'s field-list arg (parsed
   * as {@link org.opensearch.sql.ast.expression.RelevanceFieldList}) to a {@code MAP["fld1", 1.5,
   * "fld2", 2.0]} so the OpenSearch pushdown's relevance UDF receives a {@code Map<String,
   * Double>}. Cast each name to VARCHAR to avoid CHAR(N) length widening across entries.
   */
  private SqlNode relevanceFieldListExpr(org.opensearch.sql.ast.expression.RelevanceFieldList rfl) {
    org.apache.calcite.sql.SqlDataTypeSpec varcharSpec =
        new org.apache.calcite.sql.SqlDataTypeSpec(
            new org.apache.calcite.sql.SqlBasicTypeNameSpec(
                org.apache.calcite.sql.type.SqlTypeName.VARCHAR, POS),
            POS);
    List<SqlNode> args = new ArrayList<>();
    for (java.util.Map.Entry<String, Float> entry : rfl.getFieldList().entrySet()) {
      args.add(
          new SqlBasicCall(
              org.apache.calcite.sql.fun.SqlLibraryOperators.SAFE_CAST,
              List.of(SqlLiteral.createCharString(entry.getKey(), POS), varcharSpec),
              POS));
      args.add(SqlLiteral.createApproxNumeric(entry.getValue() + "E0", POS));
    }
    return new SqlBasicCall(SqlStdOperatorTable.MAP_VALUE_CONSTRUCTOR, args, POS);
  }

  /**
   * Translate a PPL named argument {@code name=value} (used by match/match_phrase/multi_match and
   * other named-arg functions) into a {@code MAP[name, value]} entry. The pushdown layer collects
   * these into a single MAP-typed argument. String-literal values are cast to VARCHAR to avoid
   * Calcite's CHAR(N) length widening across entries (which right-pads shorter values and breaks
   * the OpenSearch pushdown's exact-string match).
   */
  private SqlNode unresolvedArgExpr(org.opensearch.sql.ast.expression.UnresolvedArgument ua) {
    SqlNode value = expr(ua.getValue());
    if (value instanceof SqlLiteral lit
        && lit.getTypeName() == org.apache.calcite.sql.type.SqlTypeName.CHAR) {
      org.apache.calcite.sql.SqlDataTypeSpec varcharSpec =
          new org.apache.calcite.sql.SqlDataTypeSpec(
              new org.apache.calcite.sql.SqlBasicTypeNameSpec(
                  org.apache.calcite.sql.type.SqlTypeName.VARCHAR, POS),
              POS);
      value =
          new SqlBasicCall(
              org.apache.calcite.sql.fun.SqlLibraryOperators.SAFE_CAST,
              List.of(value, varcharSpec),
              POS);
    }
    return new SqlBasicCall(
        SqlStdOperatorTable.MAP_VALUE_CONSTRUCTOR,
        List.of(SqlLiteral.createCharString(ua.getArgName(), POS), value),
        POS);
  }

  /**
   * Translate PPL {@code INTERVAL N <unit>} to a Calcite {@link SqlLiteral#createInterval}. The AST
   * stores the value as an {@link Literal} and the unit as an enum; map the unit to Calcite's
   * {@link org.apache.calcite.avatica.util.TimeUnit} and emit the literal with the appropriate
   * {@link org.apache.calcite.sql.SqlIntervalQualifier}. Non-literal values fall back to an
   * unresolved-function call (matches v2 emission for that rare path).
   */
  private SqlNode intervalExpr(org.opensearch.sql.ast.expression.Interval i) {
    Object v = i.getValue() instanceof Literal lit ? lit.getValue() : null;
    if (v == null) {
      SqlNode value = expr(i.getValue());
      return new SqlBasicCall(
          new org.apache.calcite.sql.SqlUnresolvedFunction(
              new SqlIdentifier("interval", POS),
              null,
              null,
              null,
              null,
              org.apache.calcite.sql.SqlFunctionCategory.USER_DEFINED_FUNCTION),
          List.of(value, SqlLiteral.createCharString(i.getUnit().name(), POS)),
          POS);
    }
    String literalStr = v.toString();
    org.apache.calcite.avatica.util.TimeUnit unit =
        switch (i.getUnit()) {
          case MICROSECOND -> org.apache.calcite.avatica.util.TimeUnit.MICROSECOND;
          case MILLISECOND -> org.apache.calcite.avatica.util.TimeUnit.MILLISECOND;
          case SECOND -> org.apache.calcite.avatica.util.TimeUnit.SECOND;
          case MINUTE -> org.apache.calcite.avatica.util.TimeUnit.MINUTE;
          case HOUR -> org.apache.calcite.avatica.util.TimeUnit.HOUR;
          case DAY -> org.apache.calcite.avatica.util.TimeUnit.DAY;
          case WEEK -> org.apache.calcite.avatica.util.TimeUnit.WEEK;
          case MONTH -> org.apache.calcite.avatica.util.TimeUnit.MONTH;
          case QUARTER -> org.apache.calcite.avatica.util.TimeUnit.QUARTER;
          case YEAR -> org.apache.calcite.avatica.util.TimeUnit.YEAR;
          default ->
              throw new UnsupportedOperationException("Unsupported interval unit: " + i.getUnit());
        };
    org.apache.calcite.sql.SqlIntervalQualifier qualifier =
        new org.apache.calcite.sql.SqlIntervalQualifier(unit, null, POS);
    int sign = 1;
    if (literalStr.startsWith("-")) {
      sign = -1;
      literalStr = literalStr.substring(1);
    }
    return SqlLiteral.createInterval(sign, literalStr, qualifier, POS);
  }

  /**
   * Translate {@code <field> IN (v1, v2, ...)} to a {@link SqlStdOperatorTable#IN} call. PPL's type
   * compatibility check (v2 raises SemanticCheckException for STRING vs NUMERIC mix) needs a
   * row-type oracle and is deferred — Calcite's validator coerces silently in the meantime.
   */
  private SqlNode inExpr(org.opensearch.sql.ast.expression.In in) {
    // PPL forbids mixing string and numeric literals inside the IN value list. v2 raises a
    // SemanticCheckException at analyze time. Without a row-type oracle on the field side, do a
    // best-effort check on the literals themselves: if the list contains both string and
    // numeric literals, reject. This catches `field in (4180, 5686, '6077')`-style typos.
    boolean hasString = false;
    boolean hasNumeric = false;
    for (UnresolvedExpression v : in.getValueList()) {
      if (v instanceof Literal lit) {
        DataType t = lit.getType();
        if (t == DataType.STRING) hasString = true;
        else if (t == DataType.INTEGER
            || t == DataType.LONG
            || t == DataType.SHORT
            || t == DataType.DOUBLE
            || t == DataType.FLOAT
            || t == DataType.DECIMAL) hasNumeric = true;
      }
    }
    if (hasString && hasNumeric) {
      // Match v2's error-message shape so PPL clients (and tests) get a stable string.
      // The "fields type" is reported as the wider numeric (LONG) since the literal types
      // alone don't tell us the column type — without an oracle this is a best-effort check.
      List<String> typeNames = new ArrayList<>();
      for (UnresolvedExpression v : in.getValueList()) {
        if (v instanceof Literal lit) {
          typeNames.add(lit.getType().name());
        } else {
          typeNames.add("UNKNOWN");
        }
      }
      throw new IllegalArgumentException(
          "In expression types are incompatible: fields type LONG, values type " + typeNames);
    }
    SqlNode field = expr(in.getField());
    SqlNodeList values = new SqlNodeList(POS);
    for (UnresolvedExpression v : in.getValueList()) {
      values.add(expr(v));
    }
    return new SqlBasicCall(SqlStdOperatorTable.IN, List.of(field, values), POS);
  }

  /**
   * Translate a PPL {@code Case} (CASE WHEN cond THEN val ... ELSE elseVal END) to a Calcite {@link
   * org.apache.calcite.sql.fun.SqlCase}. PPL stores the WHEN/THEN pairs as a list of {@link
   * org.opensearch.sql.ast.expression.When} nodes plus an optional ELSE expression.
   */
  private SqlNode caseExpr(org.opensearch.sql.ast.expression.Case node) {
    SqlNodeList whens = new SqlNodeList(POS);
    SqlNodeList thens = new SqlNodeList(POS);
    boolean allStringResults = true;
    for (org.opensearch.sql.ast.expression.When when : node.getWhenClauses()) {
      whens.add(expr(when.getCondition()));
      thens.add(expr(when.getResult()));
      allStringResults &=
          when.getResult() instanceof Literal lit && lit.getType() == DataType.STRING;
    }
    SqlNode elseExpr =
        node.getElseClause().isPresent()
            ? expr(node.getElseClause().get())
            : SqlLiteral.createNull(POS);
    if (node.getElseClause().isPresent()) {
      allStringResults &=
          node.getElseClause().get() instanceof Literal lit && lit.getType() == DataType.STRING;
    }
    // Calcite widens unequal-length CHAR literals to a common CHAR(N), right-padding shorter
    // values with spaces. PPL keeps strings VARCHAR-typed (no padding). When all THEN/ELSE
    // values are string literals, cast each to VARCHAR so the CASE result type stays VARCHAR.
    if (allStringResults && !thens.isEmpty()) {
      org.apache.calcite.sql.SqlDataTypeSpec varcharSpec =
          new org.apache.calcite.sql.SqlDataTypeSpec(
              new org.apache.calcite.sql.SqlBasicTypeNameSpec(
                  org.apache.calcite.sql.type.SqlTypeName.VARCHAR, POS),
              POS);
      for (int i = 0; i < thens.size(); i++) {
        thens.set(
            i,
            new SqlBasicCall(
                org.apache.calcite.sql.fun.SqlLibraryOperators.SAFE_CAST,
                List.of(thens.get(i), varcharSpec),
                POS));
      }
      if (node.getElseClause().isPresent()) {
        elseExpr =
            new SqlBasicCall(
                org.apache.calcite.sql.fun.SqlLibraryOperators.SAFE_CAST,
                List.of(elseExpr, varcharSpec),
                POS);
      }
    }
    return new org.apache.calcite.sql.fun.SqlCase(POS, null, whens, thens, elseExpr);
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
    // PPL parses the bare token `null` as a Field("null") (no typed null literal in PPL grammar).
    // Inside coalesce/ifnull, the v2 path's QualifiedNameResolver replaces such operands with a
    // typed NULL. Mirror that here at SqlNode time — without it, `coalesce(null, 42)` fails the
    // validator with "Field [null] not found". (Missing-field replacement requires an oracle and
    // is left for the row-type-aware path.)
    String fnLower = fn.getFuncName().toLowerCase(java.util.Locale.ROOT);
    boolean isCoalesce = "coalesce".equals(fnLower) || "ifnull".equals(fnLower);
    List<SqlNode> args = new ArrayList<>(fn.getFuncArgs().size());
    for (UnresolvedExpression a : fn.getFuncArgs()) {
      if (isCoalesce && isNullLiteralRef(a)) {
        args.add(SqlLiteral.createNull(POS));
      } else {
        args.add(expr(a));
      }
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
    // PPL `isnull(x)` / `isnotnull(x)` / `like(s, p)` parse as Function calls but Calcite expects
    // SqlOperator postfix/infix. Map directly. Other operator-shaped functions (regex, ...) come
    // through PPLBuiltinOperators via the validator-resolved unresolved-function path.
    String name = fn.getFuncName().toLowerCase(java.util.Locale.ROOT);
    org.apache.calcite.sql.SqlOperator opOverride =
        switch (name) {
          case "isnull", "is_null", "is null" -> SqlStdOperatorTable.IS_NULL;
          case "isnotnull", "is_not_null", "is not null", "ispresent" ->
              SqlStdOperatorTable.IS_NOT_NULL;
          case "like" -> SqlStdOperatorTable.LIKE;
          case "not_like", "not like" -> SqlStdOperatorTable.NOT_LIKE;
          case "ilike" -> org.apache.calcite.sql.fun.SqlLibraryOperators.ILIKE;
          case "not_ilike", "not ilike" -> org.apache.calcite.sql.fun.SqlLibraryOperators.NOT_ILIKE;
          // Standard SQL conditional functions: COALESCE / NULLIF. PPL's funcExpr unresolved-
          // function path can't always resolve them via case-sensitive validator lookup;
          // dispatch directly.
          case "coalesce" -> SqlStdOperatorTable.COALESCE;
          case "nullif" -> SqlStdOperatorTable.NULLIF;
          // Common scalar functions whose case-sensitive validator lookup misses the lowercase
          // PPL form. Direct mapping eliminates the "No match found for function signature"
          // error for these names.
          case "abs" -> SqlStdOperatorTable.ABS;
          case "ceil", "ceiling" -> SqlStdOperatorTable.CEIL;
          case "floor" -> SqlStdOperatorTable.FLOOR;
          case "exp" -> SqlStdOperatorTable.EXP;
          case "ln" -> SqlStdOperatorTable.LN;
          case "log10" -> SqlStdOperatorTable.LOG10;
          case "sqrt" -> SqlStdOperatorTable.SQRT;
          case "round" -> SqlStdOperatorTable.ROUND;
          // PPL MOD: divide-by-zero returns NULL and operand types promote wider than
          // SqlStdOperatorTable.MOD's signature allows. Bind to the PPL UDF.
          case "mod" -> org.opensearch.sql.expression.function.PPLBuiltinOperators.MOD;
          case "pow", "power" -> SqlStdOperatorTable.POWER;
          // PPL named-arithmetic forms parse as regular function calls; route them to the
          // matching binary operator. These differ from the operator forms (`a + b`) which
          // arithmeticOperator() handles above.
          case "add" -> SqlStdOperatorTable.PLUS;
          case "subtract" -> SqlStdOperatorTable.MINUS;
          case "multiply" -> SqlStdOperatorTable.MULTIPLY;
          case "divide" -> org.opensearch.sql.expression.function.PPLBuiltinOperators.DIVIDE;
          case "modulus" -> org.opensearch.sql.expression.function.PPLBuiltinOperators.MOD;
          // Two-arg atan: PPL `atan(y, x)` is mathematically atan2; map to ATAN2.
          case "atan2" -> SqlStdOperatorTable.ATAN2;
          // PPL `signum` / `sign` both return -1/0/1 of a numeric; Calcite's SIGN op handles both.
          case "sign", "signum" -> SqlStdOperatorTable.SIGN;
          // PPL `conv(x, from, to)` — base conversion. Registered in PPLBuiltinOperators under
          // the SQL-conventional name CONVERT.
          case "conv" -> org.opensearch.sql.expression.function.PPLBuiltinOperators.CONV;
          // Date-part extractors: bind to PPL UDFs that handle EXPR_DATE / EXPR_TIMESTAMP. The
          // standard YEAR/QUARTER/MONTH/HOUR/MINUTE/SECOND operators rewrite to EXTRACT(<unit>
          // FROM <datetime>) and EXTRACT only accepts Calcite's built-in DATETIME types.
          case "year" -> org.opensearch.sql.expression.function.PPLBuiltinOperators.YEAR;
          case "quarter" -> org.opensearch.sql.expression.function.PPLBuiltinOperators.QUARTER;
          case "month", "month_of_year" ->
              org.opensearch.sql.expression.function.PPLBuiltinOperators.MONTH;
          case "dayofyear", "day_of_year" ->
              org.opensearch.sql.expression.function.PPLBuiltinOperators.DAY_OF_YEAR;
          case "dayofmonth", "day_of_month", "day" ->
              org.opensearch.sql.expression.function.PPLBuiltinOperators.DAY;
          case "dayofweek", "day_of_week" ->
              org.opensearch.sql.expression.function.PPLBuiltinOperators.DAY_OF_WEEK;
          case "hour", "hour_of_day" ->
              org.opensearch.sql.expression.function.PPLBuiltinOperators.HOUR;
          case "minute", "minute_of_hour" ->
              org.opensearch.sql.expression.function.PPLBuiltinOperators.MINUTE;
          case "second", "second_of_minute" ->
              org.opensearch.sql.expression.function.PPLBuiltinOperators.SECOND;
          case "week", "week_of_year", "weekofyear" ->
              org.opensearch.sql.expression.function.PPLBuiltinOperators.WEEK;
          case "weekday" -> org.opensearch.sql.expression.function.PPLBuiltinOperators.WEEKDAY;
          case "yearweek" -> org.opensearch.sql.expression.function.PPLBuiltinOperators.YEARWEEK;
          // Date-arithmetic UDFs that share names with Calcite-standard operators (which expect
          // INTERVAL qualifiers as the first arg, not strings). Bind directly to PPL UDFs.
          case "timestampadd" ->
              org.opensearch.sql.expression.function.PPLBuiltinOperators.TIMESTAMPADD;
          case "timestampdiff" ->
              org.opensearch.sql.expression.function.PPLBuiltinOperators.TIMESTAMPDIFF;
          case "last_day" -> org.opensearch.sql.expression.function.PPLBuiltinOperators.LAST_DAY;
          case "extract" -> org.opensearch.sql.expression.function.PPLBuiltinOperators.EXTRACT;
          case "adddate", "date_add" ->
              org.opensearch.sql.expression.function.PPLBuiltinOperators.ADDDATE;
          case "subdate", "date_sub" ->
              org.opensearch.sql.expression.function.PPLBuiltinOperators.SUBDATE;
          case "addtime" -> org.opensearch.sql.expression.function.PPLBuiltinOperators.ADDTIME;
          case "subtime" -> org.opensearch.sql.expression.function.PPLBuiltinOperators.SUBTIME;
          case "datediff" -> org.opensearch.sql.expression.function.PPLBuiltinOperators.DATEDIFF;
          case "timediff" -> org.opensearch.sql.expression.function.PPLBuiltinOperators.TIMEDIFF;
          case "lower", "lcase" -> SqlStdOperatorTable.LOWER;
          case "upper", "ucase" -> SqlStdOperatorTable.UPPER;
          case "substring", "substr" -> SqlStdOperatorTable.SUBSTRING;
          // PPL `replace` is regex-replace (AstExpressionBuilder remaps `regexp_replace` → REPLACE
          // builtin name). Don't intercept via SqlStdOperatorTable.REPLACE (literal-replace);
          // let it flow through the validator-resolved unresolved-function path which finds
          // PPLBuiltinOperators.REGEXP_REPLACE.
          // PPL `concat` is variadic; Calcite's std CONCAT is binary `||`. Use the library's
          // CONCAT_FUNCTION which accepts arbitrary arity.
          case "concat" -> org.apache.calcite.sql.fun.SqlLibraryOperators.CONCAT_FUNCTION;
          case "length", "char_length", "character_length" ->
              org.apache.calcite.sql.fun.SqlLibraryOperators.LENGTH;
          // PPL `locate`/`position` need a function-call shape that Calcite POSITION doesn't
          // accept directly (POSITION uses `IN` keyword syntax). Defer to PPLBuiltinOperators
          // via the unresolved-function path; the validator's name lookup will find LOCATE.
          default -> null;
        };
    // PPL `isempty(x)` — NULL or empty string. PPL `isblank(x)` — NULL or whitespace-only.
    // Both desugar to OR-chains because Calcite's IS_EMPTY postfix operator at the validator
    // level only accepts collection types. Mirrors the legacy SqlNode visitor's permissive
    // implementations (without the post-RelNode shuttle that would rewrite to v2's
    // IS_EMPTY(TRIM(...)) shape — those rewrites only matter for explain-plan parity).
    if (("isempty".equals(name) || "isblank".equals(name)) && args.size() == 1) {
      SqlNode a = args.get(0);
      SqlNode toCheck = a;
      if ("isblank".equals(name)) {
        // For isblank, strip whitespace before length check.
        toCheck =
            new SqlBasicCall(
                SqlStdOperatorTable.REPLACE,
                List.of(
                    a, SqlLiteral.createCharString(" ", POS), SqlLiteral.createCharString("", POS)),
                POS);
      }
      SqlNode lenZero =
          new SqlBasicCall(
              SqlStdOperatorTable.EQUALS,
              List.of(
                  new SqlBasicCall(
                      org.apache.calcite.sql.fun.SqlLibraryOperators.LENGTH, List.of(toCheck), POS),
                  SqlLiteral.createExactNumeric("0", POS)),
              POS);
      return new SqlBasicCall(
          SqlStdOperatorTable.OR,
          List.of(new SqlBasicCall(SqlStdOperatorTable.IS_NULL, List.of(a), POS), lenZero),
          POS);
    }
    // PPL `Like(field, pattern [, caseSensitive])` — wrap with an ESCAPE clause so that PPL
    // queries can use `\%` / `\_` to match literal `%` / `_`. The 3-arg form picks LIKE vs
    // ILIKE from the boolean third arg; the 2-arg form is case-sensitive by default.
    // (ILike(...) flows through the simple opOverride mapping; this branch only handles the
    // explicit-Like name.)
    if ("like".equals(name) && (args.size() == 2 || args.size() == 3)) {
      SqlNode escape = SqlLiteral.createCharString("\\", POS);
      org.apache.calcite.sql.SqlOperator likeOp = SqlStdOperatorTable.LIKE;
      if (args.size() == 3
          && args.get(2) instanceof SqlLiteral lit
          && Boolean.FALSE.equals(lit.getValue())) {
        likeOp = org.apache.calcite.sql.fun.SqlLibraryOperators.ILIKE;
      } else if (args.size() == 2
          && Boolean.TRUE.equals(
              org.opensearch.sql.calcite.CalcitePlanContext.isLegacyPreferred())) {
        // PPL's legacy v2 syntax binds 2-arg LIKE as case-insensitive — match v2 emission shape.
        likeOp = org.apache.calcite.sql.fun.SqlLibraryOperators.ILIKE;
      }
      return new SqlBasicCall(likeOp, List.of(args.get(0), args.get(1), escape), POS);
    }
    // atan: 1-arg → ATAN, 2-arg → ATAN2 (PPL allows both arities under the same name).
    if ("atan".equals(name)) {
      return new SqlBasicCall(
          args.size() == 2 ? SqlStdOperatorTable.ATAN2 : SqlStdOperatorTable.ATAN, args, POS);
    }
    // log: 1-arg natural log; 2-arg log(b, x) is log_x(b) per PPL semantics. Calcite's
    // SqlLibraryOperators.LOG signature is LOG(x, base), so swap operand order for the 2-arg form.
    if ("log".equals(name)) {
      if (args.size() == 1) {
        return new SqlBasicCall(SqlStdOperatorTable.LN, args, POS);
      }
      if (args.size() == 2) {
        return new SqlBasicCall(
            org.apache.calcite.sql.fun.SqlLibraryOperators.LOG,
            List.of(args.get(1), args.get(0)),
            POS);
      }
    }
    // PPL `replace(str, pattern, replacement)` is a regex replace (PCRE-style); SQL's standard
    // REPLACE does literal substring replace only. Dispatch to REGEXP_REPLACE_3 and convert PPL's
    // \1 \2 backrefs to Java's $1 $2. Validate the pattern at translation time so syntax errors
    // surface as 400 Bad Request (IllegalArgumentException), not 500 from runtime
    // PatternSyntaxException.
    if ("replace".equals(name) && args.size() == 3) {
      if (args.get(1) instanceof SqlLiteral patLit
          && patLit.getTypeName() == org.apache.calcite.sql.type.SqlTypeName.CHAR) {
        String pattern = ((org.apache.calcite.util.NlsString) patLit.getValue()).getValue();
        try {
          java.util.regex.Pattern.compile(pattern);
        } catch (java.util.regex.PatternSyntaxException pse) {
          throw new IllegalArgumentException(
              String.format("Invalid regex pattern '%s': %s", pattern, pse.getDescription()), pse);
        }
      }
      SqlNode replacement = args.get(2);
      if (replacement instanceof SqlLiteral lit
          && lit.getTypeName() == org.apache.calcite.sql.type.SqlTypeName.CHAR) {
        String s = ((org.apache.calcite.util.NlsString) lit.getValue()).getValue();
        StringBuilder converted = new StringBuilder();
        for (int i = 0; i < s.length(); i++) {
          char c = s.charAt(i);
          if (c == '\\' && i + 1 < s.length() && Character.isDigit(s.charAt(i + 1))) {
            converted.append('$');
          } else if (c == '$') {
            converted.append("\\$");
          } else {
            converted.append(c);
          }
        }
        replacement = SqlLiteral.createCharString(converted.toString(), POS);
      }
      return new SqlBasicCall(
          org.apache.calcite.sql.fun.SqlLibraryOperators.REGEXP_REPLACE_3,
          List.of(args.get(0), args.get(1), replacement),
          POS);
    }
    // PPL `trim(str)` -> TRIM(BOTH ' ' FROM str). Calcite's TRIM is keyword-syntax that doesn't
    // bind via SqlBasicCall function-call form; emulate via REGEXP_REPLACE.
    if ("trim".equals(name) && args.size() == 1) {
      return new SqlBasicCall(
          org.apache.calcite.sql.fun.SqlLibraryOperators.REGEXP_REPLACE_3,
          List.of(
              args.get(0),
              SqlLiteral.createCharString("^\\s+|\\s+$", POS),
              SqlLiteral.createCharString("", POS)),
          POS);
    }
    // PPL `locate(sub, str [, start])` -> INSTR(str, sub [, start]). PPL's argument order has the
    // substring first; INSTR (Oracle-library) takes the full string first. Swap operands.
    if ("locate".equals(name) && (args.size() == 2 || args.size() == 3)) {
      List<SqlNode> swapped = new ArrayList<>();
      swapped.add(args.get(1));
      swapped.add(args.get(0));
      if (args.size() == 3) swapped.add(args.get(2));
      return new SqlBasicCall(org.apache.calcite.sql.fun.SqlLibraryOperators.INSTR, swapped, POS);
    }
    // Niladic datetime helpers — PPL parses `now()`, `current_date()`, etc. as Function calls
    // with no args. Calcite's SqlStdOperatorTable defines LOCALTIME / CURRENT_DATE as niladic
    // SqlOperators that reject the SqlBasicCall(empty-args) shape, so dispatch to the PPL UDF
    // variants which mirror v2's PPLFuncImpTable mappings.
    if (args.isEmpty()) {
      switch (name) {
        case "now":
        case "current_timestamp":
        case "localtime":
        case "localtimestamp":
          return new SqlBasicCall(
              org.opensearch.sql.expression.function.PPLBuiltinOperators.NOW, args, POS);
        case "curtime":
        case "current_time":
          return new SqlBasicCall(
              org.opensearch.sql.expression.function.PPLBuiltinOperators.CURRENT_TIME, args, POS);
        case "curdate":
        case "current_date":
          return new SqlBasicCall(
              org.opensearch.sql.expression.function.PPLBuiltinOperators.CURRENT_DATE, args, POS);
        case "utc_timestamp":
          return new SqlBasicCall(
              org.opensearch.sql.expression.function.PPLBuiltinOperators.UTC_TIMESTAMP, args, POS);
        case "utc_time":
          return new SqlBasicCall(
              org.opensearch.sql.expression.function.PPLBuiltinOperators.UTC_TIME, args, POS);
        default:
          // fall through to other handlers
      }
    }
    // PPL `regexp_match(str, pattern)` / `regexp(str, pattern)` map to REGEXP_CONTAINS. Cast a
    // CHAR-typed pattern literal to VARCHAR so the validator picks the canonical operator
    // overload (CHAR-typed pattern literal would otherwise miss the signature).
    if (("regexp_match".equals(name) || "regexp".equals(name)) && args.size() == 2) {
      SqlNode pattern = args.get(1);
      if (pattern instanceof SqlLiteral lit
          && lit.getTypeName() == org.apache.calcite.sql.type.SqlTypeName.CHAR) {
        org.apache.calcite.sql.SqlDataTypeSpec varcharSpec =
            new org.apache.calcite.sql.SqlDataTypeSpec(
                new org.apache.calcite.sql.SqlBasicTypeNameSpec(
                    org.apache.calcite.sql.type.SqlTypeName.VARCHAR, POS),
                POS);
        pattern =
            new SqlBasicCall(
                org.apache.calcite.sql.fun.SqlLibraryOperators.SAFE_CAST,
                List.of(pattern, varcharSpec),
                POS);
      }
      return new SqlBasicCall(
          org.apache.calcite.sql.fun.SqlLibraryOperators.REGEXP_CONTAINS,
          List.of(args.get(0), pattern),
          POS);
    }
    // PPL `strcmp(s1, s2)` follows MySQL: returns -1 / 0 / 1. Express via CASE — Calcite's
    // STRCMP sign convention differs from PPL's.
    if ("strcmp".equals(name) && args.size() == 2) {
      SqlNode a = args.get(0);
      SqlNode b = args.get(1);
      SqlNodeList whens = new SqlNodeList(POS);
      whens.add(new SqlBasicCall(SqlStdOperatorTable.LESS_THAN, List.of(a, b), POS));
      whens.add(new SqlBasicCall(SqlStdOperatorTable.EQUALS, List.of(a, b), POS));
      SqlNodeList thens = new SqlNodeList(POS);
      thens.add(SqlLiteral.createExactNumeric("-1", POS));
      thens.add(SqlLiteral.createExactNumeric("0", POS));
      return new org.apache.calcite.sql.fun.SqlCase(
          POS, null, whens, thens, SqlLiteral.createExactNumeric("1", POS));
    }
    // PPL `split(str, delim)` returns each character as a separate element when delim is empty.
    // Calcite's std SPLIT(str, '') returns a single-element array with the whole string. Mirror
    // PPLFuncImpTable: `CASE WHEN delim='' THEN REGEXP_EXTRACT_ALL(str, '.') ELSE SPLIT(str,
    // delim)`.
    if ("split".equals(name) && args.size() == 2) {
      SqlNode str = args.get(0);
      SqlNode delim = args.get(1);
      SqlNode emptyLit = SqlLiteral.createCharString("", POS);
      SqlNode isEmpty = new SqlBasicCall(SqlStdOperatorTable.EQUALS, List.of(delim, emptyLit), POS);
      SqlNode anyChar = SqlLiteral.createCharString(".", POS);
      SqlNode regexExtract =
          new SqlBasicCall(
              org.apache.calcite.sql.fun.SqlLibraryOperators.REGEXP_EXTRACT_ALL,
              List.of(str, anyChar),
              POS);
      SqlNode split =
          new SqlBasicCall(
              org.apache.calcite.sql.fun.SqlLibraryOperators.SPLIT, List.of(str, delim), POS);
      SqlNodeList whens = new SqlNodeList(POS);
      whens.add(isEmpty);
      SqlNodeList thens = new SqlNodeList(POS);
      thens.add(regexExtract);
      return new org.apache.calcite.sql.fun.SqlCase(POS, null, whens, thens, split);
    }
    // PPL `mvindex(array, index [, end])` — Calcite has no direct 3-arg counterpart. Map the
    // 2-arg form to ITEM with PPL's 0-based / negative-from-end index normalisation, and the
    // 3-arg form to ARRAY_SLICE(array, normStart+1, len).
    if ("mvindex".equals(name) && (args.size() == 2 || args.size() == 3)) {
      SqlNode arr = args.get(0);
      SqlNode arrLen =
          new SqlBasicCall(
              org.apache.calcite.sql.fun.SqlLibraryOperators.ARRAY_LENGTH, List.of(arr), POS);
      org.apache.calcite.sql.SqlDataTypeSpec intSpec =
          new org.apache.calcite.sql.SqlDataTypeSpec(
              new org.apache.calcite.sql.SqlBasicTypeNameSpec(
                  org.apache.calcite.sql.type.SqlTypeName.INTEGER, POS),
              POS);
      SqlNode startIdx =
          new SqlBasicCall(
              org.apache.calcite.sql.fun.SqlLibraryOperators.SAFE_CAST,
              List.of(args.get(1), intSpec),
              POS);
      SqlNode zero = SqlLiteral.createExactNumeric("0", POS);
      SqlNode one = SqlLiteral.createExactNumeric("1", POS);
      if (args.size() == 2) {
        SqlNode isNeg =
            new SqlBasicCall(SqlStdOperatorTable.LESS_THAN, List.of(startIdx, zero), POS);
        SqlNode lenPlusIdx =
            new SqlBasicCall(SqlStdOperatorTable.PLUS, List.of(arrLen, startIdx), POS);
        SqlNode negCase = new SqlBasicCall(SqlStdOperatorTable.PLUS, List.of(lenPlusIdx, one), POS);
        SqlNode posCase = new SqlBasicCall(SqlStdOperatorTable.PLUS, List.of(startIdx, one), POS);
        SqlNodeList caseWhens = new SqlNodeList(POS);
        caseWhens.add(isNeg);
        SqlNodeList caseThens = new SqlNodeList(POS);
        caseThens.add(negCase);
        SqlNode norm =
            new org.apache.calcite.sql.fun.SqlCase(POS, null, caseWhens, caseThens, posCase);
        return new SqlBasicCall(SqlStdOperatorTable.ITEM, List.of(arr, norm), POS);
      }
      SqlNode endIdx =
          new SqlBasicCall(
              org.apache.calcite.sql.fun.SqlLibraryOperators.SAFE_CAST,
              List.of(args.get(2), intSpec),
              POS);
      SqlNode startNeg =
          new SqlBasicCall(SqlStdOperatorTable.LESS_THAN, List.of(startIdx, zero), POS);
      SqlNode startNegCase =
          new SqlBasicCall(SqlStdOperatorTable.PLUS, List.of(arrLen, startIdx), POS);
      SqlNodeList sw = new SqlNodeList(POS);
      sw.add(startNeg);
      SqlNodeList st = new SqlNodeList(POS);
      st.add(startNegCase);
      SqlNode normStart = new org.apache.calcite.sql.fun.SqlCase(POS, null, sw, st, startIdx);
      SqlNode endNeg = new SqlBasicCall(SqlStdOperatorTable.LESS_THAN, List.of(endIdx, zero), POS);
      SqlNode endNegCase = new SqlBasicCall(SqlStdOperatorTable.PLUS, List.of(arrLen, endIdx), POS);
      SqlNodeList ew = new SqlNodeList(POS);
      ew.add(endNeg);
      SqlNodeList et = new SqlNodeList(POS);
      et.add(endNegCase);
      SqlNode normEnd = new org.apache.calcite.sql.fun.SqlCase(POS, null, ew, et, endIdx);
      SqlNode diff = new SqlBasicCall(SqlStdOperatorTable.MINUS, List.of(normEnd, normStart), POS);
      SqlNode len = new SqlBasicCall(SqlStdOperatorTable.PLUS, List.of(diff, one), POS);
      return new SqlBasicCall(
          org.apache.calcite.sql.fun.SqlLibraryOperators.ARRAY_SLICE,
          List.of(arr, normStart, len),
          POS);
    }
    if (opOverride != null) {
      return new SqlBasicCall(opOverride, args, POS);
    }
    String resolvedName = mapPplFunctionName(name, fn.getFuncName());
    return new SqlBasicCall(
        new org.apache.calcite.sql.SqlUnresolvedFunction(
            new SqlIdentifier(resolvedName, POS),
            null,
            null,
            null,
            null,
            org.apache.calcite.sql.SqlFunctionCategory.USER_DEFINED_FUNCTION),
        args,
        POS);
  }

  /**
   * PPL function names whose validator-resolved Calcite operator name differs from the PPL source
   * token. Mirrors PPLFuncImpTable's name-to-operator registrations: when no entry exists, the
   * original name is returned unchanged so the validator looks up by PPL-form name.
   */
  private static String mapPplFunctionName(String lower, String original) {
    return switch (lower) {
      case "mvjoin" -> "ARRAY_JOIN";
      case "mvcount" -> "ARRAY_LENGTH";
      case "mvsort" -> "SORT_ARRAY";
      case "mvdedup" -> "ARRAY_DISTINCT";
      case "mvcontains" -> "ARRAY_CONTAINS";
      case "mvslice" -> "ARRAY_SLICE";
      // mvmap binds to PPLBuiltinOperators.TRANSFORM via PPLFuncImpTable.
      case "mvmap" -> "transform";
      case "ifnull" -> "COALESCE";
      case "json_valid" -> "IS_JSON_VALUE";
      default -> original;
    };
  }

  /**
   * Translate {@code CAST(expr AS type)}. Numeric/string casts use SAFE_CAST. The OpenSearch UDTs
   * (DATE/TIME/TIMESTAMP/IP) dispatch to PPL UDFs because SQL's CAST cannot represent the UDT's
   * row-type — the UDF call establishes the EXPR_* type and uses PPL's format-permissive parser.
   */
  private SqlNode castExpr(Cast c) {
    SqlNode value = expr(c.getExpression());
    DataType targetType = c.getDataType();
    // PPL `cast(<numeric> as ip)` is rejected: only STRING and IP types convert to IP.
    if (targetType == DataType.IP) {
      UnresolvedExpression src = c.getExpression();
      if (src instanceof Literal lit) {
        switch (lit.getType()) {
          case SHORT, INTEGER, LONG, FLOAT, DOUBLE, DECIMAL ->
              throw new IllegalArgumentException(
                  String.format(
                      "Cannot convert %s to IP, only STRING and IP types are supported",
                      lit.getType()));
          default -> {}
        }
      }
    }
    String udtFunc =
        switch (targetType) {
          case IP -> "IP";
          case DATE -> "DATE";
          case TIME -> "TIME";
          case TIMESTAMP -> "TIMESTAMP";
          default -> null;
        };
    if (udtFunc != null) {
      return new SqlBasicCall(
          new org.apache.calcite.sql.SqlUnresolvedFunction(
              new SqlIdentifier(udtFunc, POS),
              null,
              null,
              null,
              null,
              org.apache.calcite.sql.SqlFunctionCategory.USER_DEFINED_FUNCTION),
          List.of(value),
          POS);
    }
    org.apache.calcite.sql.type.SqlTypeName tn = pplTypeToSqlType(targetType);
    // STRING target with floating-point/decimal literal source: Calcite SAFE_CAST stringifies
    // 0.99 → ".99" and 0.0 → "0E0", but PPL expects "0.99" / "0.0" (Java toString semantics).
    // Dispatch to NUMBER_TO_STRING UDF. Without an oracle we only catch literals; non-literal
    // numeric sources fall through to SAFE_CAST.
    if (tn == org.apache.calcite.sql.type.SqlTypeName.VARCHAR
        && c.getExpression() instanceof Literal numLit) {
      switch (numLit.getType()) {
        case FLOAT, DOUBLE, DECIMAL:
          return new SqlBasicCall(
              new org.apache.calcite.sql.SqlUnresolvedFunction(
                  new SqlIdentifier("NUMBER_TO_STRING", POS),
                  null,
                  null,
                  null,
                  null,
                  org.apache.calcite.sql.SqlFunctionCategory.USER_DEFINED_FUNCTION),
              List.of(value),
              POS);
        default:
          // fall through to SAFE_CAST
      }
    }
    // BOOLEAN target with literal source: PPL semantics differ from Calcite's SAFE_CAST.
    //   - numeric literal: `value != 0` (1→true, 0→false, 2→true).
    //   - string literal: '1'→true, '0'→false, anything else→NULL (Spark/Postgres semantics).
    // Without an oracle we can only handle literals; non-literal sources fall through to
    // SAFE_CAST which would return NULL for these cross-family casts.
    if (tn == org.apache.calcite.sql.type.SqlTypeName.BOOLEAN
        && c.getExpression() instanceof Literal lit) {
      switch (lit.getType()) {
        case SHORT, INTEGER, LONG, FLOAT, DOUBLE, DECIMAL:
          return new SqlBasicCall(
              SqlStdOperatorTable.NOT_EQUALS,
              List.of(value, SqlLiteral.createExactNumeric("0", POS)),
              POS);
        case STRING:
          {
            SqlNodeList whens = new SqlNodeList(POS);
            whens.add(
                new SqlBasicCall(
                    SqlStdOperatorTable.EQUALS,
                    List.of(value, SqlLiteral.createCharString("1", POS)),
                    POS));
            whens.add(
                new SqlBasicCall(
                    SqlStdOperatorTable.EQUALS,
                    List.of(value, SqlLiteral.createCharString("0", POS)),
                    POS));
            SqlNodeList thens = new SqlNodeList(POS);
            thens.add(SqlLiteral.createBoolean(true, POS));
            thens.add(SqlLiteral.createBoolean(false, POS));
            return new org.apache.calcite.sql.fun.SqlCase(
                POS, null, whens, thens, SqlLiteral.createNull(POS));
          }
        default:
          // BOOLEAN literal → BOOLEAN: identity, fall through.
      }
    }
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
      // PPL semantics for divide-by-zero is to return NULL (and PPL MOD likewise produces wider
      // type promotion that SqlStdOperatorTable.MOD doesn't match). Bind to PPLBuiltinOperators.
      case "/" -> org.opensearch.sql.expression.function.PPLBuiltinOperators.DIVIDE;
      case "%" -> org.opensearch.sql.expression.function.PPLBuiltinOperators.MOD;
      default -> null;
    };
  }

  /**
   * True if any operand of a {@code +} expression is statically a string — used to pick CONCAT over
   * PLUS when PPL's {@code +} acts as string concatenation.
   */
  /**
   * True when {@code e} is a {@link Span} (or aliased Span) over a time-unit. PPL semantics: a
   * stats with a time-span group key always filters NULL buckets regardless of bucket_nullable.
   */
  private static boolean isTimeSpanGroupKey(UnresolvedExpression e) {
    UnresolvedExpression core = e instanceof Alias al ? al.getDelegated() : e;
    return core instanceof Span sp
        && org.opensearch.sql.ast.expression.SpanUnit.isTimeUnit(sp.getUnit());
  }

  /**
   * True when {@code e} is the PPL parse shape for a bare `null` token: a {@link QualifiedName} (or
   * {@link Field} wrapping one) whose stringified name is "null" case-insensitively. PPL has no
   * typed null literal in the grammar, so any place that wants to accept the keyword `null` must
   * intercept this AST shape.
   */
  private static boolean isNullLiteralRef(UnresolvedExpression e) {
    QualifiedName qn = null;
    if (e instanceof QualifiedName q) qn = q;
    else if (e instanceof Field f && f.getField() instanceof QualifiedName q) qn = q;
    return qn != null && "null".equalsIgnoreCase(qn.toString());
  }

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
    return switch (op.toLowerCase(java.util.Locale.ROOT)) {
      case "=" -> SqlStdOperatorTable.EQUALS;
      case "!=", "<>" -> SqlStdOperatorTable.NOT_EQUALS;
      case "<" -> SqlStdOperatorTable.LESS_THAN;
      case "<=" -> SqlStdOperatorTable.LESS_THAN_OR_EQUAL;
      case ">" -> SqlStdOperatorTable.GREATER_THAN;
      case ">=" -> SqlStdOperatorTable.GREATER_THAN_OR_EQUAL;
      case "like" -> SqlStdOperatorTable.LIKE;
      case "not like", "not_like" -> SqlStdOperatorTable.NOT_LIKE;
      case "ilike" -> org.apache.calcite.sql.fun.SqlLibraryOperators.ILIKE;
      case "not ilike", "not_ilike" -> org.apache.calcite.sql.fun.SqlLibraryOperators.NOT_ILIKE;
      case "regexp" -> org.apache.calcite.sql.fun.SqlLibraryOperators.REGEXP_CONTAINS;
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
      // Calcite's createExactNumeric accepts decimal strings — DECIMAL/FLOAT/DOUBLE literals
      // surface as `1.5`/`3.14e0`-style numerics in PPL ASTs. createApproxNumeric requires
      // scientific notation (e/E exponent), so coerce missing-exponent doubles to a non-exact
      // form first.
      case DECIMAL -> {
        java.math.BigDecimal bd =
            (v instanceof java.math.BigDecimal b) ? b : new java.math.BigDecimal(v.toString());
        yield SqlLiteral.createExactNumeric(bd.toPlainString(), POS);
      }
      case FLOAT, DOUBLE -> {
        String s = v.toString();
        if (!s.contains("e") && !s.contains("E")) {
          s = s + "E0";
        }
        yield SqlLiteral.createApproxNumeric(s, POS);
      }
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
   *
   * <p>Each part is constructed with a quoted parser position so the validator's {@code
   * makeNullaryCall} doesn't shadow it with a niladic system function (e.g. unquoted {@code user} →
   * {@code CURRENT_USER}, unquoted {@code session_user} → {@code SESSION_USER}). Mirrors the legacy
   * SqlNode visitor's QPOS treatment of column references.
   */
  private SqlIdentifier toIdentifier(String name) {
    List<String> parts = name.indexOf('.') < 0 ? List.of(name) : Arrays.asList(name.split("\\."));
    if (exprFrame != null
        && !exprFrame.aliasSynonyms.isEmpty()
        && parts.size() >= 2
        && exprFrame.aliasSynonyms.containsKey(parts.get(0))) {
      List<String> rewritten = new ArrayList<>(parts);
      rewritten.set(0, exprFrame.aliasSynonyms.get(parts.get(0)));
      return quotedIdentifier(rewritten);
    }
    return quotedIdentifier(parts);
  }

  private static final SqlParserPos QPOS = SqlParserPos.ZERO.withQuoting(true);

  private static SqlIdentifier quotedIdentifier(List<String> parts) {
    List<SqlParserPos> positions = new ArrayList<>(parts.size());
    for (int i = 0; i < parts.size(); i++) {
      positions.add(QPOS);
    }
    return new SqlIdentifier(parts, null, POS, positions);
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
