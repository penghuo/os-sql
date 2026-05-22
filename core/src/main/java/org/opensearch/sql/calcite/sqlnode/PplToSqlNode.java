/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.sqlnode;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;
import org.apache.calcite.sql.SqlAggFunction;
import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlOrderBy;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.opensearch.sql.ast.AbstractNodeVisitor;
import org.opensearch.sql.ast.expression.AggregateFunction;
import org.opensearch.sql.ast.expression.Alias;
import org.opensearch.sql.ast.expression.AllFields;
import org.opensearch.sql.ast.expression.And;
import org.opensearch.sql.ast.expression.Argument;
import org.opensearch.sql.ast.expression.Case;
import org.opensearch.sql.ast.expression.Compare;
import org.opensearch.sql.ast.expression.DataType;
import org.opensearch.sql.ast.expression.Field;
import org.opensearch.sql.ast.expression.Function;
import org.opensearch.sql.ast.expression.In;
import org.opensearch.sql.ast.expression.Let;
import org.opensearch.sql.ast.expression.Literal;
import org.opensearch.sql.ast.expression.Not;
import org.opensearch.sql.ast.expression.Or;
import org.opensearch.sql.ast.expression.QualifiedName;
import org.opensearch.sql.ast.expression.UnresolvedExpression;
import org.opensearch.sql.ast.expression.WindowFunction;
import org.opensearch.sql.ast.expression.subquery.InSubquery;
import org.opensearch.sql.ast.tree.Aggregation;
import org.opensearch.sql.ast.tree.Dedupe;
import org.opensearch.sql.ast.tree.Eval;
import org.opensearch.sql.ast.tree.Expand;
import org.opensearch.sql.ast.tree.Filter;
import org.opensearch.sql.ast.tree.Flatten;
import org.opensearch.sql.ast.tree.Head;
import org.opensearch.sql.ast.tree.Limit;
import org.opensearch.sql.ast.tree.Lookup;
import org.opensearch.sql.ast.tree.Parse;
import org.opensearch.sql.ast.tree.Project;
import org.opensearch.sql.ast.tree.Relation;
import org.opensearch.sql.ast.tree.Rename;
import org.opensearch.sql.ast.tree.Sort;
import org.opensearch.sql.ast.tree.StreamWindow;
import org.opensearch.sql.ast.tree.UnresolvedPlan;
import org.opensearch.sql.ast.tree.Window;

/**
 * POC: translate a PPL {@link UnresolvedPlan} pipeline into a Calcite {@link SqlNode} tree.
 *
 * <p>Compilation strategy: accumulate consecutive pipes into a single {@link SqlSelect}, wrapping
 * the in-flight select as a subquery only when the next pipe would violate SQL semantics —
 * specifically:
 *
 * <ul>
 *   <li>WHERE after eval-extended projection (condition might reference aliases from same SELECT
 *       list; SQL only allows that in HAVING).
 *   <li>EVAL/PROJECT after ORDER BY or FETCH (extending the row set past a row-cap is wrong).
 *   <li>PROJECT after EVAL extended the projection (SELECT list aliases not visible in same list).
 *   <li>SORT/HEAD after FETCH already set.
 * </ul>
 *
 * <p>Currently handles {@code source} / {@code where} / {@code eval} / {@code fields} / {@code
 * sort} / {@code head} / {@code limit}; other commands fall through to {@link
 * UnsupportedOperationException}.
 */
public class PplToSqlNode {

  private static final SqlParserPos POS = SqlParserPos.ZERO;

  /**
   * A function-shape stand-in for the postfix {@code IS EMPTY} operator that accepts ANY operand
   * (typically a string). v2's {@code RexBuilder.makeCall(SqlStdOperatorTable.IS_EMPTY, arg)}
   * bypasses operand-type checking, but the SqlNode→SqlValidator path resolves {@code IS EMPTY}
   * by-name to the standard IS_EMPTY whose checker rejects strings. This wrapper is registered as a
   * function ("IS_EMPTY") and post-converted to {@link SqlStdOperatorTable#IS_EMPTY} via a
   * RexShuttle so the final RexCall matches v2's emission.
   */
  static final SqlOperator PERMISSIVE_IS_EMPTY =
      new org.apache.calcite.sql.SqlFunction(
          "IS_EMPTY",
          org.apache.calcite.sql.SqlKind.OTHER_FUNCTION,
          org.apache.calcite.sql.type.ReturnTypes.BOOLEAN_NOT_NULL,
          org.apache.calcite.sql.type.InferTypes.VARCHAR_1024,
          new org.apache.calcite.sql.type.SqlOperandTypeChecker() {
            @Override
            public boolean checkOperandTypes(
                org.apache.calcite.sql.SqlCallBinding callBinding, boolean throwOnFailure) {
              return true;
            }

            @Override
            public org.apache.calcite.sql.SqlOperandCountRange getOperandCountRange() {
              return org.apache.calcite.sql.type.SqlOperandCountRanges.of(1);
            }

            @Override
            public String getAllowedSignatures(SqlOperator op, String opName) {
              return "<ANY> IS EMPTY";
            }
          },
          org.apache.calcite.sql.SqlFunctionCategory.USER_DEFINED_FUNCTION);

  /**
   * Function names whose string-literal args MUST stay as CHAR (no VARCHAR cast). Pushdown for
   * regex/LIKE/replace/fillnull/transpose patterns relies on detecting CHAR-typed literals to route
   * through specific scripted-pattern paths; casting to VARCHAR breaks that detection. Most other
   * UDFs are fine with VARCHAR-cast literals (v2's RexBuilder default).
   */
  private static final java.util.Set<String> FUNC_NAMES_KEEP_CHAR_LITERAL =
      java.util.Set.of(
          "like",
          "ilike",
          "not_like",
          "not_ilike",
          "rlike",
          "regexp",
          "regexp_match",
          "regexp_replace",
          "regexp_extract",
          "regexp_extract_all",
          "regex",
          "replace",
          "fillnull",
          "transpose",
          // array(...) literals stay CHAR — the array's element type is derived from the
          // least-restrictive common type of the literals; v2 emits these as bare CHAR.
          "array",
          "mvappend",
          // mvjoin's delimiter literal stays CHAR (matches v2 emission for ARRAY_JOIN).
          "mvjoin",
          "array_join");

  // Quoted parser position — the SqlValidator's makeNullaryCall converts unquoted identifiers
  // matching system-function names (USER, CURRENT_USER, LOCALTIME, ...) into niladic function
  // calls, shadowing column references with the same name. Use this on column-reference
  // identifiers to bypass the niladic-function lookup.
  private static final SqlParserPos QPOS = SqlParserPos.ZERO.withQuoting(true);

  /**
   * Optional row-type oracle. Some PPL commands (flatten, lookup REPLACE, rename) need to know the
   * columns of the input row type at translation time to emit the right projection. The caller
   * (typically {@link SqlNodePlanner}) supplies a function that runs the partially-built {@link
   * SqlNode} through Calcite's validator and returns its row type. Without an oracle, those
   * commands raise {@link UnsupportedOperationException}.
   */
  private final java.util.function.Function<SqlNode, org.apache.calcite.rel.type.RelDataType>
      rowTypeOracle;

  /**
   * Whether the current pipeline has visible table aliases (set after a Join/Lookup or multi-source
   * Aggregation introduces them). Drives how PPL QualifiedNames with multiple parts are emitted as
   * SqlIdentifier — see {@link #qualifiedNameToFieldIdentifier}.
   */
  private boolean joinScope = false;

  /**
   * Probe FROM for {@link #tryAliasMapItemAccess}. visitJoin's ON clause is processed BEFORE the
   * SqlJoin is constructed, so the in-flight {@code state.from} only carries the left side at that
   * moment. Set this to a synthetic {@code aliasedLeft, aliasedRight} pair so the alias-prefixed
   * MAP-leaf probe can resolve {@code <alias>.<col>}.
   */
  private SqlNode joinProbeFrom;

  /**
   * Alias names of the join sides currently being processed. Used by isKnownJoinAlias during ON
   * clause evaluation, before state.joinLeftAlias/state.joinRightAlias are assigned.
   */
  private String activeJoinLeftAlias;

  private String activeJoinRightAlias;

  /**
   * Alias synonym map: alternative name → canonical SQL alias name. PPL allows both an inner
   * SubqueryAlias (e.g. `[Y as tt]`) and an explicit JOIN-arg alias (e.g. `right=t2`) to coexist on
   * the same join side. SQL only supports one alias per side, so we keep the explicit one as the
   * canonical name and record the alternative as a synonym; column-reference emission in {@link
   * #qualifiedNameToFieldIdentifier} rewrites `<synonym>.<col>` to `<canonical>.<col>`.
   */
  private final java.util.Map<String, String> joinAliasSynonyms = new java.util.HashMap<>();

  /**
   * Counter for generating unique default join aliases. Multi-join chains like `source = X | JOIN
   * ... Y | JOIN ... Z` keep the in-flight SqlJoin visible to the next ON clause; if both joins
   * used the same default name (`__l`/`__r`), the validator complains about duplicate FROM-clause
   * aliases. Bumping per join keeps subsequent default aliases unique.
   */
  private int joinAliasCounter = 0;

  /**
   * Depth of nested lambda emission. When > 0, function dispatchers can use looser operand-coercion
   * heuristics (e.g. wrap CHAR_LENGTH operand in CAST AS VARCHAR) since SqlLambda parameters are
   * typed ANY at validate time and standard operand checkers reject ANY.
   */
  private int lambdaDepth = 0;

  private String defaultLeftAlias() {
    return joinAliasCounter == 1 ? "__l" : "__l" + joinAliasCounter;
  }

  private String defaultRightAlias() {
    return joinAliasCounter == 1 ? "__r" : "__r" + joinAliasCounter;
  }

  /**
   * The current pipeline's FROM source. Set by Builder during walk so outer-class helpers (cmp /
   * expr / etc.) can introspect column types via {@link #rowTypeOracle} when needed — specifically
   * for EXPR_IP detection in {@link #cmp(Compare)} which dispatches IP-aware comparison operators.
   */
  private SqlNode currentFrom;

  /**
   * Reference to the active Builder's in-flight {@link Pipeline} state so outer-class helpers can
   * introspect pending eval projections (columns introduced by {@code eval x = ...} aren't yet in
   * {@link #currentFrom}'s row type until after a wrap). Used by {@link #tryMapOrStructItemAccess}
   * to detect MAP/STRUCT columns added by upstream eval.
   */
  private Pipeline currentState;

  /**
   * Subsearch row cap (PPL setting plugins.ppl.join.subsearch_maxout). Applied as a top-level FETCH
   * on IN-subquery body when > 0. Threaded through nested PplToSqlNode constructions so inner
   * subqueries inherit the same cap.
   */
  private final int subsearchLimit;

  private final int joinSubsearchLimit;

  public PplToSqlNode() {
    this(null, 0, 0);
  }

  public PplToSqlNode(
      java.util.function.Function<SqlNode, org.apache.calcite.rel.type.RelDataType> rowTypeOracle) {
    this(rowTypeOracle, 0, 0);
  }

  public PplToSqlNode(
      java.util.function.Function<SqlNode, org.apache.calcite.rel.type.RelDataType> rowTypeOracle,
      int subsearchLimit) {
    this(rowTypeOracle, subsearchLimit, 0);
  }

  public PplToSqlNode(
      java.util.function.Function<SqlNode, org.apache.calcite.rel.type.RelDataType> rowTypeOracle,
      int subsearchLimit,
      int joinSubsearchLimit) {
    this.rowTypeOracle = rowTypeOracle;
    this.subsearchLimit = subsearchLimit;
    this.joinSubsearchLimit = joinSubsearchLimit;
  }

  private List<String> deriveColumnNames(SqlNode partialFrom) {
    return deriveRowType(partialFrom).getFieldList().stream()
        .map(org.apache.calcite.rel.type.RelDataTypeField::getName)
        .toList();
  }

  /**
   * Probe the validator to determine the legacy PPL type name of {@code expr} when evaluated in the
   * current FROM scope. Used by typeof() expansion. Falls back to "UNKNOWN" if the oracle can't
   * deduce the type.
   */
  private String probeTypeName(SqlNode expr) {
    if (rowTypeOracle == null) return "UNKNOWN";
    try {
      SqlNodeList selectList = new SqlNodeList(POS);
      selectList.add(expr);
      SqlSelect probe =
          new SqlSelect(
              POS,
              null,
              selectList,
              currentFrom,
              null,
              null,
              null,
              null,
              null,
              null,
              null,
              null,
              null);
      org.apache.calcite.rel.type.RelDataType rowType = rowTypeOracle.apply(probe);
      org.apache.calcite.rel.type.RelDataType type = rowType.getFieldList().get(0).getType();
      return org.opensearch.sql.calcite.utils.OpenSearchTypeFactory.getLegacyTypeName(
          type, org.opensearch.sql.executor.QueryType.PPL);
    } catch (RuntimeException ignored) {
      return "UNKNOWN";
    }
  }

  /**
   * Probe the validator for the {@link SqlTypeName} of {@code expr} when evaluated in the current
   * FROM scope. Returns null if the oracle can't deduce the type (e.g. expr is a lambda param or
   * the FROM has ambiguous columns from a multi-instance same-table join).
   */
  private org.apache.calcite.sql.type.SqlTypeName probeSqlTypeName(SqlNode expr) {
    if (rowTypeOracle == null || currentFrom == null) return null;
    try {
      SqlNodeList selectList = new SqlNodeList(POS);
      selectList.add(expr);
      SqlSelect probe =
          new SqlSelect(
              POS,
              null,
              selectList,
              currentFrom,
              null,
              null,
              null,
              null,
              null,
              null,
              null,
              null,
              null);
      org.apache.calcite.rel.type.RelDataType rowType = rowTypeOracle.apply(probe);
      return rowType.getFieldList().get(0).getType().getSqlTypeName();
    } catch (RuntimeException ignored) {
      return null;
    }
  }

  /** Probe the validator to get the row type of {@code partialFrom}. */
  private org.apache.calcite.rel.type.RelDataType deriveRowType(SqlNode partialFrom) {
    if (rowTypeOracle == null) {
      throw new UnsupportedOperationException(
          "This PPL pipe needs schema introspection (flatten / rename / lookup REPLACE / etc.);"
              + " supply a row-type oracle to PplToSqlNode to enable it.");
    }
    SqlSelect probe =
        new SqlSelect(
            POS,
            null,
            starList(),
            partialFrom,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null);
    return rowTypeOracle.apply(probe);
  }

  /** Public entry point. */
  public SqlNode visit(UnresolvedPlan plan) {
    Pipeline state = new Pipeline();
    // Source-inject `__stream_seq__` only when MULTIPLE StreamWindows exist (multi-streamstats
    // needs a shared seq across pipes for stable cumulative ordering). A single streamstats
    // synthesizes its own seq locally — source injection there can cause subquery/outer seq
    // ambiguity in IN-subquery contexts.
    state.injectStreamSeq = countStreamWindows(plan) > 1;
    new Builder(state).walk(plan);
    return state.toFinalSqlNode();
  }

  /** Count StreamWindow nodes in the plan tree (descending into UnresolvedPlan children). */
  private static int countStreamWindows(UnresolvedPlan plan) {
    int n = (plan instanceof org.opensearch.sql.ast.tree.StreamWindow) ? 1 : 0;
    for (org.opensearch.sql.ast.Node child : plan.getChild()) {
      if (child instanceof UnresolvedPlan p) n += countStreamWindows(p);
    }
    return n;
  }

  /** Walks a PPL pipeline (linked through {@code child}) bottom-up. */
  private final class Builder extends AbstractNodeVisitor<Void, Void> {
    private final Pipeline state;

    Builder(Pipeline state) {
      this.state = state;
    }

    void walk(UnresolvedPlan plan) {
      currentState = state;
      plan.accept(this, null);
      currentFrom = state.from;
    }

    private void walkChild(UnresolvedPlan plan) {
      if (!plan.getChild().isEmpty()) {
        plan.getChild().get(0).accept(this, null);
      }
      // Sync after walking so outer-class helpers can introspect the current FROM for
      // type-aware dispatch (e.g. cmp() uses it for EXPR_IP detection).
      currentFrom = state.from;
    }

    /**
     * Materialize any pending outer SORT/FETCH (from upstream {@code head}/{@code limit}/{@code
     * sort}) into the in-flight inner select. Required before any pipe that changes the row set
     * (Aggregation, Dedup, Join, Lookup, RareTopN) so the row-cap applies to the input rather than
     * to the post-pipe output.
     */
    private void flushOuterIntoInner() {
      if (state.outerFetch != null || state.outerOrderBy != null || state.outerOffset != null) {
        state.orderBy = state.outerOrderBy;
        state.fetch = state.outerFetch;
        state.offset = state.outerOffset;
        if (state.outerOrderBy != null) {
          state.lastOrderBy = state.outerOrderBy;
        }
        state.outerOrderBy = null;
        state.outerFetch = null;
        state.outerOffset = null;
      }
    }

    /**
     * Split {@code state.where} into AND-conjuncts and partition them by whether all their column
     * references are present in {@code state.from}'s row type. Conjuncts whose refs are all local
     * stay in {@code state.where}; conjuncts that reference an unknown name are removed from {@code
     * state.where} and returned (the caller re-applies them AFTER {@link Pipeline#wrap}).
     *
     * <p>Used by visitors that wrap an in-flight SqlSelect when {@link #joinScope} is true (i.e.
     * we're inside an EXISTS/IN/scalar-subquery body). The wrap creates a 2-level SELECT nesting,
     * and Calcite's validator only climbs one level when resolving outer correlations — so a
     * conjunct like {@code WHERE SAL > 1000} (where SAL is on the outer table) must live above the
     * wrap, not below it.
     *
     * <p>Returns null if no conjunct needs lifting (or if the probe fails). Otherwise returns the
     * list of conjuncts to re-apply after wrap, with {@code state.where} reduced to the local-only
     * portion (or null if everything was lifted).
     */
    private java.util.List<SqlNode> liftCorrelatedConjuncts() {
      if (state.where == null || state.from == null || rowTypeOracle == null) return null;
      java.util.Set<String> localCols;
      try {
        localCols = new java.util.HashSet<>(deriveColumnNames(state.from));
      } catch (RuntimeException ignored) {
        return null;
      }
      java.util.List<SqlNode> conjuncts = splitTopLevelAnd(state.where);
      java.util.List<SqlNode> kept = new java.util.ArrayList<>();
      java.util.List<SqlNode> lifted = new java.util.ArrayList<>();
      for (SqlNode c : conjuncts) {
        if (referencesOnlyLocal(c, localCols)) {
          kept.add(c);
        } else {
          lifted.add(c);
        }
      }
      if (lifted.isEmpty()) return null;
      // Rebuild state.where from the kept conjuncts (or null when none remain).
      state.where = null;
      for (SqlNode k : kept) state.addWhere(k);
      return lifted;
    }

    /**
     * Wrap an AS-aliased FROM expression with FETCH N inside the alias. Post-conversion this
     * becomes a {@code LogicalSort(fetch=N, no collation)} in the rel tree directly above the inner
     * subquery's content — capping raw scan rows that flow into the lifted+merged correlation
     * filter at the outer wrap.
     *
     * <p>Input shape: {@code AS(<select>, <alias>)} or just {@code <select>} (rare). Output: {@code
     * AS(SqlOrderBy(<select>, [], null, fetch=N), <alias>)}.
     */
    private SqlNode wrapFromWithSubsearchFetch(SqlNode from, int fetchVal) {
      SqlNode select = from;
      SqlNode aliasName = null;
      if (from instanceof SqlBasicCall sbc && sbc.getOperator() == SqlStdOperatorTable.AS) {
        select = sbc.getOperandList().get(0);
        aliasName = sbc.getOperandList().get(1);
      }
      SqlNode fetchLit = SqlLiteral.createExactNumeric(Integer.toString(fetchVal), POS);
      SqlNode capped =
          new org.apache.calcite.sql.SqlOrderBy(
              POS, select, new SqlNodeList(POS), /* offset */ null, fetchLit);
      if (aliasName != null) {
        return new SqlBasicCall(SqlStdOperatorTable.AS, List.of(capped, aliasName), POS);
      }
      return new SqlBasicCall(
          SqlStdOperatorTable.AS, List.of(capped, new SqlIdentifier("__pipe_in_", POS)), POS);
    }

    /**
     * Flatten AND-tree at the top level into a list. Non-AND nodes return as a single-element list.
     */
    private static java.util.List<SqlNode> splitTopLevelAnd(SqlNode cond) {
      java.util.List<SqlNode> out = new java.util.ArrayList<>();
      java.util.ArrayDeque<SqlNode> stack = new java.util.ArrayDeque<>();
      stack.push(cond);
      while (!stack.isEmpty()) {
        SqlNode n = stack.pop();
        if (n instanceof SqlBasicCall sbc && sbc.getOperator() == SqlStdOperatorTable.AND) {
          for (int i = sbc.getOperandList().size() - 1; i >= 0; i--) {
            stack.push(sbc.getOperandList().get(i));
          }
        } else {
          out.add(n);
        }
      }
      return out;
    }

    /**
     * True iff every single-part SqlIdentifier in {@code expr} resolves to a name in {@code
     * localCols}. Multi-part identifiers (e.g. {@code outer.col}) are treated as outer references
     * and cause this method to return false. Non-identifier nodes (literals, function calls, etc.)
     * descend into operands.
     */
    private static boolean referencesOnlyLocal(SqlNode expr, java.util.Set<String> localCols) {
      if (expr instanceof SqlIdentifier id) {
        if (id.isStar()) return true;
        if (id.names.size() == 1) {
          return localCols.contains(id.names.get(0));
        }
        // Multi-part: outer-table-qualified ref, treat as foreign.
        return false;
      }
      if (expr instanceof SqlBasicCall call) {
        for (SqlNode op : call.getOperandList()) {
          if (op != null && !referencesOnlyLocal(op, localCols)) return false;
        }
        return true;
      }
      if (expr instanceof SqlNodeList list) {
        for (SqlNode op : list) {
          if (op != null && !referencesOnlyLocal(op, localCols)) return false;
        }
        return true;
      }
      return true;
    }

    @Override
    public Void visitRelation(Relation node, Void ignored) {
      // PPL `source=a, b` matches the existing CalciteRelNodeVisitor: pass the comma-joined name
      // through as a single scan. OpenSearch's storage engine resolves comma-separated indices
      // (and wildcard patterns) natively and dedups identical indices in the result. This mirrors
      // `relBuilder.scan(getTableQualifiedName().getParts())` in the v2 visitor.
      QualifiedName joinedName = node.getTableQualifiedName();
      SqlNode tableId = qualifiedNameToIdentifier(joinedName);
      if (state.injectStreamSeq) {
        // Wrap the source with `SELECT *, ROW_NUMBER() OVER () AS __stream_seq__ FROM table`.
        // All streamstats pipes downstream reference this synthesized column for stable ordering.
        SqlNode rnWindow =
            org.apache.calcite.sql.SqlWindow.create(
                null,
                null,
                new SqlNodeList(POS),
                new SqlNodeList(POS),
                SqlLiteral.createBoolean(false, POS),
                null,
                null,
                null,
                POS);
        SqlNode rnOver =
            new SqlBasicCall(
                SqlStdOperatorTable.OVER,
                List.of(new SqlBasicCall(SqlStdOperatorTable.ROW_NUMBER, List.of(), POS), rnWindow),
                POS);
        SqlNodeList selectList = new SqlNodeList(POS);
        selectList.add(SqlIdentifier.star(POS));
        selectList.add(asAlias(rnOver, "__stream_seq__"));
        SqlSelect wrapped =
            new SqlSelect(
                POS,
                null,
                selectList,
                tableId,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null);
        state.setFrom(wrapped);
      } else {
        state.setFrom(tableId);
      }
      return null;
    }

    @Override
    public Void visitSubqueryAlias(org.opensearch.sql.ast.tree.SubqueryAlias node, Void ignored) {
      // PPL `source=X as i` parses as SubqueryAlias("i", Relation(X)). Visit the inner relation,
      // then wrap state.from in `<from> AS <alias>` so qualified references like `i.col` resolve.
      walkChild(node);
      if (state.from != null) {
        state.from =
            new SqlBasicCall(
                SqlStdOperatorTable.AS,
                List.of(state.from, new SqlIdentifier(node.getAlias(), POS)),
                POS);
        joinScope = true;
        // Record the alias so downstream wrap()s (in Stats, Sort, etc.) can re-attach it.
        state.subqueryAliasName = node.getAlias();
      }
      return null;
    }

    @Override
    public Void visitFilter(Filter node, Void ignored) {
      walkChild(node);
      // PPL `head N | where ...` filters from the first N rows. Without this flush, the row-cap
      // would apply outermost (after filter), which is wrong.
      flushOuterIntoInner();
      // WHERE evaluates before SELECT; aliases introduced by upstream Eval/Project aren't legal
      // in a same-level WHERE. Wrap if projection has already been customised, or if we have
      // a row-cap from the flush above.
      //
      // Also wrap when an upstream filter already added a where clause — PPL preserves each
      // filter as a separate LogicalFilter rel. Without the wrap we collapse adjacent filters
      // into one AND-combined filter. The wrap inserts an implicit SELECT * which becomes a
      // LogicalProject in the rel; the trim-identity-projects shuttle in SqlNodePlanner removes
      // those before final emission.
      if (state.evalExtended
          || state.projectionReplaced
          || state.orderBy != null
          || state.fetch != null
          || state.where != null) {
        // Inside a subquery body (joinScope), an outer-correlated WHERE conjunct (e.g.
        // `where SAL > 1000` where SAL is on the outer table) must be lifted ABOVE the
        // wrap. Calcite's validator only climbs ONE SELECT level when resolving outer
        // correlations; if the conjunct stays sealed inside the inner SELECT, the wrap
        // creates a two-level nesting and the validator fails to find the outer column.
        // Helper: split state.where into local-only vs has-foreign-ref conjuncts.
        java.util.List<SqlNode> liftedToOuter = null;
        if (joinScope && state.where != null) {
          liftedToOuter = liftCorrelatedConjuncts();
        }
        state.wrap();
        if (liftedToOuter != null) {
          for (SqlNode c : liftedToOuter) state.addWhere(c);
          // SUBSEARCH_MAXOUT cap injection (Option B1).
          //
          // The lift just collapsed an upstream `where <corr>` and a downstream `where <non-corr>`
          // into a single combined filter at this outer wrap level. The post-conversion v2 shuttle
          // sees one combined filter and splits it — placing the cap BETWEEN corr and non-corr,
          // which matches single-pipe semantics (testSubsearchMaxOut2) but NOT separate-pipe
          // semantics (testSubsearchMaxOut3 — cap should sit above the raw scan instead, with all
          // filters above it).
          //
          // Inject a FETCH on the inner SELECT we just wrapped so the cap applies to raw scan
          // rows BEFORE any of the lifted/merged filters. The post-conversion shuttle still adds
          // a LogicalSystemLimit under the corr filter; the FETCH-derived LogicalSort sits below
          // that and dominates the row count. Only fires when subsearchLimit > 0.
          if (subsearchLimit > 0) {
            state.from = wrapFromWithSubsearchFetch(state.from, subsearchLimit);
          }
        }
      }
      state.addWhere(expr(node.getCondition()));
      return null;
    }

    @Override
    public Void visitSearch(org.opensearch.sql.ast.tree.Search node, Void ignored) {
      walkChild(node);
      // PPL Search => query_string(MAP('query', text)) filter. The PPL relevance UDF declares a
      // strict operand checker (14-MAP and 25-MAP forms with optional positions 1-13/24); the
      // composite-or in CompositeOperandTypeChecker rejects single-MAP calls because both
      // alternatives' position-0-required check passes but the overall arity check trips on the
      // 14-element list. Wrap the operand list to satisfy at minimum a 1-MAP form by emitting
      // through SqlNodePlanner.permissiveQueryString — a helper SqlOperator that delegates to
      // the real QUERY_STRING but accepts variadic MAP operands.
      // Cast the VALUE to VARCHAR explicitly (not the key). Without the cast, Calcite widens
      // unequal-length CHAR literals to a common max-length CHAR(N) and right-pads the shorter
      // value with spaces — this turns `'severityText:ERR*'` into `'severityText:ERR*    '` which
      // breaks the OpenSearch pushdown's exact-string match. Mirrors v2 emission shape (key bare,
      // value cast to VARCHAR).
      SqlNode queryArg =
          new SqlBasicCall(
              SqlStdOperatorTable.MAP_VALUE_CONSTRUCTOR,
              List.of(
                  SqlLiteral.createCharString("query", POS),
                  castTo(
                      SqlLiteral.createCharString(node.getQueryString(), POS),
                      org.apache.calcite.sql.type.SqlTypeName.VARCHAR)),
              POS);
      // Build the call against the actual QUERY_STRING operator. Validator's operand-check still
      // rejects single-MAP, so we use a SqlUnresolvedFunction with a name that PPLBuiltinOperators
      // resolves by name lookup but bypasses operand-checker re-resolution. The validator finds
      // the operator and invokes its operand checker — same behavior. Approach used: emit as
      // SqlBasicCall against a permissive wrapper operator that we register in SqlNodePlanner.
      SqlNode call =
          new SqlBasicCall(
              org.opensearch.sql.calcite.sqlnode.PermissiveRelevanceFunctions.QUERY_STRING,
              List.of(queryArg),
              POS);
      state.addWhere(call);
      return null;
    }

    @Override
    public Void visitEval(Eval node, Void ignored) {
      walkChild(node);
      // Eval extends the row set; if a row-cap already pinned it, wrap.
      if (state.orderBy != null || state.fetch != null) {
        state.wrap();
      }
      // After a JOIN with field-list (which sets a deduped projection), a downstream eval that
      // references a column name like `department` would otherwise resolve against state.from
      // (the SqlJoin) where both sides expose the same column → "ambiguous" validator error. Wrap
      // so the eval references the projected (deduped) columns of the wrapped subquery instead.
      // Heuristic: state.projectionReplaced is true and no eval aliases yet — only commands like
      // join/lookup/stats reach this combination. Eval-only chains have evalAliasNames populated.
      if (state.projectionReplaced
          && state.evalAliasNames.isEmpty()
          && !state.evalExtended
          && state.from instanceof org.apache.calcite.sql.SqlJoin) {
        state.wrap();
      }
      // If we have an oracle and the projection is still SELECT *, seed it with the non-metadata
      // columns enumerated from the FROM. This stops the eval-extended SELECT list from inheriting
      // OpenSearch's hidden metadata fields (_id, _index, _score, ...) via SELECT *.
      if (state.projection == null
          && rowTypeOracle != null
          && state.from != null
          && !node.getExpressionList().isEmpty()) {
        List<String> cols = null;
        try {
          cols = deriveColumnNames(state.from);
        } catch (RuntimeException ignored2) {
          // Probe failed — likely because state.from is a multi-instance same-table SqlJoin
          // and `*` expansion finds ambiguous column names. Fall through; the eval will keep
          // SELECT * which the validator will reject downstream with the same error.
        }
        if (cols != null) {
          boolean hasMeta =
              cols.stream()
                  .anyMatch(
                      org.opensearch.sql.calcite.plan.OpenSearchConstants.METADATAFIELD_TYPE_MAP
                          ::containsKey);
          // Drop struct parents only when this eval introduces a DOTTED-name alias that would
          // collide with a struct parent (e.g. `eval resource.X = ...`). Don't drop flat dotted
          // leaves at the seed — downstream `eval` / `where` may reference them directly. The
          // user-facing leaf-vs-parent pruning happens at the implicit-fields-* projection
          // (visitProject's default branch).
          boolean hasDottedAlias = false;
          for (Let let : node.getExpressionList()) {
            if (letName(let).contains(".")) {
              hasDottedAlias = true;
              break;
            }
          }
          java.util.Set<String> ancestorStructs =
              hasDottedAlias
                  ? collectAncestorStructs(new java.util.HashSet<>(cols))
                  : java.util.Collections.emptySet();
          if (hasMeta || !ancestorStructs.isEmpty()) {
            List<SqlNode> seeded = new ArrayList<>();
            for (String c : cols) {
              if (org.opensearch.sql.calcite.plan.OpenSearchConstants.METADATAFIELD_TYPE_MAP
                  .containsKey(c)) {
                continue;
              }
              if (ancestorStructs.contains(c)) {
                continue;
              }
              seeded.add(new SqlIdentifier(c, POS));
            }
            state.projection = seeded;
          }
        }
      }
      // PPL `eval a=X, b=fn(a)` is left-to-right: b references a's value. SQL doesn't allow
      // SELECT-list aliases to be visible inside the same SELECT list. Wrap between assignments
      // when an earlier eval-introduced name is referenced by a later expression in the same
      // pipe — and across pipes when a separate eval references a previously-introduced alias.
      // Also include any projected EXPLICIT-AS aliases from upstream pipes (e.g. eventstats sets
      // a projection with new aliases like avg_age that aren't recorded in evalAliasNames).
      // Only count `expr AS alias` entries (SqlBasicCall with AS) — not bare-identifier projections
      // which are passthrough and don't introduce new alias names.
      java.util.Set<String> definedSoFar = new java.util.HashSet<>(state.evalAliasNames);
      if (state.projection != null) {
        for (SqlNode n : state.projection) {
          if (n instanceof SqlBasicCall bc && bc.getOperator() == SqlStdOperatorTable.AS) {
            String aliasName = identifierName(n);
            if (aliasName != null) definedSoFar.add(aliasName);
          }
        }
      }
      for (Let let : node.getExpressionList()) {
        UnresolvedExpression rhs = let.getExpression();
        if (referencesAny(rhs, definedSoFar)) {
          state.wrap();
          definedSoFar = new java.util.HashSet<>();
          // Sync the outer-class probe context so type-aware dispatch (e.g. cast() probing
          // numeric vs IP source) sees the wrapped inner SELECT containing prior eval aliases.
          currentFrom = state.from;
        }
        // After any wrap above, `arr` may now be a column of the subquery FROM. PPL eval
        // semantics: re-binding an existing column overrides it. Without override-handling,
        // SELECT *, expr AS arr produces an ambiguous `arr`. Detect this and rewrite the
        // current projection (or seed an explicit one) so the existing entry is dropped before
        // we append the override.
        String name = letName(let);
        java.util.Set<String> existingNames = new java.util.HashSet<>(state.evalAliasNames);
        if (rowTypeOracle != null && state.from != null) {
          try {
            existingNames.addAll(deriveColumnNames(state.from));
          } catch (RuntimeException ignored2) {
            // probe failed; skip override detection
          }
        }
        if (existingNames.contains(name)) {
          if (state.projection != null) {
            List<SqlNode> filtered = new ArrayList<>();
            for (SqlNode n : state.projection) {
              String existing = identifierName(n);
              if (existing != null && existing.equals(name)) {
                continue;
              }
              filtered.add(n);
            }
            // If after filter we still have a `*` and the FROM contains the name, we must
            // expand the star explicitly to remove the overlapping column.
            boolean stillHasStar =
                filtered.stream().anyMatch(n -> n instanceof SqlIdentifier id && id.isStar());
            if (stillHasStar && state.from != null) {
              try {
                List<String> cols = deriveColumnNames(state.from);
                if (cols.contains(name)) {
                  List<SqlNode> expanded = new ArrayList<>();
                  for (SqlNode n : filtered) {
                    if (n instanceof SqlIdentifier id && id.isStar()) {
                      for (String c : cols) {
                        if (!c.equals(name)
                            && !org.opensearch.sql.calcite.plan.OpenSearchConstants
                                .METADATAFIELD_TYPE_MAP
                                .containsKey(c)) {
                          expanded.add(new SqlIdentifier(c, POS));
                        }
                      }
                    } else {
                      expanded.add(n);
                    }
                  }
                  filtered = expanded;
                }
              } catch (RuntimeException ignored3) {
                // probe failed
              }
            }
            state.projection = filtered;
          } else if (state.from != null) {
            try {
              List<String> cols =
                  deriveColumnNames(state.from).stream()
                      .filter(
                          c ->
                              !c.equals(name)
                                  && !org.opensearch.sql.calcite.plan.OpenSearchConstants
                                      .METADATAFIELD_TYPE_MAP
                                      .containsKey(c))
                      .toList();
              List<SqlNode> seeded = new ArrayList<>();
              for (String c : cols) {
                seeded.add(new SqlIdentifier(c, POS));
              }
              state.projection = seeded;
              state.projectionReplaced = true;
            } catch (RuntimeException ignored2) {
              // probe failed
            }
          }
          state.evalAliasNames.remove(name);
        }
        SqlNode rhsExpr = expr(rhs);
        // For a bare string literal RHS (e.g. `eval t='03:45.5'`), cast to VARCHAR explicitly.
        // PPL string literals are VARCHAR semantically; without this cast Calcite's
        // type-coercion of `eval t='03:45.5' | convert mstime(t)` inlines `t` as a CHAR literal,
        // which the explain test display shows as `'03:45.5'` instead of `'03:45.5':VARCHAR`.
        // Limit to bare string literals (not function-call results) to avoid breaking LIKE/regex
        // patterns whose pushdown relies on CHAR-typed match.
        if (rhs instanceof Literal lit && lit.getType() == DataType.STRING) {
          rhsExpr = charToVarchar(rhsExpr);
        }
        // FieldFormat: PPL `fieldformat alias = "prefix" . expr . "suffix"` parses into a Let
        // with optional concatPrefix/concatSuffix literals. Rewrite to NULL-preserving SQL
        // concat (`||`) — v2's emission shape uses the `||` operator (binary, NULL-propagating),
        // not the CONCAT_FUNCTION (which coerces NULL to empty string).
        if (let.getConcatPrefix() != null || let.getConcatSuffix() != null) {
          SqlNode strValue =
              new SqlBasicCall(
                  SqlStdOperatorTable.CAST,
                  List.of(
                      rhsExpr,
                      new org.apache.calcite.sql.SqlDataTypeSpec(
                          new org.apache.calcite.sql.SqlBasicTypeNameSpec(
                              org.apache.calcite.sql.type.SqlTypeName.VARCHAR, POS),
                          POS)),
                  POS);
          // Cast prefix/suffix string literals to VARCHAR — Calcite's createCharString produces
          // CHAR which when concatenated via `||` would produce CHAR-padded output. v2's
          // emission shows `||('$':VARCHAR, TOSTRING(...))` with explicit VARCHAR.
          rhsExpr = strValue;
          if (let.getConcatPrefix() != null) {
            SqlNode prefix =
                charToVarchar(
                    SqlLiteral.createCharString(let.getConcatPrefix().getValue().toString(), POS));
            rhsExpr = new SqlBasicCall(SqlStdOperatorTable.CONCAT, List.of(prefix, rhsExpr), POS);
          }
          if (let.getConcatSuffix() != null) {
            SqlNode suffix =
                charToVarchar(
                    SqlLiteral.createCharString(let.getConcatSuffix().getValue().toString(), POS));
            rhsExpr = new SqlBasicCall(SqlStdOperatorTable.CONCAT, List.of(rhsExpr, suffix), POS);
          }
        }
        state.addEvalAlias(rhsExpr, name);
        definedSoFar.add(name);
      }
      return null;
    }

    /** Returns true if the AST contains a QualifiedName whose first part is in the names set. */
    private static boolean referencesAny(
        org.opensearch.sql.ast.Node node, java.util.Set<String> names) {
      if (node == null) return false;
      if (node instanceof QualifiedName qn
          && !qn.getParts().isEmpty()
          && names.contains(qn.getParts().get(0))) {
        return true;
      }
      for (org.opensearch.sql.ast.Node child : node.getChild()) {
        if (referencesAny(child, names)) return true;
      }
      return false;
    }

    @Override
    public Void visitProject(Project node, Void ignored) {
      walkChild(node);
      if (node.isExcluded()) {
        // `fields - a, b` => SELECT <all-cols-minus-a-b> FROM ...
        // SQL has no native EXCEPT-projection, so we enumerate the input columns via the oracle
        // and emit an explicit SELECT list that omits the excluded names. Metadata fields are
        // also filtered out so downstream pipes don't see _id/_index/_score.
        if (state.evalExtended || state.projectionReplaced) {
          state.wrap();
        }
        // `fields - a, b, *suffix*` may include wildcards; expand them against the input row
        // type and produce a unified excluded-name set.
        List<String> cols = deriveColumnNames(state.from);
        java.util.Set<String> excluded = new java.util.HashSet<>();
        for (UnresolvedExpression e : node.getProjectList()) {
          String name;
          if (e instanceof Field f) {
            name = f.getField().toString();
          } else if (e instanceof QualifiedName qn) {
            name = qn.toString();
          } else {
            throw new UnsupportedOperationException(
                "fields - operand must be a column reference, got: " + e.getClass());
          }
          if (name.contains("*")) {
            // Expand wildcard against input cols.
            StringBuilder rx = new StringBuilder("^");
            for (int i = 0; i < name.length(); i++) {
              char ch = name.charAt(i);
              if (ch == '*') {
                rx.append(".*");
              } else {
                rx.append(java.util.regex.Pattern.quote(String.valueOf(ch)));
              }
            }
            rx.append("$");
            java.util.regex.Pattern pat = java.util.regex.Pattern.compile(rx.toString());
            for (String c : cols) {
              if (pat.matcher(c).matches()) {
                excluded.add(c);
              }
            }
          } else {
            excluded.add(name);
          }
        }
        List<SqlNode> selects = new ArrayList<>();
        for (String c : cols) {
          if (excluded.contains(c)) continue;
          if (org.opensearch.sql.calcite.plan.OpenSearchConstants.METADATAFIELD_TYPE_MAP
              .containsKey(c)) continue;
          selects.add(new SqlIdentifier(c, POS));
        }
        state.setProjection(selects);
        return null;
      }
      // PPL wraps every parsed query in a synthesized top-level "project AllFields" — i.e. the
      // bare PPL "source=T" implicitly ends with "| fields *". On OpenSearch indices this
      // expansion picks up metadata fields (_id, _index, _score, ...) which PPL hides; if any
      // are present we must enumerate the non-meta columns. Otherwise leave it as a no-op so
      // downstream pipes (especially Sort) aren't wrapped into informational subqueries.
      if (isSelectStar(node)) {
        // When upstream eval added a dotted-name alias whose parent struct is ALSO listed in the
        // projection as a bare passthrough identifier (e.g. `fields agent | eval agent.name =
        // 'test'` produces `[agent, 'test' AS \`agent.name\`]`), the implicit final `fields *`
        // would expose both — but PPL's tryToRemoveNestedFields drops the leaf. Mirror that here
        // by stripping projection entries whose name is `<x>.<...>` when `<x>` is also referenced
        // as a bare identifier in the projection. Only walk projection identifiers (not the FROM
        // row type) so that an eval OVERRIDE on a real flattened nested leaf — which already
        // dropped the struct-parent columns from the projection upstream — survives intact.
        if (rowTypeOracle != null
            && state.from != null
            && state.evalExtended
            && state.projection != null) {
          java.util.Set<String> projBareNames = new java.util.LinkedHashSet<>();
          for (SqlNode p : state.projection) {
            if (p instanceof SqlIdentifier id && !id.isStar()) {
              projBareNames.add(id.toString());
            }
          }
          List<SqlNode> filteredProj = new ArrayList<>();
          boolean changed = false;
          for (SqlNode p : state.projection) {
            String n = identifierName(p);
            if (n != null) {
              int lastDot = n.lastIndexOf('.');
              if (lastDot != -1 && projBareNames.contains(n.substring(0, lastDot))) {
                changed = true;
                continue;
              }
            }
            filteredProj.add(p);
          }
          if (changed) {
            state.setProjection(filteredProj);
          }
        }
        // Enumerate-and-filter metadata + ancestor structs when the projection still contains a
        // `*` star — either the default SELECT * (state.projection == null) or an eval-extended
        // projection of shape `[*, expr AS alias, ...]`. Once we rebuild with explicit identifiers
        // the `*` is gone and we leave it untouched on subsequent passes.
        boolean projectionHasStar =
            state.projection == null
                || state.projection.stream()
                    .anyMatch(p -> p instanceof SqlIdentifier id && id.isStar());
        if (rowTypeOracle != null && state.from != null && projectionHasStar) {
          List<String> cols = deriveColumnNames(state.from);
          java.util.Set<String> colSet = new java.util.HashSet<>(cols);
          boolean hasMeta =
              cols.stream()
                  .anyMatch(
                      org.opensearch.sql.calcite.plan.OpenSearchConstants.METADATAFIELD_TYPE_MAP
                          ::containsKey);
          // In eval-extended mode, also drop columns whose name was overridden by an eval alias.
          java.util.Set<String> overriddenByEval = new java.util.HashSet<>(state.evalAliasNames);
          // Two pruning shapes depending on whether downstream wants struct parents or flat leaves:
          //   - Default (no dotted alias in projection/eval): keep struct parents, drop flat
          //     dotted leaves whose parent is in scope. Mirrors v2's tryToRemoveNestedFields.
          //   - Dotted alias present (e.g. `eval resource.X = ...`, `bin resource.X`): keep flat
          //     leaves, drop struct parents (ancestors). The flat leaf is what the user named and
          //     the parent struct would conflict with the eval/bin override.
          boolean hasDottedAlias = state.evalAliasNames.stream().anyMatch(n -> n.contains("."));
          if (!hasDottedAlias && state.projection != null) {
            for (SqlNode p : state.projection) {
              String nm = identifierName(p);
              if (nm != null && nm.contains(".")) {
                hasDottedAlias = true;
                break;
              }
            }
          }
          java.util.Set<String> ancestorStructs;
          java.util.Set<String> dottedLeafChildren;
          if (hasDottedAlias) {
            ancestorStructs = collectAncestorStructs(colSet);
            dottedLeafChildren = java.util.Collections.emptySet();
          } else {
            ancestorStructs = java.util.Collections.emptySet();
            dottedLeafChildren = new java.util.HashSet<>();
            for (String c : cols) {
              int lastDot = c.lastIndexOf('.');
              if (lastDot != -1 && colSet.contains(c.substring(0, lastDot))) {
                dottedLeafChildren.add(c);
              }
            }
          }
          if (hasMeta
              || !ancestorStructs.isEmpty()
              || !dottedLeafChildren.isEmpty()
              || !overriddenByEval.isEmpty()) {
            if (state.orderBy != null || state.fetch != null) {
              state.wrap();
            }
            // Preserve any non-`*` projection entries (eval aliases) so they survive the rebuild.
            List<SqlNode> evalEntries = new ArrayList<>();
            if (state.projection != null) {
              for (SqlNode p : state.projection) {
                if (p instanceof SqlIdentifier id && id.isStar()) continue;
                evalEntries.add(p);
              }
            }
            List<SqlNode> projection = new ArrayList<>();
            for (String c : cols) {
              if (org.opensearch.sql.calcite.plan.OpenSearchConstants.METADATAFIELD_TYPE_MAP
                  .containsKey(c)) {
                continue;
              }
              if (ancestorStructs.contains(c)) {
                continue;
              }
              if (dottedLeafChildren.contains(c)) {
                continue;
              }
              if (overriddenByEval.contains(c)) {
                continue;
              }
              projection.add(new SqlIdentifier(c, POS));
            }
            projection.addAll(evalEntries);
            state.setProjection(projection);
            return null;
          }
        }
        return null;
      }
      // SQL aliases in the SELECT list aren't visible inside the same SELECT list, so a project
      // after an eval/rename/etc. that introduced new names must wrap. Likewise wrap if a
      // row-cap was already applied.
      if (state.evalExtended
          || state.projectionReplaced
          || state.orderBy != null
          || state.fetch != null) {
        // PPL's `fields` projection sits OUTSIDE an upstream HEAD — v2 emits
        // `LogicalProject(fields) <- LogicalSort(fetch=N) <- LogicalProject(eval-extended)`.
        // When outerFetch is set (from `head N`), flush it into the inner pipeline BEFORE wrapping
        // so it gets attached to the wrapped subquery as inner FETCH, then the wrap pushes
        // eval-extended projection + sort/fetch into a subquery, leaving the outer projection
        // (this fields list) as the outermost layer.
        //
        // Don't flush when only outerOrderBy is set (no fetch): an inner ORDER BY without FETCH
        // is informational and gets stripped by Calcite's optimizer, which loses the sort.
        if (state.outerFetch != null) {
          flushOuterIntoInner();
        }
        state.wrap();
      }
      // Pre-resolve wildcards (e.g. `account*`, `*name`, `*a*`) by enumerating input columns.
      // PPL `fields` accepts shell-style wildcards; SQL has no equivalent. We expand them
      // against the oracle row type and substitute matching column identifiers.
      List<String> inputCols = null;
      if (rowTypeOracle != null && state.from != null) {
        try {
          inputCols = deriveColumnNames(state.from);
        } catch (RuntimeException ignored2) {
          // probe failed; wildcards will fail at validate time below
        }
      }
      // Compute per-side column sets when state.from is a SqlJoin so bare unqualified column
      // refs can be qualified with the left alias (PPL semantics) when they would otherwise be
      // validator-ambiguous.
      java.util.Set<String> ambiguousJoinCols = null;
      if (joinScope
          && state.joinLeftAlias != null
          && state.joinRightAlias != null
          && state.from instanceof org.apache.calcite.sql.SqlJoin sj) {
        try {
          java.util.Set<String> leftSet = new java.util.HashSet<>(deriveColumnNames(sj.getLeft()));
          java.util.Set<String> rightSet =
              new java.util.HashSet<>(deriveColumnNames(sj.getRight()));
          ambiguousJoinCols = new java.util.HashSet<>(leftSet);
          ambiguousJoinCols.retainAll(rightSet);
        } catch (RuntimeException ignoredAmb) {
          // probe failed; skip qualification
        }
      }
      List<SqlNode> selects = new ArrayList<>(node.getProjectList().size());
      java.util.Set<String> selectedNames = new java.util.LinkedHashSet<>();
      for (UnresolvedExpression e : node.getProjectList()) {
        if (e instanceof AllFields) {
          selects.add(SqlIdentifier.star(POS));
          continue;
        }
        String name = null;
        if (e instanceof Field f && f.getField() instanceof QualifiedName qn) {
          name = qn.toString();
        } else if (e instanceof QualifiedName qn) {
          name = qn.toString();
        }
        if (name != null && name.contains("*") && inputCols != null) {
          // Translate PPL shell-style wildcard to a regex anchored by ^...$. Each `*` becomes
          // `.*`; every other character is regex-quoted.
          StringBuilder rx = new StringBuilder("^");
          for (int i = 0; i < name.length(); i++) {
            char ch = name.charAt(i);
            if (ch == '*') {
              rx.append(".*");
            } else {
              rx.append(java.util.regex.Pattern.quote(String.valueOf(ch)));
            }
          }
          rx.append("$");
          java.util.regex.Pattern pat = java.util.regex.Pattern.compile(rx.toString());
          boolean anyMatch = false;
          for (String c : inputCols) {
            if (org.opensearch.sql.calcite.plan.OpenSearchConstants.METADATAFIELD_TYPE_MAP
                .containsKey(c)) {
              continue;
            }
            if (pat.matcher(c).matches()) {
              anyMatch = true;
              if (selectedNames.add(c)) {
                selects.add(new SqlIdentifier(c, POS));
              }
            }
          }
          if (!anyMatch) {
            // No matches at all — mirror v2's "wildcard pattern [<name>] matches no fields"
            // error message.
            throw new IllegalArgumentException("wildcard pattern [" + name + "] matches no fields");
          }
          continue;
        }
        // Dedupe: if a wildcard already added this column, skip the redundant explicit ref.
        if (name != null && selectedNames.contains(name)) {
          continue;
        }
        SqlNode emit;
        if (e instanceof Field f) {
          emit = expr(f.getField());
        } else {
          emit = expr(e);
        }
        // When expr() emits a MAP/STRUCT navigation as ITEM(...) (see tryMapOrStructItemAccess),
        // Calcite's auto-naming gives it `EXPR$N`. Preserve PPL's expected column name (the
        // dotted-form `info.city`) as an explicit alias.
        if (name != null
            && name.contains(".")
            && emit instanceof SqlBasicCall sbcItem
            && sbcItem.getOperator() == SqlStdOperatorTable.ITEM) {
          emit = asAlias(emit, name);
        }
        // PPL semantics: a bare `<col>` after a JOIN binds to the LEFT side. The validator sees
        // the column on both sides and raises "Column 'X' is ambiguous". Qualify with the left
        // alias when both sides expose the same column AND the user wrote a bare single-part ref
        // (no `.` and not aliased upstream) AND the alias is known.
        if (ambiguousJoinCols != null
            && state.joinLeftAlias != null
            && name != null
            && !name.contains(".")
            && !name.contains("*")
            && ambiguousJoinCols.contains(name)
            && emit instanceof SqlIdentifier idBare
            && idBare.names.size() == 1
            && !idBare.isStar()) {
          emit =
              asAlias(
                  new SqlIdentifier(java.util.Arrays.asList(state.joinLeftAlias, name), POS), name);
        }
        // PPL's `fields a.country, b.country` over a JOIN: the first surfaces as column `country`
        // and the second collides — Calcite renames to `country0`. v2 aliases the second as
        // literal `b.country`. Detect when this fields-list contains a multi-part dotted name
        // whose LEAF collides with a previously-selected leaf, and emit it as `<full-name> AS
        // \`<full-name>\`` so the response schema matches v2.
        if (joinScope
            && name != null
            && name.contains(".")
            && !name.contains("*")
            && emit instanceof SqlIdentifier id
            && id.names.size() > 1) {
          String leaf = id.names.get(id.names.size() - 1);
          String prefix = id.names.get(0);
          // Collide when prior selection used the same leaf as its rendered column name OR
          // when the leaf appears on BOTH join sides AND the dotted prefix matches the right
          // alias. PPL semantics: bare leaves prefer the left side, so right-side qualified
          // refs to columns that exist on both must keep their dotted prefix to disambiguate
          // (otherwise SQL renders them as just the leaf, identical to a left-side reference).
          boolean shouldAlias = selectedNames.contains(leaf);
          if (!shouldAlias
              && ambiguousJoinCols != null
              && ambiguousJoinCols.contains(leaf)
              && state.joinRightAlias != null
              && state.joinRightAlias.equals(prefix)) {
            shouldAlias = true;
          }
          if (shouldAlias) {
            emit = asAlias(emit, name);
          } else {
            // Reserve the leaf name to detect future collisions on the same fields list.
            selectedNames.add(leaf);
          }
        }
        selects.add(emit);
        if (name != null) {
          selectedNames.add(name);
        }
      }
      // Mirror v2's visitProject behavior: when the input schema exposes the special _highlight
      // column (set by relevance queries), include it in the explicit projection so result-set
      // consumers downstream can read highlight snippets.
      if (rowTypeOracle != null && state.from != null) {
        try {
          List<String> cols = deriveColumnNames(state.from);
          if (cols.contains(org.opensearch.sql.expression.HighlightExpression.HIGHLIGHT_FIELD)) {
            selects.add(
                new SqlIdentifier(
                    org.opensearch.sql.expression.HighlightExpression.HIGHLIGHT_FIELD, POS));
          }
        } catch (RuntimeException ignored2) {
          // probe failed; skip
        }
      }
      state.setProjection(selects);
      return null;
    }

    private boolean isSelectStar(Project node) {
      List<UnresolvedExpression> list = node.getProjectList();
      return list.size() == 1 && list.get(0) instanceof AllFields;
    }

    @Override
    public Void visitRename(Rename node, Void ignored) {
      walkChild(node);
      if (state.evalExtended || state.projectionReplaced) {
        state.wrap();
      }
      // Filter metadata fields so the rename projection doesn't expose them.
      List<String> cols =
          deriveColumnNames(state.from).stream()
              .filter(
                  c ->
                      !org.opensearch.sql.calcite.plan.OpenSearchConstants.METADATAFIELD_TYPE_MAP
                          .containsKey(c))
              .toList();
      // PPL allows chained renames within a single command: `rename A as B, B as C` should
      // collapse into A→C. We track each column's CURRENT name through sequential rename
      // application. PPL also supports wildcard patterns (*ame → *AME).
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
        }
        if (!wildcard) {
          // Find the column whose current name matches origin.
          for (java.util.Map.Entry<String, String> e : currentName.entrySet()) {
            if (origin.equals(e.getValue())) {
              e.setValue(target);
              break;
            }
          }
        } else {
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
        }
      }
      // PPL: when a rename target collides with an existing column, the existing column is
      // dropped — even when the rename source doesn't match any column (the target is still
      // dropped). Track target names from the rename list directly, but exclude no-op renames
      // (origin == target).
      java.util.Set<String> renamedTargets = new java.util.HashSet<>();
      for (org.opensearch.sql.ast.expression.Map m : node.getRenameList()) {
        String origin = ((Field) m.getOrigin()).getField().toString();
        String target = ((Field) m.getTarget()).getField().toString();
        if (!target.contains("*") && !origin.equals(target)) {
          renamedTargets.add(target);
        }
      }
      List<SqlNode> selects = new ArrayList<>();
      for (String c : cols) {
        String t = currentName.get(c);
        if (!t.equals(c)) {
          selects.add(asAlias(new SqlIdentifier(c, POS), t));
        } else if (renamedTargets.contains(c)) {
          // Original column dropped because another rename targets this name.
          continue;
        } else {
          selects.add(new SqlIdentifier(c, POS));
        }
      }
      // PPL also allows renaming a MAP/STRUCT-leaf path (e.g. `rename doc.user.name as username`
      // after `spath input=doc`). Such rename origins don't appear as flat columns in `cols`; lower
      // each non-wildcard origin that wasn't matched above through tryMapOrStructItemAccess and
      // emit `ITEM(...) AS target`. Skip wildcard renames — those only match flat names.
      for (org.opensearch.sql.ast.expression.Map m : node.getRenameList()) {
        String origin = ((Field) m.getOrigin()).getField().toString();
        String target = ((Field) m.getTarget()).getField().toString();
        if (origin.contains("*") || target.contains("*")) continue;
        if (origin.equals(target)) continue;
        if (cols.contains(origin)) continue; // already handled by flat-column path above
        SqlNode itemAccess =
            tryMapOrStructItemAccess(
                QualifiedName.of(java.util.Arrays.asList(origin.split("\\."))));
        if (itemAccess != null) {
          selects.add(asAlias(itemAccess, target));
        }
      }
      state.setProjection(selects);
      return null;
    }

    @Override
    public Void visitFlatten(Flatten node, Void ignored) {
      walkChild(node);
      // Need schema to enumerate the struct's sub-fields (named "<flatField>.<sub>" in the
      // existing TableWithStruct convention). Materialize the in-flight FROM and probe its row
      // type via the validator-backed oracle.
      if (state.evalExtended || state.projectionReplaced) {
        state.wrap();
      }
      String fieldName = node.getField().getField().toString();
      // Filter out metadata fields when enumerating the input columns; we don't want to expose
      // _id/_index/_score in the post-flatten projection.
      List<String> allCols =
          deriveColumnNames(state.from).stream()
              .filter(
                  c ->
                      !org.opensearch.sql.calcite.plan.OpenSearchConstants.METADATAFIELD_TYPE_MAP
                          .containsKey(c))
              .toList();
      List<String> subCols = allCols.stream().filter(c -> c.startsWith(fieldName + ".")).toList();
      List<String> aliases =
          node.getAliases() != null
              ? node.getAliases()
              : subCols.stream().map(c -> c.substring(fieldName.length() + 1)).toList();
      if (node.getAliases() != null && node.getAliases().size() != subCols.size()) {
        throw new IllegalArgumentException(
            String.format(
                "The number of aliases has to match the number of flattened fields. Expected %d"
                    + " (%s), got %d (%s)",
                subCols.size(),
                String.join(", ", subCols),
                node.getAliases().size(),
                String.join(", ", node.getAliases())));
      }
      // Project all input columns + aliased sub-fields. Building the SELECT list requires us
      // to enumerate inputs (we can't use SELECT * + extras because the sub-field aliases would
      // collide with the existing dotted-name columns).
      // Drop dotted-name children whose struct parent is also present in the schema — these are
      // OpenSearch's flattened-leaf duplicates and are re-projected via the parent struct (or via
      // explicit aliases for the flatten target). Mirrors v2's tryToRemoveNestedFields, applied
      // here at the explicit-projection level.
      java.util.Set<String> all = new java.util.HashSet<>(allCols);
      List<SqlNode> selects = new ArrayList<>();
      for (String c : allCols) {
        int lastDot = c.lastIndexOf('.');
        if (lastDot >= 0 && all.contains(c.substring(0, lastDot))) {
          continue;
        }
        selects.add(new SqlIdentifier(c, POS));
      }
      for (int i = 0; i < subCols.size(); i++) {
        selects.add(asAlias(new SqlIdentifier(subCols.get(i), POS), aliases.get(i)));
      }
      state.setProjection(selects);
      return null;
    }

    @Override
    public Void visitLookup(Lookup node, Void ignored) {
      walkChild(node);
      // lookup is a join — flush outer fetch onto the input side.
      flushOuterIntoInner();
      state.wrap();
      // Lookup-side: SELECT <output cols>, <key cols> FROM <lookup table>
      // Build by recursively visiting the lookup relation, then projecting just the keys + outputs.
      SqlNode lookupSide = new PplToSqlNode(rowTypeOracle).visit(node.getLookupRelation());
      java.util.Map<String, String> mapping = node.getMappingAliasMap();
      java.util.Map<String, String> output =
          new java.util.LinkedHashMap<>(node.getOutputAliasMap());
      // When the user didn't specify explicit OUTPUT fields, default to "all lookup-side columns
      // except the mapping keys" — that's what v2 does. We need the row-type oracle to enumerate.
      if (output.isEmpty() && rowTypeOracle != null) {
        try {
          List<String> lookupCols = deriveColumnNames(lookupSide);
          for (String c : lookupCols) {
            if (!mapping.containsKey(c)
                && !org.opensearch.sql.calcite.plan.OpenSearchConstants.METADATAFIELD_TYPE_MAP
                    .containsKey(c)) {
              output.put(c, c);
            }
          }
        } catch (RuntimeException ignored2) {
          // Oracle probe failed; proceed with empty output (lookup acts as filter join).
        }
      }
      // Project the lookup-side columns we need: outputs first, then mapping keys.
      // Dedupe: when an output is also a mapping key (e.g. REPLACE id, col1 with mapping {id: id})
      // the column would appear twice and the validator complains "Column 'id' is ambiguous"
      // when the join projection later references it.
      SqlNodeList lookupSelectList = new SqlNodeList(POS);
      java.util.Set<String> emittedLookupCols = new java.util.HashSet<>();
      for (java.util.Map.Entry<String, String> e : output.entrySet()) {
        if (emittedLookupCols.add(e.getKey())) {
          lookupSelectList.add(new SqlIdentifier(e.getKey(), POS));
        }
      }
      for (String key : mapping.keySet()) {
        if (emittedLookupCols.add(key)) {
          lookupSelectList.add(new SqlIdentifier(key, POS));
        }
      }
      SqlSelect lookupProject =
          new SqlSelect(
              POS, /* keywordList */
              null,
              lookupSelectList,
              lookupSide, /* where */
              null,
              /* group */ null, /* having */
              null, /* windowList */
              null,
              /* qualify */ null, /* orderBy */
              null, /* offset */
              null, /* fetch */
              null,
              /* hints */ null);
      String inputAlias = "lookup_input";
      String lookupAlias = "lookup_t";
      SqlNode aliasedLookup =
          new SqlBasicCall(
              SqlStdOperatorTable.AS,
              List.of(lookupProject, new SqlIdentifier(lookupAlias, POS)),
              POS);
      SqlNode aliasedInput =
          new SqlBasicCall(
              SqlStdOperatorTable.AS, List.of(state.from, new SqlIdentifier(inputAlias, POS)), POS);
      // Build join condition. PPL syntax `LOOKUP <table> <lookup_col> AS <source_col>` joins
      // {source.source_col = lookup_table.lookup_col}. mappingAliasMap stores inputField (the
      // lookup-table column) -> outputField (the source column). So:
      //   key   = lookup-table column name
      //   value = source column name
      SqlNode condition = null;
      for (java.util.Map.Entry<String, String> e : mapping.entrySet()) {
        SqlNode lookupCol =
            new SqlIdentifier(java.util.Arrays.asList(lookupAlias, e.getKey()), POS);
        // PPL allows the source column to be a dotted MAP/STRUCT path (e.g.
        // `LOOKUP X name AS doc.user.name` after spath). For 2+ part source paths, lower the
        // navigation through ITEM so the validator doesn't interpret it as `inputAlias.col.col`.
        String sourceName = e.getValue();
        SqlNode sourceCol;
        if (sourceName.contains(".")) {
          // Try the alias-prefixed MAP-leaf helper. Set up a temporary join probe context against
          // the just-built input alias.
          SqlNode prevProbe = joinProbeFrom;
          String prevActiveL = activeJoinLeftAlias;
          joinProbeFrom = aliasedInput;
          activeJoinLeftAlias = inputAlias;
          boolean prevJoinScope = joinScope;
          joinScope = true;
          java.util.List<String> parts = new java.util.ArrayList<>();
          parts.add(inputAlias);
          for (String p : sourceName.split("\\.")) parts.add(p);
          SqlNode itemAccess = tryAliasMapItemAccess(parts);
          joinProbeFrom = prevProbe;
          activeJoinLeftAlias = prevActiveL;
          joinScope = prevJoinScope;
          sourceCol =
              itemAccess != null
                  ? itemAccess
                  : new SqlIdentifier(java.util.Arrays.asList(inputAlias, sourceName), POS);
        } else {
          sourceCol = new SqlIdentifier(java.util.Arrays.asList(inputAlias, sourceName), POS);
        }
        SqlNode eq =
            new SqlBasicCall(SqlStdOperatorTable.EQUALS, List.of(sourceCol, lookupCol), POS);
        condition =
            condition == null
                ? eq
                : new SqlBasicCall(SqlStdOperatorTable.AND, List.of(condition, eq), POS);
      }
      org.apache.calcite.sql.SqlJoin join =
          new org.apache.calcite.sql.SqlJoin(
              POS,
              aliasedInput,
              SqlLiteral.createBoolean(false, POS),
              org.apache.calcite.sql.JoinType.LEFT.symbol(POS),
              aliasedLookup,
              org.apache.calcite.sql.JoinConditionType.ON.symbol(POS),
              condition);
      state.from = join;
      // Without the schema oracle we can't enumerate input columns to detect collisions or
      // build COALESCE for APPEND duplicates. Fall back to the simple "JOIN, no projection"
      // shape which works for the unit-test case where no duplicates exist.
      if (rowTypeOracle == null) {
        return null;
      }
      // Compute final projection: drop input columns that conflict with the lookup outputs
      // (they'd otherwise be ambiguous in subsequent pipes since both sides expose them).
      // Then append the lookup outputs under their target alias names. For APPEND, the merged
      // value is COALESCE(input.col, lookup.col); for REPLACE, the lookup value wins outright.
      List<String> inputCols =
          deriveColumnNames(aliasedInput).stream()
              .filter(
                  c ->
                      !org.opensearch.sql.calcite.plan.OpenSearchConstants.METADATAFIELD_TYPE_MAP
                          .containsKey(c))
              .toList();
      java.util.Map<String, String> lookupOutputAliases = new java.util.LinkedHashMap<>(output);
      // Collect target alias names to detect collision with input.
      java.util.Set<String> targetAliases = new java.util.HashSet<>(lookupOutputAliases.values());
      boolean isAppend = node.getOutputStrategy() == Lookup.OutputStrategy.APPEND;
      // Build the projection: emit non-colliding input columns first (in their original order),
      // then append the lookup outputs (or the COALESCE'd merged value for APPEND duplicates).
      // v2's LOOKUP places merged columns at the end of the row, so we mirror that ordering.
      List<SqlNode> selects = new ArrayList<>();
      for (String c : inputCols) {
        if (targetAliases.contains(c)) {
          // Skip — handled below in the appended section.
          continue;
        }
        selects.add(new SqlIdentifier(java.util.Arrays.asList(inputAlias, c), POS));
      }
      for (java.util.Map.Entry<String, String> e : lookupOutputAliases.entrySet()) {
        String src = e.getKey();
        String tgt = e.getValue();
        if (inputCols.contains(tgt) && isAppend) {
          // APPEND duplicate: COALESCE(input.col, lookup.col) AS col, placed at end.
          SqlNode coalesced =
              new SqlBasicCall(
                  SqlStdOperatorTable.COALESCE,
                  List.of(
                      new SqlIdentifier(java.util.Arrays.asList(inputAlias, tgt), POS),
                      new SqlIdentifier(java.util.Arrays.asList(lookupAlias, src), POS)),
                  POS);
          selects.add(asAlias(coalesced, tgt));
        } else {
          // REPLACE (or no collision): emit lookup column under its target name.
          SqlNode lookupCol = new SqlIdentifier(java.util.Arrays.asList(lookupAlias, src), POS);
          selects.add(src.equals(tgt) ? lookupCol : asAlias(lookupCol, tgt));
        }
      }
      state.setProjection(selects);
      // Force a wrap so the join's qualified columns are flattened into a single-table schema
      // before downstream pipes (especially `fields`) probe the row type. Otherwise the
      // SqlValidator may report ambiguous column when both join sides expose the same simple
      // name even though our explicit projection only selects qualified references.
      state.wrap();
      return null;
    }

    @Override
    public Void visitExpand(Expand node, Void ignored) {
      walkChild(node);
      // Always wrap; expand changes the row set (each input row becomes N rows).
      state.wrap();
      String fieldName;
      UnresolvedExpression fieldExpr = node.getField().getField();
      if (fieldExpr instanceof QualifiedName qn) {
        fieldName = qn.toString();
      } else {
        throw new UnsupportedOperationException(
            "expand requires a simple column reference, got: " + fieldExpr.getClass());
      }
      String alias = node.getAlias() != null ? node.getAlias() : fieldName;
      // Build SQL: SELECT <input>.*, t.<alias> FROM (<input>) AS s, UNNEST(s.<field>) AS t(<alias>)
      // We achieve the implicit-LATERAL CROSS JOIN by setting state.from to a SqlJoin with COMMA.
      String inputAlias = "expand_input";
      SqlNode aliasedInput =
          new SqlBasicCall(
              SqlStdOperatorTable.AS, List.of(state.from, new SqlIdentifier(inputAlias, POS)), POS);
      SqlNode unnestArg = new SqlIdentifier(java.util.Arrays.asList(inputAlias, fieldName), POS);
      SqlNode unnest = new SqlBasicCall(SqlStdOperatorTable.UNNEST, List.of(unnestArg), POS);
      SqlNode aliasedUnnest =
          new SqlBasicCall(
              SqlStdOperatorTable.AS,
              List.of(unnest, new SqlIdentifier("expand_t", POS), new SqlIdentifier(alias, POS)),
              POS);
      org.apache.calcite.sql.SqlJoin join =
          new org.apache.calcite.sql.SqlJoin(
              POS,
              aliasedInput,
              SqlLiteral.createBoolean(false, POS),
              org.apache.calcite.sql.JoinType.COMMA.symbol(POS),
              aliasedUnnest,
              org.apache.calcite.sql.JoinConditionType.NONE.symbol(POS),
              null);
      state.from = join;
      // Build an explicit projection that drops the original (array-valued) `fieldName` from
      // the input side and surfaces the unnested element as `alias`. Otherwise the join exposes
      // both `expand_input.<field>` (array) and `expand_t.<alias>`, and downstream column
      // references on `<field>`/`<alias>` are ambiguous.
      //
      // Also drop dotted DESCENDANTS of `fieldName` (OpenSearch flattens nested fields, exposing
      // both `<field>` and `<field>.<leaf>` as scan columns; the leaves should be reached via
      // expand_t struct navigation, not the source-side flat columns which the pushdown would
      // collapse to single values).
      List<SqlNode> selects = new ArrayList<>();
      String dottedPrefix = fieldName + ".";
      if (rowTypeOracle != null) {
        try {
          List<String> inputCols = deriveColumnNames(aliasedInput);
          // Drop flat dotted leaves whose parent is also in scope — matches v2's
          // tryToRemoveNestedFields default behaviour (parent struct wins, leaves drop).
          java.util.Set<String> inputColSet = new java.util.HashSet<>(inputCols);
          for (String c : inputCols) {
            if (org.opensearch.sql.calcite.plan.OpenSearchConstants.METADATAFIELD_TYPE_MAP
                .containsKey(c)) {
              continue;
            }
            // Always drop the source-side `fieldName` — the unnested element replaces it (whether
            // surfaced under the same name or the user-supplied alias).
            if (c.equals(fieldName)) {
              continue;
            }
            if (c.startsWith(dottedPrefix)) {
              continue;
            }
            // Drop flat dotted leaves whose parent struct is in scope.
            int lastDot = c.lastIndexOf('.');
            if (lastDot != -1 && inputColSet.contains(c.substring(0, lastDot))) {
              continue;
            }
            selects.add(new SqlIdentifier(java.util.Arrays.asList(inputAlias, c), POS));
          }
        } catch (RuntimeException ignored2) {
          // probe failed; fall through to SELECT *
        }
      }
      selects.add(new SqlIdentifier(java.util.Arrays.asList("expand_t", alias), POS));
      state.projection = selects.isEmpty() ? null : selects;
      state.projectionReplaced = !selects.isEmpty();
      state.evalExtended = false;
      state.evalAliasNames.clear();
      return null;
    }

    @Override
    public Void visitParse(Parse node, Void ignored) {
      walkChild(node);
      org.opensearch.sql.ast.expression.ParseMethod parseMethod = node.getParseMethod();
      String patternValue = (String) node.getPattern().getValue();
      // Wrap if there's pending state where the input field would not be visible as a real
      // column. Eval-extended projections add SELECT-list aliases that SQL doesn't allow to
      // be referenced in the same SELECT — wrap so the alias becomes a real column. Also wrap
      // for any pending row-cap.
      if (state.orderBy != null
          || state.fetch != null
          || state.evalExtended
          || state.projectionReplaced) {
        state.wrap();
      }
      SqlNode source = expr(node.getSourceField());
      if (parseMethod == org.opensearch.sql.ast.expression.ParseMethod.PATTERNS) {
        // PATTERNS method: emit a CASE that handles empty input then REGEXP_REPLACE on the
        // configured punct/word patterns. Default pattern (no `pattern=` arg) collapses
        // alphanumeric runs. The output column is named "patterns_field" by convention; callers
        // (visitPatterns) use that fixed name. Replacement is "<*>" (matches v2 behavior).
        String alias = "patterns_field";
        // Use the supplied pattern, or default to "[a-zA-Z0-9]+".
        String regex = patternValue.isEmpty() ? "[a-zA-Z0-9]+" : patternValue;
        SqlNode replaced =
            new SqlBasicCall(
                org.apache.calcite.sql.fun.SqlLibraryOperators.REGEXP_REPLACE_3,
                List.of(
                    source,
                    SqlLiteral.createCharString(regex, POS),
                    SqlLiteral.createCharString("<*>", POS)),
                POS);
        // Wrap in CASE WHEN source IS NULL OR source = '' THEN '' ELSE replaced END.
        SqlNodeList whens = new SqlNodeList(POS);
        whens.add(
            new SqlBasicCall(
                SqlStdOperatorTable.OR,
                List.of(
                    new SqlBasicCall(SqlStdOperatorTable.IS_NULL, List.of(source), POS),
                    new SqlBasicCall(
                        SqlStdOperatorTable.EQUALS,
                        List.of(source, SqlLiteral.createCharString("", POS)),
                        POS)),
                POS));
        SqlNodeList thens = new SqlNodeList(POS);
        thens.add(SqlLiteral.createCharString("", POS));
        SqlNode caseExpr =
            new org.apache.calcite.sql.fun.SqlCase(POS, null, whens, thens, replaced);
        state.addEvalAlias(caseExpr, alias);
        return null;
      }
      List<String> groupCandidates =
          org.opensearch.sql.utils.ParseUtils.getNamedGroupCandidates(
              parseMethod, patternValue, node.getArguments());
      if (groupCandidates.isEmpty()) {
        return null;
      }
      SqlNode patternLit = SqlLiteral.createCharString(patternValue, POS);
      // Mirror the existing path: PARSE(field, pattern, 'regex'|'grok') for REGEX/GROK; the
      // resulting MAP-typed value is then indexed by the named group via ITEM (i.e. arr[name]).
      SqlNode methodLit = SqlLiteral.createCharString(parseMethod.getName(), POS);
      // PARSE may produce a group whose name matches an input column (e.g. `parse email
      // '...(?<email>...)'`). Detect and override existing column to avoid ambiguity, mirroring
      // the eval-override logic.
      java.util.Set<String> existingNames = new java.util.HashSet<>(state.evalAliasNames);
      if (rowTypeOracle != null && state.from != null) {
        try {
          existingNames.addAll(deriveColumnNames(state.from));
        } catch (RuntimeException ignored2) {
          // probe failed
        }
      }
      for (String group : groupCandidates) {
        if (existingNames.contains(group)) {
          // Override: drop existing entry from in-flight projection or seed an explicit cols
          // list excluding the override target.
          if (state.projection != null) {
            List<SqlNode> filtered = new ArrayList<>();
            for (SqlNode n : state.projection) {
              String existing = identifierName(n);
              if (existing != null && existing.equals(group)) continue;
              filtered.add(n);
            }
            boolean hasStar =
                filtered.stream().anyMatch(n -> n instanceof SqlIdentifier id && id.isStar());
            if (hasStar && state.from != null) {
              try {
                List<String> cols = deriveColumnNames(state.from);
                if (cols.contains(group)) {
                  List<SqlNode> expanded = new ArrayList<>();
                  for (SqlNode n : filtered) {
                    if (n instanceof SqlIdentifier id && id.isStar()) {
                      for (String c : cols) {
                        if (!c.equals(group)
                            && !org.opensearch.sql.calcite.plan.OpenSearchConstants
                                .METADATAFIELD_TYPE_MAP
                                .containsKey(c)) {
                          expanded.add(new SqlIdentifier(c, POS));
                        }
                      }
                    } else {
                      expanded.add(n);
                    }
                  }
                  filtered = expanded;
                }
              } catch (RuntimeException ignored3) {
                // probe failed
              }
            }
            state.projection = filtered;
          } else if (state.from != null) {
            try {
              List<String> cols =
                  deriveColumnNames(state.from).stream()
                      .filter(
                          c ->
                              !c.equals(group)
                                  && !org.opensearch.sql.calcite.plan.OpenSearchConstants
                                      .METADATAFIELD_TYPE_MAP
                                      .containsKey(c))
                      .toList();
              List<SqlNode> seeded = new ArrayList<>();
              for (String c : cols) {
                seeded.add(new SqlIdentifier(c, POS));
              }
              state.projection = seeded;
              state.projectionReplaced = true;
            } catch (RuntimeException ignored2) {
              // probe failed
            }
          }
          state.evalAliasNames.remove(group);
        }
        SqlNode inner =
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
                List.of(inner, SqlLiteral.createCharString(group, POS)),
                POS);
        state.addEvalAlias(itemCall, group);
      }
      return null;
    }

    @Override
    public Void visitDedupe(Dedupe node, Void ignored) {
      walkChild(node);
      // dedup changes the row set; flush any pending outer-fetch into the inner subquery.
      flushOuterIntoInner();
      List<Argument> opts = node.getOptions();
      int allowedDup = (Integer) opts.get(0).getValue().getValue();
      boolean keepEmpty = (Boolean) opts.get(1).getValue().getValue();
      boolean consecutive = (Boolean) opts.get(2).getValue().getValue();
      if (allowedDup <= 0) {
        throw new IllegalArgumentException("Number of duplicate events must be greater than 0");
      }
      if (consecutive) {
        return visitDedupeConsecutive(node, allowedDup, keepEmpty);
      }
      // Snapshot input columns NOW (before we add the helper) so we can project them at the end
      // and drop the helper. Without an oracle, downstream sees the helper column.
      List<String> inputCols = null;
      if (rowTypeOracle != null && state.from != null) {
        // Materialize the in-flight pipeline state via wrap() so the probe doesn't include the
        // helper. Wrapping is safe here because we'll add filters and a window after this point.
        if (state.where != null
            || state.evalExtended
            || state.projectionReplaced
            || state.orderBy != null
            || state.fetch != null) {
          state.wrap();
        }
        java.util.List<String> rawCols = deriveColumnNames(state.from);
        java.util.Set<String> colSet = new java.util.HashSet<>(rawCols);
        inputCols =
            rawCols.stream()
                .filter(
                    c ->
                        !org.opensearch.sql.calcite.plan.OpenSearchConstants.METADATAFIELD_TYPE_MAP
                            .containsKey(c))
                // Drop dotted struct-leaves whose parent struct is also in scope. v2's
                // tryToRemoveNestedFields runs after dedup's helper-strip — without this, the
                // user-facing rowtype exposes both `address` and `address.city` though only
                // `address` is canonical.
                .filter(
                    c -> {
                      int lastDot = c.lastIndexOf('.');
                      return lastDot == -1 || !colSet.contains(c.substring(0, lastDot));
                    })
                .toList();
      }
      // Step 1: if !keepEmpty, add IS NOT NULL filters on the dedup fields.
      List<SqlNode> fieldNodes = new ArrayList<>(node.getFields().size());
      for (Field f : node.getFields()) {
        fieldNodes.add(expr(f.getField()));
      }
      if (!keepEmpty) {
        // Wrap if any prior pipe state would conflict with adding pure filters.
        if (state.evalExtended || state.projectionReplaced) {
          state.wrap();
        }
        for (SqlNode field : fieldNodes) {
          state.addWhere(new SqlBasicCall(SqlStdOperatorTable.IS_NOT_NULL, List.of(field), POS));
        }
      }
      // Step 2: extend projection with `ROW_NUMBER() OVER (PARTITION BY ... ORDER BY <upstream
      // sort keys>) AS _row_number_dedup_`. Propagating the upstream sort keys into the window's
      // ORDER BY makes ROW_NUMBER deterministic and preserves PPL semantics where `sort X | dedup
      // 1 Y` keeps the first Y row by X order.
      SqlNodeList partitionBy = new SqlNodeList(POS);
      for (SqlNode field : fieldNodes) {
        partitionBy.add(field);
      }
      SqlNodeList windowOrderBy = new SqlNodeList(POS);
      List<SqlNode> upstreamOrder =
          state.orderBy != null && !state.orderBy.isEmpty() ? state.orderBy : state.lastOrderBy;
      if (upstreamOrder != null) {
        for (SqlNode k : upstreamOrder) {
          windowOrderBy.add(k);
        }
      }
      SqlNode rowNumberWindow =
          org.apache.calcite.sql.SqlWindow.create(
              null,
              null,
              partitionBy,
              windowOrderBy,
              SqlLiteral.createBoolean(false, POS),
              null,
              null,
              null,
              POS);
      SqlNode rowNumberOver =
          new SqlBasicCall(
              SqlStdOperatorTable.OVER,
              List.of(
                  new SqlBasicCall(SqlStdOperatorTable.ROW_NUMBER, List.of(), POS),
                  rowNumberWindow),
              POS);
      state.addEvalAlias(rowNumberOver, "_row_number_dedup_");
      // Step 3: wrap and filter on _row_number_dedup_ <= allowedDup.
      state.wrap();
      SqlNode rowCol = new SqlIdentifier("_row_number_dedup_", POS);
      SqlNode boundCheck =
          new SqlBasicCall(
              SqlStdOperatorTable.LESS_THAN_OR_EQUAL, List.of(rowCol, intLiteral(allowedDup)), POS);
      if (keepEmpty) {
        // (field IS NULL) OR ... OR (_row_number_dedup_ <= N)
        SqlNode predicate = boundCheck;
        for (SqlNode field : fieldNodes) {
          predicate =
              new SqlBasicCall(
                  SqlStdOperatorTable.OR,
                  List.of(
                      new SqlBasicCall(SqlStdOperatorTable.IS_NULL, List.of(field), POS),
                      predicate),
                  POS);
        }
        state.addWhere(predicate);
      } else {
        state.addWhere(boundCheck);
      }
      // Step 4: drop the helper column on the way out using the pre-captured input column list.
      // Without an oracle (test-only path), downstream sees the helper column.
      state.wrap();
      if (inputCols != null) {
        List<SqlNode> proj = new ArrayList<>();
        for (String c : inputCols) {
          proj.add(new SqlIdentifier(c, POS));
        }
        state.setProjection(proj);
      }
      return null;
    }

    /**
     * `dedup consecutive=true ...` keeps a row if it differs from the previous row in any of the
     * dedup fields. We compare each field to its LAG via window functions; if any differs (or is
     * the first row), keep. With keepEmpty=false and consecutive=true, NULL fields are dropped.
     */
    private Void visitDedupeConsecutive(Dedupe node, int allowedDup, boolean keepEmpty) {
      // PPL fallback for `consecutive=true` with `allowedDup > 1`: v2 silently degrades to the
      // allowedDup=1 path (the consecutive flag dominates). The test
      // testConsecutiveImplicitFallbackV2 explicitly verifies this behavior. With consecutive=true
      // we always keep a row that differs from the previous; allowedDup is not honored separately.
      List<SqlNode> fieldNodes = new ArrayList<>(node.getFields().size());
      for (Field f : node.getFields()) {
        fieldNodes.add(expr(f.getField()));
      }
      List<String> inputCols = null;
      if (rowTypeOracle != null && state.from != null) {
        if (state.where != null
            || state.evalExtended
            || state.projectionReplaced
            || state.orderBy != null
            || state.fetch != null) {
          state.wrap();
        }
        inputCols =
            deriveColumnNames(state.from).stream()
                .filter(
                    c ->
                        !org.opensearch.sql.calcite.plan.OpenSearchConstants.METADATAFIELD_TYPE_MAP
                            .containsKey(c))
                .toList();
      }
      if (!keepEmpty) {
        if (state.evalExtended || state.projectionReplaced) {
          state.wrap();
        }
        for (SqlNode field : fieldNodes) {
          state.addWhere(new SqlBasicCall(SqlStdOperatorTable.IS_NOT_NULL, List.of(field), POS));
        }
      }
      // Add LAG(field) OVER () AS _lag_<i>_ for each dedup field, plus a row marker for the first
      // row.  Then in a wrapping select, filter to (any field IS DISTINCT FROM its lag) OR (this
      // is the first row, i.e. all lags are NULL but the row had non-null values).
      // PPL semantics: the very first row is always kept. Use ROW_NUMBER() = 1 OR (...).
      SqlNode rowNumberWindow =
          org.apache.calcite.sql.SqlWindow.create(
              null,
              null,
              new SqlNodeList(POS),
              new SqlNodeList(POS),
              SqlLiteral.createBoolean(false, POS),
              null,
              null,
              null,
              POS);
      SqlNode rowNumberOver =
          new SqlBasicCall(
              SqlStdOperatorTable.OVER,
              List.of(
                  new SqlBasicCall(SqlStdOperatorTable.ROW_NUMBER, List.of(), POS),
                  rowNumberWindow),
              POS);
      state.addEvalAlias(rowNumberOver, "_consec_rn_");
      for (int i = 0; i < fieldNodes.size(); i++) {
        SqlNode lagWindow =
            org.apache.calcite.sql.SqlWindow.create(
                null,
                null,
                new SqlNodeList(POS),
                new SqlNodeList(POS),
                SqlLiteral.createBoolean(false, POS),
                null,
                null,
                null,
                POS);
        SqlNode lagOver =
            new SqlBasicCall(
                SqlStdOperatorTable.OVER,
                List.of(
                    new SqlBasicCall(SqlStdOperatorTable.LAG, List.of(fieldNodes.get(i)), POS),
                    lagWindow),
                POS);
        state.addEvalAlias(lagOver, "_consec_lag_" + i);
      }
      state.wrap();
      // Compute "is run start" indicator: 1 when row's dedup-key differs from prior row, else 0.
      // (First row is also a run start — all lags are NULL so IS_DISTINCT_FROM is true.)
      SqlNode firstRow =
          new SqlBasicCall(
              SqlStdOperatorTable.EQUALS,
              List.of(new SqlIdentifier("_consec_rn_", POS), intLiteral(1)),
              POS);
      SqlNode isRunStart = firstRow;
      for (int i = 0; i < fieldNodes.size(); i++) {
        SqlNode field = fieldNodes.get(i);
        SqlNode lag = new SqlIdentifier("_consec_lag_" + i, POS);
        SqlNode distinct =
            new SqlBasicCall(SqlStdOperatorTable.IS_DISTINCT_FROM, List.of(field, lag), POS);
        isRunStart = new SqlBasicCall(SqlStdOperatorTable.OR, List.of(isRunStart, distinct), POS);
      }
      // Translate boolean to 1/0 flag.
      SqlNodeList runFlagWhens = new SqlNodeList(POS);
      runFlagWhens.add(isRunStart);
      SqlNodeList runFlagThens = new SqlNodeList(POS);
      runFlagThens.add(intLiteral(1));
      SqlNode runFlag =
          new org.apache.calcite.sql.fun.SqlCase(
              POS, null, runFlagWhens, runFlagThens, intLiteral(0));
      // Cumulative SUM over the global ordering gives a unique run id.
      SqlNode runIdWindow =
          org.apache.calcite.sql.SqlWindow.create(
              null,
              null,
              new SqlNodeList(POS),
              new SqlNodeList(java.util.List.of(new SqlIdentifier("_consec_rn_", POS)), POS),
              SqlLiteral.createBoolean(true, POS),
              org.apache.calcite.sql.SqlWindow.createUnboundedPreceding(POS),
              org.apache.calcite.sql.SqlWindow.createCurrentRow(POS),
              null,
              POS);
      SqlNode runIdExpr =
          new SqlBasicCall(
              SqlStdOperatorTable.OVER,
              List.of(
                  new SqlBasicCall(SqlStdOperatorTable.SUM, List.of(runFlag), POS), runIdWindow),
              POS);
      state.addEvalAlias(runIdExpr, "_consec_run_id_");
      state.wrap();
      // ROW_NUMBER() OVER (PARTITION BY _consec_run_id_ ORDER BY _consec_rn_) gives position
      // within each run. Keep rows where position <= allowedDup.
      SqlNode posWindow =
          org.apache.calcite.sql.SqlWindow.create(
              null,
              null,
              new SqlNodeList(java.util.List.of(new SqlIdentifier("_consec_run_id_", POS)), POS),
              new SqlNodeList(java.util.List.of(new SqlIdentifier("_consec_rn_", POS)), POS),
              SqlLiteral.createBoolean(false, POS),
              null,
              null,
              null,
              POS);
      SqlNode runPosExpr =
          new SqlBasicCall(
              SqlStdOperatorTable.OVER,
              List.of(new SqlBasicCall(SqlStdOperatorTable.ROW_NUMBER, List.of(), POS), posWindow),
              POS);
      state.addEvalAlias(runPosExpr, "_consec_run_pos_");
      state.wrap();
      state.addWhere(
          new SqlBasicCall(
              SqlStdOperatorTable.LESS_THAN_OR_EQUAL,
              List.of(new SqlIdentifier("_consec_run_pos_", POS), intLiteral(allowedDup)),
              POS));
      state.wrap();
      if (inputCols != null) {
        List<SqlNode> proj = new ArrayList<>();
        for (String c : inputCols) {
          proj.add(new SqlIdentifier(c, POS));
        }
        state.setProjection(proj);
      }
      return null;
    }

    @Override
    public Void visitStreamWindow(StreamWindow node, Void ignored) {
      walkChild(node);
      boolean hasReset = node.getResetBefore() != null || node.getResetAfter() != null;
      if (state.evalExtended || state.projectionReplaced) {
        state.wrap();
      }
      // v2-compatibility: streamstats global=true window=N by <map-leaf> is unimplemented in v2's
      // correlate-based emission (see CalcitePPLMapPathIT.testStreamstatsGlobalWindowByMapPath).
      // Surface the same error to keep the test contract.
      if (node.isGlobal() && node.getWindow() > 0 && rowTypeOracle != null && state.from != null) {
        try {
          List<String> stateCols = deriveColumnNames(state.from);
          for (UnresolvedExpression g : node.getGroupList()) {
            UnresolvedExpression core = g instanceof Alias al ? al.getDelegated() : g;
            String gname = null;
            if (core instanceof Field f && f.getField() instanceof QualifiedName qn) {
              gname = qn.toString();
            } else if (core instanceof QualifiedName qn) {
              gname = qn.toString();
            }
            if (gname != null && gname.contains(".") && !stateCols.contains(gname)) {
              throw new IllegalArgumentException("field [" + gname + "] not found");
            }
          }
        } catch (IllegalArgumentException iae) {
          throw iae;
        } catch (RuntimeException ignored2) {
          // probe failed — let downstream handle
        }
      }
      // ROWS frame:
      //   current=true,  window=N>0 : lower = (N-1) PRECEDING, upper = CURRENT ROW (N rows total)
      //   current=false, window=N>0 : lower = N     PRECEDING, upper = 1 PRECEDING (N rows excl)
      //   window=0 (unbounded)      : lower = UNBOUNDED PRECEDING
      // With reset, switch to a RANGE frame on a global sequence column so that resets bound
      // the window by global-row distance, not partition-local row count. Critical when a
      // segment+group partition would otherwise span resets that should isolate rows.
      int win = node.getWindow();
      int lowerOffset = node.isCurrent() ? win - 1 : win;
      SqlNode lower =
          win > 0
              ? new SqlBasicCall(
                  org.apache.calcite.sql.SqlWindow.PRECEDING_OPERATOR,
                  List.of(intLiteral(lowerOffset)),
                  POS)
              : org.apache.calcite.sql.SqlWindow.createUnboundedPreceding(POS);
      SqlNode upper =
          node.isCurrent()
              ? org.apache.calcite.sql.SqlWindow.createCurrentRow(POS)
              : new SqlBasicCall(
                  org.apache.calcite.sql.SqlWindow.PRECEDING_OPERATOR, List.of(intLiteral(1)), POS);
      // PARTITION BY for `by` clauses. With reset, prepend a synthetic segment id so each
      // segment becomes its own partition (cumulative aggregates restart on reset).
      SqlNodeList partitionBy = new SqlNodeList(POS);
      if (hasReset) {
        // Step 1: synthesize a global sequence column (ROW_NUMBER over empty partition) AND
        // reset flags. Cumulative SUM of flags gives segment ids. With RANGE frame on the seq
        // column, the window bounds reflect global-row distance — so a reset that creates a
        // new segment isolates rows whose global seq is more than (window-1) apart.
        // Skip synthesis when injectStreamSeq already added `__stream_seq__` at the source.
        boolean seqExists = false;
        if (rowTypeOracle != null && state.from != null) {
          try {
            seqExists = deriveColumnNames(state.from).contains("__stream_seq__");
          } catch (RuntimeException ignored4) {
            // probe failed; synthesize a fresh column
          }
        }
        if (!seqExists) {
          SqlNode rowSeqExpr =
              new SqlBasicCall(
                  SqlStdOperatorTable.OVER,
                  List.of(
                      new SqlBasicCall(SqlStdOperatorTable.ROW_NUMBER, List.of(), POS),
                      org.apache.calcite.sql.SqlWindow.create(
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
          state.addEvalAlias(rowSeqExpr, "__stream_seq__");
          state.wrap();
        }
        SqlNode beforeFlag =
            node.getResetBefore() != null ? caseFlag(expr(node.getResetBefore())) : intLiteral(0);
        SqlNode afterFlag =
            node.getResetAfter() != null ? caseFlag(expr(node.getResetAfter())) : intLiteral(0);
        // Cumulative SUM(before) up to current + cumulative SUM(after) up to PREVIOUS row.
        SqlNode beforeSumWindow =
            org.apache.calcite.sql.SqlWindow.create(
                null,
                null,
                new SqlNodeList(POS),
                new SqlNodeList(java.util.List.of(new SqlIdentifier("__stream_seq__", POS)), POS),
                SqlLiteral.createBoolean(true, POS),
                org.apache.calcite.sql.SqlWindow.createUnboundedPreceding(POS),
                org.apache.calcite.sql.SqlWindow.createCurrentRow(POS),
                null,
                POS);
        SqlNode afterSumWindow =
            org.apache.calcite.sql.SqlWindow.create(
                null,
                null,
                new SqlNodeList(POS),
                new SqlNodeList(java.util.List.of(new SqlIdentifier("__stream_seq__", POS)), POS),
                SqlLiteral.createBoolean(true, POS),
                org.apache.calcite.sql.SqlWindow.createUnboundedPreceding(POS),
                new SqlBasicCall(
                    org.apache.calcite.sql.SqlWindow.PRECEDING_OPERATOR,
                    List.of(intLiteral(1)),
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
        // Wrap COALESCE(after_sum, 0) so the very-first-row "no preceding sum" case is 0.
        SqlNode afterSumZero =
            new SqlBasicCall(SqlStdOperatorTable.COALESCE, List.of(afterSum, intLiteral(0)), POS);
        SqlNode segId =
            new SqlBasicCall(SqlStdOperatorTable.PLUS, List.of(beforeSum, afterSumZero), POS);
        state.addEvalAlias(segId, "__seg_id__");
        // Wrap so subsequent windows can PARTITION BY __seg_id__.
        state.wrap();
        partitionBy.add(new SqlIdentifier("__seg_id__", POS));
      }
      for (UnresolvedExpression g : node.getGroupList()) {
        UnresolvedExpression core = (g instanceof Alias a) ? a.getDelegated() : g;
        partitionBy.add(expr(core));
      }
      // Streamstats cumulative semantics need a stable ORDER BY in the window. SQL ROWS-frame
      // aggregates with an empty ORDER BY collapse to the whole-partition aggregate (== global
      // stats, not cumulative). When upstream provided a sort key, use it; otherwise fall back
      // to a synthesized ROW_NUMBER() column that locks in current row order. Skip if the reset
      // branch above already created `__stream_seq__`.
      //
      // Skip ORDER BY synthesis entirely when ALL window functions are ARG_MIN/ARG_MAX (i.e.,
      // earliest/latest) — these aggregates carry their own ordering via the second arg, and v2
      // emits `OVER (ROWS UNBOUNDED PRECEDING)` without an extra ORDER BY for them.
      boolean allArgMinMax = !node.getWindowFunctionList().isEmpty();
      for (UnresolvedExpression e : node.getWindowFunctionList()) {
        UnresolvedExpression core = e instanceof Alias al ? al.getDelegated() : e;
        if (!(core instanceof WindowFunction wf)) {
          allArgMinMax = false;
          break;
        }
        UnresolvedExpression fn = wf.getFunction();
        String fnName = null;
        if (fn instanceof org.opensearch.sql.ast.expression.AggregateFunction af) {
          fnName = af.getFuncName().toLowerCase(java.util.Locale.ROOT);
        } else if (fn instanceof Function f) {
          fnName = f.getFuncName().toLowerCase(java.util.Locale.ROOT);
        }
        if (!"earliest".equals(fnName) && !"latest".equals(fnName)) {
          allArgMinMax = false;
          break;
        }
      }
      SqlNodeList orderBy = new SqlNodeList(POS);
      if (state.lastOrderBy != null && !state.lastOrderBy.isEmpty()) {
        for (SqlNode k : state.lastOrderBy) orderBy.add(k);
      } else if (hasReset) {
        // Already created `__stream_seq__` above for the reset branch. Reuse for ORDER BY.
        orderBy.add(new SqlIdentifier("__stream_seq__", POS));
      } else if (allArgMinMax) {
        // No ORDER BY needed — ARG_MIN/ARG_MAX carry their own ordering via the time field arg.
      } else {
        // If upstream already has `__stream_seq__` (from a prior streamstats), reuse it so
        // multi-streamstats pipes share a stable ordering. Otherwise synthesize a fresh one.
        boolean hasExistingSeq = false;
        if (rowTypeOracle != null && state.from != null) {
          try {
            hasExistingSeq = deriveColumnNames(state.from).contains("__stream_seq__");
          } catch (RuntimeException ignored3) {
            // probe failed; fall through to synthesizing a new column
          }
        }
        if (hasExistingSeq) {
          orderBy.add(new SqlIdentifier("__stream_seq__", POS));
        } else {
          SqlNode rowSeqExpr =
              new SqlBasicCall(
                  SqlStdOperatorTable.OVER,
                  List.of(
                      new SqlBasicCall(SqlStdOperatorTable.ROW_NUMBER, List.of(), POS),
                      org.apache.calcite.sql.SqlWindow.create(
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
          state.addEvalAlias(rowSeqExpr, "__stream_seq__");
          state.wrap();
          orderBy.add(new SqlIdentifier("__stream_seq__", POS));
        }
      }
      List<SqlNode> selects = new ArrayList<>();
      // Enumerate input columns explicitly so OpenSearch metadata fields don't leak into the
      // streamstats projection (PPL hides `_id`, `_index`, `_score`, ...). Always drop the
      // synthetic `__seg_id__` helper. Keep `__stream_seq__` when:
      //   - injectStreamSeq is on (multi-streamstats needs the seq to flow downstream), OR
      //   - the current streamstats has a `by` partition (downstream `reverse` needs a stable
      //     order key to flip; v2 keeps the partitioned seq's collation visible).
      // The top-level stripSyntheticSeqColumns drops the helper from the user-facing schema.
      final boolean keepStreamSeq = state.injectStreamSeq || !node.getGroupList().isEmpty();
      List<String> inputCols = null;
      if (rowTypeOracle != null && state.from != null) {
        inputCols =
            deriveColumnNames(state.from).stream()
                .filter(
                    c ->
                        !org.opensearch.sql.calcite.plan.OpenSearchConstants.METADATAFIELD_TYPE_MAP
                                .containsKey(c)
                            && !c.equals("__seg_id__")
                            && (keepStreamSeq || !c.equals("__stream_seq__")))
                .toList();
        for (String c : inputCols) {
          selects.add(new SqlIdentifier(c, POS));
        }
      } else {
        selects.add(SqlIdentifier.star(POS));
      }
      // PPL streamstats `bucket_nullable=false ... by X, Y` excludes rows where any partition
      // key is NULL from the window aggregate. v2 emits `WHEN IS NOT NULL [AND ...] THEN agg ELSE
      // null` — match that shape.
      SqlNode partitionNullCheck = null;
      if (!node.isBucketNullable() && !node.getGroupList().isEmpty()) {
        for (UnresolvedExpression g : node.getGroupList()) {
          UnresolvedExpression core = (g instanceof Alias a) ? a.getDelegated() : g;
          SqlNode pk = expr(core);
          SqlNode isNotNull = new SqlBasicCall(SqlStdOperatorTable.IS_NOT_NULL, List.of(pk), POS);
          partitionNullCheck =
              partitionNullCheck == null
                  ? isNotNull
                  : new SqlBasicCall(
                      SqlStdOperatorTable.AND, List.of(partitionNullCheck, isNotNull), POS);
        }
      }
      // PPL streamstats global semantics:
      //   - `global=true`  → window is N globally-preceding rows filtered by group partition.
      //                     Use RANGE frame on the global seq column.
      //   - `global=false` (default) → window is N rows within the group partition (ROWS frame).
      //   - With reset, also use RANGE on the global seq so reset boundaries respect global
      //     distance (a reset N rows away from current row excludes everything before it).
      SqlNodeList effectiveOrderBy = orderBy;
      boolean useRange = false;
      boolean hasGroup = !node.getGroupList().isEmpty();
      boolean seqInScope = false;
      if (rowTypeOracle != null && state.from != null) {
        try {
          seqInScope = deriveColumnNames(state.from).contains("__stream_seq__");
        } catch (RuntimeException ignored5) {
          // probe failed — fall through with ROWS frame
        }
      }
      if (hasReset || (node.isGlobal() && hasGroup && seqInScope && win > 0)) {
        effectiveOrderBy = new SqlNodeList(POS);
        effectiveOrderBy.add(new SqlIdentifier("__stream_seq__", POS));
        useRange = true;
      }
      // streamstats dc(x) prep — Calcite forbids COUNT(DISTINCT) OVER and the
      // partition-wide dense_rank trick is wrong for cumulative semantics. Emit
      // is_first_<i> = (case when x is null then 0 when row_number() over (partition
      // by [g,] x order by seq) = 1 then 1 else 0 end) as a wrapped column, then in
      // the agg loop swap dc(x) for SUM(is_first_<i>) OVER (... rows unbounded
      // preceding). Splitting into two SELECTs avoids the "Aggregate expressions
      // cannot be nested" error from putting ROW_NUMBER OVER inside SUM OVER.
      java.util.Map<Integer, String> dcIsFirstColForFnIdx = new java.util.HashMap<>();
      List<UnresolvedExpression> wfns = node.getWindowFunctionList();
      boolean dcWrapNeeded = false;
      for (int i = 0; i < wfns.size(); i++) {
        Alias al = (Alias) wfns.get(i);
        WindowFunction wf = (WindowFunction) al.getDelegated();
        if (isDistinctCount(wf.getFunction()) && !effectiveOrderBy.getList().isEmpty()) {
          UnresolvedExpression argExpr = null;
          if (wf.getFunction() instanceof org.opensearch.sql.ast.expression.AggregateFunction af) {
            argExpr = af.getField();
          } else if (wf.getFunction() instanceof Function f && !f.getFuncArgs().isEmpty()) {
            argExpr = f.getFuncArgs().get(0);
          }
          if (argExpr == null) continue;
          SqlNodeList rnPart = new SqlNodeList(POS);
          for (SqlNode p : partitionBy.getList()) rnPart.add(p);
          rnPart.add(expr(argExpr));
          SqlNode rnWindow =
              org.apache.calcite.sql.SqlWindow.create(
                  null,
                  null,
                  rnPart,
                  effectiveOrderBy,
                  SqlLiteral.createBoolean(false, POS),
                  null,
                  null,
                  null,
                  POS);
          SqlNode rn =
              new SqlBasicCall(
                  SqlStdOperatorTable.OVER,
                  List.of(
                      new SqlBasicCall(SqlStdOperatorTable.ROW_NUMBER, List.of(), POS), rnWindow),
                  POS);
          SqlNode isNullArg =
              new SqlBasicCall(SqlStdOperatorTable.IS_NULL, List.of(expr(argExpr)), POS);
          SqlNode rnEqOne =
              new SqlBasicCall(SqlStdOperatorTable.EQUALS, List.of(rn, intLiteral(1)), POS);
          SqlNodeList isFirstWhens = new SqlNodeList(POS);
          isFirstWhens.add(isNullArg);
          isFirstWhens.add(rnEqOne);
          SqlNodeList isFirstThens = new SqlNodeList(POS);
          isFirstThens.add(intLiteral(0));
          isFirstThens.add(intLiteral(1));
          SqlNode isFirst =
              new org.apache.calcite.sql.fun.SqlCase(
                  POS, null, isFirstWhens, isFirstThens, intLiteral(0));
          String colName = "_dc_is_first_" + i + "_";
          state.addEvalAlias(isFirst, colName);
          dcIsFirstColForFnIdx.put(i, colName);
          dcWrapNeeded = true;
        }
      }
      if (dcWrapNeeded) {
        state.wrap();
        // Re-derive input cols / partition exprs after wrap. The wrapped subquery
        // exposes both original columns and the is_first columns.
        if (rowTypeOracle != null && state.from != null) {
          inputCols =
              deriveColumnNames(state.from).stream()
                  .filter(
                      c ->
                          !org.opensearch.sql.calcite.plan.OpenSearchConstants
                                  .METADATAFIELD_TYPE_MAP
                                  .containsKey(c)
                              && !c.equals("__seg_id__")
                              && !c.startsWith("_dc_is_first_")
                              && (keepStreamSeq || !c.equals("__stream_seq__")))
                  .toList();
          selects = new ArrayList<>();
          for (String c : inputCols) {
            selects.add(new SqlIdentifier(c, POS));
          }
        }
      }
      for (int i = 0; i < wfns.size(); i++) {
        Alias al = (Alias) wfns.get(i);
        WindowFunction wf = (WindowFunction) al.getDelegated();
        rejectUnsupportedEventstatsAgg(wf.getFunction());
        SqlNode aggNode;
        if (dcIsFirstColForFnIdx.containsKey(i)) {
          // SUM(is_first_<i>) — outer running window applied below as normal.
          aggNode =
              new SqlBasicCall(
                  SqlStdOperatorTable.SUM,
                  List.of(new SqlIdentifier(dcIsFirstColForFnIdx.get(i), POS)),
                  POS);
        } else {
          aggNode = aggCall(wf.getFunction(), true);
        }
        SqlNode window;
        if (dcIsFirstColForFnIdx.containsKey(i)) {
          // dc(x) → SUM(is_first_<i>) OVER (PARTITION BY [g] ORDER BY seq
          //                                  ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW).
          // Always cumulative regardless of streamstats `window=N` setting — distinct count is
          // running over the full preceding stream by PPL semantics.
          window =
              org.apache.calcite.sql.SqlWindow.create(
                  null,
                  null,
                  partitionBy,
                  effectiveOrderBy,
                  SqlLiteral.createBoolean(true, POS),
                  org.apache.calcite.sql.SqlWindow.createUnboundedPreceding(POS),
                  org.apache.calcite.sql.SqlWindow.createCurrentRow(POS),
                  null,
                  POS);
        } else {
          window =
              org.apache.calcite.sql.SqlWindow.create(
                  null,
                  null,
                  partitionBy,
                  effectiveOrderBy,
                  /* isRows */ SqlLiteral.createBoolean(!useRange, POS),
                  lower,
                  upper,
                  null,
                  POS);
        }
        SqlNode over = new SqlBasicCall(SqlStdOperatorTable.OVER, List.of(aggNode, window), POS);
        // Wrap windowed AVG with CASE WHEN COUNT > 0 (PPL: empty -> NULL). For sample variance/
        // stddev (var_samp/stddev_samp), the divisor is (n-1) — Calcite returns 0 when n<=1, but
        // PPL expects NULL. Wrap those with CASE WHEN COUNT > 1 THEN ... ELSE NULL END.
        if (aggNode instanceof SqlBasicCall bc
            && bc.getOperandList().size() == 1
            && (bc.getOperator() == SqlStdOperatorTable.AVG
                || bc.getOperator() == SqlStdOperatorTable.VAR_POP
                || bc.getOperator() == SqlStdOperatorTable.VAR_SAMP
                || bc.getOperator() == SqlStdOperatorTable.STDDEV_POP
                || bc.getOperator() == SqlStdOperatorTable.STDDEV_SAMP)) {
          // AVG/VAR_POP/STDDEV_POP -> NULL when no non-null rows (count > 0).
          // VAR_SAMP/STDDEV_SAMP -> NULL when n<=1 (Bessel's correction divides by n-1).
          int minCount =
              (bc.getOperator() == SqlStdOperatorTable.VAR_SAMP
                      || bc.getOperator() == SqlStdOperatorTable.STDDEV_SAMP)
                  ? 1
                  : 0;
          SqlNode countCall =
              new SqlBasicCall(SqlStdOperatorTable.COUNT, List.of(bc.getOperandList().get(0)), POS);
          SqlNode countOver =
              new SqlBasicCall(SqlStdOperatorTable.OVER, List.of(countCall, window), POS);
          SqlNode countGtMin =
              new SqlBasicCall(
                  SqlStdOperatorTable.GREATER_THAN, List.of(countOver, intLiteral(minCount)), POS);
          SqlNodeList whens0 = new SqlNodeList(POS);
          whens0.add(countGtMin);
          SqlNodeList thens0 = new SqlNodeList(POS);
          thens0.add(over);
          over =
              new org.apache.calcite.sql.fun.SqlCase(
                  POS, null, whens0, thens0, SqlLiteral.createNull(POS));
        }
        SqlNode finalExpr = over;
        if (partitionNullCheck != null) {
          SqlNodeList whens = new SqlNodeList(POS);
          whens.add(partitionNullCheck);
          SqlNodeList thens = new SqlNodeList(POS);
          thens.add(over);
          finalExpr =
              new org.apache.calcite.sql.fun.SqlCase(
                  POS, null, whens, thens, SqlLiteral.createNull(POS));
        }
        selects.add(asAlias(finalExpr, al.getName()));
      }
      state.setProjection(selects);
      // TODO: streamstats by + reverse — testStreamstatsByWithReverse expects reverse to flip the
      // implicit __stream_seq__ order. Setting state.lastOrderBy = effectiveOrderBy enables a
      // downstream reverse to set an outer ORDER BY referencing __stream_seq__, but the column
      // is dropped from the projection (selects above) because the test schema doesn't include
      // it. The OS engine then trips ArrayIndexOutOfBoundsException trying to reference index 8
      // in an 8-column row. Proper fix: keep __stream_seq__ in the inner SELECT and drop it
      // only at the final output. Deferred.
      return null;
    }

    /** CASE WHEN <cond> THEN 1 ELSE 0 END — used for reset flag accumulation. */
    /** True if {@code e} is distinct_count / dc / distinct_count_approx. */
    private static boolean isDistinctCount(UnresolvedExpression e) {
      String name = null;
      if (e instanceof org.opensearch.sql.ast.expression.AggregateFunction af) {
        name = af.getFuncName();
      } else if (e instanceof Function f) {
        name = f.getFuncName();
      }
      if (name == null) return false;
      String lower = name.toLowerCase(java.util.Locale.ROOT);
      return lower.equals("distinct_count")
          || lower.equals("dc")
          || lower.equals("distinct_count_approx");
    }

    /**
     * Emulate {@code COUNT(DISTINCT x) OVER (PARTITION BY p)} (which Calcite rejects) using two
     * dense_rank windows: forward-rank + reverse-rank - 1 yields the partition's distinct value
     * count including NULL. PPL semantics ignore NULL in distinct_count, so subtract one when the
     * partition contains any NULL value.
     */
    private SqlNode distinctCountOverEmulation(UnresolvedExpression dcFn, SqlNodeList partitionBy) {
      UnresolvedExpression argExpr = null;
      if (dcFn instanceof org.opensearch.sql.ast.expression.AggregateFunction af) {
        argExpr = af.getField();
      } else if (dcFn instanceof Function f && !f.getFuncArgs().isEmpty()) {
        argExpr = f.getFuncArgs().get(0);
      }
      if (argExpr == null) {
        throw new UnsupportedOperationException("distinct_count requires a field argument");
      }
      SqlNode argAsc = expr(argExpr);
      SqlNode argDesc = new SqlBasicCall(SqlStdOperatorTable.DESC, List.of(expr(argExpr)), POS);
      SqlNodeList orderAsc = new SqlNodeList(POS);
      orderAsc.add(argAsc);
      SqlNodeList orderDesc = new SqlNodeList(POS);
      orderDesc.add(argDesc);
      SqlNode wAsc =
          org.apache.calcite.sql.SqlWindow.create(
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
          org.apache.calcite.sql.SqlWindow.create(
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
      SqlNode countDistinctIncludingNull =
          new SqlBasicCall(SqlStdOperatorTable.MINUS, List.of(sum, intLiteral(1)), POS);
      // If any NULL in partition, subtract 1. MAX(IS_NULL flag) over partition is 1 if any null.
      SqlNodeList wWhole = new SqlNodeList(POS);
      SqlNode maxNullWindow =
          org.apache.calcite.sql.SqlWindow.create(
              null,
              null,
              partitionBy,
              wWhole,
              SqlLiteral.createBoolean(false, POS),
              null,
              null,
              null,
              POS);
      SqlNodeList isNullWhens = new SqlNodeList(POS);
      isNullWhens.add(new SqlBasicCall(SqlStdOperatorTable.IS_NULL, List.of(expr(argExpr)), POS));
      SqlNodeList isNullThens = new SqlNodeList(POS);
      isNullThens.add(intLiteral(1));
      SqlNode nullFlag =
          new org.apache.calcite.sql.fun.SqlCase(
              POS, null, isNullWhens, isNullThens, intLiteral(0));
      SqlNode anyNullInPart =
          new SqlBasicCall(
              SqlStdOperatorTable.OVER,
              List.of(
                  new SqlBasicCall(SqlStdOperatorTable.MAX, List.of(nullFlag), POS), maxNullWindow),
              POS);
      // Result = rank-sum - 1 - (anyNullInPart) — reduces by one whenever a null bucket exists.
      return new SqlBasicCall(
          SqlStdOperatorTable.MINUS, List.of(countDistinctIncludingNull, anyNullInPart), POS);
    }

    /** Wrap {@code expr} in a CASE that returns NULL when any partition key is NULL. */
    private SqlNode partitionNullCheckOrNull(
        List<SqlNode> partitionExprs, boolean apply, SqlNode innerExpr) {
      if (!apply || partitionExprs.isEmpty()) return innerExpr;
      // v2 emits `CASE WHEN <key> IS NOT NULL [AND ...] THEN <agg> ELSE null END`. Match that
      // emission shape (vs the equivalent `WHEN IS NULL THEN null ELSE agg`) so the explain
      // plans agree.
      SqlNode check = null;
      for (SqlNode pk : partitionExprs) {
        SqlNode isNotNull = new SqlBasicCall(SqlStdOperatorTable.IS_NOT_NULL, List.of(pk), POS);
        check =
            check == null
                ? isNotNull
                : new SqlBasicCall(SqlStdOperatorTable.AND, List.of(check, isNotNull), POS);
      }
      SqlNodeList whens = new SqlNodeList(POS);
      whens.add(check);
      SqlNodeList thens = new SqlNodeList(POS);
      thens.add(innerExpr);
      return new org.apache.calcite.sql.fun.SqlCase(
          POS, null, whens, thens, SqlLiteral.createNull(POS));
    }

    /**
     * Eventstats/streamstats reject percentile aggregations (no window-context impl). v2 throws
     * UnsupportedOperationException with a specific message; tests assert on it.
     */
    private static void rejectUnsupportedEventstatsAgg(UnresolvedExpression e) {
      String name = null;
      if (e instanceof org.opensearch.sql.ast.expression.AggregateFunction af) {
        name = af.getFuncName();
      } else if (e instanceof Function f) {
        name = f.getFuncName();
      }
      if (name == null) return;
      String upper = name.toUpperCase(java.util.Locale.ROOT);
      if (upper.equals("PERCENTILE")
          || upper.equals("PERCENTILE_APPROX")
          || upper.equals("MEDIAN")) {
        throw new UnsupportedOperationException("Unexpected window function: " + upper);
      }
    }

    private SqlNode caseFlag(SqlNode cond) {
      SqlNodeList whens = new SqlNodeList(POS);
      whens.add(cond);
      SqlNodeList thens = new SqlNodeList(POS);
      thens.add(intLiteral(1));
      return new org.apache.calcite.sql.fun.SqlCase(POS, null, whens, thens, intLiteral(0));
    }

    @Override
    public Void visitWindow(Window node, Void ignored) {
      walkChild(node);
      // eventstats appends agg-OVER columns to the row. Wrap if there's pending state that would
      // make alias visibility ambiguous in the new projection.
      if (state.evalExtended || state.projectionReplaced) {
        state.wrap();
      }
      // Projection is "<input cols>, <each window func> AS <alias>". Enumerate the inputs
      // explicitly so we can filter out OpenSearch metadata columns (PPL hides _id, _index,
      // _score, ...). Fall back to `*` when no oracle is available.
      List<SqlNode> selects = new ArrayList<>();
      if (rowTypeOracle != null && state.from != null) {
        List<String> inputCols =
            deriveColumnNames(state.from).stream()
                .filter(
                    c ->
                        !org.opensearch.sql.calcite.plan.OpenSearchConstants.METADATAFIELD_TYPE_MAP
                            .containsKey(c))
                .toList();
        for (String c : inputCols) {
          selects.add(new SqlIdentifier(c, POS));
        }
      } else {
        selects.add(SqlIdentifier.star(POS));
      }
      SqlNodeList partitionBy = new SqlNodeList(POS);
      List<SqlNode> partitionExprs = new ArrayList<>();
      for (UnresolvedExpression p : node.getGroupList()) {
        UnresolvedExpression core = (p instanceof Alias a) ? a.getDelegated() : p;
        SqlNode k = expr(core);
        partitionBy.add(k);
        partitionExprs.add(k);
      }
      // PPL `eventstats bucket_nullable=false ... by X, Y` excludes rows where any partition key
      // is NULL from the aggregate. v2 emits `WHEN <key> IS NOT NULL [AND ...] THEN <agg> ELSE
      // null`. partitionNullCheck stores the all-not-null check.
      boolean nullBuckets = node.isBucketNullable() || partitionExprs.isEmpty();
      SqlNode partitionNullCheck = null;
      if (!nullBuckets) {
        for (SqlNode pk : partitionExprs) {
          SqlNode isNotNull = new SqlBasicCall(SqlStdOperatorTable.IS_NOT_NULL, List.of(pk), POS);
          partitionNullCheck =
              partitionNullCheck == null
                  ? isNotNull
                  : new SqlBasicCall(
                      SqlStdOperatorTable.AND, List.of(partitionNullCheck, isNotNull), POS);
        }
      }
      for (UnresolvedExpression item : node.getWindowFunctionList()) {
        Alias al = (Alias) item;
        WindowFunction wf = (WindowFunction) al.getDelegated();
        rejectUnsupportedEventstatsAgg(wf.getFunction());
        // Calcite forbids COUNT(DISTINCT x) in OVER. Emulate via the standard SQL identity
        //   dc(x) = dense_rank() OVER (PARTITION BY p ORDER BY x ASC NULLS LAST)
        //         + dense_rank() OVER (PARTITION BY p ORDER BY x DESC NULLS LAST) - 1
        //         - (1 if any null else 0)
        // Skipping the null-correction here yields PPL's "ignore nulls" semantic for
        // distinct_count.
        if (isDistinctCount(wf.getFunction())) {
          SqlNode dcOver = distinctCountOverEmulation(wf.getFunction(), partitionBy);
          SqlNode finalDc =
              partitionNullCheckOrNull(partitionExprs, !node.isBucketNullable(), dcOver);
          selects.add(asAlias(finalDc, al.getName()));
          continue;
        }
        SqlNode aggNode = aggCall(wf.getFunction(), true);
        SqlNode window =
            org.apache.calcite.sql.SqlWindow.create(
                /* declName */ null,
                /* refName */ null,
                partitionBy,
                /* orderList */ new SqlNodeList(POS),
                /* isRows */ SqlLiteral.createBoolean(false, POS),
                /* lowerBound */ null,
                /* upperBound */ null,
                /* allowPartial */ null,
                POS);
        SqlNode over = new SqlBasicCall(SqlStdOperatorTable.OVER, List.of(aggNode, window), POS);
        // Calcite's standard AVG over a partition with all-null values returns 0 (because the
        // underlying convertlet rewrite to SUM/COUNT yields 0/0 = 0 in the enumerable runtime).
        // PPL semantics: NULL when no rows contribute. Wrap AVG with CASE WHEN COUNT > 0.
        // Sample variance/stddev (var_samp/stddev_samp) divide by (n-1); Calcite returns 0 when
        // n<=1 but PPL expects NULL — wrap with COUNT > 1.
        if (aggNode instanceof SqlBasicCall bc
            && bc.getOperandList().size() == 1
            && (bc.getOperator() == SqlStdOperatorTable.AVG
                || bc.getOperator() == SqlStdOperatorTable.VAR_POP
                || bc.getOperator() == SqlStdOperatorTable.VAR_SAMP
                || bc.getOperator() == SqlStdOperatorTable.STDDEV_POP
                || bc.getOperator() == SqlStdOperatorTable.STDDEV_SAMP)) {
          // AVG/VAR_POP/STDDEV_POP -> NULL when no non-null rows.
          // VAR_SAMP/STDDEV_SAMP -> NULL when n<=1 (Bessel's correction).
          int minCount =
              (bc.getOperator() == SqlStdOperatorTable.VAR_SAMP
                      || bc.getOperator() == SqlStdOperatorTable.STDDEV_SAMP)
                  ? 1
                  : 0;
          SqlNode countCall =
              new SqlBasicCall(SqlStdOperatorTable.COUNT, List.of(bc.getOperandList().get(0)), POS);
          SqlNode countOver =
              new SqlBasicCall(SqlStdOperatorTable.OVER, List.of(countCall, window), POS);
          SqlNode countGtMin =
              new SqlBasicCall(
                  SqlStdOperatorTable.GREATER_THAN, List.of(countOver, intLiteral(minCount)), POS);
          SqlNodeList whens = new SqlNodeList(POS);
          whens.add(countGtMin);
          SqlNodeList thens = new SqlNodeList(POS);
          thens.add(over);
          over =
              new org.apache.calcite.sql.fun.SqlCase(
                  POS, null, whens, thens, SqlLiteral.createNull(POS));
        }
        // bucket_nullable=false: wrap in CASE WHEN <all partition keys NOT NULL> THEN over ELSE
        // null. partitionNullCheck stores the all-not-null check.
        SqlNode finalExpr = over;
        if (partitionNullCheck != null) {
          SqlNodeList whens = new SqlNodeList(POS);
          whens.add(partitionNullCheck);
          SqlNodeList thens = new SqlNodeList(POS);
          thens.add(over);
          finalExpr =
              new org.apache.calcite.sql.fun.SqlCase(
                  POS, null, whens, thens, SqlLiteral.createNull(POS));
        }
        selects.add(asAlias(finalExpr, al.getName()));
      }
      state.setProjection(selects);
      // Mark projection as replaced so subsequent pipes wrap, but we must NOT mark evalExtended
      // (that would force the next where to wrap unnecessarily; aliases here are visible to
      // downstream pipes via the wrap).
      return null;
    }

    @Override
    public Void visitAggregation(Aggregation node, Void ignored) {
      walkChild(node);
      // PPL `head N | stats ...` caps the input to the aggregate at N rows. Outer-level
      // sort/fetch normally survive across pipes (informational on intermediate layers, applied
      // outermost), but Aggregation changes the row set — so we have to materialize any pending
      // outer SORT/FETCH into the subquery before aggregating.
      flushOuterIntoInner();
      // Aggregation destroys the row-level sort key collation; downstream `reverse` would have
      // nothing meaningful to flip without an explicit new sort.
      state.lastOrderBy = null;
      // Aggregation always changes the row set; wrap any pending pipe state into a subquery so
      // GROUP BY operates on the input rows, not the post-aggregation rows.
      boolean hadPreAggComplexity =
          state.where != null
              || state.evalExtended
              || state.projectionReplaced
              || state.orderBy != null
              || state.fetch != null;
      if (hadPreAggComplexity) {
        state.wrap();
      }
      // PPL stats output ordering: aggregations first, then group-by columns (span before
      // explicit by-fields). v2's visitAggregation explicitly reorders the row layout to
      // (metrics, group-by) and prepends span to the group expressions — match that here.
      List<SqlNode> aggSelects = new ArrayList<>();
      List<SqlNode> groupSelects = new ArrayList<>();
      List<SqlNode> groupKeys = new ArrayList<>();
      // Span goes first in the group list when present.
      if (node.getSpan() != null) {
        UnresolvedExpression spanExpr = node.getSpan();
        String spanAlias = null;
        UnresolvedExpression spanCore = spanExpr;
        if (spanExpr instanceof Alias al) {
          spanAlias = al.getName();
          spanCore = al.getDelegated();
        }
        // When PPL doesn't supply an explicit alias for `span(field, N[unit])`, generate
        // the auto-derived display name (matching v2's `span(field,N[unit])`).
        if (spanAlias == null && spanCore instanceof org.opensearch.sql.ast.expression.Span sp) {
          spanAlias = formatSpanAlias(sp);
        }
        SqlNode key = expr(spanCore);
        // Pre-wrap state.from with the span computed as an alias, so the GROUP BY references the
        // alias name and Calcite's pre-aggregate Project shows `span(field,N[unit])=[SPAN(...)]`
        // instead of `$f0=[SPAN(...)]`. This matches v2's emission shape.
        //
        // Skip when state.from contains a SqlJoin: the wrap() collapses the join's per-side
        // aliases (`a`, `b`) into a single anonymous subquery, which breaks downstream
        // qualified references like `b.country` in `stats by ..., b.country`. For the join
        // path, leave the SPAN call inline at the GROUP BY (cosmetic mismatch, but functionally
        // correct).
        boolean joinUpstream =
            state.from instanceof org.apache.calcite.sql.SqlJoin || state.joinLeftAlias != null;
        // Skip pre-wrap when ANY pre-agg complexity (where/eval/projection/sort/fetch) was
        // already wrapped above — the wrap-Project would expose all source cols incl metadata
        // above the user's filter/eval, producing an extra wide Project layer that v2 doesn't
        // have. Only do pre-wrap for the simple `source=t | stats count() by span(...)` case.
        if (spanAlias != null && !joinUpstream && !hadPreAggComplexity) {
          // Add an evalAlias for the span column, then wrap so it becomes a real column.
          state.addEvalAlias(key, spanAlias);
          state.wrap();
          // Replace the SPAN call with a bare reference to the alias name.
          SqlNode aliasRef = new SqlIdentifier(spanAlias, POS);
          groupKeys.add(aliasRef);
          groupSelects.add(aliasRef);
        } else if (spanAlias != null) {
          // Join upstream — keep span call inline and emit alias on the GROUP BY (won't affect
          // the inner pre-aggregate Project's column name but produces correct results).
          groupKeys.add(key);
          groupSelects.add(asAlias(key, spanAlias));
        } else {
          groupKeys.add(key);
          groupSelects.add(key);
        }
      }
      java.util.List<String> groupKeyNames = new java.util.ArrayList<>();
      if (node.getGroupExprList() != null) {
        for (UnresolvedExpression g : node.getGroupExprList()) {
          SqlNode key;
          String alias = null;
          UnresolvedExpression core = g;
          if (g instanceof Alias a) {
            key = expr(a.getDelegated());
            alias = a.getName();
            core = a.getDelegated();
          } else {
            key = expr(g);
          }
          // Capture the group key's source name so we can detect dotted aggregation paths over it.
          if (core instanceof Field f && f.getField() instanceof QualifiedName qn) {
            groupKeyNames.add(qn.toString());
          } else if (core instanceof QualifiedName qn) {
            groupKeyNames.add(qn.toString());
          }
          groupKeys.add(key);
          groupSelects.add(alias != null ? asAlias(key, alias) : key);
        }
      }
      // PPL `stats <agg(group.leaf)> by group` requires nested-aggregation pushdown which the
      // SqlNode pipeline does not implement. Mirror v2's CalciteEnumerableNestedAggregate runtime
      // error at translation time: if any agg arg dotted-name shares a prefix with a group key,
      // fail with the documented "Cannot execute nested aggregation" message. v2 raises this at
      // execution time when the rule can't apply; we surface it earlier via ErrorReport.
      for (UnresolvedExpression a : node.getAggExprList()) {
        UnresolvedExpression core = a instanceof Alias al ? al.getDelegated() : a;
        if (core instanceof org.opensearch.sql.ast.expression.AggregateFunction af) {
          UnresolvedExpression argExpr = af.getField();
          if (argExpr instanceof Field f) {
            argExpr = f.getField();
          }
          if (argExpr instanceof QualifiedName qn) {
            String argName = qn.toString();
            for (String gk : groupKeyNames) {
              if (argName.startsWith(gk + ".")) {
                throw org.opensearch.sql.common.error.ErrorReport.wrap(
                        new IllegalArgumentException(
                            "Cannot execute nested aggregation on " + argName))
                    .build();
              }
            }
            // Nested aggregation pushdown only applies when the upstream input is the raw scan
            // (no `head N`, filter, eval, or other complexity that would force the agg to run
            // post-pushdown). When `head` is upstream of stats with a dotted agg arg, v2's
            // pushdown rule fails to apply at the OpenSearch composite-source level and the
            // execution-time check raises "Cannot execute nested aggregation on ...". Surface
            // this earlier (before execution).
            //
            // Skip when the dotted path is a MAP/STRUCT-leaf access (e.g. spath-rewritten
            // `doc.user.age` after `eval doc = json_extract_all(doc)`). Those navigate into a
            // MAP-typed column via tryMapOrStructItemAccess and don't go through OpenSearch
            // nested-aggregation pushdown — they're plain SQL ITEM access aggregations.
            int lastDot = argName.lastIndexOf('.');
            boolean isMapLeafAccess = false;
            if (lastDot >= 0) {
              SqlNode itemAccess =
                  tryMapOrStructItemAccess(
                      QualifiedName.of(java.util.Arrays.asList(argName.split("\\."))));
              isMapLeafAccess = itemAccess != null;
            }
            if (lastDot >= 0
                && hadPreAggComplexity
                && !groupKeyNames.isEmpty()
                && !isMapLeafAccess) {
              throw org.opensearch.sql.common.error.ErrorReport.wrap(
                      new IllegalArgumentException(
                          "Cannot execute nested aggregation on " + argName))
                  .build();
            }
          }
        }
      }
      for (UnresolvedExpression a : node.getAggExprList()) {
        if (a instanceof Alias al) {
          aggSelects.add(asAlias(aggCall(al.getDelegated()), al.getName()));
        } else {
          aggSelects.add(aggCall(a));
        }
      }
      List<SqlNode> selects = new ArrayList<>(aggSelects.size() + groupSelects.size());
      selects.addAll(aggSelects);
      selects.addAll(groupSelects);
      // PPL `stats ... by X bucket_nullable=false` excludes rows where any group key is NULL.
      // The argExprList carries the option literal; default is true (keep null buckets).
      boolean bucketNullable = true;
      if (node.getArgExprList() != null) {
        for (org.opensearch.sql.ast.expression.Argument arg : node.getArgExprList()) {
          if (org.opensearch.sql.ast.expression.Argument.BUCKET_NULLABLE.equals(arg.getArgName())
              && arg.getValue() instanceof org.opensearch.sql.ast.expression.Literal lit
              && Boolean.FALSE.equals(lit.getValue())) {
            bucketNullable = false;
          }
        }
      }
      if (!bucketNullable && !groupKeys.isEmpty()) {
        // For span-based group keys, filter on the field (not the SPAN call) — v2's emission
        // shape uses the source field directly, which is also pushdown-friendly.
        UnresolvedExpression spanCore =
            node.getSpan() instanceof Alias al ? al.getDelegated() : node.getSpan();
        SqlNode spanFieldKey = null;
        if (spanCore instanceof org.opensearch.sql.ast.expression.Span sp) {
          spanFieldKey = expr(sp.getField());
        }
        for (int i = 0; i < groupKeys.size(); i++) {
          SqlNode k = groupKeys.get(i);
          // First slot is the SPAN group key when present; replace with field.
          if (i == 0 && spanFieldKey != null && node.getSpan() != null) {
            k = spanFieldKey;
          }
          state.addWhere(new SqlBasicCall(SqlStdOperatorTable.IS_NOT_NULL, List.of(k), POS));
        }
      } else if (groupKeys.isEmpty() && !node.getAggExprList().isEmpty()) {
        // doc_count optimization: when there's no GROUP BY and every agg is count(field) or
        // dc(field) with the same single field arg (no count(*) and no mixed fields), add
        // IS NOT NULL(field) so OpenSearch pushdown emits an `exists` query (uses doc_count
        // instead of value_count). Mirrors v2's CalciteRelNodeVisitor.java:1462-1483
        // (aggregateWithTrimming doc_count optimization).
        java.util.Set<String> distinctFields = new java.util.LinkedHashSet<>();
        boolean allEligible = true;
        for (UnresolvedExpression a : node.getAggExprList()) {
          UnresolvedExpression core = a instanceof Alias al ? al.getDelegated() : a;
          String name = null;
          UnresolvedExpression argExpr = null;
          if (core instanceof org.opensearch.sql.ast.expression.AggregateFunction af) {
            name = af.getFuncName().toLowerCase(java.util.Locale.ROOT);
            argExpr = af.getField();
          } else if (core instanceof Function f) {
            name = f.getFuncName().toLowerCase(java.util.Locale.ROOT);
            argExpr = f.getFuncArgs().isEmpty() ? null : f.getFuncArgs().get(0);
          }
          boolean isCountish =
              "count".equals(name)
                  || "distinct_count".equals(name)
                  || "dc".equals(name)
                  || "distinct_count_approx".equals(name);
          if (!isCountish || argExpr == null || argExpr instanceof AllFields) {
            allEligible = false;
            break;
          }
          QualifiedName qn = null;
          if (argExpr instanceof Field f && f.getField() instanceof QualifiedName qq) qn = qq;
          else if (argExpr instanceof QualifiedName qq) qn = qq;
          if (qn == null) {
            allEligible = false;
            break;
          }
          distinctFields.add(qn.toString());
        }
        if (allEligible && distinctFields.size() == 1) {
          String fieldName = distinctFields.iterator().next();
          state.addWhere(
              new SqlBasicCall(
                  SqlStdOperatorTable.IS_NOT_NULL,
                  List.of(
                      qualifiedNameToFieldIdentifier(
                          QualifiedName.of(java.util.Arrays.asList(fieldName.split("\\."))))),
                  POS));
        }
      } else if (node.getSpan() != null && isTimeSpan(node.getSpan())) {
        // Time-unit Span always filters NULL bucket (matches v2 visitAggregation behavior at
        // CalciteRelNodeVisitor.java:1646-1648). Without this, head/limit upstream that picks
        // rows whose span field is NULL will produce a NULL-keyed group that PPL hides.
        // Filter on the FIELD itself (not the wrapped SPAN call) — v2 emits
        // IS NOT NULL($field), not IS NOT NULL(SPAN($field, ...)). The field-level filter is
        // more pushdown-friendly (term-not-exists query vs script).
        UnresolvedExpression spanCore =
            node.getSpan() instanceof Alias al ? al.getDelegated() : node.getSpan();
        SqlNode nullCheckTarget;
        if (spanCore instanceof org.opensearch.sql.ast.expression.Span sp) {
          nullCheckTarget = expr(sp.getField());
        } else {
          nullCheckTarget = expr(spanCore);
        }
        state.addWhere(
            new SqlBasicCall(SqlStdOperatorTable.IS_NOT_NULL, List.of(nullCheckTarget), POS));
      }
      state.setProjection(selects);
      state.setGroupBy(groupKeys);
      // PPL's stats output ordering with a span() group key follows insertion order from the
      // OpenSearch terms aggregation, which v2's RelBuilder pipeline preserves. For
      // SqlNode pipelines built directly on a JOIN'd source (where the validator's GROUP BY
      // doesn't naturally inherit the join collation), add an implicit ORDER BY on the span
      // column. For non-join sources, v2's RelBuilder produces an unordered aggregate too —
      // adding our own sort here causes cosmetic plan-shape diffs in CalciteExplainIT/Big5.
      // Only fire after a JOIN or when state.from carries a SqlJoin (multi-join chains).
      boolean hasJoinUpstream =
          state.from instanceof org.apache.calcite.sql.SqlJoin || state.joinLeftAlias != null;
      if (hasJoinUpstream
          && node.getSpan() != null
          && state.outerOrderBy == null
          && state.lastOrderBy == null) {
        UnresolvedExpression spanExpr = node.getSpan();
        SqlNode sortKey;
        if (spanExpr instanceof Alias al) {
          sortKey = new SqlIdentifier(al.getName(), POS);
        } else {
          sortKey = expr(spanExpr);
        }
        state.setOuterOrderBy(java.util.List.of(sortKey));
      }
      return null;
    }

    @Override
    public Void visitSort(Sort node, Void ignored) {
      walkChild(node);
      // Consecutive sorts: PPL `... | sort A | sort B` should produce TWO LogicalSort nodes (the
      // outer sort wins, but the inner sort is preserved for stable ordering on ties). v2 keeps
      // both. Without flushing, the second sort overwrites the first and the inner ordering is
      // lost (issue 5125). Flush the prior outer ORDER BY into the inner pipeline (wrap) before
      // applying the new outer sort.
      //
      // Also flush when there's a pending outerFetch (from `head N`). PPL `head N | sort A`
      // semantically applies head first, then sorts the N retained rows; v2 emits this as
      // `Sort(sort0=A) > Sort(fetch=N) > Scan` (two separate LogicalSort nodes). Without
      // flushing, both ORDER BY and FETCH would attach to the same SqlSelect and Calcite
      // would collapse them into a single LogicalSort.
      if (state.outerOrderBy != null || state.outerFetch != null) {
        flushOuterIntoInner();
        state.wrap();
      }
      List<SqlNode> keys = new ArrayList<>(node.getSortList().size());
      for (Field f : node.getSortList()) {
        SqlNode key = expr(f.getField());
        Sort.SortOption opt = analyzeSortOption(f.getFieldArgs());
        if (opt.getSortOrder() == Sort.SortOrder.DESC) {
          key = new SqlBasicCall(SqlStdOperatorTable.DESC, List.of(key), POS);
        }
        SqlOperator nullsOp =
            opt.getNullOrder() == Sort.NullOrder.NULL_LAST
                ? SqlStdOperatorTable.NULLS_LAST
                : SqlStdOperatorTable.NULLS_FIRST;
        key = new SqlBasicCall(nullsOp, List.of(key), POS);
        keys.add(key);
      }
      state.setOuterOrderBy(keys);
      if (node.getCount() != null && node.getCount() != 0) {
        state.setOuterFetch(intLiteral(node.getCount()));
      }
      return null;
    }

    @Override
    public Void visitHead(Head node, Void ignored) {
      walkChild(node);
      // Consecutive limit/head: PPL `... | head 5 | head 10` should produce TWO LogicalSort
      // nodes (5 inside 10) so the inner cap takes effect first. v2 preserves both. Without
      // flushing, the second head overwrites the first and the inner cap is lost. Only flush
      // when an existing outerFetch is present (sort+head is a single composite outer order +
      // limit; flushing those splits them inappropriately).
      if (state.outerFetch != null) {
        flushOuterIntoInner();
        state.wrap();
      }
      state.setOuterFetch(intLiteral(node.getSize()));
      Integer fromOffset = node.getFrom();
      if (fromOffset != null && fromOffset > 0) {
        state.outerOffset = intLiteral(fromOffset);
      }
      return null;
    }

    @Override
    public Void visitLimit(Limit node, Void ignored) {
      walkChild(node);
      // Consecutive limit: same flush-and-wrap as visitHead so two limits stack rather than
      // the second overwriting the first.
      if (state.outerFetch != null) {
        flushOuterIntoInner();
        state.wrap();
      }
      state.setOuterFetch(intLiteral(node.getLimit()));
      return null;
    }

    @Override
    public Void visitReverse(org.opensearch.sql.ast.tree.Reverse node, Void ignored) {
      walkChild(node);
      // PPL `reverse` flips the existing ordering. SQL has no "reverse" — but if the pipeline has
      // an outer ORDER BY (from sort/head), we can flip every key's direction. Without an existing
      // sort, fall back: try a default `@timestamp DESC` (matches v2's behavior). If neither is
      // available, the command becomes a no-op.
      if (state.outerOrderBy != null && !state.outerOrderBy.isEmpty()) {
        List<SqlNode> reversed = new ArrayList<>(state.outerOrderBy.size());
        for (SqlNode key : state.outerOrderBy) {
          reversed.add(reverseSortKey(key));
        }
        state.setOuterOrderBy(reversed);
        return null;
      }
      // The outer order may have been flushed into the inner pipeline already (e.g. by an
      // upstream where/aggregation). Reverse the flushed orderBy in place if present.
      if (state.orderBy != null && !state.orderBy.isEmpty()) {
        List<SqlNode> reversed = new ArrayList<>(state.orderBy.size());
        for (SqlNode key : state.orderBy) {
          reversed.add(reverseSortKey(key));
        }
        state.orderBy = reversed;
        state.lastOrderBy = reversed;
        return null;
      }
      // After projection-driven wraps, both inner and outer order may have been swallowed by
      // the subquery boundary. Fall back to lastOrderBy — the latest sort keys we've seen —
      // and apply them as a NEW outer ORDER BY in reversed direction. The keys may reference
      // columns that the current projection has dropped, but those columns are still visible
      // in the wrapped subquery's row type.
      if (state.lastOrderBy != null && !state.lastOrderBy.isEmpty()) {
        List<SqlNode> reversed = new ArrayList<>(state.lastOrderBy.size());
        for (SqlNode key : state.lastOrderBy) {
          reversed.add(reverseSortKey(key));
        }
        state.setOuterOrderBy(reversed);
        return null;
      }
      // No prior sort. Probe the in-flight pipeline row type for either:
      //   1. An upstream `__stream_seq__` (kept by streamstats so reverse can reverse the
      //      synthetic ROW_NUMBER ordering) — flip to DESC so the rows come back in reverse seq.
      //   2. The `@timestamp` implicit field — fall back to @timestamp DESC.
      // The strip pass at SqlNodePlanner removes __stream_seq__ from the user-facing schema.
      if (rowTypeOracle != null) {
        try {
          SqlNode pipelineSnapshot = state.toFinalSqlNode();
          List<String> cols = deriveColumnNames(pipelineSnapshot);
          SqlNode chosenKey = null;
          if (cols.contains("__stream_seq__")) {
            chosenKey = new SqlIdentifier("__stream_seq__", POS);
          } else if (cols.contains(
              org.opensearch.sql.calcite.plan.OpenSearchConstants.IMPLICIT_FIELD_TIMESTAMP)) {
            chosenKey =
                new SqlIdentifier(
                    org.opensearch.sql.calcite.plan.OpenSearchConstants.IMPLICIT_FIELD_TIMESTAMP,
                    POS);
          }
          if (chosenKey != null) {
            SqlNode key = new SqlBasicCall(SqlStdOperatorTable.DESC, List.of(chosenKey), POS);
            key = new SqlBasicCall(SqlStdOperatorTable.NULLS_FIRST, List.of(key), POS);
            state.setOuterOrderBy(List.of(key));
          }
        } catch (RuntimeException ignored2) {
          // probe failed; reverse becomes a no-op
        }
      }
      return null;
    }

    @Override
    public Void visitConvert(org.opensearch.sql.ast.tree.Convert node, Void ignored) {
      walkChild(node);
      // PPL `convert fn(field) [AS alias]`: if alias is absent (target==source) replace the
      // source column in-place; otherwise append an aliased column. Conversion functions (auto,
      // num, tonumber, rmcomma, ctime, mktime, ...) are registered on PPLBuiltinOperators and
      // resolved by the validator.
      if (state.evalExtended || state.projectionReplaced) {
        state.wrap();
      }
      List<org.opensearch.sql.ast.expression.Let> conversions = node.getConversions();
      if (conversions == null || conversions.isEmpty()) {
        return null;
      }
      List<String> cols =
          deriveColumnNames(state.from).stream()
              .filter(
                  c ->
                      !org.opensearch.sql.calcite.plan.OpenSearchConstants.METADATAFIELD_TYPE_MAP
                          .containsKey(c))
              .toList();
      java.util.Map<String, SqlNode> replacements = new java.util.LinkedHashMap<>();
      List<SqlNode> additions = new ArrayList<>();
      List<String> additionNames = new ArrayList<>();
      String timeFormat = node.getTimeFormat();
      for (org.opensearch.sql.ast.expression.Let conv : conversions) {
        String target = conv.getVar().getField().toString();
        UnresolvedExpression rhs = conv.getExpression();
        if (rhs instanceof Field srcField) {
          // `convert none(field) AS alias` — copy/rename only, no conversion. PPL's AstBuilder
          // emits a bare Field for the `none(...)` shorthand. When target == source, it's a
          // no-op; with an alias, append the source under the new name.
          String source = srcField.getField().toString();
          if (!target.equals(source)) {
            additions.add(expr(srcField));
            additionNames.add(target);
          }
          continue;
        }
        if (!(rhs instanceof Function fn)) {
          throw new IllegalArgumentException("Convert command requires function call expressions");
        }
        if (fn.getFuncArgs().size() != 1 || !(fn.getFuncArgs().get(0) instanceof Field srcField)) {
          throw new IllegalArgumentException(
              "Convert function must have exactly one field argument");
        }
        String source = srcField.getField().toString();
        // Time conversion functions (ctime, mktime) accept a custom timeformat as a 2nd arg when
        // the convert command supplies one. Mirror v2's resolveConvertFunction.
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
          additions.add(call);
          additionNames.add(target);
        }
      }
      List<SqlNode> selects = new ArrayList<>();
      for (String c : cols) {
        if (replacements.containsKey(c)) {
          selects.add(asAlias(replacements.get(c), c));
        } else {
          selects.add(new SqlIdentifier(c, POS));
        }
      }
      for (int i = 0; i < additions.size(); i++) {
        selects.add(asAlias(additions.get(i), additionNames.get(i)));
      }
      state.setProjection(selects);
      return null;
    }

    @Override
    public Void visitRegex(org.opensearch.sql.ast.tree.Regex node, Void ignored) {
      walkChild(node);
      // `regex field=pattern` / `regex field!=pattern` is a row filter.
      if (state.evalExtended || state.projectionReplaced) {
        state.wrap();
      }
      SqlNode field = expr(node.getField());
      // Cast pattern string to VARCHAR explicitly. Without the cast, Calcite emits a CHAR-typed
      // literal which doesn't match v2's emission shape (REGEXP_CONTAINS($0, 'pat':VARCHAR)).
      SqlNode pattern =
          castTo(expr(node.getPattern()), org.apache.calcite.sql.type.SqlTypeName.VARCHAR);
      SqlNode call =
          new SqlBasicCall(
              org.apache.calcite.sql.fun.SqlLibraryOperators.REGEXP_CONTAINS,
              List.of(field, pattern),
              POS);
      if (node.isNegated()) {
        call = new SqlBasicCall(SqlStdOperatorTable.NOT, List.of(call), POS);
      }
      state.addWhere(call);
      return null;
    }

    @Override
    public Void visitMvExpand(org.opensearch.sql.ast.tree.MvExpand node, Void ignored) {
      walkChild(node);
      // mvexpand is the same shape as expand but with an optional limit. Unlike expand, mvexpand
      // is a no-op for non-array fields. Without an oracle we can't introspect the field type, so
      // we always emit the UNNEST-join shape; if the field is scalar at runtime, validation will
      // catch it.
      state.wrap();
      String fieldName;
      UnresolvedExpression fieldExpr = node.getField().getField();
      if (fieldExpr instanceof QualifiedName qn) {
        fieldName = qn.toString();
      } else {
        throw new UnsupportedOperationException(
            "mvexpand requires a simple column reference, got: " + fieldExpr.getClass());
      }
      // PPL semantics: mvexpand on a non-array field is a no-op (passes input through unchanged).
      // Mirrors v2's CalciteRelNodeVisitor.visitMvExpand line 4097-4101. Probe the field's type
      // via rowTypeOracle; if it isn't ARRAY/MULTISET/MAP, skip the UNNEST emission. Without this
      // check, a scalar field like `skills_int` (INTEGER) trips Calcite's UNNEST operand checker.
      if (rowTypeOracle != null) {
        try {
          org.apache.calcite.rel.type.RelDataType rowType = deriveRowType(state.from);
          org.apache.calcite.rel.type.RelDataTypeField f =
              rowType.getField(fieldName, false, false);
          if (f != null) {
            org.apache.calcite.sql.type.SqlTypeName tn = f.getType().getSqlTypeName();
            boolean isExpandable =
                tn == org.apache.calcite.sql.type.SqlTypeName.ARRAY
                    || tn == org.apache.calcite.sql.type.SqlTypeName.MULTISET
                    || tn == org.apache.calcite.sql.type.SqlTypeName.MAP;
            if (!isExpandable) {
              // No-op for scalar field. Apply optional limit as outer fetch.
              if (node.getLimit() != null && node.getLimit() > 0) {
                state.setOuterFetch(intLiteral(node.getLimit()));
              }
              return null;
            }
          }
        } catch (RuntimeException ignored2) {
          // probe failed; fall through to UNNEST emission and let validator decide
        }
      }
      String inputAlias = "mvexpand_input";
      SqlNode aliasedInput =
          new SqlBasicCall(
              SqlStdOperatorTable.AS, List.of(state.from, new SqlIdentifier(inputAlias, POS)), POS);
      SqlNode unnestArg = new SqlIdentifier(java.util.Arrays.asList(inputAlias, fieldName), POS);
      SqlNode unnest = new SqlBasicCall(SqlStdOperatorTable.UNNEST, List.of(unnestArg), POS);
      SqlNode aliasedUnnest =
          new SqlBasicCall(
              SqlStdOperatorTable.AS,
              List.of(
                  unnest, new SqlIdentifier("mvexpand_t", POS), new SqlIdentifier(fieldName, POS)),
              POS);
      org.apache.calcite.sql.SqlJoin join =
          new org.apache.calcite.sql.SqlJoin(
              POS,
              aliasedInput,
              SqlLiteral.createBoolean(false, POS),
              org.apache.calcite.sql.JoinType.COMMA.symbol(POS),
              aliasedUnnest,
              org.apache.calcite.sql.JoinConditionType.NONE.symbol(POS),
              null);
      state.from = join;
      // Build an explicit projection that drops the original (array-valued) `fieldName` from the
      // input side and surfaces the unnested element as `fieldName`. Otherwise the join produces
      // both `mvexpand_input.skills` (array) and `mvexpand_t.skills` (element) and downstream
      // references trip the validator's ambiguity check.
      //
      // Also drop dotted DESCENDANTS of `fieldName` (e.g. `skills.name`, `skills.level` for
      // mvexpand on `skills`). OpenSearch's nested-field flattening exposes both the parent array
      // (skills) AND its leaves (skills.X) as separate scan columns. If we project the leaves
      // here, the OpenSearch pushdown returns flattened single values for them — bypassing the
      // LATERAL UNNEST and producing one row per source doc instead of one row per array element.
      // The leaves should be reached via mvexpand_t.skills.X (struct navigation on the unnested
      // element), not via the source-side flat columns.
      List<SqlNode> selects = new ArrayList<>();
      String dottedPrefix = fieldName + ".";
      if (rowTypeOracle != null) {
        try {
          List<String> inputCols = deriveColumnNames(aliasedInput);
          java.util.Set<String> inputColSet = new java.util.HashSet<>(inputCols);
          for (String c : inputCols) {
            if (org.opensearch.sql.calcite.plan.OpenSearchConstants.METADATAFIELD_TYPE_MAP
                .containsKey(c)) {
              continue;
            }
            if (c.equals(fieldName)) {
              continue;
            }
            if (c.startsWith(dottedPrefix)) {
              continue;
            }
            // Drop flat dotted leaves whose parent struct is in scope (v2's default
            // tryToRemoveNestedFields behaviour).
            int lastDot = c.lastIndexOf('.');
            if (lastDot != -1 && inputColSet.contains(c.substring(0, lastDot))) {
              continue;
            }
            selects.add(new SqlIdentifier(java.util.Arrays.asList(inputAlias, c), POS));
          }
        } catch (RuntimeException ignored2) {
          // probe failed; fall through to SELECT *
        }
      }
      // Add the unnested element column as `fieldName`.
      selects.add(new SqlIdentifier(java.util.Arrays.asList("mvexpand_t", fieldName), POS));
      state.projection = selects.isEmpty() ? null : selects;
      state.projectionReplaced = !selects.isEmpty();
      state.evalExtended = false;
      state.evalAliasNames.clear();
      // Limit caps the per-row array expansion. PPL semantics: each input doc emits at most N
      // unnested rows. Implement via ROW_NUMBER() OVER (PARTITION BY <input cols>) <= N — the
      // partition key needs to uniquely identify each input row, so include all non-dotted
      // input cols (which together identify the doc — a single OpenSearch hit's projection).
      if (node.getLimit() != null && node.getLimit() > 0) {
        // Wrap so the projection becomes a real subquery row-set.
        state.wrap();
        // Add ROW_NUMBER OVER (PARTITION BY <all input cols except `fieldName`>) eval alias.
        SqlNodeList partitionBy = new SqlNodeList(POS);
        if (rowTypeOracle != null) {
          try {
            for (String c : deriveColumnNames(state.from)) {
              if (org.opensearch.sql.calcite.plan.OpenSearchConstants.METADATAFIELD_TYPE_MAP
                  .containsKey(c)) {
                continue;
              }
              if (c.equals(fieldName)) {
                continue;
              }
              partitionBy.add(new SqlIdentifier(c, POS));
            }
          } catch (RuntimeException ignored2) {
            // probe failed; PARTITION BY remains empty (limit applies globally)
          }
        }
        SqlNode rowNumWindow =
            org.apache.calcite.sql.SqlWindow.create(
                null,
                null,
                partitionBy,
                new SqlNodeList(POS),
                SqlLiteral.createBoolean(false, POS),
                null,
                null,
                null,
                POS);
        SqlNode rowNumCall =
            new SqlBasicCall(
                SqlStdOperatorTable.OVER,
                List.of(
                    new SqlBasicCall(SqlStdOperatorTable.ROW_NUMBER, List.of(), POS), rowNumWindow),
                POS);
        state.addEvalAlias(rowNumCall, "__mvexpand_rn__");
        state.wrap();
        state.addWhere(
            new SqlBasicCall(
                SqlStdOperatorTable.LESS_THAN_OR_EQUAL,
                List.of(new SqlIdentifier("__mvexpand_rn__", POS), intLiteral(node.getLimit())),
                POS));
      }
      return null;
    }

    @Override
    public Void visitFillNull(org.opensearch.sql.ast.tree.FillNull node, Void ignored) {
      walkChild(node);
      // fillnull: replace NULLs in listed fields (or all fields) with a value via COALESCE. We
      // need the input columns either to enumerate (replacementForAll) or to keep field order
      // intact (per-pair). With the row-type oracle, project COALESCE(field, replacement) for
      // matched fields and pass the rest through.
      if (state.evalExtended || state.projectionReplaced) {
        state.wrap();
      }
      List<String> cols =
          deriveColumnNames(state.from).stream()
              .filter(
                  c ->
                      !org.opensearch.sql.calcite.plan.OpenSearchConstants.METADATAFIELD_TYPE_MAP
                          .containsKey(c))
              .toList();
      java.util.Map<String, UnresolvedExpression> perField = new java.util.HashMap<>();
      for (org.apache.commons.lang3.tuple.Pair<Field, UnresolvedExpression> p :
          node.getReplacementPairs()) {
        perField.put(p.getLeft().getField().toString(), p.getRight());
      }
      UnresolvedExpression forAll =
          node.getReplacementForAll().isPresent() ? node.getReplacementForAll().get() : null;
      // Probe column types for strict type-compatibility checking. v2's visitFillNull rejects
      // mismatched replacement types up-front (e.g. `fillnull value=0` against a STRING column).
      // Without this guard, Calcite's COALESCE happily coerces types, masking real PPL errors.
      org.apache.calcite.rel.type.RelDataType inputRowType = null;
      if (rowTypeOracle != null) {
        try {
          inputRowType = deriveRowType(state.from);
        } catch (RuntimeException ignored2) {
          // probe failed; skip type check
        }
      }
      List<SqlNode> selects = new ArrayList<>();
      for (String c : cols) {
        SqlNode fieldRef = new SqlIdentifier(c, POS);
        UnresolvedExpression repl = perField.get(c);
        if (repl == null && forAll != null && perField.isEmpty()) {
          repl = forAll;
        }
        if (repl != null) {
          SqlNode replExpr = expr(repl);
          if (inputRowType != null) {
            validateFillNullTypeCompatibility(c, repl, replExpr, inputRowType);
          }
          SqlNode coalesce =
              new SqlBasicCall(SqlStdOperatorTable.COALESCE, List.of(fieldRef, replExpr), POS);
          selects.add(asAlias(coalesce, c));
        } else {
          selects.add(fieldRef);
        }
      }
      // PPL also allows fillnull on MAP/STRUCT-leaf paths (e.g. `fillnull using
      // doc.user.name='N/A'`
      // after `spath input=doc`). Such pair-keys aren't flat columns; lower each unmatched per-pair
      // through tryMapOrStructItemAccess and emit `COALESCE(ITEM(...), repl) AS "doc.user.name"`.
      // The aliased column then surfaces as a flat name for downstream `fields doc.user.name`.
      for (java.util.Map.Entry<String, UnresolvedExpression> e : perField.entrySet()) {
        String fieldName = e.getKey();
        if (cols.contains(fieldName)) continue; // already handled above
        SqlNode itemAccess =
            tryMapOrStructItemAccess(
                QualifiedName.of(java.util.Arrays.asList(fieldName.split("\\."))));
        if (itemAccess != null) {
          SqlNode replExpr = expr(e.getValue());
          SqlNode coalesce =
              new SqlBasicCall(SqlStdOperatorTable.COALESCE, List.of(itemAccess, replExpr), POS);
          selects.add(asAlias(coalesce, fieldName));
        }
      }
      state.setProjection(selects);
      return null;
    }

    /**
     * Reject fillnull replacement values whose SQL type family doesn't match the target field's.
     * Mirrors v2's CalciteRelNodeVisitor.validateFillNullTypeCompatibility error shape so tests
     * pattern-matching the message keep working.
     */
    private void validateFillNullTypeCompatibility(
        String fieldName,
        UnresolvedExpression replExpr,
        SqlNode replSqlNode,
        org.apache.calcite.rel.type.RelDataType inputRowType) {
      org.apache.calcite.rel.type.RelDataTypeField field =
          inputRowType.getField(fieldName, false, false);
      if (field == null) return;
      org.apache.calcite.rel.type.RelDataType fieldType = field.getType();
      org.apache.calcite.rel.type.RelDataTypeFamily fieldFamily = fieldType.getFamily();
      // Probe replacement type via the oracle by selecting it.
      org.apache.calcite.rel.type.RelDataType replType;
      try {
        SqlNodeList sel = new SqlNodeList(POS);
        sel.add(replSqlNode);
        SqlSelect probe =
            new SqlSelect(
                POS, null, sel, state.from, null, null, null, null, null, null, null, null, null);
        replType = rowTypeOracle.apply(probe).getFieldList().get(0).getType();
      } catch (RuntimeException e) {
        return;
      }
      org.apache.calcite.rel.type.RelDataTypeFamily replFamily = replType.getFamily();
      if (fieldFamily != replFamily
          && fieldFamily != org.apache.calcite.sql.type.SqlTypeFamily.NULL
          && replFamily != org.apache.calcite.sql.type.SqlTypeFamily.NULL) {
        // PPL string literals parse as CHAR in Calcite but the v2 path reports VARCHAR; map
        // CHAR→VARCHAR in the user-facing error so test assertions on the message match.
        org.apache.calcite.sql.type.SqlTypeName replTn = replType.getSqlTypeName();
        if (replTn == org.apache.calcite.sql.type.SqlTypeName.CHAR) {
          replTn = org.apache.calcite.sql.type.SqlTypeName.VARCHAR;
        }
        org.apache.calcite.sql.type.SqlTypeName fieldTn = fieldType.getSqlTypeName();
        if (fieldTn == org.apache.calcite.sql.type.SqlTypeName.CHAR) {
          fieldTn = org.apache.calcite.sql.type.SqlTypeName.VARCHAR;
        }
        throw new org.opensearch.sql.exception.SemanticCheckException(
            String.format(
                "fillnull failed: replacement value type %s is not compatible with field '%s' "
                    + "(type: %s). The replacement value type must match the field type.",
                replTn, fieldName, fieldTn));
      }
    }

    @Override
    public Void visitUnion(org.opensearch.sql.ast.tree.Union node, Void ignored) {
      // PPL `| union [<plan1>, <plan2>, ...]` — UNION ALL of N datasets.
      List<SqlNode> branches = new ArrayList<>();
      for (UnresolvedPlan ds : node.getDatasets()) {
        UnresolvedPlan pruned =
            ds.accept(new org.opensearch.sql.ast.EmptySourcePropagateVisitor(), null);
        branches.add(new PplToSqlNode(rowTypeOracle).visit(pruned));
      }
      if (branches.size() < 2) {
        throw new IllegalArgumentException(
            "Union command requires at least two datasets. Provided: " + branches.size());
      }
      // Pad mismatched branch schemas with NULL aliases so the UNION succeeds. Same logic as
      // visitAppend / visitMultisearch.
      if (rowTypeOracle != null) {
        try {
          List<List<String>> branchCols = new ArrayList<>();
          List<org.apache.calcite.rel.type.RelDataType> branchRowTypes = new ArrayList<>();
          List<String> unified = new ArrayList<>();
          for (SqlNode b : branches) {
            // Clone before deriveRowType — see commit 7061d500e4: validator mutates
            // SqlIdentifier.names in-place; branches are reused as the union operands.
            org.apache.calcite.rel.type.RelDataType rt =
                deriveRowType(org.opensearch.sql.calcite.sqlnode.SqlNodePlanner.deepClone(b));
            branchRowTypes.add(rt);
            List<String> cols =
                rt.getFieldList().stream()
                    .map(org.apache.calcite.rel.type.RelDataTypeField::getName)
                    .toList();
            branchCols.add(cols);
            for (String c : cols) {
              if (!unified.contains(c)) {
                unified.add(c);
              }
            }
          }
          // Build a "merged" reference row-type that picks the UDT column type from any branch
          // that has it. The padSelectWithReference uses this to wrap NULL pads in UDT-preserving
          // function calls.
          org.apache.calcite.rel.type.RelDataType mergedRowType =
              mergeRowTypesForUdtPad(branchRowTypes);
          List<SqlNode> padded = new ArrayList<>();
          for (int i = 0; i < branches.size(); i++) {
            padded.add(
                padSelectWithReference(
                    ensureFetchPreservesOrder(branches.get(i)),
                    branchCols.get(i),
                    unified,
                    mergedRowType));
          }
          branches = padded;
        } catch (RuntimeException ignored2) {
          // probe failed; fall through
        }
      }
      SqlNode union = branches.get(0);
      for (int i = 1; i < branches.size(); i++) {
        union =
            new SqlBasicCall(SqlStdOperatorTable.UNION_ALL, List.of(union, branches.get(i)), POS);
      }
      // Wrap in `SELECT * FROM (<union>)` so the result is still a query expression that can have
      // pipes appended. Mirroring how visitRelation sets state.from.
      state.setFrom(union);
      if (node.getMaxout() != null && node.getMaxout() > 0) {
        state.setOuterFetch(intLiteral(node.getMaxout()));
      }
      return null;
    }

    @Override
    public Void visitAppend(org.opensearch.sql.ast.tree.Append node, Void ignored) {
      walkChild(node);
      // `append [<subsearch>]` is UNION ALL of the main pipeline and the subsearch. Schemas may
      // differ — PPL pads missing columns with NULL on each side. Resolve both sides' column
      // sets via the oracle and emit explicit NULL-padded SELECT lists so the UNION succeeds.
      SqlNode mainBody = state.toFinalSqlNode();
      // Apply v2's EmptySourcePropagateVisitor so `append [ ]` / `append [ | stats ... ]`
      // (empty-source subsearch) collapses to a Values([]) which we render as a no-op SELECT.
      UnresolvedPlan prunedSubSearch =
          node.getSubSearch()
              .accept(new org.opensearch.sql.ast.EmptySourcePropagateVisitor(), null);
      SqlNode sub = new PplToSqlNode(rowTypeOracle).visit(prunedSubSearch);
      SqlNode unioned;
      if (rowTypeOracle != null) {
        try {
          // Clone before deriveRowType — see commit 7061d500e4: the validator's fullyQualify
          // mutates SqlIdentifier.names in-place, and bodies are reused as union operands.
          org.apache.calcite.rel.type.RelDataType mainRowType =
              deriveRowType(org.opensearch.sql.calcite.sqlnode.SqlNodePlanner.deepClone(mainBody));
          org.apache.calcite.rel.type.RelDataType subRowType =
              deriveRowType(org.opensearch.sql.calcite.sqlnode.SqlNodePlanner.deepClone(sub));
          List<String> mainCols =
              mainRowType.getFieldList().stream()
                  .map(org.apache.calcite.rel.type.RelDataTypeField::getName)
                  .toList();
          List<String> subCols =
              subRowType.getFieldList().stream()
                  .map(org.apache.calcite.rel.type.RelDataTypeField::getName)
                  .toList();
          // Reject same-named columns with different types (PPL append type-conflict semantics).
          for (String c : mainCols) {
            if (!subCols.contains(c)) continue;
            org.apache.calcite.sql.type.SqlTypeName mt =
                mainRowType.getField(c, false, false).getType().getSqlTypeName();
            org.apache.calcite.sql.type.SqlTypeName st =
                subRowType.getField(c, false, false).getType().getSqlTypeName();
            if (mt != st) {
              throw new IllegalArgumentException(
                  String.format(
                      "Unable to process column '%s' due to incompatible types: %s vs %s",
                      c, mt, st));
            }
          }
          // Build the unified column list: main's order first, then sub's columns that aren't in
          // main (preserving sub's order).
          List<String> unified = new ArrayList<>(mainCols);
          for (String c : subCols) {
            if (!unified.contains(c)) {
              unified.add(c);
            }
          }
          // SQL UNION ALL doesn't preserve operand sort order. If sub ends with a SqlOrderBy
          // (no FETCH), attach a large FETCH to materialize the sort so the order survives the
          // union. PPL's `append` wants sub's sorted-row-output to appear in that order.
          SqlNode subWithFetch = ensureFetchPreservesOrder(sub);
          SqlNode mainPadded = padSelectWithReference(mainBody, mainCols, unified, subRowType);
          SqlNode subPadded = padSelectWithReference(subWithFetch, subCols, unified, mainRowType);
          unioned =
              new SqlBasicCall(SqlStdOperatorTable.UNION_ALL, List.of(mainPadded, subPadded), POS);
        } catch (IllegalArgumentException iae) {
          throw iae;
        } catch (RuntimeException ignored2) {
          // Oracle probe failed; fall back to plain UNION ALL.
          unioned = new SqlBasicCall(SqlStdOperatorTable.UNION_ALL, List.of(mainBody, sub), POS);
        }
      } else {
        unioned = new SqlBasicCall(SqlStdOperatorTable.UNION_ALL, List.of(mainBody, sub), POS);
      }
      state.reset();
      state.setFrom(unioned);
      return null;
    }

    /**
     * If {@code body} is a {@link SqlOrderBy} without an explicit FETCH, attach a large FETCH so
     * the sort survives downstream UNION ALL. SQL semantics: a subquery's ORDER BY is generally
     * meaningless without an accompanying LIMIT/FETCH; with one, optimizers respect it.
     */
    private SqlNode ensureFetchPreservesOrder(SqlNode body) {
      if (!(body instanceof SqlOrderBy ob)) return body;
      if (ob.fetch != null) return body;
      // INTEGER MAX_VALUE — effectively unbounded but presents a concrete row-cap that prevents
      // optimizers from dropping the sort.
      SqlNode bigLimit = SqlLiteral.createExactNumeric(String.valueOf(Integer.MAX_VALUE), POS);
      return new SqlOrderBy(POS, ob.query, ob.orderList, ob.offset, bigLimit);
    }

    /**
     * Build a synthetic row-type that picks the UDT column type from any branch that has it. Used
     * by padSelectWithReference when union-style operators (union/multisearch) need a unified
     * reference for UDT-preserving NULL pads. Falls back to the first branch's row-type when no UDT
     * is found, since padSelectWithReference doesn't need anything special for non-UDT cols.
     */
    private org.apache.calcite.rel.type.RelDataType mergeRowTypesForUdtPad(
        java.util.List<org.apache.calcite.rel.type.RelDataType> rowTypes) {
      if (rowTypes.isEmpty()) return null;
      // Collect each unique column name, preferring a branch where the column type is a PPL UDT.
      java.util.LinkedHashMap<String, org.apache.calcite.rel.type.RelDataType> merged =
          new java.util.LinkedHashMap<>();
      for (org.apache.calcite.rel.type.RelDataType rt : rowTypes) {
        for (org.apache.calcite.rel.type.RelDataTypeField f : rt.getFieldList()) {
          org.apache.calcite.rel.type.RelDataType existing = merged.get(f.getName());
          if (existing == null) {
            merged.put(f.getName(), f.getType());
          } else if (!(existing
                  instanceof org.opensearch.sql.calcite.type.AbstractExprRelDataType<?>)
              && f.getType()
                  instanceof org.opensearch.sql.calcite.type.AbstractExprRelDataType<?>) {
            // Prefer UDT over non-UDT.
            merged.put(f.getName(), f.getType());
          }
        }
      }
      org.apache.calcite.rel.type.RelDataTypeFactory tf =
          org.opensearch.sql.calcite.utils.OpenSearchTypeFactory.TYPE_FACTORY;
      return tf.builder()
          .addAll(
              merged.entrySet().stream()
                  .map(e -> new java.util.AbstractMap.SimpleEntry<>(e.getKey(), e.getValue()))
                  .toList())
          .build();
    }

    /**
     * Wrap {@code body} (whose row type lists {@code present}) in a SELECT that produces every
     * column in {@code unified} by emitting NULLs for absent ones. Order follows {@code unified}.
     *
     * <p>Cast CHAR columns to VARCHAR to defeat the SQL UNION's "least-restrictive type" pad-
     * to-CHAR(N) rule, which surfaces user-visible trailing-space padding (`"Illinois "`) when
     * unions mix CHAR(M) and CHAR(N) string literals.
     */
    private SqlNode padSelect(SqlNode body, List<String> present, List<String> unified) {
      return padSelectWithReference(body, present, unified, null);
    }

    /**
     * Pad-select variant that uses {@code referenceRowType} (the OTHER union side's row type) to
     * dispatch UDT-preserving wrappers around NULL pads. Without this, an absent EXPR_TIMESTAMP
     * column's NULL literal types as generic NULL/VARCHAR and the UNION's least-restrictive
     * computation collapses the UDT to VARCHAR (string).
     */
    private SqlNode padSelectWithReference(
        SqlNode body,
        List<String> present,
        List<String> unified,
        org.apache.calcite.rel.type.RelDataType referenceRowType) {
      org.apache.calcite.rel.type.RelDataType bodyRowType = null;
      if (rowTypeOracle != null) {
        try {
          // Deep-clone before probing — body is also used as the wrapping SELECT's FROM and
          // gets re-validated when the union is later embedded in a larger query (e.g.
          // appendpipe re-walk). The validator's fullyQualify mutates SqlIdentifier.names in
          // place, so without isolation, a probe on the outer level can fail with "Table
          // 'EXPR$1' not found" because the inner WHERE was mutated to reference an
          // auto-alias from a different scope.
          bodyRowType =
              deriveRowType(org.opensearch.sql.calcite.sqlnode.SqlNodePlanner.deepClone(body));
        } catch (RuntimeException ignored2) {
          // probe failed; cast all to VARCHAR by name only
        }
      }
      SqlNodeList selectList = new SqlNodeList(POS);
      for (String c : unified) {
        if (present.contains(c)) {
          SqlNode colRef = new SqlIdentifier(c, POS);
          boolean isChar = false;
          if (bodyRowType != null) {
            org.apache.calcite.rel.type.RelDataTypeField field =
                bodyRowType.getField(c, false, false);
            if (field != null
                && field.getType().getSqlTypeName()
                    == org.apache.calcite.sql.type.SqlTypeName.CHAR) {
              isChar = true;
            }
          }
          if (isChar) {
            selectList.add(
                asAlias(castTo(colRef, org.apache.calcite.sql.type.SqlTypeName.VARCHAR), c));
          } else {
            selectList.add(colRef);
          }
        } else {
          // Wrap NULL through PPL's UDT functions when the reference (other-side) column is a
          // PPL UDT — otherwise the NULL collapses into VARCHAR/NULL and corrupts the
          // least-restrictive type computation.
          SqlNode nullPad = SqlLiteral.createNull(POS);
          if (referenceRowType != null) {
            org.apache.calcite.rel.type.RelDataTypeField refField =
                referenceRowType.getField(c, false, false);
            if (refField != null
                && refField.getType()
                    instanceof org.opensearch.sql.calcite.type.AbstractExprRelDataType<?> exprT) {
              org.opensearch.sql.data.type.ExprType refExprType = exprT.getExprType();
              String udfName = null;
              if (refExprType == org.opensearch.sql.data.type.ExprCoreType.TIMESTAMP) {
                udfName = "TIMESTAMP";
              } else if (refExprType == org.opensearch.sql.data.type.ExprCoreType.DATE) {
                udfName = "DATE";
              } else if (refExprType == org.opensearch.sql.data.type.ExprCoreType.TIME) {
                udfName = "TIME";
              } else if (refExprType == org.opensearch.sql.data.type.ExprCoreType.IP) {
                udfName =
                    org.opensearch.sql.calcite.utils.UserDefinedFunctionUtils.IP_FUNCTION_NAME;
              }
              if (udfName != null) {
                // Cast NULL to VARCHAR first so the UDT dispatcher's STRING-input branch
                // applies. NullPolicy.ANY then short-circuits the implementor at runtime.
                SqlNode typedNull =
                    castTo(
                        SqlLiteral.createNull(POS),
                        org.apache.calcite.sql.type.SqlTypeName.VARCHAR);
                nullPad =
                    new SqlBasicCall(
                        new org.apache.calcite.sql.SqlUnresolvedFunction(
                            new SqlIdentifier(udfName, POS),
                            null,
                            null,
                            null,
                            null,
                            org.apache.calcite.sql.SqlFunctionCategory.USER_DEFINED_FUNCTION),
                        List.of(typedNull),
                        POS);
              }
            }
          }
          selectList.add(asAlias(nullPad, c));
        }
      }
      return new SqlSelect(
          POS,
          /* keywordList */ null,
          selectList,
          body,
          /* where */ null,
          /* group */ null,
          /* having */ null,
          /* windowList */ null,
          /* qualify */ null,
          /* orderBy */ null,
          /* offset */ null,
          /* fetch */ null,
          /* hints */ null);
    }

    @Override
    public Void visitJoin(org.opensearch.sql.ast.tree.Join node, Void ignored) {
      // PPL JOIN: `<left> | join [type=...] [on <condition>] [<right>]`. Walk left into state,
      // build right from a fresh visitor, then assemble a SqlJoin.
      walkChild(node);
      // Bump counter so each join in a chain uses unique default `__l`/`__r` aliases (prevents
      // duplicate FROM-clause errors when nested SqlJoins are kept visible to subsequent ON).
      joinAliasCounter++;
      if (node.getJoinHint() != null && !node.getJoinHint().getHints().isEmpty()) {
        // Hint metadata is not surfaced into SqlNode; skip silently for now.
      }
      // Materialize the in-flight pipeline so the join LHS is a settled subquery. Preserve table
      // names without re-aliasing — PPL queries reference columns as `EMP.DEPTNO` expecting the
      // table identifier itself to be in scope. Only alias when an explicit leftAlias is given
      // or when the LHS isn't a bare table identifier (e.g. it's a subquery).
      //
      // Multi-join chain optimization: when the in-flight pipeline is purely a SqlJoin (no
      // accumulated projection/where/group/etc.) — i.e. another JOIN previously ran with no
      // intervening pipes — keep the SqlJoin as the leftSide directly so previously-aliased
      // tables (`t1`, `t2`) stay in scope for the next join's ON clause. Wrapping it as a
      // SELECT * subquery hides those aliases.
      SqlNode leftSide;
      if (state.from instanceof org.apache.calcite.sql.SqlJoin
          && state.where == null
          && state.projection == null
          && state.groupBy == null
          && state.orderBy == null
          && state.fetch == null
          && state.outerOrderBy == null
          && state.outerFetch == null
          && state.outerOffset == null) {
        leftSide = state.from;
      } else {
        leftSide = state.toFinalSqlNode();
      }
      // PPL allows both an inner `as tt` SubqueryAlias and an explicit `right=t2` join arg to
      // coexist on the same right side. The PPL parser nests them as
      // SubqueryAlias("t2", Project([*], SubqueryAlias("tt", Y))) when both are given. SQL only
      // supports one alias per relation; keep the explicit join-arg alias as canonical and
      // record the inner alias as a synonym so column refs like `tt.X` rewrite to `t2.X`.
      registerNestedAliasSynonyms(node.getRight(), node.getRightAlias().orElse(null));
      registerNestedAliasSynonyms(node.getChildren().get(0), node.getLeftAlias().orElse(null));
      // PPL also allows referencing a bare-table side by its raw table name in the JOIN's ON
      // clause: `source = X | JOIN ON X.col = Y.col Y`. The maybeAlias step below wraps such
      // unaliased sides under `__l`/`__r` defaults, dropping the table name from scope. Register
      // the table name(s) as synonyms for the assigned default alias so multi-part identifier
      // navigation in expr() rewrites them.
      //
      // Only register when not already present — in a multi-join chain, the first JOIN already
      // registered the table->__l synonym; the second JOIN's `joinAliasCounter` has bumped to
      // produce `__l2`, but the prior table is still aliased as `__l` in the inherited SqlJoin.
      String leftDefault = defaultLeftAlias();
      String rightDefault = defaultRightAlias();
      if (node.getLeftAlias().isEmpty()) {
        for (String t : extractRelationTableNames(node.getChildren().get(0))) {
          joinAliasSynonyms.putIfAbsent(t, leftDefault);
        }
      }
      if (node.getRightAlias().isEmpty()) {
        for (String t : extractRelationTableNames(node.getRight())) {
          joinAliasSynonyms.putIfAbsent(t, rightDefault);
        }
      }
      SqlNode rightSide = new PplToSqlNode(rowTypeOracle).visit(node.getRight());
      // PPL `plugins.ppl.join.subsearch_maxout` cluster setting caps the right side to N rows
      // total (not per partition). v2 calls addSysLimitForJoinSubsearch which adds a top-level
      // LogicalSystemLimit. Mirror that here by wrapping the right side's SqlSelect with FETCH.
      if (joinSubsearchLimit > 0) {
        SqlNode capLit = SqlLiteral.createExactNumeric(Integer.toString(joinSubsearchLimit), POS);
        if (rightSide instanceof org.apache.calcite.sql.SqlSelect _sel && _sel.getFetch() == null) {
          _sel.setFetch(capLit);
        } else if (rightSide instanceof org.apache.calcite.sql.SqlOrderBy _ob
            && _ob.fetch == null) {
          rightSide =
              new org.apache.calcite.sql.SqlOrderBy(
                  POS, _ob.query, _ob.orderList, _ob.offset, capLit);
        } else {
          // Wrap as SELECT * FROM (rightSide) AS __jss_inner__ FETCH N
          SqlNodeList ssel = new SqlNodeList(POS);
          ssel.add(SqlIdentifier.star(POS));
          SqlNode wrapped =
              new SqlSelect(
                  POS, null, ssel, rightSide, null, null, null, null, null, null, null, capLit,
                  null);
          rightSide = wrapped;
        }
      }
      // PPL `join max=N F1 F2 ...` dedups the right side to keep at most N rows per
      // <F1,F2,...> partition. v2 builds this via DedupNotNull; we emit equivalent SQL using
      // ROW_NUMBER() OVER (PARTITION BY F1,F2,...) <= N at the right-side wrapper.
      Integer maxArg = null;
      if (node.getArgumentMap() != null && node.getArgumentMap().get("max") != null) {
        Object v = node.getArgumentMap().get("max").getValue();
        if (v instanceof Integer i) maxArg = i;
      }
      // Build the partition-by list. For `join max=N F1 F2 ...` use the join fields directly.
      // For `join max=N on l.X = r.Y` extract the right-side column refs from the equi-join
      // condition (the columns Y where each conjunct is `l.X = r.Y`).
      java.util.List<String> maxPartitionCols = null;
      if (maxArg != null && maxArg > 0) {
        if (node.getJoinFields().isPresent() && !node.getJoinFields().get().isEmpty()) {
          maxPartitionCols = new java.util.ArrayList<>();
          for (Field f : node.getJoinFields().get()) {
            maxPartitionCols.add(f.getField().toString());
          }
        } else if (node.getJoinCondition().isPresent() && node.getRightAlias().isPresent()) {
          String rAlias = node.getRightAlias().get();
          maxPartitionCols = extractRightEquiJoinCols(node.getJoinCondition().get(), rAlias);
          if (maxPartitionCols == null || maxPartitionCols.isEmpty()) {
            maxPartitionCols = null;
          }
        }
      }
      if (maxArg != null && maxArg > 0 && maxPartitionCols != null) {
        SqlNodeList partitionBy = new SqlNodeList(POS);
        for (String col : maxPartitionCols) {
          partitionBy.add(new SqlIdentifier(col, POS));
        }
        SqlNode rowNumWindow =
            org.apache.calcite.sql.SqlWindow.create(
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
        innerSelects.add(asAlias(rowNum, "__join_max_rn__"));
        SqlNode innerSelect =
            new SqlSelect(
                POS,
                null,
                innerSelects,
                rightSide,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null);
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
                List.of(new SqlIdentifier("__join_max_rn__", POS), intLiteral(maxArg)),
                POS);
        SqlNode capped =
            new SqlSelect(
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
                null,
                null);
        rightSide = capped;
      }
      // PPL `join F1 F2` (no explicit ON, no per-side alias) below emits a synthetic ON
      // `__l.F = __r.F` condition. For that path to validate, both join sides MUST carry the
      // matching alias even when they are bare table identifiers — otherwise the synthetic
      // `__l`/`__r` references resolve to a non-existent table. Force-alias both sides when the
      // join condition path will reference the default aliases.
      boolean willUseDefaultAliases =
          !node.getJoinCondition().isPresent()
              && node.getJoinFields().isPresent()
              && !node.getJoinFields().get().isEmpty();
      SqlNode aliasedLeft;
      SqlNode aliasedRight;
      if (willUseDefaultAliases) {
        aliasedLeft =
            new SqlBasicCall(
                SqlStdOperatorTable.AS,
                List.of(
                    leftSide,
                    new SqlIdentifier(node.getLeftAlias().orElse(defaultLeftAlias()), POS)),
                POS);
        aliasedRight =
            new SqlBasicCall(
                SqlStdOperatorTable.AS,
                List.of(
                    rightSide,
                    new SqlIdentifier(node.getRightAlias().orElse(defaultRightAlias()), POS)),
                POS);
      } else {
        aliasedLeft =
            maybeAlias(
                leftSide, node.getLeftAlias().orElse(null), /* defaultAlias */ defaultLeftAlias());
        aliasedRight =
            maybeAlias(
                rightSide,
                node.getRightAlias().orElse(null), /* defaultAlias */
                defaultRightAlias());
      }
      // SEMI/ANTI joins in standard SQL: rewrite as WHERE EXISTS / NOT EXISTS subquery on the
      // left side. Calcite's SqlNode dialect doesn't accept LEFT_SEMI_JOIN/LEFT_ANTI_JOIN
      // directly (raises "Dialect does not support feature"), but the EXISTS rewrite produces
      // semantically equivalent results.
      if (node.getJoinType() == org.opensearch.sql.ast.tree.Join.JoinType.SEMI
          || node.getJoinType() == org.opensearch.sql.ast.tree.Join.JoinType.ANTI) {
        boolean isAnti = node.getJoinType() == org.opensearch.sql.ast.tree.Join.JoinType.ANTI;
        String lAlias = node.getLeftAlias().orElse(defaultLeftAlias());
        String rAlias = node.getRightAlias().orElse(defaultRightAlias());
        SqlNode aliasedLeftSemi =
            new SqlBasicCall(
                SqlStdOperatorTable.AS, List.of(leftSide, new SqlIdentifier(lAlias, POS)), POS);
        SqlNode aliasedRightSemi =
            new SqlBasicCall(
                SqlStdOperatorTable.AS, List.of(rightSide, new SqlIdentifier(rAlias, POS)), POS);
        SqlNode subCond;
        if (node.getJoinCondition().isPresent()) {
          joinScope = true;
          subCond = expr(node.getJoinCondition().get());
        } else if (node.getJoinFields().isPresent() && !node.getJoinFields().get().isEmpty()) {
          SqlNode joinCond = null;
          for (Field f : node.getJoinFields().get()) {
            String fname = f.getField().toString();
            SqlNode l = new SqlIdentifier(java.util.Arrays.asList(lAlias, fname), POS);
            SqlNode r = new SqlIdentifier(java.util.Arrays.asList(rAlias, fname), POS);
            SqlNode eq = new SqlBasicCall(SqlStdOperatorTable.EQUALS, List.of(l, r), POS);
            joinCond =
                joinCond == null
                    ? eq
                    : new SqlBasicCall(SqlStdOperatorTable.AND, List.of(joinCond, eq), POS);
          }
          subCond = joinCond;
        } else {
          subCond = SqlLiteral.createBoolean(true, POS);
        }
        SqlNodeList semiSelectList = new SqlNodeList(POS);
        semiSelectList.add(SqlIdentifier.star(POS));
        SqlNode subQuery =
            new SqlSelect(
                POS,
                null,
                semiSelectList,
                aliasedRightSemi,
                subCond,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null);
        SqlNode existsCall = new SqlBasicCall(SqlStdOperatorTable.EXISTS, List.of(subQuery), POS);
        SqlNode whereCond =
            isAnti
                ? new SqlBasicCall(SqlStdOperatorTable.NOT, List.of(existsCall), POS)
                : existsCall;
        // Modify state.from to be the aliased left + add WHERE [NOT] EXISTS so downstream pipes
        // can still reference `<lAlias>.<col>` against the left side directly. Building a wrap
        // SELECT here would shadow the lAlias from outer reference scope.
        state.reset();
        state.from = aliasedLeftSemi;
        state.addWhere(whereCond);
        joinScope = true;
        return null;
      }
      org.apache.calcite.sql.JoinType jt =
          switch (node.getJoinType()) {
            case INNER -> org.apache.calcite.sql.JoinType.INNER;
            case LEFT -> org.apache.calcite.sql.JoinType.LEFT;
            case RIGHT -> org.apache.calcite.sql.JoinType.RIGHT;
            case FULL -> org.apache.calcite.sql.JoinType.FULL;
            case CROSS -> org.apache.calcite.sql.JoinType.CROSS;
            case SEMI, ANTI ->
                throw new UnsupportedOperationException(
                    "SEMI/ANTI join not yet supported in SqlNode POC");
          };
      // PPL `cross join ON cond` is permitted at the parser level; v2 falls back to INNER JOIN
      // since CROSS + ON is illegal in standard SQL. Mirror that fallback so Calcite doesn't
      // reject the call with "Cannot specify condition following CROSS JOIN".
      if (jt == org.apache.calcite.sql.JoinType.CROSS
          && (node.getJoinCondition().isPresent()
              || (node.getJoinFields().isPresent() && !node.getJoinFields().get().isEmpty()))) {
        jt = org.apache.calcite.sql.JoinType.INNER;
      }
      SqlNode condition = null;
      org.apache.calcite.sql.JoinConditionType condType =
          org.apache.calcite.sql.JoinConditionType.NONE;
      // Set joinScope BEFORE evaluating the join condition so `EMP.DEPTNO`-style references
      // resolve as multi-part SQL navigation rather than dotted single identifiers.
      joinScope = true;
      // Set joinProbeFrom + activeJoinLeftAlias/RightAlias so tryAliasMapItemAccess can resolve
      // `<alias>.<col>` against the synthetic JOIN we're about to build. state.from is still the
      // left side and state.joinLeftAlias/RightAlias aren't set until after ON eval.
      SqlNode prevJoinProbe = joinProbeFrom;
      String prevActiveL = activeJoinLeftAlias;
      String prevActiveR = activeJoinRightAlias;
      activeJoinLeftAlias = node.getLeftAlias().orElse(null);
      activeJoinRightAlias = node.getRightAlias().orElse(null);
      joinProbeFrom =
          new org.apache.calcite.sql.SqlJoin(
              POS,
              aliasedLeft,
              SqlLiteral.createBoolean(false, POS),
              org.apache.calcite.sql.JoinType.INNER.symbol(POS),
              aliasedRight,
              org.apache.calcite.sql.JoinConditionType.ON.symbol(POS),
              SqlLiteral.createBoolean(true, POS));
      if (node.getJoinCondition().isPresent()) {
        condition = expr(node.getJoinCondition().get());
        condType = org.apache.calcite.sql.JoinConditionType.ON;
      } else if (node.getJoinFields().isPresent() && !node.getJoinFields().get().isEmpty()) {
        // PPL `join F1, F2 ...` — equivalent to USING but emit as ON clause with explicit
        // equality so qualified `left.X` / `right.X` references survive in our explicit
        // projection (USING auto-dedupes and prevents qualified access). v2 also uses ON.
        SqlNode joinCond = null;
        String lAlias = node.getLeftAlias().orElse(defaultLeftAlias());
        String rAlias = node.getRightAlias().orElse(defaultRightAlias());
        for (Field f : node.getJoinFields().get()) {
          String name = f.getField().toString();
          SqlNode l = new SqlIdentifier(java.util.Arrays.asList(lAlias, name), POS);
          SqlNode r = new SqlIdentifier(java.util.Arrays.asList(rAlias, name), POS);
          SqlNode eq = new SqlBasicCall(SqlStdOperatorTable.EQUALS, List.of(l, r), POS);
          joinCond =
              joinCond == null
                  ? eq
                  : new SqlBasicCall(SqlStdOperatorTable.AND, List.of(joinCond, eq), POS);
        }
        condition = joinCond;
        condType = org.apache.calcite.sql.JoinConditionType.ON;
        // Mark for v2-style overwrite-true projection below by setting joinScope+a marker.
        // We'll rely on node.getJoinFields().isPresent() at the projection step.
      } else if (jt != org.apache.calcite.sql.JoinType.CROSS) {
        // No condition + non-cross join: v2 derives auto-conditions from duplicated field names —
        // emit `__l.X = __r.X` for each shared column. Falls back to `ON true` when no shared
        // columns or oracle probe fails (matches Calcite's CROSS-equivalent behavior).
        SqlNode autoCond = null;
        if (rowTypeOracle != null) {
          try {
            List<String> leftCols =
                deriveColumnNames(aliasedLeft).stream()
                    .filter(
                        c ->
                            !org.opensearch.sql.calcite.plan.OpenSearchConstants
                                .METADATAFIELD_TYPE_MAP
                                .containsKey(c))
                    .toList();
            List<String> rightCols =
                deriveColumnNames(aliasedRight).stream()
                    .filter(
                        c ->
                            !org.opensearch.sql.calcite.plan.OpenSearchConstants
                                .METADATAFIELD_TYPE_MAP
                                .containsKey(c))
                    .toList();
            java.util.Set<String> rightSet = new java.util.HashSet<>(rightCols);
            String lAlias = node.getLeftAlias().orElse(defaultLeftAlias());
            String rAlias = node.getRightAlias().orElse(defaultRightAlias());
            for (String c : leftCols) {
              if (!rightSet.contains(c)) continue;
              SqlNode l = new SqlIdentifier(java.util.Arrays.asList(lAlias, c), POS);
              SqlNode r = new SqlIdentifier(java.util.Arrays.asList(rAlias, c), POS);
              SqlNode eq = new SqlBasicCall(SqlStdOperatorTable.EQUALS, List.of(l, r), POS);
              autoCond =
                  autoCond == null
                      ? eq
                      : new SqlBasicCall(SqlStdOperatorTable.AND, List.of(autoCond, eq), POS);
            }
          } catch (RuntimeException ignoredAuto) {
            // probe failed; fall back to ON true
          }
        }
        condition = autoCond != null ? autoCond : SqlLiteral.createBoolean(true, POS);
        condType = org.apache.calcite.sql.JoinConditionType.ON;
      }
      org.apache.calcite.sql.SqlJoin join =
          new org.apache.calcite.sql.SqlJoin(
              POS,
              aliasedLeft,
              SqlLiteral.createBoolean(false, POS),
              jt.symbol(POS),
              aliasedRight,
              condType.symbol(POS),
              condition);
      state.reset();
      state.setFrom(join);
      // Restore previous join-eval context now that ON-clause evaluation is complete.
      joinProbeFrom = prevJoinProbe;
      activeJoinLeftAlias = prevActiveL;
      activeJoinRightAlias = prevActiveR;
      // Track the side aliases so visitProject can disambiguate bare column refs after the join.
      // PPL semantics: `name` after `... | join left=a right=b ON ...` binds to LEFT side (`a`).
      state.joinLeftAlias = node.getLeftAlias().orElse(null);
      state.joinRightAlias = node.getRightAlias().orElse(null);
      // joinScope was already set true before the condition was evaluated; keep it true so
      // downstream pipes can also reference `<table>.<column>` against the joined sides.
      // Match v2: when join uses field-list syntax (`join F1 F2 ...`, no explicit ON), drop
      // duplicate columns from the LEFT side. With overwrite=true (default), v2's
      // projectExcept removes analyzeFieldsForLookUp(field, isSourceTable=true) — the
      // LEFT-side duplicate. The RIGHT side keeps both the join field and any other
      // duplicates. Resulting layout: [left.<non-dup>, right.<all>] in original join order.
      boolean joinUsesFieldList =
          node.getJoinFields().isPresent()
              && !node.getJoinFields().get().isEmpty()
              && !node.getJoinCondition().isPresent();
      // Also dedup duplicate columns when no JoinCondition + no JoinFields path was taken
      // (auto-derived ON condition above). v2 honors `overwrite=true|false` to choose which
      // side's duplicate columns to drop. Default (overwrite=true): drop LEFT dups. Mirror that.
      boolean joinUsesAutoCondition =
          !node.getJoinCondition().isPresent()
              && !(node.getJoinFields().isPresent() && !node.getJoinFields().get().isEmpty())
              && jt != org.apache.calcite.sql.JoinType.CROSS;
      if ((joinUsesFieldList || joinUsesAutoCondition) && rowTypeOracle != null) {
        try {
          List<String> leftCols =
              deriveColumnNames(aliasedLeft).stream()
                  .filter(
                      c ->
                          !org.opensearch.sql.calcite.plan.OpenSearchConstants
                              .METADATAFIELD_TYPE_MAP
                              .containsKey(c))
                  .toList();
          List<String> rightCols =
              deriveColumnNames(aliasedRight).stream()
                  .filter(
                      c ->
                          !org.opensearch.sql.calcite.plan.OpenSearchConstants
                                  .METADATAFIELD_TYPE_MAP
                                  .containsKey(c)
                              && !"__join_max_rn__".equals(c))
                  .toList();
          java.util.Set<String> rightSet = new java.util.HashSet<>(rightCols);
          java.util.Set<String> leftSet = new java.util.HashSet<>(leftCols);
          String lAlias = node.getLeftAlias().orElse(defaultLeftAlias());
          String rAlias = node.getRightAlias().orElse(defaultRightAlias());
          // overwrite default = true (drop LEFT side duplicates, keep RIGHT side as-is).
          boolean overwrite = true;
          if (node.getArgumentMap() != null && node.getArgumentMap().get("overwrite") != null) {
            Object val = node.getArgumentMap().get("overwrite").getValue();
            if (val instanceof Boolean b) overwrite = b;
          }
          List<SqlNode> selects = new ArrayList<>();
          if (overwrite) {
            for (String c : leftCols) {
              if (rightSet.contains(c)) continue;
              selects.add(new SqlIdentifier(java.util.Arrays.asList(lAlias, c), POS));
            }
            for (String c : rightCols) {
              selects.add(new SqlIdentifier(java.util.Arrays.asList(rAlias, c), POS));
            }
          } else {
            // overwrite=false: keep LEFT all, drop RIGHT duplicates.
            for (String c : leftCols) {
              selects.add(new SqlIdentifier(java.util.Arrays.asList(lAlias, c), POS));
            }
            for (String c : rightCols) {
              if (leftSet.contains(c)) continue;
              selects.add(new SqlIdentifier(java.util.Arrays.asList(rAlias, c), POS));
            }
          }
          state.setProjection(selects);
        } catch (RuntimeException ignored2) {
          // Probe failed — fall through with default join projection.
        }
      }
      return null;
    }

    @Override
    public Void visitRex(org.opensearch.sql.ast.tree.Rex node, Void ignored) {
      walkChild(node);
      if (node.getMode() == org.opensearch.sql.ast.tree.Rex.RexMode.SED) {
        return visitRexSed(node);
      }
      String patternStr = (String) node.getPattern().getValue();
      List<String> namedGroups =
          org.opensearch.sql.expression.parse.RegexCommonUtils.getNamedGroupCandidates(patternStr);
      if (namedGroups.isEmpty()) {
        throw new IllegalArgumentException(
            "Rex pattern must contain at least one named capture group");
      }
      if (state.orderBy != null || state.fetch != null) {
        state.wrap();
      }
      // If the source field is a dotted MAP/STRUCT-leaf whose top-level identifier has been
      // overridden by an upstream eval (e.g. spath rewrites to `eval doc = json_extract_all(doc)`),
      // SQL's SELECT-list alias visibility means a subsequent expression in the SAME select list
      // sees the ORIGINAL `doc`, not the overridden one. Wrap first so the override becomes the
      // FROM-side `doc` before we emit ITEM(doc, ...) as a new eval-alias.
      if (node.getField() instanceof QualifiedName fieldQn
          && fieldQn.getParts().size() >= 2
          && state.evalAliasNames.contains(fieldQn.getParts().get(0))) {
        state.wrap();
      }
      SqlNode source = expr(node.getField());
      SqlNode patternLit = SqlLiteral.createCharString(patternStr, POS);
      // Reuse the PARSE-style invocation pattern; named-group extraction lives on the
      // PARSE/REGEXP_EXTRACT UDF surface.
      for (String group : namedGroups) {
        SqlNode call;
        if (node.getMaxMatch().isPresent() && node.getMaxMatch().get() > 1) {
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
                      intLiteral(node.getMaxMatch().get())),
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
        state.addEvalAlias(call, group);
      }
      if (node.getOffsetField().isPresent()) {
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
        state.addEvalAlias(offsetCall, node.getOffsetField().get());
      }
      return null;
    }

    @Override
    public Void visitReplace(org.opensearch.sql.ast.tree.Replace node, Void ignored) {
      walkChild(node);
      // `replace "a" WITH "b" IN field1, field2` — replace each listed field's value with
      // chained REPLACE(value, pattern_i, replacement_i) for each pattern/replacement pair.
      // Other columns pass through unchanged.
      if (state.evalExtended || state.projectionReplaced) {
        state.wrap();
      }
      // Full column list (incl. metadata) for error-message parity with v2; filtered list for
      // selection.
      List<String> allCols = deriveColumnNames(state.from);
      List<String> cols =
          allCols.stream()
              .filter(
                  c ->
                      !org.opensearch.sql.calcite.plan.OpenSearchConstants.METADATAFIELD_TYPE_MAP
                          .containsKey(c))
              .toList();
      java.util.Set<String> targets = new java.util.HashSet<>();
      for (Field f : node.getFieldList()) {
        targets.add(f.getField().toString());
      }
      // Validate target fields exist in the input row type. Mirror v2's exact error format so
      // tests that pattern-match on the message keep working — input list includes metadata
      // fields ordered as [user-fields..., _id, _index, _score, _maxscore, _sort, _routing].
      // For MAP/STRUCT-leaf paths (e.g. `doc.user.name` after spath), validate that the leaf is
      // navigable through tryMapOrStructItemAccess instead of looking for a flat column.
      java.util.Set<String> colsSet = new java.util.HashSet<>(cols);
      java.util.Map<String, SqlNode> mapLeafAccess = new java.util.HashMap<>();
      for (String t : targets) {
        if (colsSet.contains(t)) continue;
        SqlNode itemAccess =
            tryMapOrStructItemAccess(QualifiedName.of(java.util.Arrays.asList(t.split("\\."))));
        if (itemAccess == null) {
          throw new IllegalArgumentException(
              "field [" + t + "] not found; input fields are: " + allCols);
        }
        mapLeafAccess.put(t, itemAccess);
      }
      List<SqlNode> selects = new ArrayList<>();
      for (String c : cols) {
        SqlNode fieldRef = new SqlIdentifier(c, POS);
        if (targets.contains(c)) {
          selects.add(asAlias(buildReplaceChain(fieldRef, node), c));
        } else {
          selects.add(fieldRef);
        }
      }
      // Emit MAP/STRUCT-leaf rewrites as additional aliased projections so downstream
      // `fields doc.user.name` finds the column under its dotted name.
      for (java.util.Map.Entry<String, SqlNode> e : mapLeafAccess.entrySet()) {
        selects.add(asAlias(buildReplaceChain(e.getValue(), node), e.getKey()));
      }
      state.setProjection(selects);
      return null;
    }

    private SqlNode buildReplaceChain(SqlNode value, org.opensearch.sql.ast.tree.Replace node) {
      for (org.opensearch.sql.ast.tree.ReplacePair pair : node.getReplacePairs()) {
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
                      castTo(
                          SqlLiteral.createCharString(regexPattern, POS),
                          org.apache.calcite.sql.type.SqlTypeName.VARCHAR),
                      castTo(
                          SqlLiteral.createCharString(regexReplacement, POS),
                          org.apache.calcite.sql.type.SqlTypeName.VARCHAR)),
                  POS);
        } else {
          // Cast string literals to VARCHAR explicitly. Without the cast, Calcite widens
          // unequal-length CHAR literals (e.g. 'IL' and 'Illinois') to a common max-length
          // CHAR(N) and right-pads shorter values with spaces — breaking exact-string match
          // and mismatching v2's emission shape.
          value =
              new SqlBasicCall(
                  SqlStdOperatorTable.REPLACE,
                  List.of(
                      value,
                      castTo(
                          expr(pair.getPattern()), org.apache.calcite.sql.type.SqlTypeName.VARCHAR),
                      castTo(
                          expr(pair.getReplacement()),
                          org.apache.calcite.sql.type.SqlTypeName.VARCHAR)),
                  POS);
        }
      }
      return value;
    }

    @Override
    public Void visitAppendPipe(org.opensearch.sql.ast.tree.AppendPipe node, Void ignored) {
      walkChild(node);
      // appendpipe [<sub>] = (<main>) UNION ALL (<main> | <sub>). The subquery is appended to the
      // current pipeline, which we already have in state. Build a deep-copy SqlNode of the main
      // pipeline, then build a second pipeline by attaching <sub> to a copy of the main AST.
      SqlNode mainBody = state.toFinalSqlNode();
      // Re-walk: attach sub to the main child and visit fresh.
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
      subTail.attach(node.getChild().get(0));
      SqlNode subBody = new PplToSqlNode(rowTypeOracle).visit(subqueryPlan);
      // Pad the two sides so heterogeneous schemas can union (PPL pads missing columns with
      // NULL). Same logic as visitAppend / visitMultisearch / visitUnion.
      SqlNode union;
      if (rowTypeOracle != null) {
        try {
          // Deep-clone before deriveRowType to isolate the validator's fullyQualify mutations
          // — mainBody and subBody are also used as the union's operands, and the mutations
          // would leak into the final tree, breaking later validation passes.
          org.apache.calcite.rel.type.RelDataType mainRowType =
              deriveRowType(org.opensearch.sql.calcite.sqlnode.SqlNodePlanner.deepClone(mainBody));
          org.apache.calcite.rel.type.RelDataType subRowType =
              deriveRowType(org.opensearch.sql.calcite.sqlnode.SqlNodePlanner.deepClone(subBody));
          List<String> mainCols =
              mainRowType.getFieldList().stream()
                  .map(org.apache.calcite.rel.type.RelDataTypeField::getName)
                  .toList();
          List<String> subCols =
              subRowType.getFieldList().stream()
                  .map(org.apache.calcite.rel.type.RelDataTypeField::getName)
                  .toList();
          // Validate: if a column appears in both sides with different SQL type names, PPL
          // rejects the appendpipe with "due to incompatible types". v2 uses ExprCoreType
          // equality (BIGINT != DOUBLE) — stricter than SQL type-family compatibility.
          for (String c : mainCols) {
            if (!subCols.contains(c)) continue;
            org.apache.calcite.rel.type.RelDataType mt =
                mainRowType.getField(c, false, false).getType();
            org.apache.calcite.rel.type.RelDataType st =
                subRowType.getField(c, false, false).getType();
            if (mt.getSqlTypeName() != st.getSqlTypeName()) {
              throw new IllegalArgumentException(
                  String.format(
                      "Unable to process column '%s' due to incompatible types: %s vs %s",
                      c, mt.getSqlTypeName(), st.getSqlTypeName()));
            }
          }
          List<String> unified = new ArrayList<>(mainCols);
          for (String c : subCols) {
            if (!unified.contains(c)) {
              unified.add(c);
            }
          }
          // Pass the OTHER side's row type as the reference so an absent UDT column (e.g.
          // EXPR_TIMESTAMP birthdate) gets wrapped in the matching PPL UDT function rather than
          // typed as plain VARCHAR. Without this, UNION's least-restrictive type derivation
          // collapses the UDT to VARCHAR.
          SqlNode mainPadded = padSelectWithReference(mainBody, mainCols, unified, subRowType);
          SqlNode subPadded =
              padSelectWithReference(
                  ensureFetchPreservesOrder(subBody), subCols, unified, mainRowType);
          union =
              new SqlBasicCall(SqlStdOperatorTable.UNION_ALL, List.of(mainPadded, subPadded), POS);
        } catch (IllegalArgumentException iae) {
          throw iae;
        } catch (RuntimeException ignored2) {
          union = new SqlBasicCall(SqlStdOperatorTable.UNION_ALL, List.of(mainBody, subBody), POS);
        }
      } else {
        union = new SqlBasicCall(SqlStdOperatorTable.UNION_ALL, List.of(mainBody, subBody), POS);
      }
      state.reset();
      state.setFrom(union);
      return null;
    }

    @Override
    public Void visitSpath(org.opensearch.sql.ast.tree.SPath node, Void ignored) {
      // SPath is a JSON-extract Eval rewrite. Delegate.
      return visitEval(node.rewriteAsEval(), ignored);
    }

    @Override
    public Void visitNoMv(org.opensearch.sql.ast.tree.NoMv node, Void ignored) {
      String fieldName = node.getField().getField().toString();
      // Walk upstream first so state captures any prior eval-introduced aliases.
      walkChild(node);
      // Determine if `fieldName` is a known column or upstream eval-alias. If neither, treat as
      // missing → null. If known but scalar (not ARRAY), reject 4xx.
      String missingOrScalarReason = null;
      boolean fieldExists = false;
      if (rowTypeOracle != null && state.from != null) {
        try {
          // First check upstream eval-aliases (added to projection by previous eval).
          if (state.evalAliasNames.contains(fieldName)) {
            // Upstream eval set `arr = array(...)`; assume ARRAY-typed, defer scalar check to
            // validator. Cannot easily probe alias's type without committing the wrap.
            fieldExists = true;
          } else {
            org.apache.calcite.rel.type.RelDataType rt = deriveRowType(state.from);
            org.apache.calcite.rel.type.RelDataTypeField fld = rt.getField(fieldName, false, false);
            if (fld == null) {
              // Missing field — emit `eval <field> = null`.
              state.addEvalAlias(SqlLiteral.createNull(POS), fieldName);
              return null;
            }
            org.apache.calcite.sql.type.SqlTypeName tn = fld.getType().getSqlTypeName();
            if (tn != org.apache.calcite.sql.type.SqlTypeName.ARRAY
                && tn != org.apache.calcite.sql.type.SqlTypeName.MULTISET) {
              missingOrScalarReason =
                  "Field [" + fieldName + "] is not an ARRAY type; nomv requires MVJOIN-able input";
            } else {
              fieldExists = true;
            }
          }
        } catch (RuntimeException ignoredProbe) {
          // probe failed; fall through
        }
      }
      if (missingOrScalarReason != null) {
        throw new IllegalArgumentException(missingOrScalarReason);
      }
      // nomv → eval X = coalesce(mvjoin(array_compact(X), '\n'), '').
      UnresolvedPlan rewritten = node.rewriteAsEval();
      if (rewritten instanceof Eval e) {
        Eval detachedEval = new Eval(e.getExpressionList());
        return visitEval(detachedEval, ignored);
      }
      return null;
    }

    @Override
    public Void visitMvCombine(org.opensearch.sql.ast.tree.MvCombine node, Void ignored) {
      walkChild(node);
      // mvcombine field [delim=...]: groups other columns and aggregates the named field into an
      // array (or delim-joined string). v2 implements this as a stats-collect-list aggregation
      // with an implicit group on all other columns. Rewriting as an Aggregation would require
      // schema introspection of the input row to enumerate the implicit group keys; emit as a
      // wrapped GROUP BY <all others>, COLLECT(field) projection.
      if (state.evalExtended || state.projectionReplaced) {
        state.wrap();
      }
      String fieldName = node.getField().getField().toString();
      List<String> cols =
          deriveColumnNames(state.from).stream()
              .filter(
                  c ->
                      !org.opensearch.sql.calcite.plan.OpenSearchConstants.METADATAFIELD_TYPE_MAP
                          .containsKey(c))
              .toList();
      // PPL allows mvcombine on a MAP/STRUCT-leaf path (e.g. `mvcombine doc.user.name` after
      // spath). Without a flat column matching the dotted name, lower the leaf through
      // tryMapOrStructItemAccess and emit ARRAY[ITEM(...)] AS "<dotted>" plus pass-through of
      // existing cols. Per-row 1-element array matches the test expectation that mvcombine on a
      // scalar leaf surfaces it as a single-element array column.
      if (!cols.contains(fieldName) && fieldName.contains(".")) {
        SqlNode itemAccess =
            tryMapOrStructItemAccess(
                QualifiedName.of(java.util.Arrays.asList(fieldName.split("\\."))));
        if (itemAccess != null) {
          List<SqlNode> selects = new ArrayList<>();
          for (String c : cols) {
            selects.add(new SqlIdentifier(c, POS));
          }
          // Per-row: NULL source → NULL array (matches v2's null-skipping mvcombine semantics);
          // else ARRAY[ITEM(...)] (1-element array since the leaf is scalar). Use a CASE WHEN
          // expression rather than COALESCE — ARRAY[null] is a 1-element array whose content is
          // null, which IS NULL evaluates as false on. Need explicit null check.
          SqlNode isNullCheck =
              new SqlBasicCall(SqlStdOperatorTable.IS_NULL, List.of(itemAccess), POS);
          SqlNode arrayCtor =
              new SqlBasicCall(
                  org.apache.calcite.sql.fun.SqlStdOperatorTable.ARRAY_VALUE_CONSTRUCTOR,
                  List.of(itemAccess),
                  POS);
          SqlNodeList whens = new SqlNodeList(POS);
          whens.add(isNullCheck);
          SqlNodeList thens = new SqlNodeList(POS);
          thens.add(SqlLiteral.createNull(POS));
          SqlNode arrayWrap =
              new org.apache.calcite.sql.fun.SqlCase(POS, null, whens, thens, arrayCtor);
          selects.add(asAlias(arrayWrap, fieldName));
          state.setProjection(selects);
          // Wrap so downstream `fields` sees the aliased array column as a real flat row-type
          // entry instead of falling back to ITEM(doc, 'user.name') against the wrapped FROM
          // (which produces a scalar, not the wrapped 1-element array).
          state.wrap();
          return null;
        }
      }
      // PPL surfaces a 4xx when the target field doesn't exist in the input row.
      if (!cols.contains(fieldName)) {
        throw new IllegalArgumentException("Field [" + fieldName + "] not found.");
      }
      List<SqlNode> selects = new ArrayList<>();
      List<SqlNode> groupKeys = new ArrayList<>();
      for (String c : cols) {
        if (c.equals(fieldName)) {
          // ARRAY_AGG(field) FILTER (WHERE field IS NOT NULL) AS field — returns ARRAY<T> and
          // mirrors v2's null-skipping mvcombine semantics. SqlStdOperatorTable.COLLECT yields
          // MULTISET<T> which the response shapes as a string scalar; ARRAY_AGG keeps the array
          // shape. The FILTER clause excludes NULLs from the aggregation so the resulting array
          // never contains nulls (matching v2's array-of-non-null-values output).
          SqlNode agg =
              new SqlBasicCall(
                  org.apache.calcite.sql.fun.SqlLibraryOperators.ARRAY_AGG,
                  List.of(new SqlIdentifier(c, POS)),
                  POS);
          SqlNode filterCond =
              new SqlBasicCall(
                  SqlStdOperatorTable.IS_NOT_NULL, List.of(new SqlIdentifier(c, POS)), POS);
          SqlNode aggFiltered =
              new SqlBasicCall(SqlStdOperatorTable.FILTER, List.of(agg, filterCond), POS);
          selects.add(asAlias(aggFiltered, c));
        } else {
          SqlNode key = new SqlIdentifier(c, POS);
          selects.add(key);
          groupKeys.add(key);
        }
      }
      state.setProjection(selects);
      state.setGroupBy(groupKeys);
      // Wrap so the next downstream pipe sees the aggregated rows as a fresh subquery.
      // Without this, Calcite's converter may collapse identity projections away. v2's
      // mvcombine emission preserves an outer LogicalProject layer over the LogicalAggregate.
      state.wrap();
      return null;
    }

    @Override
    public Void visitRareTopN(org.opensearch.sql.ast.tree.RareTopN node, Void ignored) {
      walkChild(node);
      // rare/top changes the row set; flush pending outer fetch into the inner subquery.
      flushOuterIntoInner();
      // rare/top: GROUP BY (groups + fields) then ROW_NUMBER() OVER (PARTITION BY groups
      // ORDER BY count [DESC for top, ASC for rare]) <= k.
      org.opensearch.sql.ast.expression.Argument.ArgumentMap argumentMap =
          org.opensearch.sql.ast.expression.Argument.ArgumentMap.of(node.getArguments());
      String countFieldName =
          (String)
              argumentMap
                  .get(org.opensearch.sql.ast.tree.RareTopN.Option.countField.name())
                  .getValue();
      Boolean showCount =
          (Boolean)
              argumentMap
                  .get(org.opensearch.sql.ast.tree.RareTopN.Option.showCount.name())
                  .getValue();
      Boolean useNull =
          (Boolean)
              argumentMap
                  .get(org.opensearch.sql.ast.tree.RareTopN.Option.useNull.name())
                  .getValue();
      // Wrap because we're emitting an aggregation followed by a window.
      if (state.where != null
          || state.evalExtended
          || state.projectionReplaced
          || state.orderBy != null
          || state.fetch != null) {
        state.wrap();
      }
      // Step 1: SELECT <groups>, <fields>, COUNT(*) AS countFieldName GROUP BY <groups, fields>.
      // For dotted MAP/STRUCT leaves (e.g. `doc.user.name`), expr() returns an ITEM(...) call.
      // Wrap each such projection with an explicit alias so the post-wrap row type carries the
      // expected dotted name as a flat column — step 3 references it by name via SqlIdentifier.
      List<SqlNode> aggSelects = new ArrayList<>();
      List<SqlNode> groupKeys = new ArrayList<>();
      List<SqlNode> partitionKeys = new ArrayList<>();
      List<String> groupExprNames = new ArrayList<>();
      List<String> fieldNames = new ArrayList<>();
      for (UnresolvedExpression g : node.getGroupExprList()) {
        SqlNode k = expr(g);
        groupKeys.add(k);
        String alias = null;
        if (g instanceof QualifiedName qn) alias = qn.toString();
        else if (g instanceof Field f) alias = f.getField().toString();
        if (alias != null
            && alias.contains(".")
            && k instanceof SqlBasicCall sbc
            && sbc.getOperator() == SqlStdOperatorTable.ITEM) {
          aggSelects.add(asAlias(k, alias));
          // After the wrap below, the dotted name is a flat column; partitionKeys (used in the
          // OVER clause) must reference that flat name, not re-emit the ITEM() against `doc`.
          partitionKeys.add(new SqlIdentifier(alias, POS));
        } else {
          aggSelects.add(k);
          partitionKeys.add(k);
        }
        groupExprNames.add(alias);
      }
      for (Field f : node.getFields()) {
        SqlNode k = expr(f.getField());
        groupKeys.add(k);
        String alias = f.getField().toString();
        if (alias.contains(".")
            && k instanceof SqlBasicCall sbc
            && sbc.getOperator() == SqlStdOperatorTable.ITEM) {
          aggSelects.add(asAlias(k, alias));
        } else {
          aggSelects.add(k);
        }
        fieldNames.add(alias);
      }
      SqlNode countCall =
          new SqlBasicCall(SqlStdOperatorTable.COUNT, List.of(SqlIdentifier.star(POS)), POS);
      aggSelects.add(asAlias(countCall, countFieldName));
      if (Boolean.FALSE.equals(useNull) && !groupKeys.isEmpty()) {
        for (SqlNode k : groupKeys) {
          state.addWhere(new SqlBasicCall(SqlStdOperatorTable.IS_NOT_NULL, List.of(k), POS));
        }
      }
      state.setProjection(aggSelects);
      state.setGroupBy(groupKeys);
      state.wrap();
      // Step 2: ROW_NUMBER() OVER (PARTITION BY <groups> ORDER BY count [DESC]) <= k filter.
      SqlNodeList partitionBy = new SqlNodeList(POS);
      for (SqlNode k : partitionKeys) {
        partitionBy.add(k);
      }
      SqlNodeList orderBy = new SqlNodeList(POS);
      SqlNode countRef = new SqlIdentifier(countFieldName, POS);
      if (node.getCommandType() == org.opensearch.sql.ast.tree.RareTopN.CommandType.TOP) {
        orderBy.add(new SqlBasicCall(SqlStdOperatorTable.DESC, List.of(countRef), POS));
      } else {
        orderBy.add(countRef);
      }
      SqlNode window =
          org.apache.calcite.sql.SqlWindow.create(
              null,
              null,
              partitionBy,
              orderBy,
              SqlLiteral.createBoolean(false, POS),
              null,
              null,
              null,
              POS);
      SqlNode over =
          new SqlBasicCall(
              SqlStdOperatorTable.OVER,
              List.of(new SqlBasicCall(SqlStdOperatorTable.ROW_NUMBER, List.of(), POS), window),
              POS);
      state.addEvalAlias(over, "_row_number_rare_top_");
      state.wrap();
      state.addWhere(
          new SqlBasicCall(
              SqlStdOperatorTable.LESS_THAN_OR_EQUAL,
              List.of(
                  new SqlIdentifier("_row_number_rare_top_", POS),
                  intLiteral(node.getNoOfResults())),
              POS));
      state.wrap();
      // Step 3: project away helper column (and count if !showCount).
      // After the wraps, dotted MAP-leaves are now flat columns under their alias name. Reference
      // them by SqlIdentifier rather than re-running expr() (which would re-emit ITEM(...) on a
      // FROM scope that no longer has the parent MAP).
      List<SqlNode> finalSelects = new ArrayList<>();
      int gi = 0;
      for (UnresolvedExpression g : node.getGroupExprList()) {
        String alias = groupExprNames.get(gi++);
        if (alias != null && alias.contains(".")) {
          finalSelects.add(new SqlIdentifier(alias, POS));
        } else {
          finalSelects.add(expr(g));
        }
      }
      int fi = 0;
      for (Field f : node.getFields()) {
        String alias = fieldNames.get(fi++);
        if (alias.contains(".")) {
          finalSelects.add(new SqlIdentifier(alias, POS));
        } else {
          finalSelects.add(expr(f.getField()));
        }
      }
      if (Boolean.TRUE.equals(showCount)) {
        finalSelects.add(new SqlIdentifier(countFieldName, POS));
      }
      state.setProjection(finalSelects);
      return null;
    }

    @Override
    public Void visitTrendline(org.opensearch.sql.ast.tree.Trendline node, Void ignored) {
      walkChild(node);
      // PPL trendline sma(N, field) [as alias] [...]:
      //   1. Optional pre-sort by sortByField.
      //   2. Pre-filter: WHERE <field> IS NOT NULL — rows with null in the data field don't
      //      contribute to the window.
      //   3. CASE WHEN COUNT(*) OVER (ROWS N-1 PRECEDING) > N-1
      //        THEN AVG(field) OVER (ROWS N-1 PRECEDING) ELSE NULL END AS alias
      //   4. Project replaces the existing column when alias matches an existing field name
      //      (mirroring v2's projectPlusOverriding).
      if (state.evalExtended || state.projectionReplaced) {
        state.wrap();
      }
      // Step 1: pre-filter NULL data-field values for each computation.
      for (org.opensearch.sql.ast.tree.Trendline.TrendlineComputation c : node.getComputations()) {
        SqlNode fieldRef = expr(c.getDataField().getField());
        state.addWhere(new SqlBasicCall(SqlStdOperatorTable.IS_NOT_NULL, List.of(fieldRef), POS));
      }
      // The optional pre-sort must be embedded in the window ORDER BY so the OVER clause iterates
      // rows in that order (a separate pre-sort wrapped in a subquery is treated as informational
      // by SQL and discarded). We also keep an outer ORDER BY so the result preserves the sort.
      SqlNodeList windowOrderBy = new SqlNodeList(POS);
      if (node.getSortByField().isPresent()) {
        Field sortField = node.getSortByField().get();
        Sort.SortOption opt = analyzeSortOption(sortField.getFieldArgs());
        SqlNode key = expr(sortField.getField());
        if (opt.getSortOrder() == Sort.SortOrder.DESC) {
          key = new SqlBasicCall(SqlStdOperatorTable.DESC, List.of(key), POS);
        }
        windowOrderBy.add(key);
        state.setOuterOrderBy(List.of(key));
      }
      state.wrap();
      // Step 3: enumerate input columns (without the trendline aliases) so we can override
      // existing fields rather than emit a SELECT * that would conflict.
      List<String> inputCols =
          rowTypeOracle != null && state.from != null
              ? deriveColumnNames(state.from).stream()
                  .filter(
                      c ->
                          !org.opensearch.sql.calcite.plan.OpenSearchConstants
                              .METADATAFIELD_TYPE_MAP
                              .containsKey(c))
                  .toList()
              : null;
      java.util.Map<String, SqlNode> aliasOverrides = new java.util.LinkedHashMap<>();
      List<SqlNode> appended = new ArrayList<>();
      List<String> appendedNames = new ArrayList<>();
      for (org.opensearch.sql.ast.tree.Trendline.TrendlineComputation c : node.getComputations()) {
        int n = c.getNumberOfDataPoints();
        SqlNode lower =
            new SqlBasicCall(
                org.apache.calcite.sql.SqlWindow.PRECEDING_OPERATOR,
                List.of(intLiteral(n - 1)),
                POS);
        SqlNode upper = org.apache.calcite.sql.SqlWindow.createCurrentRow(POS);
        SqlNode window =
            org.apache.calcite.sql.SqlWindow.create(
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
        // COUNT(*) OVER (ROWS N-1 PRECEDING) — number of rows seen so far.
        SqlNode countCall =
            new SqlBasicCall(SqlStdOperatorTable.COUNT, List.of(SqlIdentifier.star(POS)), POS);
        SqlNode countOver =
            new SqlBasicCall(SqlStdOperatorTable.OVER, List.of(countCall, window), POS);
        SqlNode countCondition =
            new SqlBasicCall(
                SqlStdOperatorTable.GREATER_THAN, List.of(countOver, intLiteral(n - 1)), POS);
        // Window agg per type.
        SqlNode windowedAgg;
        switch (c.getComputationType()) {
          case SMA -> {
            SqlNode agg = new SqlBasicCall(SqlStdOperatorTable.AVG, List.of(fieldRef), POS);
            windowedAgg = new SqlBasicCall(SqlStdOperatorTable.OVER, List.of(agg, window), POS);
          }
          case WMA -> {
            // WMA = Σ(i * NTH_VALUE(field, i)) / (N*(N+1)/2). Cast the integer divisor to DOUBLE
            // so the result is double-precision (matching v2's type promotion); without the cast,
            // INT/INT yields INT and the BigDecimal divide rounds to integer.
            SqlNode divider = null;
            for (int i = 1; i <= n; i++) {
              SqlNode nth =
                  new SqlBasicCall(
                      SqlStdOperatorTable.NTH_VALUE, List.of(fieldRef, intLiteral(i)), POS);
              SqlNode nthOver =
                  new SqlBasicCall(SqlStdOperatorTable.OVER, List.of(nth, window), POS);
              SqlNode weighted =
                  new SqlBasicCall(
                      SqlStdOperatorTable.MULTIPLY, List.of(intLiteral(i), nthOver), POS);
              divider =
                  divider == null
                      ? weighted
                      : new SqlBasicCall(SqlStdOperatorTable.PLUS, List.of(divider, weighted), POS);
            }
            SqlNode divisor =
                castTo(intLiteral(n * (n + 1) / 2), org.apache.calcite.sql.type.SqlTypeName.DOUBLE);
            windowedAgg =
                new SqlBasicCall(
                    org.opensearch.sql.expression.function.PPLBuiltinOperators.DIVIDE,
                    List.of(divider, divisor),
                    POS);
          }
          default ->
              throw new UnsupportedOperationException(
                  "Unsupported trendline type: " + c.getComputationType());
        }
        // CASE WHEN count > N-1 THEN windowedAgg ELSE NULL END
        SqlNodeList whens = new SqlNodeList(POS);
        whens.add(countCondition);
        SqlNodeList thens = new SqlNodeList(POS);
        thens.add(windowedAgg);
        SqlNode caseExpr =
            new org.apache.calcite.sql.fun.SqlCase(
                POS, null, whens, thens, SqlLiteral.createNull(POS));
        String alias = c.getAlias();
        if (inputCols != null && inputCols.contains(alias)) {
          aliasOverrides.put(alias, caseExpr);
        } else {
          appended.add(caseExpr);
          appendedNames.add(alias);
        }
      }
      // Step 4: build the SELECT list, overriding input columns where alias collides.
      List<SqlNode> selects = new ArrayList<>();
      if (inputCols != null) {
        for (String c : inputCols) {
          if (aliasOverrides.containsKey(c)) {
            selects.add(asAlias(aliasOverrides.get(c), c));
          } else {
            selects.add(new SqlIdentifier(c, POS));
          }
        }
      } else {
        // No oracle — fall back to SELECT *, then aliased trendline columns. Alias collisions
        // may produce ambiguous columns; users can disambiguate via explicit alias.
        selects.add(SqlIdentifier.star(POS));
      }
      for (int i = 0; i < appended.size(); i++) {
        selects.add(asAlias(appended.get(i), appendedNames.get(i)));
      }
      state.setProjection(selects);
      return null;
    }

    @Override
    public Void visitMultisearch(org.opensearch.sql.ast.tree.Multisearch node, Void ignored) {
      // multisearch [<plan1>, <plan2>, ...] — UNION ALL of N subsearches, ordered by @timestamp
      // descending if such a column exists. PPL's Multisearch differs from Union by ordering;
      // emit UNION ALL and a top-level ORDER BY @timestamp DESC if the column is present.
      List<SqlNode> branches = new ArrayList<>();
      for (UnresolvedPlan sub : node.getSubsearches()) {
        UnresolvedPlan pruned =
            sub.accept(new org.opensearch.sql.ast.EmptySourcePropagateVisitor(), null);
        branches.add(new PplToSqlNode(rowTypeOracle).visit(pruned));
      }
      if (branches.isEmpty()) {
        throw new IllegalArgumentException("Multisearch requires at least one subsearch");
      }
      // Schema-pad each branch so heterogeneous subsearches can union: PPL pads missing columns
      // with NULL across branches.
      if (rowTypeOracle != null && branches.size() > 1) {
        try {
          List<org.apache.calcite.rel.type.RelDataType> branchRowTypes = new ArrayList<>();
          List<List<String>> branchCols = new ArrayList<>();
          List<String> unified = new ArrayList<>();
          for (SqlNode b : branches) {
            // Clone before deriveRowType — see commit 7061d500e4.
            org.apache.calcite.rel.type.RelDataType rt =
                deriveRowType(org.opensearch.sql.calcite.sqlnode.SqlNodePlanner.deepClone(b));
            branchRowTypes.add(rt);
            List<String> cols =
                rt.getFieldList().stream()
                    .map(org.apache.calcite.rel.type.RelDataTypeField::getName)
                    .toList();
            branchCols.add(cols);
            for (String c : cols) {
              if (!unified.contains(c)) {
                unified.add(c);
              }
            }
          }
          // Validate: same-named columns across branches must have same SqlTypeName.
          // PPL multisearch rejects type conflicts (testMultisearchTypeConflictWithStats,
          // testMultisearchWithDirectTypeConflict).
          for (String col : unified) {
            org.apache.calcite.sql.type.SqlTypeName seenType = null;
            for (int i = 0; i < branches.size(); i++) {
              if (!branchCols.get(i).contains(col)) continue;
              org.apache.calcite.sql.type.SqlTypeName t =
                  branchRowTypes.get(i).getField(col, false, false).getType().getSqlTypeName();
              if (seenType == null) {
                seenType = t;
              } else if (seenType != t) {
                throw new IllegalArgumentException(
                    String.format(
                        "Unable to process column '%s' due to incompatible types: %s vs %s",
                        col, seenType, t));
              }
            }
          }
          org.apache.calcite.rel.type.RelDataType mergedRowType =
              mergeRowTypesForUdtPad(branchRowTypes);
          List<SqlNode> padded = new ArrayList<>();
          for (int i = 0; i < branches.size(); i++) {
            padded.add(
                padSelectWithReference(
                    ensureFetchPreservesOrder(branches.get(i)),
                    branchCols.get(i),
                    unified,
                    mergedRowType));
          }
          branches = padded;
        } catch (IllegalArgumentException iae) {
          throw iae;
        } catch (RuntimeException ignored2) {
          // probe failed; fall through to plain union
        }
      }
      SqlNode union = branches.get(0);
      for (int i = 1; i < branches.size(); i++) {
        union =
            new SqlBasicCall(SqlStdOperatorTable.UNION_ALL, List.of(union, branches.get(i)), POS);
      }
      state.setFrom(union);
      // Optional auto-sort by @timestamp DESC for time-interleaved multisearch results. Probe
      // the unioned schema via the oracle; silent no-op when the field is missing.
      if (rowTypeOracle != null) {
        try {
          List<String> cols = deriveColumnNames(union);
          if (cols.contains(
              org.opensearch.sql.calcite.plan.OpenSearchConstants.IMPLICIT_FIELD_TIMESTAMP)) {
            SqlNode key =
                new SqlIdentifier(
                    org.opensearch.sql.calcite.plan.OpenSearchConstants.IMPLICIT_FIELD_TIMESTAMP,
                    POS);
            // v2 emits bare DESC (no NULLS modifier) via relBuilder.desc(timestampRef).
            // Don't wrap in NULLS_LAST — Calcite displays bare DESC as `DESC` and
            // `DESC` + NULLS_LAST as `DESC-nulls-last`.
            key = new SqlBasicCall(SqlStdOperatorTable.DESC, List.of(key), POS);
            state.setOuterOrderBy(List.of(key));
          }
        } catch (RuntimeException ignored2) {
          // Oracle probe failed (e.g. type-mismatched union branches); accept the union as-is.
        }
      }
      return null;
    }

    @Override
    public Void visitAddTotals(org.opensearch.sql.ast.tree.AddTotals node, Void ignored) {
      walkChild(node);
      java.util.Map<String, org.opensearch.sql.ast.expression.Literal> options = node.getOptions();
      boolean addRow = options == null || getBoolOption(options, "row", true);
      boolean addCol = options != null && getBoolOption(options, "col", false);
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
      List<Field> fields = node.getFieldList();
      if (fields == null || fields.isEmpty()) {
        if (rowTypeOracle == null) {
          throw new UnsupportedOperationException(
              "addtotals with implicit field list requires the schema oracle");
        }
        // Implicit field list: enumerate the in-flight pipeline's numeric columns. Mirrors
        // v2 (CalciteRelNodeVisitor.visitAddTotals's "no field list" branch), which derives
        // the field set from the row type.
        SqlNode pipelineSnapshot = state.toFinalSqlNode();
        org.apache.calcite.rel.type.RelDataType rt = deriveRowType(pipelineSnapshot);
        fields = new ArrayList<>();
        for (org.apache.calcite.rel.type.RelDataTypeField rf : rt.getFieldList()) {
          String c = rf.getName();
          if (org.opensearch.sql.calcite.plan.OpenSearchConstants.METADATAFIELD_TYPE_MAP
              .containsKey(c)) {
            continue;
          }
          org.apache.calcite.sql.type.SqlTypeName tn = rf.getType().getSqlTypeName();
          if (tn != null && org.apache.calcite.sql.type.SqlTypeName.NUMERIC_TYPES.contains(tn)) {
            fields.add(new Field(new QualifiedName(c)));
          }
        }
      }
      // Wrap when there's pending pipeline state that would conflict with the new sum column —
      // notably when an upstream `stats` set groupBy/projection, since those aliases aren't
      // visible inside the same SELECT list. Also wrap on order/fetch so the row-cap binds first.
      if (state.orderBy != null
          || state.fetch != null
          || state.groupBy != null
          || state.projectionReplaced
          || state.evalExtended) {
        state.wrap();
      }
      // Step 1 (row=true): append per-row sum column.
      if (addRow) {
        SqlNode sum = expr(fields.get(0).getField());
        for (int i = 1; i < fields.size(); i++) {
          sum =
              new SqlBasicCall(
                  SqlStdOperatorTable.PLUS, List.of(sum, expr(fields.get(i).getField())), POS);
        }
        state.addEvalAlias(sum, alias);
      }
      // Step 2 (col=true): UNION-ALL with a summary row containing SUM(field) per listed field
      // and NULLs (or the label) for non-aggregated columns. Requires schema introspection so
      // the summary row aligns with the main pipeline's column ordering.
      if (addCol) {
        if (rowTypeOracle == null) {
          throw new UnsupportedOperationException(
              "addtotals col=true requires the schema oracle for column enumeration");
        }
        // Flush outer ORDER BY / FETCH into the inner subquery so LIMIT survives the UNION
        // wrapping. SqlOrderBy as a FROM-clause subquery has surprising unparse behavior; an
        // explicit inner SqlSelect with FETCH is more reliable.
        flushOuterIntoInner();
        SqlNode mainBody = state.toFinalSqlNode();
        // deriveColumnNames probes mainBody via a `SELECT * FROM (mainBody)` validator probe;
        // the validator's fullyQualify mutates SqlIdentifier names in place. Use a deep clone for
        // the probe so the live mainBody tree (reused as the union's main-side and inside the
        // agg-side FROM) stays pristine. Otherwise downstream validation can produce duplicate
        // Sort+Project layers.
        List<String> cols =
            deriveColumnNames(
                org.opensearch.sql.calcite.sqlnode.SqlNodePlanner.deepClone(mainBody));
        java.util.Set<String> aggFieldNames = new java.util.HashSet<>();
        for (Field f : fields) {
          aggFieldNames.add(f.getField().toString());
        }
        // When labelField is set and not already a column, the main side must add it as
        // CAST(NULL AS VARCHAR(N)) and the summary side as the label literal — so the UNION row
        // type is uniform. Without the explicit CAST, NULL on main typecasts to BIGINT (the
        // default at union-resolution time) while the summary side is VARCHAR, blocking the
        // UNION's least-restrictive type derivation. v2 emits both as VARCHAR(N) where N is
        // label.length() — match that shape so explain plans agree.
        boolean addLabelField = labelField != null && !cols.contains(labelField);
        if (addLabelField) {
          // Append the labelField directly to mainBody's existing select list (don't wrap),
          // so the converted rel keeps the eval-extended projection in a single Project layer
          // rather than splitting it across two. Mirrors v2's emission where CustomSum and
          // all_emp_total live in the same Project.
          //
          // VARCHAR length: v2 uses max(label.length(), labelField.length()) so the literal
          // type can hold both the user-supplied label string AND the longer column name as a
          // synthesized fallback.
          int labelLen = Math.max(label.length(), labelField.length());
          org.apache.calcite.sql.SqlDataTypeSpec varcharLenSpec =
              new org.apache.calcite.sql.SqlDataTypeSpec(
                  new org.apache.calcite.sql.SqlBasicTypeNameSpec(
                      org.apache.calcite.sql.type.SqlTypeName.VARCHAR, labelLen, POS),
                  POS);
          SqlNode nullVarchar =
              new SqlBasicCall(
                  SqlStdOperatorTable.CAST,
                  List.of(SqlLiteral.createNull(POS), varcharLenSpec),
                  POS);
          // Try to mutate mainBody's select list in place if it's a plain SqlSelect. Otherwise
          // fall back to wrapping with an outer SELECT.
          if (mainBody instanceof SqlSelect mainSel) {
            SqlNodeList sel = mainSel.getSelectList();
            sel.add(asAlias(nullVarchar, labelField));
          } else {
            SqlNodeList mainProj = new SqlNodeList(POS);
            for (String c : cols) {
              mainProj.add(new SqlIdentifier(c, POS));
            }
            mainProj.add(asAlias(nullVarchar, labelField));
            mainBody =
                new SqlSelect(
                    POS, null, mainProj, mainBody, null, null, null, null, null, null, null, null,
                    null);
          }
          cols = new ArrayList<>(cols);
          cols.add(labelField);
        }
        // Build summary subquery: SELECT SUM(f1) AS f1, ... FROM (mainBody).
        SqlNodeList aggList = new SqlNodeList(POS);
        for (Field f : fields) {
          String name = f.getField().toString();
          aggList.add(
              asAlias(
                  new SqlBasicCall(
                      SqlStdOperatorTable.SUM, List.of(new SqlIdentifier(name, POS)), POS),
                  name));
        }
        SqlNode aggFrom =
            new SqlBasicCall(
                SqlStdOperatorTable.AS,
                List.of(mainBody, new SqlIdentifier("_addt_main_", POS)),
                POS);
        SqlSelect aggSelect =
            new SqlSelect(
                POS,
                /* keywordList */ null,
                aggList,
                aggFrom,
                /* where */ null,
                /* group */ null,
                /* having */ null,
                /* windowList */ null,
                /* qualify */ null,
                /* orderBy */ null,
                /* offset */ null,
                /* fetch */ null,
                /* hints */ null);
        // Wrap aggSelect so we can SELECT f1, NULL AS otherCol... from it.
        SqlNode aggAliased =
            new SqlBasicCall(
                SqlStdOperatorTable.AS,
                List.of(aggSelect, new SqlIdentifier("_addt_agg_", POS)),
                POS);
        SqlNodeList summaryProj = new SqlNodeList(POS);
        // VARCHAR(max(label.length, labelField.length)) — match the main side's NULL cast width
        // so UNION's row-type derivation produces a stable VARCHAR(N).
        int summaryLabelLen =
            labelField != null ? Math.max(label.length(), labelField.length()) : label.length();
        org.apache.calcite.sql.SqlDataTypeSpec labelVarcharSpec =
            new org.apache.calcite.sql.SqlDataTypeSpec(
                new org.apache.calcite.sql.SqlBasicTypeNameSpec(
                    org.apache.calcite.sql.type.SqlTypeName.VARCHAR, summaryLabelLen, POS),
                POS);
        for (String c : cols) {
          if (aggFieldNames.contains(c)) {
            summaryProj.add(asAlias(new SqlIdentifier(c, POS), c));
          } else if (labelField != null && c.equals(labelField)) {
            summaryProj.add(
                asAlias(
                    new SqlBasicCall(
                        SqlStdOperatorTable.CAST,
                        List.of(SqlLiteral.createCharString(label, POS), labelVarcharSpec),
                        POS),
                    c));
          } else {
            summaryProj.add(asAlias(SqlLiteral.createNull(POS), c));
          }
        }
        SqlSelect summarySelect =
            new SqlSelect(
                POS,
                /* keywordList */ null,
                summaryProj,
                aggAliased,
                /* where */ null,
                /* group */ null,
                /* having */ null,
                /* windowList */ null,
                /* qualify */ null,
                /* orderBy */ null,
                /* offset */ null,
                /* fetch */ null,
                /* hints */ null);
        SqlNode union =
            new SqlBasicCall(SqlStdOperatorTable.UNION_ALL, List.of(mainBody, summarySelect), POS);
        state.reset();
        state.setFrom(union);
      }
      return null;
    }

    private static boolean getBoolOption(
        java.util.Map<String, org.opensearch.sql.ast.expression.Literal> options,
        String key,
        boolean defaultVal) {
      org.opensearch.sql.ast.expression.Literal v = options.get(key);
      if (v == null) return defaultVal;
      Object raw = v.getValue();
      if (raw instanceof Boolean b) return b;
      if (raw instanceof String s)
        return "true".equalsIgnoreCase(s) || "t".equalsIgnoreCase(s) || "1".equals(s);
      return defaultVal;
    }

    @Override
    public Void visitAddColTotals(org.opensearch.sql.ast.tree.AddColTotals node, Void ignored) {
      // addcoltotals appends a SUMMARY ROW at the end containing per-column SUM of numeric
      // columns. (Despite the name, this isn't a per-row total — that's `addtotals row=true`.)
      // Implementation: UNION ALL the input with a SELECT that does SUM(col) for each numeric
      // column and NULL/label for the rest, ordered to match the input schema.
      walkChild(node);
      if (rowTypeOracle == null || state.from == null) {
        throw new UnsupportedOperationException(
            "addcoltotals requires the schema oracle for column enumeration");
      }
      java.util.Map<String, org.opensearch.sql.ast.expression.Literal> options = node.getOptions();
      String labelField =
          options != null && options.containsKey("labelfield")
              ? options.get("labelfield").getValue().toString()
              : null;
      String label =
          options != null && options.containsKey("label")
              ? options.get("label").getValue().toString()
              : "Total";
      List<Field> explicitFields = node.getFieldList();
      java.util.Set<String> aggFieldNames = new java.util.HashSet<>();
      for (Field f : explicitFields) {
        aggFieldNames.add(f.getField().toString());
      }
      if (state.orderBy != null || state.fetch != null) {
        state.wrap();
      }
      // Flush outer ORDER BY / FETCH into the inner subquery so an upstream `head N` survives
      // the UNION wrapping. SqlOrderBy as a FROM-clause subquery has surprising unparse
      // behavior; an explicit inner SqlSelect with FETCH is more reliable.
      flushOuterIntoInner();
      // Probe the row type of the COMPLETE in-flight pipeline (including any prior projection
      // pipes), not just state.from. After `| fields a, b`, only those columns should appear.
      // Deep-clone mainBodyInner before the probe so the validator's fullyQualify mutations
      // don't propagate to the live tree (which is reused as the union's main side and
      // referenced inside summarySelect.from). Without isolation, mutations from the probe
      // double-apply during the union's validation pass, causing duplicate Sort+Project layers
      // in the converted rel.
      SqlNode mainBodyInner = state.toFinalSqlNode();
      org.apache.calcite.rel.type.RelDataType rt =
          deriveRowType(org.opensearch.sql.calcite.sqlnode.SqlNodePlanner.deepClone(mainBodyInner));
      java.util.Map<String, org.apache.calcite.sql.type.SqlTypeName> numericCols =
          new java.util.LinkedHashMap<>();
      List<String> allCols = new ArrayList<>();
      for (org.apache.calcite.rel.type.RelDataTypeField rf : rt.getFieldList()) {
        String c = rf.getName();
        if (org.opensearch.sql.calcite.plan.OpenSearchConstants.METADATAFIELD_TYPE_MAP.containsKey(
            c)) {
          continue;
        }
        allCols.add(c);
        org.apache.calcite.sql.type.SqlTypeName tn = rf.getType().getSqlTypeName();
        if (tn != null && org.apache.calcite.sql.type.SqlTypeName.NUMERIC_TYPES.contains(tn)) {
          numericCols.put(c, tn);
        }
      }
      // When labelField names a column that isn't already in the projection, materialize it as
      // a new appended column on both sides: a CHAR-padded blank on data rows, the label string
      // on the summary row. This matches PPL's `addcoltotals labelfield='Grand Total'`
      // behaviour. Using a CHAR-typed placeholder (rather than NULL) avoids UNION row-type
      // resolution promoting the column to BIGINT to satisfy default-NULL coercion.
      boolean appendLabelField = labelField != null && !allCols.contains(labelField);
      StringBuilder labelPad = new StringBuilder();
      for (int i = 0; i < label.length(); i++) {
        labelPad.append(' ');
      }
      // Build summary subquery from the projected main body.
      SqlNodeList mainProj = new SqlNodeList(POS);
      for (String c : allCols) {
        mainProj.add(new SqlIdentifier(c, POS));
      }
      if (appendLabelField) {
        mainProj.add(asAlias(SqlLiteral.createCharString(labelPad.toString(), POS), labelField));
      }
      SqlSelect mainProjected =
          new SqlSelect(
              POS,
              null,
              mainProj,
              mainBodyInner,
              null,
              null,
              null,
              null,
              null,
              null,
              null,
              null,
              null);
      // Summary: SELECT (sum or NULL/label) ... FROM main projected.
      SqlNodeList summaryProj = new SqlNodeList(POS);
      for (String c : allCols) {
        boolean shouldAgg =
            numericCols.containsKey(c) && (aggFieldNames.isEmpty() || aggFieldNames.contains(c));
        if (shouldAgg) {
          SqlNode sum =
              new SqlBasicCall(SqlStdOperatorTable.SUM, List.of(new SqlIdentifier(c, POS)), POS);
          summaryProj.add(asAlias(sum, c));
        } else if (labelField != null && c.equals(labelField)) {
          summaryProj.add(asAlias(SqlLiteral.createCharString(label, POS), c));
        } else {
          summaryProj.add(asAlias(SqlLiteral.createNull(POS), c));
        }
      }
      if (appendLabelField) {
        summaryProj.add(asAlias(SqlLiteral.createCharString(label, POS), labelField));
      }
      SqlSelect summarySelect =
          new SqlSelect(
              POS,
              null,
              summaryProj,
              mainProjected,
              null,
              null,
              null,
              null,
              null,
              null,
              null,
              null,
              null);
      SqlNode union =
          new SqlBasicCall(
              SqlStdOperatorTable.UNION_ALL, List.of(mainProjected, summarySelect), POS);
      state.reset();
      state.setFrom(union);
      return null;
    }

    @Override
    public Void visitTranspose(org.opensearch.sql.ast.tree.Transpose node, Void ignored) {
      walkChild(node);
      // transpose: pivot N data rows into N columns, with the input row identifier becoming the
      // pivot key. v2 builds this as `MAX(field) FILTER (WHERE row_idx = N)` for each of the
      // first `maxRows` rows. Implementation needs:
      //   1. Add ROW_NUMBER() OVER () AS _row_idx_.
      //   2. Add a boolean flag column per row: _is_row_N_ = (_row_idx_ = N).
      //   3. UNPIVOT-style: SELECT column-name-as-string, MAX(col1) FILTER _is_row_1_ AS "row 1",
      //      ... GROUP BY column-name.
      //   The "column-name-as-string" column requires unpivoting all input columns into a long
      //   format first. This is two-stage and needs schema introspection. Implement a minimal
      //   version.
      if (rowTypeOracle == null) {
        throw new UnsupportedOperationException(
            "transpose requires schema introspection (row-type oracle)");
      }
      // Materialize the upstream pipeline as a single subquery so its post-projection row type
      // (e.g. after `| fields firstname, age, balance`) is what we transpose. Otherwise probing
      // state.from alone returns the SOURCE table's full column list, ignoring upstream
      // projection narrowing.
      SqlNode pipelineSnapshot = state.toFinalSqlNode();
      List<String> cols =
          deriveColumnNames(pipelineSnapshot).stream()
              .filter(
                  c ->
                      !org.opensearch.sql.calcite.plan.OpenSearchConstants.METADATAFIELD_TYPE_MAP
                          .containsKey(c))
              .toList();
      int maxRows = node.getMaxRows();
      String columnAlias = node.getColumnName() != null ? node.getColumnName() : "column";
      // Step 1: build a UNION ALL of (column_name, value) tuples for each input column.
      // Each branch: SELECT '<col>' AS column, <col> AS value, ROW_NUMBER() OVER () AS _rn_
      List<SqlNode> unionBranches = new ArrayList<>();
      SqlNode currentFrom = pipelineSnapshot;
      for (String c : cols) {
        SqlNodeList sl = new SqlNodeList(POS);
        // CAST the literal to VARCHAR — UNION ALL of CHAR literals would CHAR-pad to the
        // longest branch's width, leaving trailing spaces in shorter column names.
        SqlNode colNameLit =
            new SqlBasicCall(
                SqlStdOperatorTable.CAST,
                List.of(
                    SqlLiteral.createCharString(c, POS),
                    new org.apache.calcite.sql.SqlDataTypeSpec(
                        new org.apache.calcite.sql.SqlBasicTypeNameSpec(
                            org.apache.calcite.sql.type.SqlTypeName.VARCHAR, POS),
                        POS)),
                POS);
        sl.add(asAlias(colNameLit, columnAlias));
        sl.add(
            asAlias(
                new SqlBasicCall(
                    SqlStdOperatorTable.CAST,
                    List.of(
                        new SqlIdentifier(c, POS),
                        new org.apache.calcite.sql.SqlDataTypeSpec(
                            new org.apache.calcite.sql.SqlBasicTypeNameSpec(
                                org.apache.calcite.sql.type.SqlTypeName.VARCHAR, POS),
                            POS)),
                    POS),
                "_value_"));
        SqlNode rnWindow =
            org.apache.calcite.sql.SqlWindow.create(
                null,
                null,
                new SqlNodeList(POS),
                new SqlNodeList(POS),
                SqlLiteral.createBoolean(false, POS),
                null,
                null,
                null,
                POS);
        SqlNode rnOver =
            new SqlBasicCall(
                SqlStdOperatorTable.OVER,
                List.of(new SqlBasicCall(SqlStdOperatorTable.ROW_NUMBER, List.of(), POS), rnWindow),
                POS);
        sl.add(asAlias(rnOver, "_rn_"));
        SqlSelect branch =
            new SqlSelect(
                POS,
                /* keywordList */ null,
                sl,
                currentFrom,
                /* where */ null,
                /* group */ null,
                /* having */ null,
                /* windowList */ null,
                /* qualify */ null,
                /* orderBy */ null,
                /* offset */ null,
                /* fetch */ null,
                /* hints */ null);
        unionBranches.add(branch);
      }
      SqlNode unioned = unionBranches.get(0);
      for (int i = 1; i < unionBranches.size(); i++) {
        unioned =
            new SqlBasicCall(
                SqlStdOperatorTable.UNION_ALL, List.of(unioned, unionBranches.get(i)), POS);
      }
      // Step 2: GROUP BY column, with MAX(value) FILTER (WHERE _rn_ = N) AS "row N" for each N.
      state.reset();
      state.setFrom(unioned);
      List<SqlNode> selects = new ArrayList<>();
      selects.add(new SqlIdentifier(columnAlias, POS));
      for (int n = 1; n <= maxRows; n++) {
        SqlNode filterCond =
            new SqlBasicCall(
                SqlStdOperatorTable.EQUALS,
                List.of(new SqlIdentifier("_rn_", POS), intLiteral(n)),
                POS);
        SqlNode maxCall =
            new SqlBasicCall(
                SqlStdOperatorTable.MAX, List.of(new SqlIdentifier("_value_", POS)), POS);
        SqlNode filtered =
            new SqlBasicCall(SqlStdOperatorTable.FILTER, List.of(maxCall, filterCond), POS);
        selects.add(asAlias(filtered, "row " + n));
      }
      state.setProjection(selects);
      state.setGroupBy(List.of(new SqlIdentifier(columnAlias, POS)));
      return null;
    }

    @Override
    public Void visitBin(org.opensearch.sql.ast.tree.Bin node, Void ignored) {
      walkChild(node);
      if (state.evalExtended || state.projectionReplaced) {
        state.wrap();
      }
      // Each Bin subclass dispatches to a different SPAN_BUCKET / WIDTH_BUCKET /
      // MINSPAN_BUCKET / RANGE_BUCKET operator on PPLBuiltinOperators. The validator resolves
      // these by name. Note: time-based span (e.g. `bin SAL span=1d`) needs additional dispatch
      // through TimeSpanHelper that we don't replicate here — those will surface a validator
      // error if the operand type is wrong.
      // Bin.getField() returns a Field AST node whose toString includes "Field(...)" wrapper.
      // Unwrap to the inner QualifiedName so fieldName matches the actual column identifier.
      UnresolvedExpression rawFieldExpr = node.getField();
      if (rawFieldExpr instanceof Field f) {
        rawFieldExpr = f.getField();
      }
      String fieldName =
          (rawFieldExpr instanceof QualifiedName qn) ? qn.toString() : rawFieldExpr.toString();
      String alias = node.getAlias() != null ? node.getAlias() : fieldName;
      SqlNode fieldRef = expr(node.getField());
      SqlNode bucketCall;
      if (node instanceof org.opensearch.sql.ast.tree.SpanBin sb) {
        // For time-based fields (EXPR_TIMESTAMP/DATE/TIME) with a span like "1d"/"5m"/"30s",
        // dispatch to SPAN(field, intervalValue, unit) rather than SPAN_BUCKET (numeric-only).
        // Mirrors v2's TimeSpanHelper.createTimeSpanExpression at SpanBinHandler.java#37.
        SqlNode timeSpanCall =
            tryTimeSpanCall(fieldRef, sb.getSpan(), sb.getAligntime(), state.from, fieldName);
        SqlNode logSpanCall = timeSpanCall == null ? tryLogSpanCall(fieldRef, sb.getSpan()) : null;
        if (timeSpanCall != null) {
          bucketCall = timeSpanCall;
        } else if (logSpanCall != null) {
          bucketCall = logSpanCall;
        } else {
          bucketCall =
              new SqlBasicCall(
                  org.opensearch.sql.expression.function.PPLBuiltinOperators.SPAN_BUCKET,
                  List.of(fieldRef, expr(sb.getSpan())),
                  POS);
        }
      } else if (node instanceof org.opensearch.sql.ast.tree.CountBin cb) {
        // WIDTH_BUCKET(field, num_bins, data_range=max-min, max_value=max).
        // Always use the window-derived MIN/MAX of the field — start/end on bin command are
        // accepted at parse time but not used by v2's CountBinHandler either; the bin width is
        // computed from the actual data range by the WIDTH_BUCKET impl.
        SqlNode minVal = minOver(fieldRef);
        SqlNode maxVal = maxOver(fieldRef);
        SqlNode dataRange =
            new SqlBasicCall(SqlStdOperatorTable.MINUS, List.of(maxVal, minVal), POS);
        bucketCall =
            new SqlBasicCall(
                org.opensearch.sql.expression.function.PPLBuiltinOperators.WIDTH_BUCKET,
                List.of(fieldRef, intLiteral(cb.getBins()), dataRange, maxVal),
                POS);
      } else if (node instanceof org.opensearch.sql.ast.tree.MinSpanBin msb) {
        // MINSPAN_BUCKET(field, min_span, data_range=max-min, max_value=max).
        // Same start/end semantic as CountBin: window-derived range.
        SqlNode minVal = minOver(fieldRef);
        SqlNode maxVal = maxOver(fieldRef);
        SqlNode dataRange =
            new SqlBasicCall(SqlStdOperatorTable.MINUS, List.of(maxVal, minVal), POS);
        bucketCall =
            new SqlBasicCall(
                org.opensearch.sql.expression.function.PPLBuiltinOperators.MINSPAN_BUCKET,
                List.of(fieldRef, expr(msb.getMinspan()), dataRange, maxVal),
                POS);
      } else if (node instanceof org.opensearch.sql.ast.tree.RangeBin rb) {
        // RANGE_BUCKET takes 5 args: (field, min, max, start, end). v2's argument order is
        // (field, MIN(field) OVER, MAX(field) OVER, start, end) — matching that here.
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
            "Unknown Bin subtype: " + node.getClass().getSimpleName());
      }
      // Project: emit non-bin columns in original order, then append the bin column at the end
      // (v2's emission shape — visible in the testBinWithNestedFieldWithoutExplicitProjection
      // result column order).
      if (rowTypeOracle != null && state.from != null) {
        List<String> rawCols = deriveColumnNames(state.from);
        java.util.Set<String> rawColSet = new java.util.HashSet<>(rawCols);
        java.util.Set<String> ancestorStructs = collectAncestorStructs(rawColSet);
        List<String> cols =
            rawCols.stream()
                .filter(
                    c ->
                        !org.opensearch.sql.calcite.plan.OpenSearchConstants.METADATAFIELD_TYPE_MAP
                            .containsKey(c))
                .filter(c -> !ancestorStructs.contains(c))
                .toList();
        List<SqlNode> selects = new ArrayList<>();
        for (String c : cols) {
          if (c.equals(alias)) continue; // skip the source field; bin replaces it at the end
          selects.add(new SqlIdentifier(c, POS));
        }
        selects.add(asAlias(bucketCall, alias));
        state.setProjection(selects);
      } else {
        state.addEvalAlias(bucketCall, alias);
      }
      return null;
    }

    /**
     * Identify "ancestor struct" columns — those whose name is a strict dotted prefix of any other
     * column. OpenSearch's mapping flattening emits both the parent struct (e.g. {@code resource})
     * and its leaves ({@code resource.attributes...sdk.version}); user-facing output should surface
     * only the leaves. Returns an empty set when no such ancestor exists, which keeps standalone
     * MAP/struct columns (like the spath-rewritten {@code doc}) intact.
     */
    private java.util.Set<String> collectAncestorStructs(java.util.Set<String> colSet) {
      java.util.Set<String> ancestors = new java.util.HashSet<>();
      for (String c : colSet) {
        int lastDot = c.lastIndexOf('.');
        while (lastDot != -1) {
          String prefix = c.substring(0, lastDot);
          if (colSet.contains(prefix)) {
            ancestors.add(prefix);
          }
          lastDot = c.lastIndexOf('.', lastDot - 1);
        }
      }
      return ancestors;
    }

    /** MIN(field) OVER () — used as a default range bound for default-bin magnitude inference. */
    private SqlNode minOver(SqlNode field) {
      return aggOver(SqlStdOperatorTable.MIN, field);
    }

    private SqlNode maxOver(SqlNode field) {
      return aggOver(SqlStdOperatorTable.MAX, field);
    }

    private SqlNode aggOver(SqlOperator agg, SqlNode field) {
      SqlNode window =
          org.apache.calcite.sql.SqlWindow.create(
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
          SqlStdOperatorTable.OVER,
          List.of(new SqlBasicCall(agg, List.of(field), POS), window),
          POS);
    }

    @Override
    public Void visitPatterns(org.opensearch.sql.ast.tree.Patterns node, Void ignored) {
      walkChild(node);
      String aliasField = "patterns_field";
      org.opensearch.sql.ast.expression.PatternMethod method = node.getPatternMethod();
      org.opensearch.sql.ast.expression.PatternMode mode = node.getPatternMode();
      if (method == org.opensearch.sql.ast.expression.PatternMethod.SIMPLE_PATTERN) {
        org.opensearch.sql.ast.expression.Literal patternLit =
            node.getArguments() != null
                ? node.getArguments().get(org.opensearch.sql.common.patterns.PatternUtils.PATTERN)
                : null;
        if (patternLit == null) {
          patternLit = org.opensearch.sql.ast.dsl.AstDSL.stringLiteral("");
        }
        org.opensearch.sql.ast.tree.Parse parseNode =
            new org.opensearch.sql.ast.tree.Parse(
                org.opensearch.sql.ast.expression.ParseMethod.PATTERNS,
                node.getSourceField(),
                patternLit,
                node.getArguments());
        visitParse(parseNode, ignored);
        // SIMPLE_PATTERN show_numbered_token=true: visitParse emitted patterns_field with literal
        // "<*>" replacement. v2 handles this by calling PATTERN_PARSER(patterns_field, sourceField)
        // which returns a struct {pattern: replaced-with-numbered-tokens, tokens: map}. We use
        // ITEM(...,'pattern') to OVERRIDE the patterns_field value with the numbered version, and
        // ITEM(...,'tokens') to add the tokens column. Mirrors flattenParsedPattern at
        // CalciteRelNodeVisitor.java#4290-4329.
        boolean showNumbered = false;
        if (node.getShowNumberedToken() instanceof org.opensearch.sql.ast.expression.Literal lit
            && Boolean.TRUE.equals(lit.getValue())) {
          showNumbered = true;
        }
        if (showNumbered && mode != org.opensearch.sql.ast.expression.PatternMode.AGGREGATION) {
          // Wrap so patterns_field becomes a real column referenceable by PATTERN_PARSER below.
          state.wrap();
          // Override the existing patterns_field column: seed projection from FROM cols minus
          // patterns_field so the new addEvalAlias doesn't collide.
          if (rowTypeOracle != null && state.from != null) {
            try {
              List<String> cols =
                  deriveColumnNames(state.from).stream()
                      .filter(
                          c ->
                              !c.equals("patterns_field")
                                  && !org.opensearch.sql.calcite.plan.OpenSearchConstants
                                      .METADATAFIELD_TYPE_MAP
                                      .containsKey(c))
                      .toList();
              List<SqlNode> seeded = new ArrayList<>();
              for (String c : cols) {
                seeded.add(new SqlIdentifier(c, POS));
              }
              state.projection = seeded;
              state.projectionReplaced = true;
              state.evalAliasNames.remove("patterns_field");
            } catch (RuntimeException ignored6) {
              // probe failed; addEvalAlias may collide
            }
          }
          SqlNode patternsFieldRef = new SqlIdentifier("patterns_field", POS);
          SqlNode sourceFieldRef = expr(node.getSourceField());
          SqlNode parserCall =
              new SqlBasicCall(
                  new org.apache.calcite.sql.SqlUnresolvedFunction(
                      new SqlIdentifier("PATTERN_PARSER", POS),
                      null,
                      null,
                      null,
                      null,
                      org.apache.calcite.sql.SqlFunctionCategory.USER_DEFINED_FUNCTION),
                  List.of(patternsFieldRef, sourceFieldRef),
                  POS);
          SqlNode patternItem =
              new SqlBasicCall(
                  SqlStdOperatorTable.ITEM,
                  List.of(parserCall, SqlLiteral.createCharString("pattern", POS)),
                  POS);
          // Use SAFE_CAST to match v2's emission shape for SIMPLE_PATTERN with show_numbered.
          SqlNode patternCast =
              new SqlBasicCall(
                  org.apache.calcite.sql.fun.SqlLibraryOperators.SAFE_CAST,
                  List.of(
                      patternItem,
                      new org.apache.calcite.sql.SqlDataTypeSpec(
                          new org.apache.calcite.sql.SqlBasicTypeNameSpec(
                              org.apache.calcite.sql.type.SqlTypeName.VARCHAR, POS),
                          POS)),
                  POS);
          state.addEvalAlias(patternCast, "patterns_field");
          SqlNode tokensItem =
              new SqlBasicCall(
                  SqlStdOperatorTable.ITEM,
                  List.of(parserCall, SqlLiteral.createCharString("tokens", POS)),
                  POS);
          state.addEvalAlias(tokensItem, "tokens");
        }
      } else if (method == org.opensearch.sql.ast.expression.PatternMethod.BRAIN) {
        // BRAIN AGG mode is structurally different from LABEL: aggregate INTERNAL_PATTERN
        // (returns ARRAY<MAP>), then UNNEST/Correlate to expand each cluster MAP into its own
        // row, then SAFE_CAST(ITEM(map, 'pattern' / 'pattern_count' / 'tokens' / 'sample_logs'))
        // for the four flattened columns. Mirrors v2's
        // CalciteRelNodeVisitor.visitPatterns#1127-1153 path. Handle here and return early; the
        // downstream `if (mode == AGGREGATION)` block is for SIMPLE_PATTERN/BRAIN-LABEL flows.
        if (mode == org.opensearch.sql.ast.expression.PatternMode.AGGREGATION) {
          emitBrainAggregationMode(node, aliasField);
          return null;
        }
        // BRAIN: SAFE_CAST(ITEM(PATTERN_PARSER(field, pattern(field, max_sample, buffer,
        //                                       show_numbered) OVER (PARTITION BY <by>),
        //                       show_numbered),
        //                  'pattern')).
        // The `pattern` operator is registered as a SqlAggFunction (window aggregate). We
        // promote it to a side-by-side eventstats-style projection: build a wrapping pipeline
        // that adds `_pattern_agg_` as a window column, then reference it in the PATTERN_PARSER
        // call. This avoids the SqlToRelConverter's "convertExpression on agg without OVER"
        // path when the aggregate is nested.
        if (state.evalExtended || state.projectionReplaced) {
          state.wrap();
        }
        SqlNode source = expr(node.getSourceField());
        SqlNode maxSampleCount =
            node.getPatternMaxSampleCount() != null
                ? expr(node.getPatternMaxSampleCount())
                : intLiteral(10);
        SqlNode bufferLimit =
            node.getPatternBufferLimit() != null
                ? expr(node.getPatternBufferLimit())
                : intLiteral(100000);
        SqlNode showNumbered =
            node.getShowNumberedToken() != null
                ? expr(node.getShowNumberedToken())
                : SqlLiteral.createBoolean(false, POS);
        SqlNodeList partitionBy = new SqlNodeList(POS);
        if (node.getPartitionByList() != null) {
          for (UnresolvedExpression p : node.getPartitionByList()) {
            partitionBy.add(expr(stripAlias(p)));
          }
        }
        SqlNode patternWindow =
            org.apache.calcite.sql.SqlWindow.create(
                null,
                null,
                partitionBy,
                new SqlNodeList(POS),
                SqlLiteral.createBoolean(false, POS),
                null,
                null,
                null,
                POS);
        // Build INTERNAL_PATTERN args: 4 fixed + optional BRAIN tuning params (alphabetical). v2's
        // visitPatterns sorts by Argument::getArgName: frequency_threshold_percentage first, then
        // variable_count_threshold. Match that ordering.
        List<SqlNode> patternArgs = new ArrayList<>();
        patternArgs.add(source);
        patternArgs.add(maxSampleCount);
        patternArgs.add(bufferLimit);
        patternArgs.add(showNumbered);
        if (node.getArguments() != null) {
          List<String> brainKeys =
              node.getArguments().keySet().stream()
                  .filter(
                      org.opensearch.sql.common.patterns.PatternUtils.VALID_BRAIN_PARAMETERS
                          ::contains)
                  .sorted()
                  .toList();
          for (String k : brainKeys) {
            patternArgs.add(
                expr(
                    new org.opensearch.sql.ast.expression.Literal(
                        node.getArguments().get(k).getValue(),
                        node.getArguments().get(k).getType())));
          }
        }
        SqlNode patternAgg =
            new SqlBasicCall(
                org.opensearch.sql.expression.function.PPLBuiltinOperators.INTERNAL_PATTERN,
                patternArgs,
                POS);
        SqlNode patternOver =
            new SqlBasicCall(SqlStdOperatorTable.OVER, List.of(patternAgg, patternWindow), POS);
        // Inline OVER inside PATTERN_PARSER (mirroring v2's projectPlus emission). Producing a
        // single Project with the OVER nested matches v2's RelNode shape and prevents Calcite
        // from pushing a downstream Sort/Fetch below the window-bearing project (which would
        // restrict the BRAIN aggregator to fewer input rows than the user expects).
        SqlNode source2 = expr(node.getSourceField());
        SqlNode parserCall =
            new SqlBasicCall(
                new org.apache.calcite.sql.SqlUnresolvedFunction(
                    new SqlIdentifier("PATTERN_PARSER", POS),
                    null,
                    null,
                    null,
                    null,
                    org.apache.calcite.sql.SqlFunctionCategory.USER_DEFINED_FUNCTION),
                List.of(source2, patternOver, showNumbered),
                POS);
        SqlNode itemCall =
            new SqlBasicCall(
                SqlStdOperatorTable.ITEM,
                List.of(parserCall, SqlLiteral.createCharString("pattern", POS)),
                POS);
        // v2 emits SAFE_CAST(ITEM(PATTERN_PARSER, 'pattern')) — the result of ITEM may not be
        // a string (struct field), and SAFE_CAST returns NULL on type mismatch instead of
        // throwing. Match v2's emission shape.
        SqlNode safeCast =
            new SqlBasicCall(
                org.apache.calcite.sql.fun.SqlLibraryOperators.SAFE_CAST,
                List.of(
                    itemCall,
                    new org.apache.calcite.sql.SqlDataTypeSpec(
                        new org.apache.calcite.sql.SqlBasicTypeNameSpec(
                            org.apache.calcite.sql.type.SqlTypeName.VARCHAR, POS),
                        POS)),
                POS);
        state.addEvalAlias(safeCast, aliasField);
        if (showNumbered instanceof SqlLiteral lit && Boolean.TRUE.equals(lit.getValue())) {
          // tokens is a MAP<VARCHAR, ARRAY<VARCHAR>> (struct in PPL response). Don't cast to
          // VARCHAR — that mangles the response schema. PATTERN_PARSER's return type already
          // exposes tokens as a map; ITEM(...,'tokens') preserves the map type.
          SqlNode tokensItem =
              new SqlBasicCall(
                  SqlStdOperatorTable.ITEM,
                  List.of(parserCall, SqlLiteral.createCharString("tokens", POS)),
                  POS);
          state.addEvalAlias(tokensItem, "tokens");
        }
        // Wrap so the patterns_field projection (which contains a window function over the full
        // input) lives inside its own SELECT. Without this wrap, a downstream `head N` lands in
        // the same SELECT as the OVER and Calcite's SortProjectTranspose may push FETCH below
        // the window — restricting BRAIN to N input rows instead of all rows.
        state.wrap();
      } else {
        throw new IllegalArgumentException("Unknown patterns method: " + method);
      }
      // Apply mode=AGGREGATION: GROUP BY (partitionByList ++ patterns_field), with
      // pattern_count = COUNT(*) and sample_logs = COLLECT(source field) approximation of v2's
      // TAKE().
      if (mode == org.opensearch.sql.ast.expression.PatternMode.AGGREGATION) {
        state.wrap();
        List<SqlNode> aggSelects = new ArrayList<>();
        List<SqlNode> groupKeys = new ArrayList<>();
        if (node.getPartitionByList() != null) {
          for (UnresolvedExpression p : node.getPartitionByList()) {
            SqlNode key = expr(stripAlias(p));
            groupKeys.add(key);
            aggSelects.add(key);
          }
        }
        SqlNode patField = new SqlIdentifier(aliasField, POS);
        groupKeys.add(patField);
        aggSelects.add(patField);
        aggSelects.add(
            asAlias(
                new SqlBasicCall(SqlStdOperatorTable.COUNT, List.of(SqlIdentifier.star(POS)), POS),
                "pattern_count"));
        // sample_logs = TAKE(source_field, max_sample_count) — the v2 path uses the PPL TAKE
        // aggregator (returns ARRAY) rather than standard SQL COLLECT (returns MULTISET, which
        // Calcite reports as STRING in the response schema).
        SqlNode maxSampleCount = expr(node.getPatternMaxSampleCount());
        aggSelects.add(
            asAlias(
                new SqlBasicCall(
                    org.opensearch.sql.expression.function.PPLBuiltinOperators.TAKE,
                    List.of(expr(node.getSourceField()), maxSampleCount),
                    POS),
                "sample_logs"));
        state.setProjection(aggSelects);
        state.setGroupBy(groupKeys);
        // SIMPLE_PATTERN AGGREGATION show_numbered_token=true: after the GROUP BY produces
        // patterns_field + sample_logs, run PATTERN_PARSER(patterns_field, sample_logs) and
        // override patterns_field with the numbered version + add tokens column. Mirrors v2's
        // flattenParsedPattern call at CalciteRelNodeVisitor.java#1057-1077.
        boolean showNumberedAgg = false;
        // SIMPLE_PATTERN with show_numbered=true: re-run PATTERN_PARSER post-grouping. BRAIN's
        // numbered tokens use a different format (`<token1>`) than the SIMPLE_PATTERN wildcard
        // (`<*>`), so PATTERN_PARSER's evalSamples (WILDCARD_PATTERN-based) can't extract them.
        // BRAIN AGG show_numbered=true is handled via a dedicated path below.
        if (method == org.opensearch.sql.ast.expression.PatternMethod.SIMPLE_PATTERN
            && node.getShowNumberedToken() instanceof org.opensearch.sql.ast.expression.Literal lit
            && Boolean.TRUE.equals(lit.getValue())) {
          showNumberedAgg = true;
        }
        if (showNumberedAgg) {
          state.wrap();
          // Build PARSER call referencing wrapped subquery's columns.
          SqlNode patternsFieldRef = new SqlIdentifier("patterns_field", POS);
          SqlNode sampleLogsRef = new SqlIdentifier("sample_logs", POS);
          SqlNode parserCall =
              new SqlBasicCall(
                  new org.apache.calcite.sql.SqlUnresolvedFunction(
                      new SqlIdentifier("PATTERN_PARSER", POS),
                      null,
                      null,
                      null,
                      null,
                      org.apache.calcite.sql.SqlFunctionCategory.USER_DEFINED_FUNCTION),
                  List.of(patternsFieldRef, sampleLogsRef),
                  POS);
          SqlNode patternItem =
              new SqlBasicCall(
                  SqlStdOperatorTable.ITEM,
                  List.of(parserCall, SqlLiteral.createCharString("pattern", POS)),
                  POS);
          SqlNode patternCast =
              new SqlBasicCall(
                  SqlStdOperatorTable.CAST,
                  List.of(
                      patternItem,
                      new org.apache.calcite.sql.SqlDataTypeSpec(
                          new org.apache.calcite.sql.SqlBasicTypeNameSpec(
                              org.apache.calcite.sql.type.SqlTypeName.VARCHAR, POS),
                          POS)),
                  POS);
          SqlNode tokensItem =
              new SqlBasicCall(
                  SqlStdOperatorTable.ITEM,
                  List.of(parserCall, SqlLiteral.createCharString("tokens", POS)),
                  POS);
          // Build explicit projection in v2's order: [partitionBy ..., patterns_field,
          // pattern_count, tokens, sample_logs]. Mirrors projectPlusOverriding at
          // CalciteRelNodeVisitor.java#1066-1077.
          List<SqlNode> reordered = new ArrayList<>();
          if (node.getPartitionByList() != null) {
            for (UnresolvedExpression p : node.getPartitionByList()) {
              UnresolvedExpression core = stripAlias(p);
              String pname =
                  (p instanceof Alias al)
                      ? al.getName()
                      : (core instanceof QualifiedName qn ? qn.toString() : null);
              if (pname != null) {
                reordered.add(new SqlIdentifier(pname, POS));
              }
            }
          }
          reordered.add(asAlias(patternCast, "patterns_field"));
          reordered.add(new SqlIdentifier("pattern_count", POS));
          reordered.add(asAlias(tokensItem, "tokens"));
          reordered.add(new SqlIdentifier("sample_logs", POS));
          state.projection = reordered;
          state.projectionReplaced = true;
        }
      }
      return null;
    }

    /**
     * Emit BRAIN AGG mode as: aggregate INTERNAL_PATTERN (returns ARRAY&lt;MAP&gt;) → CROSS JOIN
     * LATERAL UNNEST → ITEM access for each MAP key. Mirrors v2's {@code
     * aggregate(...).buildExpandRelNode(...).flattenParsedPattern(aggMode=true,...)}.
     *
     * <p>SQL shape (no group by):
     *
     * <pre>
     *   SELECT SAFE_CAST(t.patterns_field['pattern'] AS VARCHAR) AS patterns_field,
     *          SAFE_CAST(t.patterns_field['pattern_count'] AS BIGINT) AS pattern_count,
     *          SAFE_CAST(t.patterns_field['tokens'] AS MAP&lt;VARCHAR, VARCHAR ARRAY&gt;) AS tokens, -- only when show_numbered=true
     *          SAFE_CAST(t.patterns_field['sample_logs'] AS VARCHAR ARRAY) AS sample_logs
     *   FROM (SELECT pattern(field, ...) AS patterns_field FROM &lt;input&gt;) AS s,
     *        LATERAL UNNEST(s.patterns_field) AS t(patterns_field)
     * </pre>
     *
     * <p>With partitionBy, the inner SELECT also groups by partitionBy and surfaces those keys.
     */
    private void emitBrainAggregationMode(
        org.opensearch.sql.ast.tree.Patterns node, String aliasField) {
      // Bake any pending eval/projection state into a subquery before reshaping FROM into the
      // aggregate+UNNEST cross-join.
      if (state.evalExtended || state.projectionReplaced) {
        state.wrap();
      }
      SqlNode source = expr(node.getSourceField());
      SqlNode maxSampleCount =
          node.getPatternMaxSampleCount() != null
              ? expr(node.getPatternMaxSampleCount())
              : intLiteral(10);
      SqlNode bufferLimit =
          node.getPatternBufferLimit() != null
              ? expr(node.getPatternBufferLimit())
              : intLiteral(100000);
      SqlNode showNumbered =
          node.getShowNumberedToken() != null
              ? expr(node.getShowNumberedToken())
              : SqlLiteral.createBoolean(false, POS);
      boolean showNumberedTrue =
          showNumbered instanceof SqlLiteral lit && Boolean.TRUE.equals(lit.getValue());
      // Build INTERNAL_PATTERN args: 4 fixed + optional BRAIN tuning params (alphabetical).
      List<SqlNode> patternArgs = new ArrayList<>();
      patternArgs.add(source);
      patternArgs.add(maxSampleCount);
      patternArgs.add(bufferLimit);
      patternArgs.add(showNumbered);
      if (node.getArguments() != null) {
        List<String> brainKeys =
            node.getArguments().keySet().stream()
                .filter(
                    org.opensearch.sql.common.patterns.PatternUtils.VALID_BRAIN_PARAMETERS
                        ::contains)
                .sorted()
                .toList();
        for (String k : brainKeys) {
          patternArgs.add(
              expr(
                  new org.opensearch.sql.ast.expression.Literal(
                      node.getArguments().get(k).getValue(),
                      node.getArguments().get(k).getType())));
        }
      }
      SqlNode patternAgg =
          new SqlBasicCall(
              org.opensearch.sql.expression.function.PPLBuiltinOperators.INTERNAL_PATTERN,
              patternArgs,
              POS);
      // Inner SELECT: pattern aggregate (+ partitionBy keys when present).
      List<SqlNode> innerProjects = new ArrayList<>();
      List<String> partitionNames = new ArrayList<>();
      if (node.getPartitionByList() != null) {
        for (UnresolvedExpression p : node.getPartitionByList()) {
          UnresolvedExpression core = stripAlias(p);
          String pname =
              (p instanceof Alias al)
                  ? al.getName()
                  : (core instanceof QualifiedName qn ? qn.toString() : null);
          partitionNames.add(pname);
          innerProjects.add(expr(core));
        }
      }
      innerProjects.add(asAlias(patternAgg, aliasField));
      // Build inner SqlSelect: SELECT <partitionBy>..., pattern(...) AS patterns_field FROM <from>
      // [GROUP BY <partitionBy>]
      SqlNode innerFrom = state.from;
      org.apache.calcite.sql.SqlSelect inner =
          new org.apache.calcite.sql.SqlSelect(
              POS,
              null,
              new SqlNodeList(innerProjects, POS),
              innerFrom,
              null,
              partitionNames.isEmpty()
                  ? null
                  : new SqlNodeList(
                      node.getPartitionByList().stream().map(p -> expr(stripAlias(p))).toList(),
                      POS),
              null,
              null,
              null,
              null,
              null,
              null,
              null);
      String innerAlias = "_brain_agg_";
      SqlNode aliasedInner =
          new SqlBasicCall(
              SqlStdOperatorTable.AS, List.of(inner, new SqlIdentifier(innerAlias, POS)), POS);
      // LATERAL UNNEST(<innerAlias>.patterns_field) AS t(patterns_field)
      SqlNode unnestArg = new SqlIdentifier(java.util.Arrays.asList(innerAlias, aliasField), POS);
      SqlNode unnest = new SqlBasicCall(SqlStdOperatorTable.UNNEST, List.of(unnestArg), POS);
      String unnestAlias = "_brain_unnest_";
      SqlNode aliasedUnnest =
          new SqlBasicCall(
              SqlStdOperatorTable.AS,
              List.of(
                  unnest, new SqlIdentifier(unnestAlias, POS), new SqlIdentifier(aliasField, POS)),
              POS);
      // CROSS JOIN (comma) implicit-LATERAL: SqlJoin with COMMA join type.
      org.apache.calcite.sql.SqlJoin join =
          new org.apache.calcite.sql.SqlJoin(
              POS,
              aliasedInner,
              SqlLiteral.createBoolean(false, POS),
              org.apache.calcite.sql.JoinType.COMMA.symbol(POS),
              aliasedUnnest,
              org.apache.calcite.sql.JoinConditionType.NONE.symbol(POS),
              null);
      // Outer projection: partitionBy keys + 4 ITEM-access columns.
      List<SqlNode> outerProjects = new ArrayList<>();
      for (String pname : partitionNames) {
        outerProjects.add(new SqlIdentifier(java.util.Arrays.asList(innerAlias, pname), POS));
      }
      SqlNode unnestedRef =
          new SqlIdentifier(java.util.Arrays.asList(unnestAlias, aliasField), POS);
      outerProjects.add(asAlias(safeCastItem(unnestedRef, "pattern", "VARCHAR"), aliasField));
      outerProjects.add(
          asAlias(safeCastItem(unnestedRef, "pattern_count", "BIGINT"), "pattern_count"));
      if (showNumberedTrue) {
        outerProjects.add(asAlias(safeCastTokens(unnestedRef), "tokens"));
      }
      outerProjects.add(asAlias(safeCastSampleLogs(unnestedRef), "sample_logs"));
      // Reset and install the new shape: from=join, projection=outerProjects.
      state.reset();
      state.setFrom(join);
      state.setProjection(outerProjects);
    }

    private SqlNode safeCastItem(SqlNode mapRef, String key, String typeName) {
      SqlNode item =
          new SqlBasicCall(
              SqlStdOperatorTable.ITEM,
              List.of(mapRef, SqlLiteral.createCharString(key, POS)),
              POS);
      return new SqlBasicCall(
          org.apache.calcite.sql.fun.SqlLibraryOperators.SAFE_CAST,
          List.of(
              item,
              new org.apache.calcite.sql.SqlDataTypeSpec(
                  new org.apache.calcite.sql.SqlBasicTypeNameSpec(
                      org.apache.calcite.sql.type.SqlTypeName.valueOf(typeName), POS),
                  POS)),
          POS);
    }

    /** SAFE_CAST(map['tokens'] AS MAP&lt;VARCHAR, VARCHAR ARRAY&gt;). */
    private SqlNode safeCastTokens(SqlNode mapRef) {
      SqlNode item =
          new SqlBasicCall(
              SqlStdOperatorTable.ITEM,
              List.of(mapRef, SqlLiteral.createCharString("tokens", POS)),
              POS);
      // tokens is MAP<VARCHAR, ARRAY<VARCHAR>> — preserve via ITEM access; let the validator
      // infer/coerce instead of forcing a SqlDataTypeSpec (Calcite's parser-side MAP/ARRAY
      // type spec construction is complicated to build by hand).
      return item;
    }

    /** SAFE_CAST(map['sample_logs'] AS VARCHAR ARRAY). */
    private SqlNode safeCastSampleLogs(SqlNode mapRef) {
      SqlNode item =
          new SqlBasicCall(
              SqlStdOperatorTable.ITEM,
              List.of(mapRef, SqlLiteral.createCharString("sample_logs", POS)),
              POS);
      // ARRAY<VARCHAR> — let the validator infer.
      return item;
    }

    @Override
    public Void visitAppendCol(org.opensearch.sql.ast.tree.AppendCol node, Void ignored) {
      walkChild(node);
      // appendcol [<sub>]: left join the main pipeline (with ROW_NUMBER()) against the subsearch
      // (also with ROW_NUMBER()) on the synthetic row number, drop the helper columns. The
      // subsearch evaluates against the same source as the main pipeline.
      // Step 1: materialize the in-flight main pipeline as a subquery aliased "_main_", with a
      // helper column _rn_main_.
      SqlNode mainBody = state.toFinalSqlNode();
      // Wrap mainBody in a SELECT *, ROW_NUMBER() OVER () AS _rn_main_ FROM (mainBody).
      SqlNode mainWithRn = withRowNumber(mainBody, "_rn_main_");
      // Step 2: build the subsearch. Attach the main pipeline's child source so the subsearch
      // operates on the same dataset.
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
      subTail.attach((UnresolvedPlan) node.getChild().get(0));
      SqlNode subBody = new PplToSqlNode(rowTypeOracle).visit(subqueryPlan);
      SqlNode subWithRn = withRowNumber(subBody, "_rn_sub_");
      // Step 3: LEFT JOIN ON _rn_main_ = _rn_sub_.
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
              org.apache.calcite.sql.JoinType.LEFT.symbol(POS),
              aliasedSub,
              org.apache.calcite.sql.JoinConditionType.ON.symbol(POS),
              joinCond);
      state.reset();
      state.setFrom(join);
      // Mirror v2 visitAppendCol projection semantics. For override=false: drop sub's duplicate
      // columns and surface main's. For override=true: for shared cols, emit
      // CASE WHEN _rn_main_=_rn_sub_ THEN sub.col ELSE main.col END. Drop _rn_main_/_rn_sub_.
      if (rowTypeOracle != null) {
        try {
          java.util.List<String> mainCols = deriveColumnNames(mainBody);
          java.util.List<String> subCols = deriveColumnNames(subBody);
          java.util.List<SqlNode> projection = new java.util.ArrayList<>();
          if (node.isOverride()) {
            java.util.Set<String> mainSet = new java.util.HashSet<>(mainCols);
            java.util.Set<String> subSet = new java.util.HashSet<>(subCols);
            SqlNode caseCond =
                new SqlBasicCall(
                    SqlStdOperatorTable.EQUALS,
                    List.of(
                        new SqlIdentifier(java.util.Arrays.asList("_main_", "_rn_main_"), POS),
                        new SqlIdentifier(java.util.Arrays.asList("_sub_", "_rn_sub_"), POS)),
                    POS);
            for (String c : mainCols) {
              if (c.equals("_rn_main_")) continue;
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
                projection.add(asAlias(caseExpr, c));
              } else {
                projection.add(new SqlIdentifier(java.util.Arrays.asList("_main_", c), POS));
              }
            }
            for (String c : subCols) {
              if (c.equals("_rn_sub_")) continue;
              if (!mainSet.contains(c)) {
                projection.add(new SqlIdentifier(java.util.Arrays.asList("_sub_", c), POS));
              }
            }
          } else {
            java.util.Set<String> mainSet = new java.util.HashSet<>(mainCols);
            for (String c : mainCols) {
              if (!c.equals("_rn_main_")) {
                projection.add(new SqlIdentifier(java.util.Arrays.asList("_main_", c), POS));
              }
            }
            for (String c : subCols) {
              if (!mainSet.contains(c) && !c.equals("_rn_sub_")) {
                projection.add(new SqlIdentifier(java.util.Arrays.asList("_sub_", c), POS));
              }
            }
          }
          // Include _rn_main_ so an outer ORDER BY can reference it; the row-number column is
          // stripped from the user-facing output by SqlNodePlanner.stripSyntheticSeqColumns.
          // Without this ORDER BY, LEFT JOIN doesn't preserve main's row order at the SQL level
          // and a downstream `head N` can return the wrong rows.
          projection.add(
              asAlias(
                  new SqlIdentifier(java.util.Arrays.asList("_main_", "_rn_main_"), POS),
                  "_rn_main_"));
          state.setProjection(projection);
          // Defer ordering to the outermost level so it doesn't conflict with any inner pipe's
          // ORDER BY. The outer-fetch/limit picks N rows after this sort; the projection's
          // _rn_main_ column resolves at the outer level since it's now in scope.
          state.setOuterOrderBy(List.of(new SqlIdentifier("_rn_main_", POS)));
        } catch (RuntimeException ignoredAcCols) {
          // probe failed; downstream may still hit ambiguity, but main pipeline didn't break.
        }
      }
      return null;
    }

    /**
     * Wrap a query expression in `SELECT *, ROW_NUMBER() OVER (ORDER BY ...) AS <alias> FROM (q)`.
     *
     * <p>If the inner expression is a SqlOrderBy or SqlSelect with an ORDER BY clause, propagate
     * those sort columns into the OVER's ORDER BY. Without this, ROW_NUMBER OVER () assigns
     * arbitrary numbers (in pushdown order from the storage engine), breaking positional alignment
     * when downstream pipes (appendcol) join two row-numbered streams expecting them to be sorted
     * identically.
     */
    private SqlNode withRowNumber(SqlNode body, String alias) {
      SqlNodeList orderByList = extractTopOrderBy(body);
      SqlNode rnWindow =
          org.apache.calcite.sql.SqlWindow.create(
              null,
              null,
              new SqlNodeList(POS),
              orderByList != null ? orderByList : new SqlNodeList(POS),
              SqlLiteral.createBoolean(false, POS),
              null,
              null,
              null,
              POS);
      SqlNode rnOver =
          new SqlBasicCall(
              SqlStdOperatorTable.OVER,
              List.of(new SqlBasicCall(SqlStdOperatorTable.ROW_NUMBER, List.of(), POS), rnWindow),
              POS);
      SqlNodeList selectList = new SqlNodeList(POS);
      selectList.add(SqlIdentifier.star(POS));
      selectList.add(asAlias(rnOver, alias));
      return new SqlSelect(
          POS,
          /* keywordList */ null,
          selectList,
          body,
          /* where */ null,
          /* group */ null,
          /* having */ null,
          /* windowList */ null,
          /* qualify */ null,
          /* orderBy */ null,
          /* offset */ null,
          /* fetch */ null,
          /* hints */ null);
    }

    /**
     * Extract the top-level ORDER BY clause from a query expression. Returns null if the expression
     * has no ORDER BY (the caller will fall back to OVER () with empty ORDER BY).
     */
    private SqlNodeList extractTopOrderBy(SqlNode body) {
      if (body instanceof org.apache.calcite.sql.SqlOrderBy ob) {
        return ob.orderList;
      }
      if (body instanceof SqlSelect sel && sel.getOrderList() != null) {
        return sel.getOrderList();
      }
      return null;
    }

    @Override
    public Void visitGraphLookup(org.opensearch.sql.ast.tree.GraphLookup node, Void ignored) {
      // GraphLookup is implemented as a polymorphic table function (PTF). Emission shape:
      //
      //   FROM TABLE(GRAPH_LOOKUP(
      //     TABLE (<source pipeline with FETCH 100>),  -- previous pipes
      //     TABLE (<lookup table [with WHERE filter]>),
      //     'startField' | NULL,        -- string, NULL when literal-start mode
      //     'fromField',                -- string
      //     'toField',                  -- string
      //     'outputField',              -- string
      //     'depthField' | NULL,        -- string, optional
      //     maxDepth, bidirectional, supportArray, batchMode, usePIT))
      //
      // A planner rule rewrites LogicalTableFunctionScan(GRAPH_LOOKUP(...)) → LogicalGraphLookup
      // so the existing OpenSearch storage machinery picks it up.
      SqlNode sourceSqlNode;
      if (node.getStartValues() != null) {
        // Literal-start mode: SELECT 0 AS _dummy_ — one-row dummy source. VALUES (0) by itself
        // doesn't validate cleanly through the rowTypeOracle probe; a proper SELECT does.
        org.apache.calcite.sql.SqlNodeList sel = new SqlNodeList(POS);
        sel.add(
            org.apache.calcite.sql.fun.SqlStdOperatorTable.AS.createCall(
                POS, SqlLiteral.createExactNumeric("0", POS), new SqlIdentifier("_dummy_", POS)));
        sourceSqlNode =
            new SqlSelect(
                POS,
                null,
                sel,
                /* from */ null,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null);
      } else {
        if (node.getChild() == null || node.getChild().isEmpty()) {
          throw new IllegalArgumentException(
              "graphLookup field-reference start requires a piped source. Use literal start values"
                  + " (e.g. start='value') for top-level graphLookup.");
        }
        walkChild(node);
        // SELECT <non-meta cols> FROM <innerFrom> FETCH 100 — explicit non-meta columns mirror
        // v2's tryToRemoveMetaFields and prevent _id/_index/_score from showing in graphLookup
        // output.
        SqlNode innerFrom = state.from;
        sourceSqlNode =
            new SqlSelect(
                POS,
                /* keywordList */ null,
                buildNonMetaSelectList(innerFrom),
                /* from */ innerFrom,
                /* where */ state.where,
                /* groupBy */ null,
                /* having */ null,
                /* windowDecls */ null,
                /* qualify */ null,
                /* orderBy */ null,
                /* offset */ null,
                /* fetch */ intLiteral(100),
                /* hints */ null);
        // Save user-set outer fetch/order so they apply to the post-graphLookup pipeline (not the
        // pre-graphLookup source which is captured in sourceSqlNode).
        SqlNode savedOuterFetch = state.outerFetch;
        SqlNode savedOuterOffset = state.outerOffset;
        java.util.List<SqlNode> savedOuterOrderBy = state.outerOrderBy;
        state.reset();
        state.outerFetch = savedOuterFetch;
        state.outerOffset = savedOuterOffset;
        state.outerOrderBy = savedOuterOrderBy;
      }

      // Lookup side: SqlIdentifier for the table reference, optionally wrapped with WHERE filter.
      // Always wrap in `SELECT * FROM <id>` because SET_SEMANTICS_TABLE expects a query, not a
      // bare table identifier.
      org.opensearch.sql.ast.tree.UnresolvedPlan fromTablePlan = node.getFromTable();
      SqlNode lookupBase;
      if (fromTablePlan instanceof org.opensearch.sql.ast.tree.Relation rel) {
        lookupBase = qualifiedNameToIdentifier(rel.getTableQualifiedName());
      } else {
        PplToSqlNode lookupVisitor = new PplToSqlNode(rowTypeOracle);
        lookupBase = lookupVisitor.visit(fromTablePlan);
      }
      SqlNode lookupSqlNode;
      {
        SqlNode lookupWhere = node.getFilter() != null ? expr(node.getFilter()) : null;
        lookupSqlNode =
            new SqlSelect(
                POS,
                null,
                buildNonMetaSelectList(lookupBase),
                lookupBase,
                lookupWhere,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null);
      }

      // Build the GRAPH_LOOKUP function call operands.
      String fromFieldName = node.getFromField().getField().toString();
      String toFieldName = node.getToField().getField().toString();
      String outputFieldName = node.getAs().getField().toString();
      String depthFieldName = node.getDepthFieldName();
      Object maxDepthLitValue = node.getMaxDepth().getValue();
      int maxDepthValue =
          maxDepthLitValue instanceof Number n
              ? n.intValue()
              : Integer.parseInt(maxDepthLitValue.toString());
      boolean bidirectional =
          node.getDirection() == org.opensearch.sql.ast.tree.GraphLookup.Direction.BI;

      String startFieldName =
          node.getStartField() != null ? node.getStartField().getField().toString() : null;

      java.util.List<SqlNode> ptfArgs = new java.util.ArrayList<>();
      ptfArgs.add(setSemanticsTable(sourceSqlNode));
      ptfArgs.add(setSemanticsTable(lookupSqlNode));
      ptfArgs.add(stringOrNull(startFieldName));
      ptfArgs.add(SqlLiteral.createCharString(fromFieldName, POS));
      ptfArgs.add(SqlLiteral.createCharString(toFieldName, POS));
      ptfArgs.add(SqlLiteral.createCharString(outputFieldName, POS));
      ptfArgs.add(stringOrNull(depthFieldName));
      ptfArgs.add(SqlLiteral.createExactNumeric(String.valueOf(maxDepthValue), POS));
      ptfArgs.add(SqlLiteral.createBoolean(bidirectional, POS));
      ptfArgs.add(SqlLiteral.createBoolean(node.isSupportArray(), POS));
      ptfArgs.add(SqlLiteral.createBoolean(node.isBatchMode(), POS));
      ptfArgs.add(SqlLiteral.createBoolean(node.isUsePIT(), POS));
      // Trailing literal start values (only in literal-start mode) — for now stored separately;
      // the planner rule will pick them up via a side-channel. TODO: thread through the call.
      if (node.getStartValues() != null) {
        for (org.opensearch.sql.ast.expression.Literal lit : node.getStartValues()) {
          ptfArgs.add(literal(lit));
        }
      }

      SqlNode graphLookupCall = new SqlBasicCall(GraphLookupTableFunction.INSTANCE, ptfArgs, POS);
      // Wrap with COLLECTION_TABLE so it parses as `TABLE(fn(...))` in FROM.
      SqlNode tableExpr =
          new SqlBasicCall(
              org.apache.calcite.sql.fun.SqlStdOperatorTable.COLLECTION_TABLE,
              List.of(graphLookupCall),
              POS);
      // Install as the new FROM. Downstream pipes operate on graphLookup's output.
      state.setFrom(tableExpr);
      return null;
    }

    /** Emit a string SqlNode or NULL literal when the value is null. */
    private SqlNode stringOrNull(String s) {
      return s == null ? SqlLiteral.createNull(POS) : SqlLiteral.createCharString(s, POS);
    }

    /**
     * Build a select list containing every non-meta column of {@code from}. Falls back to SELECT *
     * when the row type can't be probed.
     */
    private SqlNodeList buildNonMetaSelectList(SqlNode from) {
      SqlNodeList list = new SqlNodeList(POS);
      if (rowTypeOracle != null) {
        try {
          java.util.List<String> cols = deriveColumnNames(from);
          for (String c : cols) {
            if (!org.opensearch.sql.calcite.plan.OpenSearchConstants.METADATAFIELD_TYPE_MAP
                .containsKey(c)) {
              list.add(new SqlIdentifier(c, POS));
            }
          }
          if (!list.isEmpty()) return list;
        } catch (RuntimeException ignored) {
          // probe failed — fall through to *
        }
      }
      list.add(SqlIdentifier.star(POS));
      return list;
    }

    /**
     * Wrap a relation-valued SqlNode with SET_SEMANTICS_TABLE so the validator/converter recognize
     * it as a table argument to a PTF (Polymorphic Table Function).
     */
    private SqlNode setSemanticsTable(SqlNode tableExpr) {
      return new SqlBasicCall(
          org.apache.calcite.sql.fun.SqlStdOperatorTable.SET_SEMANTICS_TABLE,
          List.of(tableExpr, new SqlNodeList(POS), new SqlNodeList(POS)),
          POS);
    }

    @Override
    public Void visitChart(org.opensearch.sql.ast.tree.Chart node, Void ignored) {
      walkChild(node);
      if (node.getAggregationFunction() == null) {
        throw new UnsupportedOperationException("chart requires an aggregation function");
      }
      if (state.where != null
          || state.evalExtended
          || state.projectionReplaced
          || state.orderBy != null
          || state.fetch != null) {
        state.wrap();
      }
      // 1D case: `chart agg by X` or `chart agg over X` — equivalent to a GROUP BY X.
      if (node.getRowSplit() == null || node.getColumnSplit() == null) {
        UnresolvedExpression splitKey =
            node.getRowSplit() != null ? node.getRowSplit() : node.getColumnSplit();
        if (splitKey == null) {
          throw new UnsupportedOperationException("chart requires a split key (`by` or `over`)");
        }
        List<SqlNode> selects = new ArrayList<>();
        List<SqlNode> groupKeys = new ArrayList<>();
        UnresolvedExpression gk = splitKey;
        String gkAlias = null;
        if (gk instanceof Alias a) {
          gkAlias = a.getName();
          gk = a.getDelegated();
        }
        SqlNode key = expr(gk);
        groupKeys.add(key);
        selects.add(gkAlias != null ? asAlias(key, gkAlias) : key);
        UnresolvedExpression aggExpr = node.getAggregationFunction();
        if (aggExpr instanceof Alias al) {
          selects.add(asAlias(aggCall(al.getDelegated()), al.getName()));
        } else {
          selects.add(aggCall(aggExpr));
        }
        // PPL chart drops rows whose split key is NULL (mirrors v2 nonNullGroupMask).
        state.addWhere(new SqlBasicCall(SqlStdOperatorTable.IS_NOT_NULL, List.of(expr(gk)), POS));
        state.setProjection(selects);
        state.setGroupBy(groupKeys);
        // v2 sorts the chart output ASC by the split key with NULLS LAST. Emit an explicit
        // ORDER BY so a downstream `reverse` has a sort key to flip (otherwise reverse becomes
        // a no-op for chart output). Use the alias when set so the sort key references the
        // projected column rather than re-evaluating the span/group expression.
        SqlNode orderRef = gkAlias != null ? new SqlIdentifier(gkAlias, POS) : key;
        SqlNode ordered = new SqlBasicCall(SqlStdOperatorTable.NULLS_LAST, List.of(orderRef), POS);
        state.setOrderBy(List.of(ordered));
        return null;
      }
      // 2D case: `chart agg over X by Y` — pivot Y as columns within X groups, with TOP-N
      // filtering of Y values by aggregate sum.
      // Strategy:
      //   1. Inner GROUP BY (X, Y) producing the aggregate.
      //   2. Compute per-Y total and rank, label rank > limit as 'OTHER' (or NULL as 'NULL').
      //   3. Re-aggregate over (X, labeled_Y).
      // This emits a single pipeline that produces (X, Y_label, agg) tuples; downstream
      // consumers should treat it as a long-format pivot rather than wide.
      org.opensearch.sql.ast.expression.Argument.ArgumentMap argMap =
          org.opensearch.sql.ast.expression.Argument.ArgumentMap.of(node.getArguments());
      int limit = argMap.get("limit") != null ? (Integer) argMap.get("limit").getValue() : 10;
      boolean useNull = argMap.get("usenull") == null || (Boolean) argMap.get("usenull").getValue();
      boolean useOther =
          argMap.get("useother") == null || (Boolean) argMap.get("useother").getValue();
      // `top` arg defaults to true (top-N). Bottom-N when explicitly bottom-K syntax is used.
      boolean top = argMap.get("top") == null || (Boolean) argMap.get("top").getValue();
      String otherLabel =
          argMap.get("otherstr") != null ? argMap.get("otherstr").getValue().toString() : "OTHER";
      String nullLabel =
          argMap.get("nullstr") != null ? argMap.get("nullstr").getValue().toString() : "NULL";
      // PPL grammar: `chart agg OVER rowSplit BY columnSplit`. The columnSplit is the value
      // pivoted into column headers — that's what gets cast to VARCHAR (only string-typed columns
      // can be reasonably labeled with OTHER/NULL substitutes). v2 lays the result out as
      // (rowSplit, columnSplit, metric).
      UnresolvedExpression rowKey = node.getRowSplit();
      UnresolvedExpression colKey = node.getColumnSplit();
      String aggAlias = "agg";
      UnresolvedExpression aggExpr = node.getAggregationFunction();
      if (aggExpr instanceof Alias al) {
        aggAlias = al.getName();
      }
      SqlNode innerRowKey = expr(stripAlias(rowKey));
      SqlNode innerColKey =
          new SqlBasicCall(
              SqlStdOperatorTable.CAST,
              List.of(
                  expr(stripAlias(colKey)),
                  new org.apache.calcite.sql.SqlDataTypeSpec(
                      new org.apache.calcite.sql.SqlBasicTypeNameSpec(
                          org.apache.calcite.sql.type.SqlTypeName.VARCHAR, POS),
                      POS)),
              POS);
      List<SqlNode> innerSelects = new ArrayList<>();
      innerSelects.add(asAlias(innerRowKey, overKeyName(rowKey)));
      innerSelects.add(asAlias(innerColKey, byKeyName(colKey)));
      innerSelects.add(
          asAlias(aggCall(aggExpr instanceof Alias al ? al.getDelegated() : aggExpr), aggAlias));
      // Match v2 visitAggregation(includeAggFieldsInNullFilter=true): drop input rows where the
      // aggregate's argument field is NULL before the inner GROUP BY. This prevents an all-null
      // input bucket from producing an empty (NULL_metric) row that survives downstream relabeling.
      // Skip for count(*)/count() which use AllFields (no specific field to null-check) and for
      // literal arguments.
      {
        UnresolvedExpression _inner = aggExpr instanceof Alias _al ? _al.getDelegated() : aggExpr;
        if (_inner instanceof org.opensearch.sql.ast.expression.AggregateFunction _af
            && _af.getField() != null
            && !(_af.getField() instanceof Literal)
            && !(_af.getField() instanceof org.opensearch.sql.ast.expression.AllFields)) {
          state.addWhere(
              new SqlBasicCall(
                  SqlStdOperatorTable.IS_NOT_NULL, List.of(expr(_af.getField())), POS));
        }
      }
      state.setProjection(innerSelects);
      state.setGroupBy(List.of(expr(stripAlias(rowKey)), expr(stripAlias(colKey))));
      state.wrap();
      String byName = byKeyName(colKey);
      String overName = overKeyName(rowKey);
      String aggFnName;
      {
        UnresolvedExpression _inner = aggExpr instanceof Alias _al ? _al.getDelegated() : aggExpr;
        if (_inner instanceof org.opensearch.sql.ast.expression.AggregateFunction _af) {
          aggFnName = _af.getFuncName().toLowerCase(java.util.Locale.ROOT);
        } else if (_inner instanceof Function _fn) {
          aggFnName = _fn.getFuncName().toLowerCase(java.util.Locale.ROOT);
        } else {
          aggFnName = "sum";
        }
      }
      // Outer re-aggregation: match v2 buildAggCall's mapping (MIN/EARLIEST→MIN, MAX/LATEST→MAX,
      // AVG→AVG, others→SUM).
      SqlOperator outerAgg =
          switch (aggFnName) {
            case "min", "earliest" -> SqlStdOperatorTable.MIN;
            case "max", "latest" -> SqlStdOperatorTable.MAX;
            case "avg" -> SqlStdOperatorTable.AVG;
            default -> SqlStdOperatorTable.SUM;
          };
      // PPL semantics: row-split NULL rows are always ignored. col-split NULLs survive when
      // useNull=true (relabeled by outer CASE) and dropped when useNull=false.
      state.addWhere(
          new SqlBasicCall(
              SqlStdOperatorTable.IS_NOT_NULL, List.of(new SqlIdentifier(overName, POS)), POS));
      if (!useNull) {
        state.addWhere(
            new SqlBasicCall(
                SqlStdOperatorTable.IS_NOT_NULL, List.of(new SqlIdentifier(byName, POS)), POS));
      }
      // Step 2: extend projection with per-Y total (window SUM partitioned by Y). This lets us
      // rank Y values without a self-join. The total survives downstream because every row of a
      // given Y carries the same value, and DENSE_RANK over (total, Y) yields a per-Y rank.
      SqlNode yTotalWindow =
          org.apache.calcite.sql.SqlWindow.create(
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
      List<SqlNode> step2Selects = new ArrayList<>();
      step2Selects.add(new SqlIdentifier(overName, POS));
      step2Selects.add(new SqlIdentifier(byName, POS));
      step2Selects.add(new SqlIdentifier(aggAlias, POS));
      step2Selects.add(asAlias(yTotalOver, "__chart_y_total__"));
      state.setProjection(step2Selects);
      state.wrap();
      // Step 3: assign DENSE_RANK over (__chart_y_total__ DESC|ASC NULLS LAST, byName NULLS LAST).
      // The secondary Y key breaks ties so each distinct Y receives a unique rank — required for a
      // correct top-N (without it, DENSE_RANK would lump tied Y values into one rank and
      // `rn <= limit` would let through more than `limit` Y values).
      SqlNode yTotalRef = new SqlIdentifier("__chart_y_total__", POS);
      SqlNode rankPrimary =
          top ? new SqlBasicCall(SqlStdOperatorTable.DESC, List.of(yTotalRef), POS) : yTotalRef;
      rankPrimary = new SqlBasicCall(SqlStdOperatorTable.NULLS_LAST, List.of(rankPrimary), POS);
      SqlNode rankSecondary =
          new SqlBasicCall(
              SqlStdOperatorTable.NULLS_LAST, List.of(new SqlIdentifier(byName, POS)), POS);
      SqlNodeList rankOrder = new SqlNodeList(List.of(rankPrimary, rankSecondary), POS);
      SqlNode rankWindow =
          org.apache.calcite.sql.SqlWindow.create(
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
              List.of(new SqlBasicCall(SqlStdOperatorTable.DENSE_RANK, List.of(), POS), rankWindow),
              POS);
      List<SqlNode> step3Selects = new ArrayList<>();
      step3Selects.add(new SqlIdentifier(overName, POS));
      step3Selects.add(new SqlIdentifier(byName, POS));
      step3Selects.add(new SqlIdentifier(aggAlias, POS));
      step3Selects.add(asAlias(rankOver, "__chart_rn__"));
      state.setProjection(step3Selects);
      state.wrap();
      // Step 4 (optional): drop non-top rows when useOther=false. NULL Y values produce
      // rank-from-NULL-total which is NULLS LAST → outside top-N, so they're filtered too —
      // matching v2 behavior (filter is applied uniformly when useOther=false).
      if (!useOther && limit > 0) {
        state.addWhere(
            new SqlBasicCall(
                SqlStdOperatorTable.LESS_THAN_OR_EQUAL,
                List.of(new SqlIdentifier("__chart_rn__", POS), intLiteral(limit)),
                POS));
      }
      // Step 5: relabel non-top Y as OTHER and NULL Y per useNull/useOther. Outer aggregate
      // re-aggregates the inner (X, Y, agg) tuples grouped by (X, label).
      SqlNode yRef = new SqlIdentifier(byName, POS);
      SqlNode rnRef = new SqlIdentifier("__chart_rn__", POS);
      SqlNodeList caseWhens = new SqlNodeList(POS);
      SqlNodeList caseThens = new SqlNodeList(POS);
      // WHEN Y IS NULL THEN <nullLabel|NULL>
      caseWhens.add(new SqlBasicCall(SqlStdOperatorTable.IS_NULL, List.of(yRef), POS));
      caseThens.add(
          useNull ? SqlLiteral.createCharString(nullLabel, POS) : SqlLiteral.createNull(POS));
      // WHEN __chart_rn__ <= limit THEN Y (only when useOther=true; with useOther=false the
      // <= limit filter has already removed non-top rows).
      if (useOther && limit > 0) {
        caseWhens.add(
            new SqlBasicCall(
                SqlStdOperatorTable.LESS_THAN_OR_EQUAL, List.of(rnRef, intLiteral(limit)), POS));
        caseThens.add(yRef);
      }
      // ELSE — when useOther=true, label as otherLabel; otherwise reuse Y (any non-NULL surviving
      // row was top-N, so just preserve it).
      SqlNode caseElse =
          useOther && limit > 0 ? SqlLiteral.createCharString(otherLabel, POS) : yRef;
      // Materialize the CASE-labeled Y into its own projection step before aggregation so the
      // outer GROUP BY references a plain column. Without this, expressing the CASE as both a
      // SELECT-list alias and a GROUP BY expression confuses Calcite's convertAgg path
      // (RexInputRef out-of-range during projection construction).
      SqlNode labeledY =
          new org.apache.calcite.sql.fun.SqlCase(POS, null, caseWhens, caseThens, caseElse);
      // Step 5a: project (overName, label AS byName, agg) — pure projection (no aggregate).
      List<SqlNode> labelProjection = new ArrayList<>();
      labelProjection.add(new SqlIdentifier(overName, POS));
      labelProjection.add(asAlias(labeledY, byName));
      labelProjection.add(new SqlIdentifier(aggAlias, POS));
      state.setProjection(labelProjection);
      state.wrap();
      // Step 5b: aggregate by (overName, byName) where byName is now the CASE-labeled column.
      List<SqlNode> outerSelects = new ArrayList<>();
      outerSelects.add(new SqlIdentifier(overName, POS));
      outerSelects.add(new SqlIdentifier(byName, POS));
      outerSelects.add(
          asAlias(
              new SqlBasicCall(outerAgg, List.of(new SqlIdentifier(aggAlias, POS)), POS),
              aggAlias));
      state.setProjection(outerSelects);
      state.setGroupBy(List.of(new SqlIdentifier(overName, POS), new SqlIdentifier(byName, POS)));
      // v2 sorts chart output by row split asc, col split asc with NULLS LAST.
      SqlNode orderRow =
          new SqlBasicCall(
              SqlStdOperatorTable.NULLS_LAST, List.of(new SqlIdentifier(overName, POS)), POS);
      SqlNode orderCol =
          new SqlBasicCall(
              SqlStdOperatorTable.NULLS_LAST, List.of(new SqlIdentifier(byName, POS)), POS);
      state.setOrderBy(List.of(orderRow, orderCol));
      return null;
    }

    /**
     * True if the given expression is a time-unit Span. PPL `stats ... by span(time, 1day)` always
     * filters NULL bucket regardless of bucket_nullable to match v2 visitAggregation behavior.
     */
    private static boolean isTimeSpan(UnresolvedExpression e) {
      UnresolvedExpression core = e instanceof Alias al ? al.getDelegated() : e;
      return core instanceof org.opensearch.sql.ast.expression.Span sp
          && org.opensearch.sql.ast.expression.SpanUnit.isTimeUnit(sp.getUnit());
    }

    private static UnresolvedExpression stripAlias(UnresolvedExpression e) {
      return e instanceof Alias a ? a.getDelegated() : e;
    }

    private static String overKeyName(UnresolvedExpression e) {
      if (e instanceof Alias a) return a.getName();
      if (e instanceof Field f && f.getField() instanceof QualifiedName qn) return qn.toString();
      if (e instanceof QualifiedName qn) return qn.toString();
      return "_over_";
    }

    private static String byKeyName(UnresolvedExpression e) {
      if (e instanceof Alias a) return a.getName();
      if (e instanceof Field f && f.getField() instanceof QualifiedName qn) return qn.toString();
      if (e instanceof QualifiedName qn) return qn.toString();
      return "_by_";
    }

    @Override
    public Void visitValues(org.opensearch.sql.ast.tree.Values node, Void ignored) {
      // PPL `append [ ]` parser emits an EmptySource Values node (zero rows). Calcite VALUES()
      // with no rows fails validation; emit a placeholder `SELECT 1 WHERE FALSE` instead so the
      // outer UNION ALL adds nothing to the main pipeline output.
      if (node.getValues() == null || node.getValues().isEmpty()) {
        SqlNodeList sel = new SqlNodeList(POS);
        sel.add(asAlias(intLiteral(1), "_dummy_"));
        SqlSelect emptySelect =
            new SqlSelect(
                POS,
                /* keywordList */ null,
                sel,
                /* from */ null,
                /* where */ SqlLiteral.createBoolean(false, POS),
                /* group */ null,
                /* having */ null,
                /* windowList */ null,
                /* qualify */ null,
                /* orderBy */ null,
                /* offset */ null,
                /* fetch */ null,
                /* hints */ null);
        state.setFrom(emptySelect);
        return null;
      }
      // VALUES (<row1>), (<row2>), ... — used as a literal-only relation by other commands.
      List<List<SqlNode>> rows = new ArrayList<>();
      for (List<Literal> row : node.getValues()) {
        List<SqlNode> rowNodes = new ArrayList<>();
        for (Literal lit : row) {
          rowNodes.add(literal(lit));
        }
        rows.add(rowNodes);
      }
      List<SqlNode> rowCalls = new ArrayList<>();
      for (List<SqlNode> r : rows) {
        rowCalls.add(new SqlBasicCall(SqlStdOperatorTable.ROW, r, POS));
      }
      SqlNode values = new SqlBasicCall(SqlStdOperatorTable.VALUES, rowCalls, POS);
      state.setFrom(values);
      return null;
    }

    /**
     * Translate `rex mode=sed` (s/pattern/replacement/[flags] or y/from/to/) into a column
     * replacement using REGEXP_REPLACE. The output overrides the input field at its original
     * ordinal — the Pipeline state achieves this by emitting an aliased SELECT-list extension and
     * letting the validator collapse the alias-same-as-column.
     */
    private Void visitRexSed(org.opensearch.sql.ast.tree.Rex node) {
      String fieldName;
      UnresolvedExpression fieldExpr = node.getField();
      if (fieldExpr instanceof Field f && f.getField() instanceof QualifiedName qn) {
        fieldName = qn.toString();
      } else if (fieldExpr instanceof QualifiedName qn) {
        fieldName = qn.toString();
      } else {
        throw new IllegalArgumentException(
            "rex mode=sed requires a simple column reference, got: " + fieldExpr.getClass());
      }
      String sed = (String) node.getPattern().getValue();
      SqlNode source = expr(fieldExpr);
      SqlNode replaceCall;
      if (sed.startsWith("s/")) {
        replaceCall = sedSubstitution(source, sed);
      } else if (sed.startsWith("y/")) {
        replaceCall = sedTransliteration(source, sed);
      } else {
        throw new RuntimeException("Unsupported sed pattern: " + sed);
      }
      // Emit as a projection that replaces the input field in place. Use full-row enumeration
      // when an oracle is available to keep field ordering stable.
      if (state.evalExtended || state.projectionReplaced) {
        state.wrap();
      }
      if (rowTypeOracle != null && state.from != null) {
        List<String> cols =
            deriveColumnNames(state.from).stream()
                .filter(
                    c ->
                        !org.opensearch.sql.calcite.plan.OpenSearchConstants.METADATAFIELD_TYPE_MAP
                            .containsKey(c))
                .toList();
        List<SqlNode> selects = new ArrayList<>();
        for (String c : cols) {
          if (c.equals(fieldName)) {
            selects.add(asAlias(replaceCall, c));
          } else {
            selects.add(new SqlIdentifier(c, POS));
          }
        }
        state.setProjection(selects);
      } else {
        // No oracle — append the replacement as an alias at the end. This shifts the column to
        // the last position which is suboptimal but functionally correct.
        state.addEvalAlias(replaceCall, fieldName);
      }
      return null;
    }

    private SqlNode sedSubstitution(SqlNode field, String sed) {
      // s/pattern/replacement/[flags]
      if (!sed.matches("s/.+/.*/.*")) {
        throw new IllegalArgumentException("Invalid sed substitution format: " + sed);
      }
      int firstDelim = sed.indexOf('/', 2);
      int secondDelim = sed.indexOf('/', firstDelim + 1);
      if (firstDelim == -1 || secondDelim == -1) {
        throw new IllegalArgumentException("Invalid sed substitution format: " + sed);
      }
      String pattern = sed.substring(2, firstDelim);
      String replacement = sed.substring(firstDelim + 1, secondDelim);
      String flags = secondDelim + 1 < sed.length() ? sed.substring(secondDelim + 1) : "";
      // Convert sed-style backrefs (\1) to Java/SQL style ($1).
      String javaReplacement = replacement.replaceAll("\\\\(\\d+)", "\\$$1");
      // The 3-arg form is global-by-default with case-sensitive matching. For sed-style flags
      // we'd need REGEXP_REPLACE_4 (with flags arg) — Calcite has it via SqlLibraryOperators.
      // BIG_QUERY's REGEXP_REPLACE_3 binds 3 args; most flags can be embedded in the pattern
      // itself (e.g. (?i) for case-insensitive, ?m for multiline), so we encode 'i' that way.
      String effectivePattern = pattern;
      boolean caseInsensitive = flags.contains("i");
      if (caseInsensitive && !pattern.startsWith("(?i)")) {
        effectivePattern = "(?i)" + pattern;
      }
      // Default REGEXP_REPLACE in BIG_QUERY mode is global. The 'g' flag is the default; ignore.
      return new SqlBasicCall(
          org.apache.calcite.sql.fun.SqlLibraryOperators.REGEXP_REPLACE_3,
          List.of(
              field,
              SqlLiteral.createCharString(effectivePattern, POS),
              SqlLiteral.createCharString(javaReplacement, POS)),
          POS);
    }

    private SqlNode sedTransliteration(SqlNode field, String sed) {
      // y/from/to/ — character-by-character substitution. SQL has no direct equivalent; we emit a
      // chain of REPLACE() calls, one per character pair. This is exactly what v2 does.
      if (!sed.matches("y/.+/.*/.*")) {
        throw new IllegalArgumentException("Invalid sed transliteration format: " + sed);
      }
      int first = sed.indexOf('/', 1);
      int second = sed.indexOf('/', first + 1);
      String from = sed.substring(2, first);
      String to = sed.substring(first + 1, second);
      if (from.length() != to.length()) {
        throw new IllegalArgumentException(
            "sed transliteration source and target must have the same length");
      }
      SqlNode current = field;
      for (int i = 0; i < from.length(); i++) {
        current =
            new SqlBasicCall(
                SqlStdOperatorTable.REPLACE,
                List.of(
                    current,
                    SqlLiteral.createCharString(String.valueOf(from.charAt(i)), POS),
                    SqlLiteral.createCharString(String.valueOf(to.charAt(i)), POS)),
                POS);
      }
      return current;
    }
  }

  /**
   * Alias a join side only when needed. If the side is already a bare table identifier and no
   * explicit alias was provided, leave it untouched so column references like `EMP.DEPTNO` keep
   * resolving against the table name.
   */
  private static SqlNode maybeAlias(SqlNode side, String explicitAlias, String defaultAlias) {
    if (explicitAlias != null) {
      return new SqlBasicCall(
          SqlStdOperatorTable.AS, List.of(side, new SqlIdentifier(explicitAlias, POS)), POS);
    }
    // Unwrap a SELECT * FROM <table> wrapper if that's all the side is.
    SqlNode core = side;
    if (core instanceof SqlOrderBy ob && ob.offset == null && ob.fetch == null) {
      core = ob.query;
    }
    if (core instanceof SqlSelect sel
        && isSelectStar(sel)
        && sel.getWhere() == null
        && sel.getGroup() == null
        && sel.getFetch() == null
        && sel.getOffset() == null) {
      core = sel.getFrom();
    }
    if (core instanceof SqlIdentifier) {
      return core;
    }
    // A bare SqlJoin (multi-join chain) keeps previously-aliased tables in scope; wrapping it
    // with a default alias hides those names from subsequent ON clauses.
    if (core instanceof org.apache.calcite.sql.SqlJoin) {
      return core;
    }
    // If the side is already aliased via SqlBasicCall(AS, [..., alias]) — produced by
    // SubqueryAlias("alias", ...) — leave it alone. Wrapping a second time produces
    // `(<x> AS n1) AS __r2`, which double-aliases and confuses validator name resolution
    // for multi-instance same-table joins (Q7/Q8: `nation AS n1` ⋈ `nation AS n2`).
    if (core instanceof SqlBasicCall sbc && sbc.getOperator() == SqlStdOperatorTable.AS) {
      return core;
    }
    return new SqlBasicCall(
        SqlStdOperatorTable.AS, List.of(side, new SqlIdentifier(defaultAlias, POS)), POS);
  }

  private static boolean isSelectStar(SqlSelect sel) {
    SqlNodeList list = sel.getSelectList();
    return list != null
        && list.size() == 1
        && list.get(0) instanceof SqlIdentifier id
        && id.isStar();
  }

  /**
   * Reverse the direction of a sort key produced by visitSort (ASC <-> DESC, NULLS_F <-> NULLS_L).
   */
  private static SqlNode reverseSortKey(SqlNode key) {
    if (key instanceof SqlBasicCall outer
        && (outer.getOperator() == SqlStdOperatorTable.NULLS_FIRST
            || outer.getOperator() == SqlStdOperatorTable.NULLS_LAST)) {
      SqlOperator flippedNulls =
          outer.getOperator() == SqlStdOperatorTable.NULLS_FIRST
              ? SqlStdOperatorTable.NULLS_LAST
              : SqlStdOperatorTable.NULLS_FIRST;
      SqlNode inner = outer.operand(0);
      SqlNode flippedInner;
      if (inner instanceof SqlBasicCall innerCall
          && innerCall.getOperator() == SqlStdOperatorTable.DESC) {
        // DESC -> ASC (drop the DESC wrapper)
        flippedInner = innerCall.operand(0);
      } else {
        // ASC -> DESC (wrap in DESC)
        flippedInner = new SqlBasicCall(SqlStdOperatorTable.DESC, List.of(inner), POS);
      }
      return new SqlBasicCall(flippedNulls, List.of(flippedInner), POS);
    }
    return key;
  }

  /** Mutable in-flight SqlSelect being assembled. */
  private static final class Pipeline {
    SqlNode from;
    SqlNode where;

    /** {@code null} means "SELECT *" (un-modified projection). */
    List<SqlNode> projection;

    List<SqlNode> groupBy;
    List<SqlNode> orderBy;
    SqlNode fetch;
    SqlNode offset;

    /**
     * Set by {@link #visit(UnresolvedPlan)} when the plan tree contains any StreamWindow. Causes
     * visitRelation to wrap the source in a SELECT that adds a `__stream_seq__` ROW_NUMBER()
     * column, so multi-streamstats share a stable global ordering.
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
     * Most recent sort keys seen anywhere in the pipeline (outer or flushed inner). Survives wrap
     * so a downstream `reverse` can recover the order even after a fields-projection narrowed
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
     * SELECT-list aliases to be referenced in the same SELECT; PPL allows it across pipes (and
     * within the same eval's left-to-right list). We track these so a downstream Eval/Where that
     * references an earlier name knows to wrap into a subquery first.
     */
    final java.util.Set<String> evalAliasNames = new java.util.HashSet<>();

    /**
     * Track the table alias set by a `source = X as <name>` SubqueryAlias so Stats / Sort / Eval
     * that wrap state.from can re-attach the alias on the wrapping SELECT — letting downstream
     * agg/sort expressions like `<name>.<col>` keep resolving.
     */
    String subqueryAliasName;

    /**
     * Most-recent JOIN's left-side alias, if any. PPL semantics: a bare column reference like
     * `name` after a join binds to the LEFT side. Calcite's validator throws "Column 'name' is
     * ambiguous" instead. Set by visitJoin so visitProject can qualify bare references with the
     * left alias when the same column also exists on the right side.
     */
    String joinLeftAlias;

    /** Right-side alias for the same scope. */
    String joinRightAlias;

    void setFrom(SqlNode f) {
      from = f;
    }

    void addWhere(SqlNode cond) {
      where =
          where == null
              ? cond
              : new SqlBasicCall(SqlStdOperatorTable.AND, List.of(where, cond), POS);
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
      } else if (!(from instanceof SqlBasicCall sbc
          && sbc.getOperator() == SqlStdOperatorTable.AS)) {
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
     * Drop all in-flight pipeline state, including the FROM. Used by commands that build a
     * brand-new source (Union, Append, Join) where the existing pipeline has already been compiled
     * into a subquery and what remains is a different FROM expression.
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
     * Build the final SqlNode tree for the whole pipeline: take the in-flight select and wrap it in
     * a top-level {@link SqlOrderBy} carrying any pending outer sort/fetch from upstream
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
                POS, /* keywordList */
                null,
                selectList,
                from, /* where */
                null,
                /* group */ null, /* having */
                null, /* windowList */
                null,
                /* qualify */ null, /* orderBy */
                null, /* offset */
                null, /* fetch */
                null,
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
  }

  /** Mirror of {@code CalciteRelNodeVisitor.analyzeSortOption} kept lightweight here. */
  private static Sort.SortOption analyzeSortOption(List<Argument> args) {
    boolean desc = false;
    for (Argument a : args) {
      if ("asc".equalsIgnoreCase(a.getArgName())) {
        desc = !((Boolean) a.getValue().getValue());
      }
    }
    return desc ? Sort.SortOption.DEFAULT_DESC : Sort.SortOption.DEFAULT_ASC;
  }

  // -- Expression translation -------------------------------------------------

  private SqlNode expr(UnresolvedExpression e) {
    if (e instanceof Literal lit) return literal(lit);
    if (e instanceof QualifiedName qn) {
      SqlNode itemAccess = tryMapOrStructItemAccess(qn);
      return itemAccess != null ? itemAccess : qualifiedNameToFieldIdentifier(qn);
    }
    if (e instanceof Field f) return expr(f.getField());
    if (e instanceof Compare c) return cmp(c);
    if (e instanceof And a)
      return new SqlBasicCall(
          SqlStdOperatorTable.AND, List.of(expr(a.getLeft()), expr(a.getRight())), POS);
    if (e instanceof Or o)
      return new SqlBasicCall(
          SqlStdOperatorTable.OR, List.of(expr(o.getLeft()), expr(o.getRight())), POS);
    if (e instanceof org.opensearch.sql.ast.expression.Xor x) {
      // SQL has no XOR — express as (left OR right) AND NOT (left AND right).
      SqlNode l = expr(x.getLeft());
      SqlNode r = expr(x.getRight());
      SqlNode or = new SqlBasicCall(SqlStdOperatorTable.OR, List.of(l, r), POS);
      SqlNode and = new SqlBasicCall(SqlStdOperatorTable.AND, List.of(l, r), POS);
      SqlNode notAnd = new SqlBasicCall(SqlStdOperatorTable.NOT, List.of(and), POS);
      return new SqlBasicCall(SqlStdOperatorTable.AND, List.of(or, notAnd), POS);
    }
    if (e instanceof Function f) return func(f);
    if (e instanceof Case c) return caseExpr(c);
    if (e instanceof Not n) {
      // PPL `NOT (field = bool_literal)` semantically equals `field != bool_literal`. Translate
      // by negating the inner comparison so the boolean-simplification path emits IS NOT TRUE/
      // IS NOT FALSE rather than NOT(<field>) (mirrors v2's optimized emission shape).
      UnresolvedExpression inner = n.getExpression();
      if (inner instanceof Compare c) {
        String innerOp = c.getOperator();
        if ((extractBoolLiteral(c.getLeft()) != null || extractBoolLiteral(c.getRight()) != null)
            && (innerOp.equals("=") || innerOp.equals("!=") || innerOp.equals("<>"))) {
          String flipped = innerOp.equals("=") ? "!=" : "=";
          return cmp(new Compare(flipped, c.getLeft(), c.getRight()));
        }
      }
      return new SqlBasicCall(SqlStdOperatorTable.NOT, List.of(expr(inner)), POS);
    }
    if (e instanceof In in) return inExpr(in);
    if (e instanceof InSubquery is) return inSubqueryExpr(is);
    if (e instanceof org.opensearch.sql.ast.expression.Span sp) return spanExpr(sp);
    if (e instanceof org.opensearch.sql.ast.expression.Cast c) return castExpr(c);
    if (e instanceof org.opensearch.sql.ast.expression.Interval i) return intervalExpr(i);
    if (e instanceof org.opensearch.sql.ast.expression.Between b) return betweenExpr(b);
    if (e instanceof org.opensearch.sql.ast.expression.subquery.ScalarSubquery ss)
      return scalarSubqueryExpr(ss);
    if (e instanceof org.opensearch.sql.ast.expression.subquery.ExistsSubquery es)
      return existsSubqueryExpr(es);
    if (e instanceof org.opensearch.sql.ast.expression.LambdaFunction lf) return lambdaExpr(lf);
    if (e instanceof org.opensearch.sql.ast.expression.UnresolvedArgument ua)
      return unresolvedArgExpr(ua);
    if (e instanceof org.opensearch.sql.ast.expression.HighlightFunction hf)
      return highlightExpr(hf);
    if (e instanceof org.opensearch.sql.ast.expression.AggregateFunction)
      // Window/eventstats/streamstats wrap an AggregateFunction inside a Function-call expression
      // path; route through aggCall so the aggregate operator is bound (not a generic UDF).
      return aggCall(e);
    if (e instanceof org.opensearch.sql.ast.expression.RelevanceFieldList rfl) {
      // PPL parses `multi_match(["a"^1.5, "b"^2], query="x")` as RelevanceFieldList(map of
      // field→weight). v2's visitor materializes this as MAP_VALUE_CONSTRUCTOR("a", 1.5, "b", 2.0)
      // so the relevance UDF receives a Map<String,Double>. Mirror that here.
      // Cast each field name to VARCHAR explicitly — without an explicit cast, mismatched-length
      // CHAR literals (e.g. "Tags" + "Title") get widened to CHAR(maxLen) and shorter values are
      // right-padded with spaces, breaking field-name resolution in the OpenSearch pushdown.
      java.util.List<SqlNode> args = new java.util.ArrayList<>();
      for (java.util.Map.Entry<String, Float> entry : rfl.getFieldList().entrySet()) {
        args.add(
            new SqlBasicCall(
                SqlStdOperatorTable.CAST,
                List.of(
                    SqlLiteral.createCharString(entry.getKey(), POS),
                    new org.apache.calcite.sql.SqlDataTypeSpec(
                        new org.apache.calcite.sql.SqlBasicTypeNameSpec(
                            org.apache.calcite.sql.type.SqlTypeName.VARCHAR, POS),
                        POS)),
                POS));
        args.add(SqlLiteral.createApproxNumeric(Float.toString(entry.getValue()) + "E0", POS));
      }
      return new SqlBasicCall(SqlStdOperatorTable.MAP_VALUE_CONSTRUCTOR, args, POS);
    }
    throw new UnsupportedOperationException(
        "Expression not yet supported in SqlNode POC: " + e.getClass().getSimpleName());
  }

  /** PPL `cast(expr AS type)` => SQL CAST(expr AS sql-type). */
  private SqlNode castExpr(org.opensearch.sql.ast.expression.Cast c) {
    SqlNode value = expr(c.getExpression());
    // PPL `cast(<numeric> as ip)` is rejected: only STRING and IP types convert to IP.
    // testCastIntegerToIp expects ExpressionEvaluationException with this exact message.
    if (c.getDataType() == DataType.IP) {
      org.opensearch.sql.ast.expression.UnresolvedExpression src = c.getExpression();
      if (src instanceof org.opensearch.sql.ast.expression.Literal lit) {
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
    org.apache.calcite.sql.type.SqlTypeName tn = pplTypeToSql(c.getDataType());
    // PPL `cast(<expr> AS IP|DATE|TIME|TIMESTAMP)`: dispatch to PPL UDF so the EXPR_* UDT result
    // type is established (SAFE_CAST cannot represent OpenSearch UDTs at the SqlNode level), and
    // the format-permissive PPL parser is used (e.g. DATE accepts both 'yyyy-MM-dd' and
    // 'yyyy-MM-dd HH:mm:ss' inputs).
    String udtFunc =
        switch (c.getDataType()) {
          case IP -> org.opensearch.sql.calcite.utils.UserDefinedFunctionUtils.IP_FUNCTION_NAME;
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
    // PPL `cast(<X> AS STRING)` quirks:
    //  - EXPR_IP source: SAFE_CAST(EXPR_IP AS VARCHAR) fails validation because EXPR_IP is a
    //    JavaType-backed UDT — dispatch to IP_TO_STRING UDF (Object.toString()).
    //  - DOUBLE/FLOAT/DECIMAL source: Calcite SAFE_CAST stringifies 0.0 → "0E0" and 0.99 → ".99".
    //    PPL expects "0.0" / "0.99" — dispatch to NUMBER_TO_STRING (Java toString semantics).
    if ((tn == org.apache.calcite.sql.type.SqlTypeName.VARCHAR
            || tn == org.apache.calcite.sql.type.SqlTypeName.CHAR)
        && rowTypeOracle != null) {
      try {
        org.apache.calcite.rel.type.RelDataType vt = probeExprType(value);
        if (vt instanceof org.opensearch.sql.calcite.type.AbstractExprRelDataType<?> exprT
            && exprT.getExprType() == org.opensearch.sql.data.type.ExprCoreType.IP) {
          return new SqlBasicCall(
              new org.apache.calcite.sql.SqlUnresolvedFunction(
                  new SqlIdentifier("IP_TO_STRING", POS),
                  null,
                  null,
                  null,
                  null,
                  org.apache.calcite.sql.SqlFunctionCategory.USER_DEFINED_FUNCTION),
              List.of(value),
              POS);
        }
        if (vt != null
            && (org.apache.calcite.sql.type.SqlTypeName.APPROX_TYPES.contains(vt.getSqlTypeName())
                || vt.getSqlTypeName() == org.apache.calcite.sql.type.SqlTypeName.DECIMAL)) {
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
        }
      } catch (RuntimeException ignoredCastIp) {
        // probe failed; fall through to SAFE_CAST
      }
    }
    // BOOLEAN→numeric: Calcite (even SAFE_CAST) rejects this at validation. PPL allows it
    // (true→1, false→0). Emulate via CASE WHEN <bool> THEN 1 ELSE 0 END.
    // NUMERIC→BOOLEAN: PPL semantic is `value != 0`. Calcite SAFE_CAST returns null for
    // numeric→boolean. Emit `value <> 0` directly. Detect numeric source via either AST literal
    // type or a probe of the SqlNode value (probe needs currentFrom).
    if (tn == org.apache.calcite.sql.type.SqlTypeName.BOOLEAN) {
      // PPL aligns with Spark/Postgres for string→boolean: '1'→true, '0'→false, else NULL.
      // Calcite SAFE_CAST returns null for any non-empty string (including '1'/'0'), so emulate
      // via CASE WHEN value='1' THEN true WHEN value='0' THEN false ELSE NULL.
      if (c.getExpression() instanceof org.opensearch.sql.ast.expression.Literal slit
          && slit.getType() == DataType.STRING) {
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
      boolean isNumericSrc = false;
      if (c.getExpression() instanceof org.opensearch.sql.ast.expression.Literal lit) {
        switch (lit.getType()) {
          case SHORT, INTEGER, LONG, FLOAT, DOUBLE, DECIMAL -> isNumericSrc = true;
          default -> {}
        }
      }
      if (!isNumericSrc && rowTypeOracle != null) {
        try {
          org.apache.calcite.rel.type.RelDataType vt = probeExprType(value);
          if (vt != null
              && org.apache.calcite.sql.type.SqlTypeName.NUMERIC_TYPES.contains(
                  vt.getSqlTypeName())) {
            isNumericSrc = true;
          }
        } catch (RuntimeException ignoredCastB) {
          // probe failed; fall through
        }
      }
      if (isNumericSrc) {
        return new SqlBasicCall(SqlStdOperatorTable.NOT_EQUALS, List.of(value, intLiteral(0)), POS);
      }
    }
    if (org.apache.calcite.sql.type.SqlTypeName.NUMERIC_TYPES.contains(tn)
        && rowTypeOracle != null) {
      try {
        org.apache.calcite.rel.type.RelDataType vt = probeExprType(value);
        if (vt != null && vt.getSqlTypeName() == org.apache.calcite.sql.type.SqlTypeName.BOOLEAN) {
          // Pick THEN/ELSE values matching the target type so the CASE result type IS the target,
          // avoiding a wrapping CAST that triggers Calcite's enumerable codegen bug
          // ("Cannot access non-final local variable case_when_value from inner class").
          SqlNode oneVal;
          SqlNode zeroVal;
          if (tn == org.apache.calcite.sql.type.SqlTypeName.BIGINT) {
            oneVal = SqlLiteral.createExactNumeric("1", POS);
            zeroVal = SqlLiteral.createExactNumeric("0", POS);
          } else {
            oneVal = intLiteral(1);
            zeroVal = intLiteral(0);
          }
          SqlNodeList whens = new SqlNodeList(POS);
          whens.add(value);
          SqlNodeList thens = new SqlNodeList(POS);
          thens.add(oneVal);
          return new org.apache.calcite.sql.fun.SqlCase(POS, null, whens, thens, zeroVal);
        }
      } catch (RuntimeException ignoredCast) {
        // probe failed; fall through
      }
    }
    org.apache.calcite.sql.SqlDataTypeSpec spec =
        new org.apache.calcite.sql.SqlDataTypeSpec(
            new org.apache.calcite.sql.SqlBasicTypeNameSpec(tn, POS), POS);
    // Use SAFE_CAST instead of strict CAST so cross-family casts like 'invalid'→DATE return NULL
    // on failure rather than failing validation. v2's visitCast
    // (CalciteRexNodeVisitor.java#724-732) uses rexBuilder.makeCast with safe=true.
    return new SqlBasicCall(
        org.apache.calcite.sql.fun.SqlLibraryOperators.SAFE_CAST, List.of(value, spec), POS);
  }

  private static org.apache.calcite.sql.type.SqlTypeName pplTypeToSql(DataType t) {
    return switch (t) {
      case BOOLEAN -> org.apache.calcite.sql.type.SqlTypeName.BOOLEAN;
      case SHORT -> org.apache.calcite.sql.type.SqlTypeName.SMALLINT;
      case INTEGER -> org.apache.calcite.sql.type.SqlTypeName.INTEGER;
      case LONG -> org.apache.calcite.sql.type.SqlTypeName.BIGINT;
      case FLOAT -> org.apache.calcite.sql.type.SqlTypeName.FLOAT;
      case DOUBLE -> org.apache.calcite.sql.type.SqlTypeName.DOUBLE;
      case DECIMAL -> org.apache.calcite.sql.type.SqlTypeName.DECIMAL;
      case STRING -> org.apache.calcite.sql.type.SqlTypeName.VARCHAR;
      case DATE -> org.apache.calcite.sql.type.SqlTypeName.DATE;
      case TIME -> org.apache.calcite.sql.type.SqlTypeName.TIME;
      case TIMESTAMP -> org.apache.calcite.sql.type.SqlTypeName.TIMESTAMP;
      default -> org.apache.calcite.sql.type.SqlTypeName.VARCHAR;
    };
  }

  /**
   * PPL {@code interval N unit} => SQL {@code INTERVAL 'N' <UNIT>} literal. Calcite distinguishes
   * year-month and day-time intervals with separate qualifier shapes; map all PPL units to a single
   * year-month or day-time slot via the corresponding TimeUnit.
   */
  private SqlNode intervalExpr(org.opensearch.sql.ast.expression.Interval i) {
    String literalStr;
    Object v =
        i.getValue() instanceof org.opensearch.sql.ast.expression.Literal lit
            ? lit.getValue()
            : null;
    if (v != null) {
      literalStr = v.toString();
    } else {
      // Non-literal interval value (e.g., expr) — fall back to legacy unresolved-function shape;
      // most PPL queries use literals.
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

  /** PPL `value BETWEEN low AND high` => SQL BETWEEN. */
  private SqlNode betweenExpr(org.opensearch.sql.ast.expression.Between b) {
    SqlNode valueNode = expr(b.getValue());
    SqlNode lowNode = expr(b.getLowerBound());
    SqlNode highNode = expr(b.getUpperBound());
    // v2's CalciteRexNodeVisitor.visitBetween rejects mixed-family operands up-front
    // (e.g. age between '35' and 38.5 — string vs numeric). Calcite's BETWEEN is more
    // permissive and silently coerces, so probe the operand types via the oracle and throw the
    // v2 error shape on family mismatch.
    if (rowTypeOracle != null && currentFrom != null) {
      try {
        org.apache.calcite.rel.type.RelDataType vT = probeExprType(valueNode);
        org.apache.calcite.rel.type.RelDataType lT = probeExprType(lowNode);
        org.apache.calcite.rel.type.RelDataType hT = probeExprType(highNode);
        if (vT != null && lT != null && hT != null) {
          org.apache.calcite.sql.type.SqlTypeFamily vF = familyOf(vT);
          org.apache.calcite.sql.type.SqlTypeFamily lF = familyOf(lT);
          org.apache.calcite.sql.type.SqlTypeFamily hF = familyOf(hT);
          if (vF != null && lF != null && hF != null && (vF != lF || vF != hF)) {
            throw new org.opensearch.sql.exception.SemanticCheckException(
                String.format(
                    "BETWEEN expression types are incompatible: [%s, %s, %s]",
                    org.opensearch.sql.calcite.utils.OpenSearchTypeFactory
                        .convertRelDataTypeToExprType(vT),
                    org.opensearch.sql.calcite.utils.OpenSearchTypeFactory
                        .convertRelDataTypeToExprType(lT),
                    org.opensearch.sql.calcite.utils.OpenSearchTypeFactory
                        .convertRelDataTypeToExprType(hT)));
          }
        }
      } catch (org.opensearch.sql.exception.SemanticCheckException sce) {
        throw sce;
      } catch (RuntimeException ignored) {
        // probe failed; defer to validator
      }
    }
    return new SqlBasicCall(
        SqlStdOperatorTable.BETWEEN, List.of(valueNode, lowNode, highNode), POS);
  }

  private org.apache.calcite.rel.type.RelDataType probeExprType(SqlNode expr) {
    SqlNodeList sel = new SqlNodeList(POS);
    sel.add(expr);
    SqlSelect probe =
        new SqlSelect(
            POS, null, sel, currentFrom, null, null, null, null, null, null, null, null, null);
    return rowTypeOracle.apply(probe).getFieldList().get(0).getType();
  }

  /** Coerce a RelDataType into one of the common families for BETWEEN type checking. */
  private static org.apache.calcite.sql.type.SqlTypeFamily familyOf(
      org.apache.calcite.rel.type.RelDataType t) {
    org.apache.calcite.sql.type.SqlTypeName n = t.getSqlTypeName();
    if (n == null) return null;
    if (org.apache.calcite.sql.type.SqlTypeName.NUMERIC_TYPES.contains(n)) {
      return org.apache.calcite.sql.type.SqlTypeFamily.NUMERIC;
    }
    if (org.apache.calcite.sql.type.SqlTypeName.CHAR_TYPES.contains(n)) {
      return org.apache.calcite.sql.type.SqlTypeFamily.CHARACTER;
    }
    if (org.apache.calcite.sql.type.SqlTypeName.DATETIME_TYPES.contains(n)) {
      return org.apache.calcite.sql.type.SqlTypeFamily.DATETIME;
    }
    if (n == org.apache.calcite.sql.type.SqlTypeName.BOOLEAN) {
      return org.apache.calcite.sql.type.SqlTypeFamily.BOOLEAN;
    }
    return null;
  }

  /** Inline scalar subquery: emit as `(<select>)`. */
  private SqlNode scalarSubqueryExpr(org.opensearch.sql.ast.expression.subquery.ScalarSubquery ss) {
    // Force joinScope inside the scalar subquery so any `<inner-alias>.<col>` or outer-table
    // qualified names emit as multi-part SqlIdentifier rather than dotted single identifiers.
    PplToSqlNode inner = new PplToSqlNode(rowTypeOracle);
    inner.joinScope = true;
    return inner.visit(ss.getQuery());
  }

  /** EXISTS subquery: emit as `EXISTS (<select>)`. */
  private SqlNode existsSubqueryExpr(org.opensearch.sql.ast.expression.subquery.ExistsSubquery es) {
    // Exists subquery body may reference outer-scope columns via `<outer-table>.<col>`. Force
    // joinScope=true so qualified names emit as multi-part SqlIdentifier (table-qualified
    // navigation) rather than a single dotted identifier. Mirrors how v2 keeps both inner and
    // outer scopes visible to the validator.
    //
    // Pass subsearchLimit through so the inner visitor's lift+wrap path can inject the
    // SUBSEARCH_MAXOUT FETCH at raw-scan level (Option B1) for tests that mix correlated and
    // non-correlated WHERE pipes (testSubsearchMaxOut3).
    PplToSqlNode inner = new PplToSqlNode(rowTypeOracle, subsearchLimit, joinSubsearchLimit);
    inner.joinScope = true;
    SqlNode subQuery = inner.visit(es.getQuery());
    return new SqlBasicCall(SqlStdOperatorTable.EXISTS, List.of(subQuery), POS);
  }

  /**
   * Lambda: forall/exists/filter/transform/reduce/etc. Emit a Calcite SqlLambda whose parameter
   * list is the PPL lambda argument identifiers and whose body is the translated body. Calcite's
   * SqlToRelConverter turns this into a RexLambda that the PPL array UDFs can invoke directly.
   */
  private SqlNode lambdaExpr(org.opensearch.sql.ast.expression.LambdaFunction lf) {
    SqlNodeList paramList = new SqlNodeList(POS);
    for (QualifiedName qn : lf.getFuncArgs()) {
      paramList.add(new SqlIdentifier(qn.toString(), POS));
    }
    lambdaDepth++;
    SqlNode body;
    try {
      body = expr(lf.getFunction());
    } finally {
      lambdaDepth--;
    }
    return new org.apache.calcite.sql.SqlLambda(POS, paramList, body);
  }

  /**
   * Named argument like `query="foo"` in PPL function calls. Emit as MAP_VALUE_CONSTRUCTOR(name,
   * value), matching v2's approach (it uses MAP_VALUE_CONSTRUCTOR to avoid blocking constants
   * reduction in optimizers).
   */
  /**
   * Walk a join-condition AST and collect right-side equi-join column names — i.e. column refs
   * qualified with {@code rAlias} that participate in an equality conjunct of the form {@code l.X =
   * r.Y} or {@code r.Y = l.X}. Returns null if the condition isn't a pure conjunction of such
   * equalities (mixed operators, subqueries, etc.) so the caller falls back to no max cap.
   */
  private static java.util.List<String> extractRightEquiJoinCols(
      UnresolvedExpression condition, String rAlias) {
    java.util.List<String> out = new java.util.ArrayList<>();
    if (!collectRightEquiJoinCols(condition, rAlias, out)) return null;
    return out;
  }

  private static boolean collectRightEquiJoinCols(
      UnresolvedExpression e, String rAlias, java.util.List<String> out) {
    if (e instanceof org.opensearch.sql.ast.expression.And and) {
      return collectRightEquiJoinCols(and.getLeft(), rAlias, out)
          && collectRightEquiJoinCols(and.getRight(), rAlias, out);
    }
    if (e instanceof org.opensearch.sql.ast.expression.EqualTo eq) {
      return addRightCol(eq.getLeft(), eq.getRight(), rAlias, out);
    }
    if (e instanceof org.opensearch.sql.ast.expression.Compare c && "=".equals(c.getOperator())) {
      return addRightCol(c.getLeft(), c.getRight(), rAlias, out);
    }
    return false;
  }

  /**
   * Walk a lambda body and collect QualifiedName references whose first part is NOT in {@code
   * declared} (the lambda's own parameter set). These are outer-scope captures. Single-part bare
   * names like {@code multiplier} are added under their own key; dotted names {@code a.b} are
   * captured as the whole qualified name only when their head is an outer ref.
   */
  private static void collectCapturedRefs(
      UnresolvedExpression e,
      java.util.Set<String> declared,
      java.util.LinkedHashMap<String, QualifiedName> out) {
    if (e == null) return;
    if (e instanceof QualifiedName qn) {
      String head = qn.getParts().isEmpty() ? qn.toString() : qn.getParts().get(0);
      if (!declared.contains(head) && !out.containsKey(head)) {
        out.put(head, qn);
      }
      return;
    }
    if (e instanceof Field fld && fld.getField() instanceof QualifiedName qn) {
      String head = qn.getParts().isEmpty() ? qn.toString() : qn.getParts().get(0);
      if (!declared.contains(head) && !out.containsKey(head)) {
        out.put(head, qn);
      }
      return;
    }
    if (e instanceof org.opensearch.sql.ast.expression.LambdaFunction nestedLf) {
      // A nested lambda has its own parameters — those shadow the outer. Don't capture body refs
      // that the inner lambda itself binds; recurse with a merged shadow set.
      java.util.Set<String> nested = new java.util.LinkedHashSet<>(declared);
      for (QualifiedName p : nestedLf.getFuncArgs()) nested.add(p.toString());
      collectCapturedRefs(nestedLf.getFunction(), nested, out);
      return;
    }
    // Recurse into all children. AST tree nodes expose getChild() returning UnresolvedExpressions
    // (actually Nodes; cast where applicable).
    for (org.opensearch.sql.ast.Node child : e.getChild()) {
      if (child instanceof UnresolvedExpression cu) {
        collectCapturedRefs(cu, declared, out);
      }
    }
  }

  private static boolean addRightCol(
      UnresolvedExpression a, UnresolvedExpression b, String rAlias, java.util.List<String> out) {
    String aRight = qualifiedNameRightCol(a, rAlias);
    String bRight = qualifiedNameRightCol(b, rAlias);
    if (aRight != null) {
      out.add(aRight);
      return true;
    }
    if (bRight != null) {
      out.add(bRight);
      return true;
    }
    return false;
  }

  private static String qualifiedNameRightCol(UnresolvedExpression e, String rAlias) {
    QualifiedName qn = null;
    if (e instanceof QualifiedName q) qn = q;
    else if (e instanceof Field f && f.getField() instanceof QualifiedName q) qn = q;
    if (qn == null) return null;
    java.util.List<String> parts = qn.getParts();
    if (parts.size() == 2 && rAlias.equalsIgnoreCase(parts.get(0))) {
      return parts.get(1);
    }
    return null;
  }

  /**
   * Detect bin span on a time-based field and emit SPAN(field, value, unit) instead of SPAN_BUCKET
   * (which is numeric-only). Returns null when the field isn't time-based or the span literal isn't
   * a recognised duration like "1d"/"5m"/"30s".
   */
  private SqlNode tryTimeSpanCall(
      SqlNode fieldRef,
      UnresolvedExpression spanExpr,
      UnresolvedExpression aligntimeExpr,
      SqlNode source,
      String fieldName) {
    if (rowTypeOracle == null || source == null || fieldName == null) return null;
    org.apache.calcite.rel.type.RelDataType fieldType;
    try {
      org.apache.calcite.rel.type.RelDataType rowType = deriveRowType(source);
      org.apache.calcite.rel.type.RelDataTypeField fld = rowType.getField(fieldName, false, false);
      if (fld == null) return null;
      fieldType = fld.getType();
    } catch (RuntimeException ignored) {
      return null;
    }
    if (!isTimeBasedType(fieldType)) return null;
    String spanStr = null;
    if (spanExpr instanceof Literal lit && lit.getValue() != null) {
      spanStr = lit.getValue().toString();
    }
    if (spanStr == null) return null;
    spanStr = spanStr.replace("'", "").replace("\"", "").trim();
    // Parse <intValue><unit> like "1d", "30s", "5m", "1y", "1mon".
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
    // Only the units accepted by Rounding.DateTimeUnit (the OpenSearch pushdown runtime
    // resolver) are supported. The PPL bin command exposes additional sub-second units
    // (us/cs/ds) but those aren't supported by the underlying time-rounding implementation,
    // so we leave them to SPAN_BUCKET (which will surface a validator-level error).
    // PPL case rule: bare "m" lowercase means minute; bare "M" uppercase means month.
    // The full-name aliases (mon/month/months) are case-insensitive.
    String unit;
    if ("M".equals(rawUnitOriginal)) {
      unit = "M";
    } else if ("us".equals(rawUnitOriginal)
        || "cs".equals(rawUnitOriginal)
        || "ds".equals(rawUnitOriginal)) {
      // Subsecond units are case-sensitive — treat the original casing as the unit name.
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
    // Monthly span: PPL semantics return YYYY-MM string per MonthSpanHandler.
    // Mirror v2 by emitting DATE_FORMAT(MAKEDATE(year, (month-1)*31 + 1), '%Y-%m') over a
    // bin-start computation in months-since-epoch arithmetic. SPAN(field, n, 'M') would
    // return a timestamp, which doesn't match the expected string output.
    if ("M".equals(unit)) {
      return buildMonthlySpan(fieldRef, value);
    }
    // Aligntime support: when aligntime is set on bin, emit FROM_UNIXTIME(FLOOR((unix-offset)/
    // interval) * interval + offset). The SPAN UDF doesn't take an alignment arg, so we have
    // to compute the alignment ourselves. This works for h/m/s units; sub-second alignment is
    // not supported (alignment offset is converted to seconds). For d, w, q, y units the
    // aligntime is ignored per v2 (TimeSpanHelper.shouldApplyAligntime).
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
    // Sub-second binning: SPAN UDF doesn't accept us/cs/ds; emit the v2
    // StandardTimeSpanHandler shape using FROM_UNIXTIME(FLOOR(unix*scale/interval)*interval/scale).
    if ("us".equals(unit) || "cs".equals(unit) || "ds".equals(unit) || "ms".equals(unit)) {
      return buildSubsecondSpan(fieldRef, value, unit);
    }
    return new SqlBasicCall(
        org.opensearch.sql.expression.function.PPLBuiltinOperators.SPAN,
        List.of(fieldRef, intLiteral(value), SqlLiteral.createCharString(unit, POS)),
        POS);
  }

  private long unitToSeconds(String unit, int value) {
    return switch (unit) {
      case "s" -> (long) value;
      case "m" -> (long) value * 60L;
      case "h" -> (long) value * 3600L;
      case "ms", "us", "cs", "ds" -> -1; // sub-second alignment unsupported here
      default -> -1;
    };
  }

  private Long parseAlignTimeOffsetSeconds(UnresolvedExpression aligntimeExpr) {
    if (!(aligntimeExpr instanceof Literal lit) || lit.getValue() == null) return null;
    String s = lit.getValue().toString().replace("'", "").replace("\"", "").trim();
    // Pure epoch number → use as-is (in seconds).
    try {
      return Long.parseLong(s);
    } catch (NumberFormatException ignored) {
      // fall through
    }
    // @d → align to start-of-day (offset 0 in days, which is 0 seconds modulo any sub-day unit).
    // @d+Nh → align to N hours after start-of-day → offset = N*3600 seconds.
    if (s.startsWith("@d")) {
      String tail = s.substring(2);
      if (tail.isEmpty()) return 0L;
      // Parse +Nh / -Nh / +Nm / -Nm.
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

  /**
   * FROM_UNIXTIME(FLOOR((UNIX_TIMESTAMP(field) - alignmentOffset) / intervalSeconds) *
   * intervalSeconds + alignmentOffset). Mirrors v2 StandardTimeSpanHandler with alignment.
   */
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
                List.of(unixSeconds, longLiteral(alignmentOffsetSeconds)),
                POS);
    SqlNode divided =
        new SqlBasicCall(
            SqlStdOperatorTable.DIVIDE, List.of(shifted, longLiteral(intervalSeconds)), POS);
    SqlNode floored = new SqlBasicCall(SqlStdOperatorTable.FLOOR, List.of(divided), POS);
    SqlNode multiplied =
        new SqlBasicCall(
            SqlStdOperatorTable.MULTIPLY, List.of(floored, longLiteral(intervalSeconds)), POS);
    SqlNode binSeconds =
        alignmentOffsetSeconds == 0
            ? multiplied
            : new SqlBasicCall(
                SqlStdOperatorTable.PLUS,
                List.of(multiplied, longLiteral(alignmentOffsetSeconds)),
                POS);
    return new SqlBasicCall(
        org.opensearch.sql.expression.function.PPLBuiltinOperators.FROM_UNIXTIME,
        List.of(binSeconds),
        POS);
  }

  /**
   * Sub-second time bin: FROM_UNIXTIME(FLOOR((UNIX_TIMESTAMP(field) * scale) / interval) * interval
   * / scale). scale = 1_000_000 (us), 1000 (ms), 100 (cs), 10 (ds).
   */
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
            SqlStdOperatorTable.MULTIPLY, List.of(unixSeconds, longLiteral(scale)), POS);
    SqlNode divided =
        new SqlBasicCall(
            SqlStdOperatorTable.DIVIDE, List.of(scaledUp, intLiteral(intervalValue)), POS);
    SqlNode floored = new SqlBasicCall(SqlStdOperatorTable.FLOOR, List.of(divided), POS);
    SqlNode multiplied =
        new SqlBasicCall(
            SqlStdOperatorTable.MULTIPLY, List.of(floored, intLiteral(intervalValue)), POS);
    SqlNode binSeconds =
        new SqlBasicCall(SqlStdOperatorTable.DIVIDE, List.of(multiplied, longLiteral(scale)), POS);
    return new SqlBasicCall(
        org.opensearch.sql.expression.function.PPLBuiltinOperators.FROM_UNIXTIME,
        List.of(binSeconds),
        POS);
  }

  private SqlNode longLiteral(long v) {
    return SqlLiteral.createExactNumeric(Long.toString(v), POS);
  }

  /**
   * Build the v2 MonthSpanHandler emission shape: DATE_FORMAT(MAKEDATE(binStartYear,
   * (binStartMonth-1)*31+1), '%Y-%m'). binStartYear = 1970 + binStartMonths / 12, binStartMonth =
   * (binStartMonths MOD 12) + 1, binStartMonths = monthsSinceEpoch - (monthsSinceEpoch MOD
   * interval), monthsSinceEpoch = (YEAR(field)-1970)*12 + (MONTH(field)-1).
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
    SqlNode yearsSinceEpoch =
        new SqlBasicCall(SqlStdOperatorTable.MINUS, List.of(yearCall, intLiteral(1970)), POS);
    SqlNode monthsFromYears =
        new SqlBasicCall(
            SqlStdOperatorTable.MULTIPLY, List.of(yearsSinceEpoch, intLiteral(12)), POS);
    SqlNode monthMinus1 =
        new SqlBasicCall(SqlStdOperatorTable.MINUS, List.of(monthCall, intLiteral(1)), POS);
    SqlNode monthsSinceEpoch =
        new SqlBasicCall(SqlStdOperatorTable.PLUS, List.of(monthsFromYears, monthMinus1), POS);
    SqlNode positionInCycle =
        new SqlBasicCall(
            SqlStdOperatorTable.MOD, List.of(monthsSinceEpoch, intLiteral(intervalMonths)), POS);
    SqlNode binStartMonths =
        new SqlBasicCall(
            SqlStdOperatorTable.MINUS, List.of(monthsSinceEpoch, positionInCycle), POS);
    SqlNode binStartYear =
        new SqlBasicCall(
            SqlStdOperatorTable.PLUS,
            List.of(
                intLiteral(1970),
                new SqlBasicCall(
                    SqlStdOperatorTable.DIVIDE, List.of(binStartMonths, intLiteral(12)), POS)),
            POS);
    SqlNode binStartMonth =
        new SqlBasicCall(
            SqlStdOperatorTable.PLUS,
            List.of(
                new SqlBasicCall(
                    SqlStdOperatorTable.MOD, List.of(binStartMonths, intLiteral(12)), POS),
                intLiteral(1)),
            POS);
    SqlNode dayOfYear =
        new SqlBasicCall(
            SqlStdOperatorTable.PLUS,
            List.of(
                new SqlBasicCall(
                    SqlStdOperatorTable.MULTIPLY,
                    List.of(
                        new SqlBasicCall(
                            SqlStdOperatorTable.MINUS, List.of(binStartMonth, intLiteral(1)), POS),
                        intLiteral(31)),
                    POS),
                intLiteral(1)),
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

  private static boolean isTimeBasedType(org.apache.calcite.rel.type.RelDataType type) {
    if (type instanceof org.opensearch.sql.calcite.type.ExprSqlType exprSqlType) {
      var udt = exprSqlType.getUdt();
      return udt == org.opensearch.sql.calcite.utils.OpenSearchTypeFactory.ExprUDT.EXPR_TIMESTAMP
          || udt == org.opensearch.sql.calcite.utils.OpenSearchTypeFactory.ExprUDT.EXPR_DATE
          || udt == org.opensearch.sql.calcite.utils.OpenSearchTypeFactory.ExprUDT.EXPR_TIME;
    }
    org.apache.calcite.sql.type.SqlTypeName tn = type.getSqlTypeName();
    return tn == org.apache.calcite.sql.type.SqlTypeName.TIMESTAMP
        || tn == org.apache.calcite.sql.type.SqlTypeName.DATE
        || tn == org.apache.calcite.sql.type.SqlTypeName.TIME;
  }

  /**
   * Detect a logarithmic bin span like "log10", "log2", "loge", "ln", or coefficient-prefixed
   * variants ("2log10", "1.5log10", arbitrary base "logBASE"). Emit the equivalent of v2's
   * LogSpanHelper: CASE WHEN field>0 THEN CAST(coef*base^floor(ln(field/coef)/ln(base)) AS VARCHAR)
   * || '-' || CAST(coef*base^(floor+1) AS VARCHAR) ELSE 'Invalid'. Returns null when the span
   * literal isn't a recognised log expression.
   */
  private SqlNode tryLogSpanCall(SqlNode fieldRef, UnresolvedExpression spanExpr) {
    String spanStr = null;
    if (spanExpr instanceof Literal lit && lit.getValue() != null) {
      spanStr = lit.getValue().toString();
    }
    if (spanStr == null) return null;
    spanStr = spanStr.replace("'", "").replace("\"", "").trim();
    String lowered = spanStr.toLowerCase(java.util.Locale.ROOT);
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
    SqlNode coefLit = doubleLiteral(coefficient);
    SqlNode baseLit = doubleLiteral(base);
    SqlNode lnBaseLit = doubleLiteral(Math.log(base));
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
        new SqlBasicCall(SqlStdOperatorTable.PLUS, List.of(binNumber, doubleLiteral(1.0)), POS);
    SqlNode basePowerBinPlusOne =
        new SqlBasicCall(SqlStdOperatorTable.POWER, List.of(baseLit, binPlusOne), POS);
    SqlNode upperBound =
        new SqlBasicCall(SqlStdOperatorTable.MULTIPLY, List.of(coefLit, basePowerBinPlusOne), POS);
    SqlNode lowerStr = castToVarchar(lowerBound);
    SqlNode upperStr = castToVarchar(upperBound);
    SqlNode firstConcat =
        new SqlBasicCall(
            SqlStdOperatorTable.CONCAT,
            List.of(lowerStr, SqlLiteral.createCharString("-", POS)),
            POS);
    SqlNode rangeStr =
        new SqlBasicCall(SqlStdOperatorTable.CONCAT, List.of(firstConcat, upperStr), POS);
    SqlNode positiveCheck =
        new SqlBasicCall(
            SqlStdOperatorTable.GREATER_THAN, List.of(fieldRef, doubleLiteral(0.0)), POS);
    SqlNodeList whens = new SqlNodeList(POS);
    whens.add(positiveCheck);
    SqlNodeList thens = new SqlNodeList(POS);
    thens.add(rangeStr);
    return new org.apache.calcite.sql.fun.SqlCase(
        POS, null, whens, thens, SqlLiteral.createCharString("Invalid", POS));
  }

  private SqlNode doubleLiteral(double v) {
    return SqlLiteral.createApproxNumeric(Double.toString(v), POS);
  }

  private SqlNode castToVarchar(SqlNode operand) {
    return new SqlBasicCall(
        SqlStdOperatorTable.CAST,
        List.of(
            operand,
            new org.apache.calcite.sql.SqlDataTypeSpec(
                new org.apache.calcite.sql.SqlBasicTypeNameSpec(
                    org.apache.calcite.sql.type.SqlTypeName.VARCHAR, POS),
                POS)),
        POS);
  }

  private SqlNode unresolvedArgExpr(org.opensearch.sql.ast.expression.UnresolvedArgument ua) {
    // Cast a string-literal value to VARCHAR explicitly. Without the cast, Calcite widens
    // unequal-length CHAR literals across multiple MAP entries to a common max-length CHAR(N)
    // and right-pads shorter values with spaces — breaking the OpenSearch pushdown's
    // exact-string match. Mirror v2 emission shape: bare key, VARCHAR-cast string value.
    SqlNode value = expr(ua.getValue());
    if (value instanceof SqlLiteral lit
        && lit.getTypeName() == org.apache.calcite.sql.type.SqlTypeName.CHAR) {
      value = castTo(value, org.apache.calcite.sql.type.SqlTypeName.VARCHAR);
    }
    return new SqlBasicCall(
        SqlStdOperatorTable.MAP_VALUE_CONSTRUCTOR,
        List.of(SqlLiteral.createCharString(ua.getArgName(), POS), value),
        POS);
  }

  /** highlight(field) — emit as a passthrough function call resolved by the validator. */
  private SqlNode highlightExpr(org.opensearch.sql.ast.expression.HighlightFunction hf) {
    return new SqlBasicCall(
        new org.apache.calcite.sql.SqlUnresolvedFunction(
            new SqlIdentifier("highlight", POS),
            null,
            null,
            null,
            null,
            org.apache.calcite.sql.SqlFunctionCategory.USER_DEFINED_FUNCTION),
        List.of(expr(hf.getHighlightField())),
        POS);
  }

  /**
   * Translate a PPL Span expression into a call to {@link
   * org.opensearch.sql.expression.function.PPLBuiltinOperators#SPAN}. Mirrors
   * CalciteRexNodeVisitor.visitSpan: SPAN(field, value, unitName-or-null).
   */
  private SqlNode spanExpr(org.opensearch.sql.ast.expression.Span sp) {
    SqlNode field = expr(sp.getField());
    SqlNode value = expr(sp.getValue());
    org.opensearch.sql.ast.expression.SpanUnit unit = sp.getUnit();
    SqlNode unitNode;
    if (unit == org.opensearch.sql.ast.expression.SpanUnit.NONE
        || unit == org.opensearch.sql.ast.expression.SpanUnit.UNKNOWN) {
      // SpanFunction.SpanImplementor uses SqlTypeUtil.isNull(unitType) to dispatch the numeric-
      // only branch. SqlLiteral.createNull(POS) types out as ANY in our path (which trips the
      // throw), so wrap it in CAST(NULL AS NULL) to land on the proper NULL SqlTypeName.
      unitNode = castTo(SqlLiteral.createNull(POS), org.apache.calcite.sql.type.SqlTypeName.NULL);
    } else {
      unitNode = SqlLiteral.createCharString(unit.getName(), POS);
    }
    return new SqlBasicCall(
        org.opensearch.sql.expression.function.PPLBuiltinOperators.SPAN,
        List.of(field, value, unitNode),
        POS);
  }

  private SqlNode literal(Literal lit) {
    Object v = lit.getValue();
    DataType t = lit.getType();
    if (v == null || t == DataType.NULL) {
      return SqlLiteral.createNull(POS);
    }
    return switch (t) {
      case BOOLEAN -> SqlLiteral.createBoolean((Boolean) v, POS);
      case INTEGER, LONG, SHORT -> SqlLiteral.createExactNumeric(v.toString(), POS);
      // PPL FLOAT/DOUBLE literals are approximate. createApproxNumeric requires scientific
      // notation, so add "E0" if it's missing. CAST the FLOAT result down to FLOAT (REAL) so it
      // doesn't widen to DOUBLE during validator type promotion.
      case FLOAT -> castTo(approxNumeric(v), org.apache.calcite.sql.type.SqlTypeName.FLOAT);
      case DOUBLE -> approxNumeric(v);
      case DECIMAL -> {
        BigDecimal bd = (v instanceof BigDecimal b) ? b : new BigDecimal(v.toString());
        yield SqlLiteral.createExactNumeric(bd.toPlainString(), POS);
      }
      case STRING -> SqlLiteral.createCharString(v.toString(), POS);
      default -> throw new UnsupportedOperationException("Literal type not yet supported: " + t);
    };
  }

  /**
   * If either operand is an EXPR_IP field, return the corresponding PPLBuiltinOperators IP
   * comparator; otherwise null. Detects EXPR_IP by looking up the LHS field in the given source's
   * row type via rowTypeOracle.
   */
  private SqlOperator ipComparisonOperator(
      String op, UnresolvedExpression left, UnresolvedExpression right, SqlNode source) {
    if (rowTypeOracle == null || source == null) return null;
    boolean leftIp = isIpExpr(left, source);
    boolean rightIp = isIpExpr(right, source);
    if (!leftIp && !rightIp) return null;
    return switch (op) {
      case "=" -> org.opensearch.sql.expression.function.PPLBuiltinOperators.EQUALS_IP;
      case "!=", "<>" -> org.opensearch.sql.expression.function.PPLBuiltinOperators.NOT_EQUALS_IP;
      case ">" -> org.opensearch.sql.expression.function.PPLBuiltinOperators.GREATER_IP;
      case ">=" -> org.opensearch.sql.expression.function.PPLBuiltinOperators.GTE_IP;
      case "<" -> org.opensearch.sql.expression.function.PPLBuiltinOperators.LESS_IP;
      case "<=" -> org.opensearch.sql.expression.function.PPLBuiltinOperators.LTE_IP;
      default -> null;
    };
  }

  private boolean isIpExpr(UnresolvedExpression e, SqlNode source) {
    String fieldName = null;
    if (e instanceof Field f && f.getField() instanceof QualifiedName qn) {
      fieldName = qn.toString();
    } else if (e instanceof QualifiedName qn) {
      fieldName = qn.toString();
    }
    if (fieldName == null) return false;
    try {
      var rowType = deriveRowType(source);
      var field = rowType.getField(fieldName, false, false);
      if (field == null) return false;
      var t = field.getType();
      if (t instanceof org.opensearch.sql.calcite.type.AbstractExprRelDataType<?> exprT) {
        return exprT.getExprType() == org.opensearch.sql.data.type.ExprCoreType.IP;
      }
    } catch (Exception ignored) {
    }
    return false;
  }

  /** Return true if {@code e} is a STRING-typed Literal AST node. */
  private static boolean isStringLiteralExpr(UnresolvedExpression e) {
    return e instanceof Literal lit
        && lit.getType() == org.opensearch.sql.ast.expression.DataType.STRING;
  }

  /** Return true if {@code e} is a BOOLEAN-typed Literal AST node. */
  private static boolean isBooleanLiteralExpr(UnresolvedExpression e) {
    return e instanceof Literal lit
        && lit.getType() == org.opensearch.sql.ast.expression.DataType.BOOLEAN;
  }

  /**
   * Extract a boolean value from a Literal that's either typed BOOLEAN or a STRING containing
   * "TRUE"/"FALSE" (case-insensitive). Returns null otherwise. PPL allows both forms when comparing
   * against a boolean field (e.g. `where male = 'TRUE'` and `where male = TRUE`).
   */
  private static Boolean extractBoolLiteral(UnresolvedExpression e) {
    if (!(e instanceof Literal lit)) return null;
    if (lit.getType() == org.opensearch.sql.ast.expression.DataType.BOOLEAN) {
      return (Boolean) lit.getValue();
    }
    if (lit.getType() == org.opensearch.sql.ast.expression.DataType.STRING) {
      String s = String.valueOf(lit.getValue());
      if ("TRUE".equalsIgnoreCase(s)) return Boolean.TRUE;
      if ("FALSE".equalsIgnoreCase(s)) return Boolean.FALSE;
    }
    return null;
  }

  /** Map a UDT ExprCoreType to its PPL constructor operator (TIMESTAMP/DATE/TIME). */
  private static SqlOperator udtConstructorOp(org.opensearch.sql.data.type.ExprCoreType ct) {
    return switch (ct) {
      case TIMESTAMP -> org.opensearch.sql.expression.function.PPLBuiltinOperators.TIMESTAMP;
      case DATE -> org.opensearch.sql.expression.function.PPLBuiltinOperators.DATE;
      case TIME -> org.opensearch.sql.expression.function.PPLBuiltinOperators.TIME;
      default -> null;
    };
  }

  private static boolean isDateTimeUdtCoreType(org.opensearch.sql.data.type.ExprCoreType ct) {
    return ct == org.opensearch.sql.data.type.ExprCoreType.DATE
        || ct == org.opensearch.sql.data.type.ExprCoreType.TIME
        || ct == org.opensearch.sql.data.type.ExprCoreType.TIMESTAMP;
  }

  /**
   * Detect the {@link org.opensearch.sql.data.type.ExprCoreType} of a Field/QualifiedName when
   * resolved against {@code source} (typically currentFrom). Returns null for non-fields, fields
   * not in the row type, or non-UDT field types. Used by cmp() to wrap a string-literal RHS in the
   * matching DATE/TIME/TIMESTAMP UDF when comparing against a UDT-typed LHS — mirroring v2's
   * type-aware comparison behavior.
   *
   * <p>Also recognizes a {@code DATE(...)}/{@code TIME(...)}/{@code TIMESTAMP(...)} function call
   * as producing the matching UDT type. This lets cross-type comparisons between UDF-typed operands
   * (e.g. {@code TIMESTAMP('...') = DATE('...')}) be detected so cmp() can widen the narrower side,
   * matching v2's CoercionUtils.widenArguments behavior.
   */
  private org.opensearch.sql.data.type.ExprCoreType exprCoreType(
      UnresolvedExpression e, SqlNode source) {
    if (e instanceof Function f) {
      String fn = f.getFuncName() == null ? "" : f.getFuncName().toLowerCase(java.util.Locale.ROOT);
      switch (fn) {
        case "timestamp":
          return org.opensearch.sql.data.type.ExprCoreType.TIMESTAMP;
        case "date":
          return org.opensearch.sql.data.type.ExprCoreType.DATE;
        case "time":
          return org.opensearch.sql.data.type.ExprCoreType.TIME;
        default:
          // fall through
      }
    }
    String fieldName = null;
    if (e instanceof Field f && f.getField() instanceof QualifiedName qn) {
      fieldName = qn.toString();
    } else if (e instanceof QualifiedName qn) {
      fieldName = qn.toString();
    }
    if (fieldName == null) return null;
    try {
      var rowType = deriveRowType(source);
      var field = rowType.getField(fieldName, false, false);
      if (field == null) return null;
      var t = field.getType();
      if (t instanceof org.opensearch.sql.calcite.type.AbstractExprRelDataType<?> exprT) {
        var et = exprT.getExprType();
        if (et instanceof org.opensearch.sql.data.type.ExprCoreType ct) {
          return ct;
        }
      }
    } catch (Exception ignored) {
    }
    return null;
  }

  private SqlNode cmp(Compare c) {
    String operator = c.getOperator();
    String op = operator.toLowerCase(java.util.Locale.ROOT);
    SqlOperator stdOp =
        switch (op) {
          case "=" -> SqlStdOperatorTable.EQUALS;
          case "!=", "<>" -> SqlStdOperatorTable.NOT_EQUALS;
          case ">" -> SqlStdOperatorTable.GREATER_THAN;
          case ">=" -> SqlStdOperatorTable.GREATER_THAN_OR_EQUAL;
          case "<" -> SqlStdOperatorTable.LESS_THAN;
          case "<=" -> SqlStdOperatorTable.LESS_THAN_OR_EQUAL;
          case "like" -> SqlStdOperatorTable.LIKE;
          case "not like", "not_like" -> SqlStdOperatorTable.NOT_LIKE;
          case "ilike" -> org.apache.calcite.sql.fun.SqlLibraryOperators.ILIKE;
          case "not ilike", "not_ilike" -> org.apache.calcite.sql.fun.SqlLibraryOperators.NOT_ILIKE;
          case "regexp" -> org.apache.calcite.sql.fun.SqlLibraryOperators.REGEXP_CONTAINS;
          default -> null;
        };
    if (stdOp != null) {
      SqlNode left = expr(c.getLeft());
      SqlNode right = expr(c.getRight());
      // Boolean simplification: `<bool_field> = TRUE` → `<bool_field>`, `<bool_field> = FALSE` →
      // `NOT(<bool_field>)`. Mirrors v2's emission shape and lets the OpenSearch pushdown emit a
      // single `term` query against the boolean. Only applies when:
      // - operator is = or !=
      // - one side is a boolean literal (TRUE/FALSE)
      // - the other side is a Field whose probed SqlTypeName is BOOLEAN
      if (op.equals("=") || op.equals("!=") || op.equals("<>")) {
        Boolean rightBool = extractBoolLiteral(c.getRight());
        Boolean leftBool = extractBoolLiteral(c.getLeft());
        SqlNode fieldSide = null;
        Boolean boolValue = null;
        if (rightBool != null) {
          fieldSide = left;
          boolValue = rightBool;
        } else if (leftBool != null) {
          fieldSide = right;
          boolValue = leftBool;
        }
        if (boolValue != null
            && probeSqlTypeName(fieldSide) == org.apache.calcite.sql.type.SqlTypeName.BOOLEAN) {
          boolean isEq = op.equals("=");
          // 4-way table:
          // - `field = true`  → `field`              (matches TRUE only)
          // - `field = false` → `NOT(field)`         (matches FALSE only; NULL→NULL drops row)
          // - `field != true` → `IS NOT TRUE(field)` (matches FALSE OR NULL)
          // - `field != false`→ `IS NOT FALSE(field)`(matches TRUE OR NULL)
          if (isEq) {
            return boolValue
                ? fieldSide
                : new SqlBasicCall(SqlStdOperatorTable.NOT, List.of(fieldSide), POS);
          } else {
            SqlOperator postfix =
                boolValue ? SqlStdOperatorTable.IS_NOT_TRUE : SqlStdOperatorTable.IS_NOT_FALSE;
            return new SqlBasicCall(postfix, List.of(fieldSide), POS);
          }
        }
      }
      // PPL allows comparing EXPR_IP with a CHAR string literal — Calcite forbids the standard
      // comparison op against a UDT operand. Dispatch to the PPLBuiltinOperators IP-aware
      // comparator when either operand is an EXPR_IP field. Mirrors PPLFuncImpTable's
      // multi-variant operator registration.
      SqlOperator ipOp = ipComparisonOperator(op, c.getLeft(), c.getRight(), currentFrom);
      if (ipOp != null) {
        // Wrap a string-literal side with IP() so the comparison is type-aware (pushdown emits
        // a range query against IP values, not lexicographic strings). Mirrors v2's emission.
        boolean leftIp = isIpExpr(c.getLeft(), currentFrom);
        boolean rightIp = isIpExpr(c.getRight(), currentFrom);
        if (leftIp && !rightIp && isStringLiteralExpr(c.getRight())) {
          right =
              new SqlBasicCall(
                  org.opensearch.sql.expression.function.PPLBuiltinOperators.IP,
                  List.of(castTo(right, org.apache.calcite.sql.type.SqlTypeName.VARCHAR)),
                  POS);
        } else if (rightIp && !leftIp && isStringLiteralExpr(c.getLeft())) {
          left =
              new SqlBasicCall(
                  org.opensearch.sql.expression.function.PPLBuiltinOperators.IP,
                  List.of(castTo(left, org.apache.calcite.sql.type.SqlTypeName.VARCHAR)),
                  POS);
        }
        return new SqlBasicCall(ipOp, List.of(left, right), POS);
      }
      // For DATE/TIME/TIMESTAMP UDT fields compared with a string literal, wrap the RHS with
      // the matching DATE/TIME/TIMESTAMP UDF so the comparison is type-aware (the OpenSearch
      // pushdown relies on this: a TIME field compared with a TIME-cast string converts to a
      // range query against time values, not lexicographic strings). Mirrors v2's emission shape.
      org.opensearch.sql.data.type.ExprCoreType leftCT = exprCoreType(c.getLeft(), currentFrom);
      org.opensearch.sql.data.type.ExprCoreType rightCT = exprCoreType(c.getRight(), currentFrom);
      SqlOperator udf = null;
      if (leftCT != null && isStringLiteralExpr(c.getRight())) {
        udf = udtConstructorOp(leftCT);
        if (udf != null) {
          right =
              new SqlBasicCall(
                  udf,
                  List.of(castTo(right, org.apache.calcite.sql.type.SqlTypeName.VARCHAR)),
                  POS);
        }
      } else if (rightCT != null && isStringLiteralExpr(c.getLeft())) {
        udf = udtConstructorOp(rightCT);
        if (udf != null) {
          left =
              new SqlBasicCall(
                  udf, List.of(castTo(left, org.apache.calcite.sql.type.SqlTypeName.VARCHAR)), POS);
        }
      }
      // Cross-type DATE/TIME/TIMESTAMP comparisons: when both operands resolve to UDT
      // datetime types but to *different* types (e.g. TIMESTAMP() vs DATE()), the runtime
      // compares them as opaque UDT VARCHARs and produces wrong booleans. v2's RexNode path
      // calls CoercionUtils.widenArguments which finds the widest common type (TIMESTAMP for
      // any DATE/TIME/TIMESTAMP mix) and casts both sides. Mirror that here by wrapping the
      // narrower side(s) in TIMESTAMP() so both operands carry TIMESTAMP semantics before the
      // comparison.
      if (leftCT != null
          && rightCT != null
          && leftCT != rightCT
          && isDateTimeUdtCoreType(leftCT)
          && isDateTimeUdtCoreType(rightCT)) {
        if (leftCT != org.opensearch.sql.data.type.ExprCoreType.TIMESTAMP) {
          left =
              new SqlBasicCall(
                  org.opensearch.sql.expression.function.PPLBuiltinOperators.TIMESTAMP,
                  List.of(left),
                  POS);
        }
        if (rightCT != org.opensearch.sql.data.type.ExprCoreType.TIMESTAMP) {
          right =
              new SqlBasicCall(
                  org.opensearch.sql.expression.function.PPLBuiltinOperators.TIMESTAMP,
                  List.of(right),
                  POS);
        }
      }
      return new SqlBasicCall(stdOp, List.of(left, right), POS);
    }
    // is null / is not null may parse via Compare with rhs literal-null.
    if (op.equals("is null")) {
      return new SqlBasicCall(SqlStdOperatorTable.IS_NULL, List.of(expr(c.getLeft())), POS);
    }
    if (op.equals("is not null")) {
      return new SqlBasicCall(SqlStdOperatorTable.IS_NOT_NULL, List.of(expr(c.getLeft())), POS);
    }
    throw new UnsupportedOperationException("Compare operator not supported: " + operator);
  }

  private SqlNode aggCall(UnresolvedExpression e) {
    return aggCall(e, false);
  }

  /**
   * Build an aggregate call. When {@code windowed} is true, the call will be used inside an OVER
   * window clause — Calcite's enumerable runtime doesn't have window-context implementations for
   * our custom NullableSqlAvgAggFunction (VAR_POP_NULLABLE etc.) or any other UDAF. For the
   * standard aggregates that Calcite already implements (VAR_POP/VAR_SAMP/STDDEV_POP/STDDEV_SAMP),
   * fall back to the standard SQL operator so the runtime can resolve a window implementor.
   */
  private SqlNode aggCall(UnresolvedExpression e, boolean windowed) {
    // PPL eventstats wraps aggregates as WindowFunction(Function("count", [])); stats wraps them
    // as AggregateFunction. Normalize so the same name-dispatch handles both.
    AggregateFunction af;
    java.util.List<UnresolvedExpression> extraArgs = java.util.List.of();
    boolean isDistinct = false;
    if (e instanceof AggregateFunction a) {
      af = a;
      isDistinct = a.getDistinct();
      extraArgs = a.getArgList() != null ? a.getArgList() : java.util.List.of();
    } else if (e instanceof Function f) {
      UnresolvedExpression arg = f.getFuncArgs().isEmpty() ? null : f.getFuncArgs().get(0);
      af = new AggregateFunction(f.getFuncName(), arg);
      // For non-AggregateFunction Function calls, extra args are positional after the field.
      if (f.getFuncArgs().size() > 1) {
        extraArgs = f.getFuncArgs().subList(1, f.getFuncArgs().size());
      }
    } else {
      throw new UnsupportedOperationException(
          "stats aggregator must be a Function or AggregateFunction, got: "
              + e.getClass().getSimpleName());
    }
    String name = af.getFuncName().toLowerCase(java.util.Locale.ROOT);
    // PPL `count()` parses as count(AllFields). Map to SQL `COUNT(*)`.
    SqlNode arg;
    if (af.getField() == null || af.getField() instanceof AllFields) {
      arg = SqlIdentifier.star(POS);
    } else {
      arg = expr(af.getField());
      // For numeric aggregations on a MAP-leaf access (ITEM(map, 'key')), the value type is
      // VARCHAR (json_extract_all returns MAP<VARCHAR, VARCHAR> with stringified scalars).
      // SUM/AVG/MIN/MAX over VARCHAR fails type validation; cast the ITEM result to DOUBLE
      // so PPL's expected numeric coercion applies. Only fires for the numeric-aggregate names
      // that actually need it.
      if (arg instanceof SqlBasicCall sbcItem
          && sbcItem.getOperator() == SqlStdOperatorTable.ITEM
          && (name.equals("sum")
              || name.equals("avg")
              || name.equals("min")
              || name.equals("max")
              || name.equals("var_pop")
              || name.equals("var_samp")
              || name.equals("stddev_pop")
              || name.equals("stddev_samp")
              || name.equals("variance")
              || name.equals("std")
              || name.equals("stddev"))) {
        arg = castTo(arg, org.apache.calcite.sql.type.SqlTypeName.DOUBLE);
      }
    }
    SqlLiteral quantifier =
        isDistinct
            ? SqlLiteral.createSymbol(org.apache.calcite.sql.SqlSelectKeyword.DISTINCT, POS)
            : null;
    // Standard SQL aggregates. For non-windowed AVG, use the nullable variant so all-NULL
    // partitions return NULL (PPL semantics) instead of triggering a runtime "Cannot convert
    // null to double" when the standard SQL AVG's NOT NULL return type meets a NULL value.
    SqlOperator stdOp =
        switch (name) {
          case "count" -> SqlStdOperatorTable.COUNT;
          case "sum" -> SqlStdOperatorTable.SUM;
          case "avg" ->
              windowed
                  ? SqlStdOperatorTable.AVG
                  : org.opensearch.sql.expression.function.PPLBuiltinOperators.AVG_NULLABLE;
          case "min" -> SqlStdOperatorTable.MIN;
          case "max" -> SqlStdOperatorTable.MAX;
          default -> null;
        };
    if (stdOp != null) {
      return new SqlBasicCall(stdOp, List.of(arg), POS, quantifier);
    }
    // distinct_count[x] / dc[x] => COUNT(DISTINCT x). DISTINCT_COUNT_APPROX uses HLL-style
    // approx by default in v2; alias to plain DISTINCT_COUNT for now.
    if (name.equals("distinct_count")
        || name.equals("dc")
        || name.equals("distinct_count_approx")) {
      return new SqlBasicCall(
          SqlStdOperatorTable.COUNT,
          List.of(arg),
          POS,
          SqlLiteral.createSymbol(org.apache.calcite.sql.SqlSelectKeyword.DISTINCT, POS));
    }
    // PPLBuiltinOperators-registered aggregates: dispatch by name to the SqlAggFunction.
    // When inside an OVER window, fall back to Calcite's standard variants for VAR_POP/STDDEV
    // since our nullable wrappers have no window-context enumerable implementation.
    SqlAggFunction pplAgg =
        switch (name) {
          case "var_pop", "varpop" ->
              windowed
                  ? SqlStdOperatorTable.VAR_POP
                  : org.opensearch.sql.expression.function.PPLBuiltinOperators.VAR_POP_NULLABLE;
          case "var_samp", "varsamp", "variance" ->
              windowed
                  ? SqlStdOperatorTable.VAR_SAMP
                  : org.opensearch.sql.expression.function.PPLBuiltinOperators.VAR_SAMP_NULLABLE;
          case "stddev_pop", "std", "stddev" ->
              windowed
                  ? SqlStdOperatorTable.STDDEV_POP
                  : org.opensearch.sql.expression.function.PPLBuiltinOperators.STDDEV_POP_NULLABLE;
          case "stddev_samp" ->
              windowed
                  ? SqlStdOperatorTable.STDDEV_SAMP
                  : org.opensearch.sql.expression.function.PPLBuiltinOperators.STDDEV_SAMP_NULLABLE;
          case "first" -> org.opensearch.sql.expression.function.PPLBuiltinOperators.FIRST;
          case "last" -> org.opensearch.sql.expression.function.PPLBuiltinOperators.LAST;
          case "take" -> org.opensearch.sql.expression.function.PPLBuiltinOperators.TAKE;
          case "list" -> org.opensearch.sql.expression.function.PPLBuiltinOperators.LIST;
          case "values" -> org.opensearch.sql.expression.function.PPLBuiltinOperators.VALUES;
          case "percentile", "percentile_approx", "median" ->
              org.opensearch.sql.expression.function.PPLBuiltinOperators.PERCENTILE_APPROX;
          default -> null;
        };
    if (pplAgg != null) {
      java.util.List<SqlNode> args = new java.util.ArrayList<>();
      args.add(arg);
      for (UnresolvedExpression ex : extraArgs) {
        // PPL passes extra args as named UnresolvedArgument(name, value); unwrap the value.
        if (ex instanceof org.opensearch.sql.ast.expression.UnresolvedArgument ua) {
          args.add(expr(ua.getValue()));
        } else {
          args.add(expr(ex));
        }
      }
      // PPL `median(field)` is sugar for percentile_approx(field, 50) — append the default
      // percentile when missing.
      if (name.equals("median") && args.size() == 1) {
        args.add(intLiteral(50));
      }
      // v2's PERCENTILE_APPROX dispatcher appends a SYMBOL flag holding the field's SqlTypeName
      // (rexBuilder.makeFlag(field.getType().getSqlTypeName())) so the runtime impl can return
      // the correct numeric type. Mirror that here for percentile/median: probe the field's
      // SqlTypeName via the row-type oracle and emit a SqlSymbolLiteral for it. Other aggregates
      // in this dispatch (var_*, stddev_*, first, last, etc.) keep their original arity.
      if (name.equals("percentile") || name.equals("percentile_approx") || name.equals("median")) {
        org.apache.calcite.sql.type.SqlTypeName fieldSqlType = probeSqlTypeName(arg);
        if (fieldSqlType != null) {
          args.add(SqlLiteral.createSymbol(fieldSqlType, POS));
        }
      }
      return new SqlBasicCall(pplAgg, args, POS, quantifier);
    }
    // Earliest / latest in PPL act as both scalar UDFs and stats aggregates. v2 uses
    // ARG_MIN(field, time_field) / ARG_MAX(field, time_field) when called inside `stats`.
    if (name.equals("earliest") || name.equals("latest")) {
      java.util.List<SqlNode> args = new java.util.ArrayList<>();
      args.add(arg);
      for (UnresolvedExpression ex : extraArgs) {
        if (ex instanceof org.opensearch.sql.ast.expression.UnresolvedArgument ua) {
          args.add(expr(ua.getValue()));
        } else {
          args.add(expr(ex));
        }
      }
      // Default the time-field to @timestamp when not supplied (matches v2's resolveTimeField).
      if (args.size() == 1) {
        args.add(
            new SqlIdentifier(
                org.opensearch.sql.calcite.plan.OpenSearchConstants.IMPLICIT_FIELD_TIMESTAMP, POS));
      }
      // In aggregate or window context, emit ARG_MIN/ARG_MAX (which are aggregates and work in
      // both Aggregate and Window contexts). In scalar context (e is Function inside an eval),
      // emit the PPL EARLIEST/LATEST scalar UDF.
      if (e instanceof AggregateFunction || windowed) {
        SqlOperator op =
            name.equals("earliest") ? SqlStdOperatorTable.ARG_MIN : SqlStdOperatorTable.ARG_MAX;
        return new SqlBasicCall(op, args, POS);
      }
      SqlOperator op =
          name.equals("earliest")
              ? org.opensearch.sql.expression.function.PPLBuiltinOperators.EARLIEST
              : org.opensearch.sql.expression.function.PPLBuiltinOperators.LATEST;
      return new SqlBasicCall(op, args, POS);
    }
    // Fallback: emit an unresolved-function call so the validator looks it up.
    java.util.List<SqlNode> allArgs = new java.util.ArrayList<>();
    allArgs.add(arg);
    for (UnresolvedExpression ex : extraArgs) {
      if (ex instanceof org.opensearch.sql.ast.expression.UnresolvedArgument ua) {
        allArgs.add(expr(ua.getValue()));
      } else {
        allArgs.add(expr(ex));
      }
    }
    return new SqlBasicCall(
        new org.apache.calcite.sql.SqlUnresolvedFunction(
            new SqlIdentifier(af.getFuncName(), POS),
            null,
            null,
            null,
            null,
            org.apache.calcite.sql.SqlFunctionCategory.USER_DEFINED_FUNCTION),
        allArgs,
        POS);
  }

  private SqlNode inSubqueryExpr(InSubquery is) {
    // Compile the subquery plan with a fresh visitor — it produces its own SqlSelect tree.
    // TODO: subsearch_maxout — neither top-level wrapping nor in-place FETCH on the subquery's
    // SqlSelect work. The in-place approach passes testSubsearchMaxOut but breaks
    // testSelfInSubquery + testFilterInSubquery (decorrelation re-orders or rewrites the
    // subquery and the FETCH ends up applied at the wrong level). Proper fix needs RelNode-level
    // injection mirroring v2's SubsearchUtils.SystemLimitInsertionShuttle.
    PplToSqlNode innerVisitor = new PplToSqlNode(rowTypeOracle, subsearchLimit);
    innerVisitor.joinScope = true;
    SqlNode subQuery = innerVisitor.visit(is.getQuery());
    // Pre-validate that the LHS column count matches the subquery output column count. Without
    // this check, Calcite raises a generic "Values passed to IN operator must have compatible
    // types" — but the v2 path's CalciteRexNodeVisitor explicitly throws SemanticCheckException
    // with a more informative column-count message. Match that shape so callers/tests that
    // pattern-match on the message keep working.
    if (rowTypeOracle != null) {
      try {
        int rhsCount = rowTypeOracle.apply(subQuery).getFieldList().size();
        if (rhsCount != is.getValue().size()) {
          throw new org.opensearch.sql.exception.SemanticCheckException(
              "The number of columns in the left hand side of an IN subquery does not match the"
                  + " number of columns in the output of subquery");
        }
      } catch (org.opensearch.sql.exception.SemanticCheckException sce) {
        throw sce;
      } catch (RuntimeException ignored) {
        // probe failed; defer to validator
      }
    }
    SqlNode left;
    if (is.getValue().size() == 1) {
      left = expr(is.getValue().get(0));
    } else {
      // Multi-column IN — wrap as a row.
      List<SqlNode> rowOperands = new ArrayList<>(is.getValue().size());
      for (UnresolvedExpression v : is.getValue()) {
        rowOperands.add(expr(v));
      }
      left = new SqlBasicCall(SqlStdOperatorTable.ROW, rowOperands, POS);
    }
    return new SqlBasicCall(SqlStdOperatorTable.IN, List.of(left, subQuery), POS);
  }

  private SqlNode inExpr(In in) {
    SqlNodeList values = new SqlNodeList(POS);
    for (UnresolvedExpression v : in.getValueList()) {
      values.add(expr(v));
    }
    SqlNode fieldExpr = expr(in.getField());
    // Pre-validate IN list type compatibility. Calcite's type coercion silently widens STRING to
    // NUMERIC for `where field IN (n1, n2, '<digit-str>')`, but PPL's v2 path raises a
    // SemanticCheckException ("In expression types are incompatible: fields type LONG, values type
    // [INTEGER, INTEGER, STRING]"). Mirror that here at translation time using the same
    // SqlTypeFamily check we use for BETWEEN. Skip if any value isn't a Literal (mixed
    // expressions).
    if (rowTypeOracle != null) {
      try {
        org.apache.calcite.rel.type.RelDataType fieldType = probeExprType(fieldExpr);
        org.apache.calcite.sql.type.SqlTypeFamily fieldFam = familyOf(fieldType);
        if (fieldFam != null) {
          List<org.apache.calcite.sql.type.SqlTypeFamily> valueFams = new ArrayList<>();
          List<org.opensearch.sql.data.type.ExprType> valueTypes = new ArrayList<>();
          boolean allLiterals = true;
          for (int i = 0; i < values.size(); i++) {
            SqlNode vNode = values.get(i);
            if (!(vNode instanceof SqlLiteral)) {
              allLiterals = false;
              break;
            }
            org.apache.calcite.rel.type.RelDataType vt = probeExprType(vNode);
            valueFams.add(familyOf(vt));
            valueTypes.add(
                org.opensearch.sql.calcite.utils.OpenSearchTypeFactory.convertRelDataTypeToExprType(
                    vt));
          }
          if (allLiterals) {
            boolean mixed = false;
            for (org.apache.calcite.sql.type.SqlTypeFamily vFam : valueFams) {
              if (vFam != fieldFam) {
                mixed = true;
                break;
              }
            }
            if (mixed) {
              org.opensearch.sql.data.type.ExprType fieldExprType =
                  org.opensearch.sql.calcite.utils.OpenSearchTypeFactory
                      .convertRelDataTypeToExprType(fieldType);
              throw new org.opensearch.sql.exception.SemanticCheckException(
                  String.format(
                      "In expression types are incompatible: fields type %s, values type %s",
                      fieldExprType, valueTypes));
            }
          }
        }
      } catch (org.opensearch.sql.exception.SemanticCheckException sce) {
        throw sce;
      } catch (RuntimeException ignoredIn) {
        // probe failed; defer to validator (which may pass via coercion)
      }
    }
    return new SqlBasicCall(SqlStdOperatorTable.IN, List.of(fieldExpr, values), POS);
  }

  /**
   * Format a Span AST as the user-visible alias: `span(<field>,<value><unit>)`. v2 uses this
   * auto-derived display name when no explicit alias is provided. Example: `span(birthdate, 1,
   * 'd')` → `span(birthdate,1d)`.
   */
  private static String formatSpanAlias(org.opensearch.sql.ast.expression.Span sp) {
    String fieldName = sp.getField().toString();
    Object valueObj =
        (sp.getValue() instanceof org.opensearch.sql.ast.expression.Literal vl)
            ? vl.getValue()
            : sp.getValue().toString();
    String unitName = org.opensearch.sql.ast.expression.SpanUnit.getName(sp.getUnit());
    return "span(" + fieldName + "," + valueObj + unitName + ")";
  }

  private SqlNode caseExpr(Case c) {
    SqlNodeList whens = new SqlNodeList(POS);
    SqlNodeList thens = new SqlNodeList(POS);
    for (org.opensearch.sql.ast.expression.When w : c.getWhenClauses()) {
      whens.add(expr(w.getCondition()));
      thens.add(charToVarchar(expr(w.getResult())));
    }
    SqlNode elseNode =
        c.getElseClause().map(e -> charToVarchar(expr(e))).orElse(SqlLiteral.createNull(POS));
    SqlNode caseValue = c.getCaseValue() == null ? null : expr(c.getCaseValue());
    return new org.apache.calcite.sql.fun.SqlCase(POS, caseValue, whens, thens, elseNode);
  }

  /**
   * Cast CHAR-string literals to VARCHAR so CASE THEN branches of different literal lengths don't
   * get CHAR-padded by Calcite's least-restrictive common-type computation. PPL semantic: string
   * literals are VARCHAR, no padding.
   */
  private SqlNode charToVarchar(SqlNode n) {
    if (n instanceof SqlLiteral lit
        && lit.getTypeName() == org.apache.calcite.sql.type.SqlTypeName.CHAR) {
      return new SqlBasicCall(
          SqlStdOperatorTable.CAST,
          List.of(
              n,
              new org.apache.calcite.sql.SqlDataTypeSpec(
                  new org.apache.calcite.sql.SqlBasicTypeNameSpec(
                      org.apache.calcite.sql.type.SqlTypeName.VARCHAR, POS),
                  POS)),
          POS);
    }
    return n;
  }

  private SqlNode func(Function f) {
    // TODO: coalesce(missing_field, ...) — v2 has special handling
    // (QualifiedNameResolver.replaceWithNullLiteralInCoalesce, issue #5175) that returns NULL for
    // unknown fields inside coalesce. Tried CAST(NULL AS VARCHAR) substitution at this point but
    // it forces VARCHAR result type for the whole coalesce, breaking testCoalesceNested
    // (numeric→string coercion). Untyped SqlLiteral.createNull(POS) gets reinterpreted as
    // "Field [null]" by the pushdown layer. Proper fix needs a NULL token that defers to the
    // common-type computation downstream — possibly a CASE WHEN false THEN <null-cast-to-something>
    // OR a SqlLiteral with SqlTypeName.NULL preserved through unparse. Deferred.
    // mvmap/transform: when the lambda body references names from the OUTER scope (e.g.
    // `mvmap(arr, arr * multiplier)`), the SqlLambda validator only sees the lambda params and
    // rejects the outer ref as "Param 'multiplier' not found". Mirror v2's CalcitePlanContext
    // captured-variable mechanism: collect outer-scope name references from the lambda body, add
    // them to the lambda's parameter list, and append them as additional outer arguments to the
    // function call. TransformFunctionImpl.eval then passes the outer value as args[2..] to the
    // lambda. Only mvmap/transform currently support this (matching v2's branch).
    String fnNameLower =
        f.getFuncName() == null ? "" : f.getFuncName().toLowerCase(java.util.Locale.ROOT);
    if ((fnNameLower.equals("mvmap") || fnNameLower.equals("transform"))
        && f.getFuncArgs().size() >= 2
        && f.getFuncArgs().get(1) instanceof org.opensearch.sql.ast.expression.LambdaFunction lf) {
      java.util.Set<String> declared = new java.util.LinkedHashSet<>();
      for (QualifiedName qn : lf.getFuncArgs()) declared.add(qn.toString());
      java.util.LinkedHashMap<String, QualifiedName> captured = new java.util.LinkedHashMap<>();
      collectCapturedRefs(lf.getFunction(), declared, captured);
      if (!captured.isEmpty()) {
        java.util.List<QualifiedName> newLambdaArgs = new ArrayList<>(lf.getFuncArgs());
        newLambdaArgs.addAll(captured.values());
        org.opensearch.sql.ast.expression.LambdaFunction extendedLambda =
            new org.opensearch.sql.ast.expression.LambdaFunction(lf.getFunction(), newLambdaArgs);
        java.util.List<UnresolvedExpression> newOuterArgs = new ArrayList<>();
        newOuterArgs.add(f.getFuncArgs().get(0));
        newOuterArgs.add(extendedLambda);
        for (QualifiedName qn : captured.values()) newOuterArgs.add(qn);
        Function rewritten = new Function(f.getFuncName(), newOuterArgs);
        return func(rewritten);
      }
    }
    List<SqlNode> args = new ArrayList<>(f.getFuncArgs().size());
    for (UnresolvedExpression a : f.getFuncArgs()) {
      args.add(expr(a));
    }
    // Cast bare string-literal args to VARCHAR for UDF calls — v2's RexBuilder defaults to
    // VARCHAR for makeCharLiteral, so explain plans show `'...':VARCHAR` for literal args. Skip
    // for functions whose pushdown semantics rely on CHAR-typed pattern matching (regex/like/
    // replace/fillnull/transpose) — those keep CHAR. Skip arithmetic/comparison operators
    // (handled below where the operands aren't function arg lists).
    String fnLowerForCast =
        f.getFuncName() == null ? "" : f.getFuncName().toLowerCase(java.util.Locale.ROOT);
    if (!FUNC_NAMES_KEEP_CHAR_LITERAL.contains(fnLowerForCast)) {
      for (int i = 0; i < args.size(); i++) {
        UnresolvedExpression a = f.getFuncArgs().get(i);
        if (a instanceof Literal lit && lit.getType() == DataType.STRING) {
          args.set(i, charToVarchar(args.get(i)));
        }
      }
    }
    // v2 coerces a bare string literal first-arg into TIMESTAMP() for date-part extraction
    // functions (week/weekofyear/yearweek). The PPL UDFs accept STRING but emit explain plans
    // showing the explicit TIMESTAMP wrap. Mirror that emission here so explain output matches.
    if (!args.isEmpty()
        && f.getFuncArgs().get(0) instanceof Literal lit
        && lit.getType() == DataType.STRING) {
      String fnLowerForWrap = fnLowerForCast;
      boolean wrapTs =
          fnLowerForWrap.equals("week")
              || fnLowerForWrap.equals("weekofyear")
              || fnLowerForWrap.equals("week_of_year")
              || fnLowerForWrap.equals("yearweek");
      if (wrapTs) {
        args.set(
            0,
            new SqlBasicCall(
                org.opensearch.sql.expression.function.PPLBuiltinOperators.TIMESTAMP,
                List.of(args.get(0)),
                POS));
      }
    }
    SqlOperator op = arithmeticOperator(f.getFuncName());
    if (op != null) {
      // PPL overloads `+` as both numeric addition and string concatenation. When any operand is
      // statically a string (literal or CAST(... AS STRING)), emit a CONCAT (`||`) call so the
      // validator picks the string-concat overload. Calcite's PLUS does not support strings.
      if (op == SqlStdOperatorTable.PLUS && hasStringOperand(f.getFuncArgs())) {
        return new SqlBasicCall(SqlStdOperatorTable.CONCAT, args, POS);
      }
      return new SqlBasicCall(op, args, POS);
    }
    // PPL parses several SQL operators as Function calls. Bind them to standard SqlOperators
    // directly so the validator doesn't have to resolve them by name (which fails because PPL
    // gives them lowercase quoted names that don't match the SqlStdOperatorTable's identifiers).
    String fnName = f.getFuncName().toLowerCase(java.util.Locale.ROOT);
    // Inside a lambda body, params are typed ANY at validate time and standard CHAR_LENGTH's
    // operand checker rejects ANY. Mirror v2's LambdaUtils.inferReturnTypeFromLambda which
    // re-infers types from the array element/captured types. We can't re-infer here, but the
    // common case is `length(<lambda-param>)` over a string array; CAST the operand to VARCHAR
    // and route to LIBRARY.LENGTH (BigQuery), which accepts STRING_TYPES (CHAR/VARCHAR), unlike
    // the standard CHAR_LENGTH whose checker is stricter and rejects bare CHARACTER expressions.
    if (lambdaDepth > 0 && fnName.equals("length") && args.size() == 1) {
      SqlNode casted = castTo(args.get(0), org.apache.calcite.sql.type.SqlTypeName.VARCHAR);
      return new SqlBasicCall(
          org.apache.calcite.sql.fun.SqlLibraryOperators.LENGTH, List.of(casted), POS);
    }
    // mvindex(array, index [, end]) — Calcite has no exact 3-arg form; map 2-arg to ITEM and
    // 3-arg to ARRAY_SLICE(array, start, end).
    if (fnName.equals("mvindex")) {
      // PPL mvindex is 0-based with negative indexing from end (-1 = last). Calcite ITEM is
      // 1-based and rejects negatives. Mirror MVIndexFunctionImp's normalization in SqlNode form:
      //   single-elem: ITEM(arr, idx<0 ? len+idx+1 : idx+1)
      //   range:       ARRAY_SLICE(arr, normStart, normEnd-normStart+1)  where neg → len+idx
      SqlNode arr = args.get(0);
      SqlNode arrLen =
          new SqlBasicCall(
              org.apache.calcite.sql.fun.SqlLibraryOperators.ARRAY_LENGTH, List.of(arr), POS);
      SqlNode startIdx = castTo(args.get(1), org.apache.calcite.sql.type.SqlTypeName.INTEGER);
      SqlNode zero = intLiteral(0);
      SqlNode one = intLiteral(1);
      if (args.size() == 2) {
        // index<0 ? len+index+1 : index+1
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
      } else if (args.size() == 3) {
        SqlNode endIdx = castTo(args.get(2), org.apache.calcite.sql.type.SqlTypeName.INTEGER);
        // normStart = start<0 ? len+start : start; normEnd = end<0 ? len+end : end
        SqlNode startNeg =
            new SqlBasicCall(SqlStdOperatorTable.LESS_THAN, List.of(startIdx, zero), POS);
        SqlNode startNegCase =
            new SqlBasicCall(SqlStdOperatorTable.PLUS, List.of(arrLen, startIdx), POS);
        SqlNodeList sw = new SqlNodeList(POS);
        sw.add(startNeg);
        SqlNodeList st = new SqlNodeList(POS);
        st.add(startNegCase);
        SqlNode normStart = new org.apache.calcite.sql.fun.SqlCase(POS, null, sw, st, startIdx);
        SqlNode endNeg =
            new SqlBasicCall(SqlStdOperatorTable.LESS_THAN, List.of(endIdx, zero), POS);
        SqlNode endNegCase =
            new SqlBasicCall(SqlStdOperatorTable.PLUS, List.of(arrLen, endIdx), POS);
        SqlNodeList ew = new SqlNodeList(POS);
        ew.add(endNeg);
        SqlNodeList et = new SqlNodeList(POS);
        et.add(endNegCase);
        SqlNode normEnd = new org.apache.calcite.sql.fun.SqlCase(POS, null, ew, et, endIdx);
        SqlNode diff =
            new SqlBasicCall(SqlStdOperatorTable.MINUS, List.of(normEnd, normStart), POS);
        SqlNode len = new SqlBasicCall(SqlStdOperatorTable.PLUS, List.of(diff, one), POS);
        return new SqlBasicCall(
            org.apache.calcite.sql.fun.SqlLibraryOperators.ARRAY_SLICE,
            List.of(arr, normStart, len),
            POS);
      }
    }
    // PPL Like(field, pattern [, caseSensitive]):
    //   - 3-arg form chooses LIKE vs ILIKE based on the boolean.
    //   - 2-arg form picks ILIKE in legacy-preferred mode (default OpenSearch PPL semantics) and
    //     LIKE otherwise. v2 path's CalciteRexNodeVisitor injects a synthetic 3rd arg that depends
    //     on isLegacyPreferred(); we mirror that here at SqlNode time.
    // PPL has a few helper functions whose v2 implementation is hand-coded in PPLFuncImpTable
    // (no SqlOperator counterpart), so we must desugar them at SqlNode time. They are usually
    // simple compositions of standard operators.
    if (fnName.equals("isempty") && args.size() == 1) {
      // PPL semantics: NULL or empty string. v2 emits `arg IS NULL OR IS_EMPTY(arg)` via
      // RexBuilder.makeCall(SqlStdOperatorTable.IS_EMPTY, arg) which bypasses validation.
      // Calcite's standard IS_EMPTY only accepts collections at the SqlValidator level;
      // emit through PERMISSIVE_IS_EMPTY (a postfix op that accepts ANY operand and produces
      // identical "IS EMPTY" explain output).
      SqlNode a = args.get(0);
      return new SqlBasicCall(
          SqlStdOperatorTable.OR,
          List.of(
              new SqlBasicCall(SqlStdOperatorTable.IS_NULL, List.of(a), POS),
              new SqlBasicCall(PERMISSIVE_IS_EMPTY, List.of(a), POS)),
          POS);
    }
    if (fnName.equals("replace") && args.size() == 3) {
      // PPL `replace(str, pattern, replacement)` is a REGEX replace (PCRE-style). Calcite's std
      // REPLACE does literal substring replace, not regex. Dispatch to REGEXP_REPLACE_3.
      // PPL uses \1 \2 for backreferences; Calcite (Java regex) uses $1 $2. Convert literal
      // string replacements at translation time. For non-literal replacement expressions, leave
      // as-is (caller must use $N — best-effort only).
      // Validate pattern at translation time so syntax errors surface as 400 Bad Request
      // (IllegalArgumentException) rather than 500 from runtime PatternSyntaxException. Mirrors
      // PPLFuncImpTable.java#760-776.
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
        // Convert PPL's \1, \2, ... -> Java's $1, $2, ...; preserve literal '$'.
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
    if (fnName.equals("strcmp") && args.size() == 2) {
      // PPL strcmp(s1, s2) follows MySQL: returns -1 if s1<s2, 0 if equal, 1 if s1>s2.
      // Express via CASE rather than relying on Calcite's STRCMP (sign convention differs).
      SqlNode a = args.get(0);
      SqlNode b = args.get(1);
      SqlNodeList whens = new SqlNodeList(POS);
      whens.add(new SqlBasicCall(SqlStdOperatorTable.LESS_THAN, List.of(a, b), POS));
      whens.add(new SqlBasicCall(SqlStdOperatorTable.EQUALS, List.of(a, b), POS));
      SqlNodeList thens = new SqlNodeList(POS);
      thens.add(intLiteral(-1));
      thens.add(intLiteral(0));
      return new org.apache.calcite.sql.fun.SqlCase(POS, null, whens, thens, intLiteral(1));
    }
    if (fnName.equals("trim") && args.size() == 1) {
      // PPL `trim(str)` -> SQL `TRIM(BOTH ' ' FROM str)`. Calcite's TRIM is keyword-syntax;
      // building it via SqlBasicCall doesn't resolve through normal function lookup. Use
      // REGEXP_REPLACE to strip leading and trailing whitespace.
      return new SqlBasicCall(
          org.apache.calcite.sql.fun.SqlLibraryOperators.REGEXP_REPLACE_3,
          List.of(
              args.get(0),
              SqlLiteral.createCharString("^\\s+|\\s+$", POS),
              SqlLiteral.createCharString("", POS)),
          POS);
    }
    if (fnName.equals("locate") && (args.size() == 2 || args.size() == 3)) {
      // PPL `locate(sub, str [, start])` -> Calcite's INSTR(str, sub [, start [, occurrence]]).
      // POSITION uses keyword syntax which doesn't bind via SqlBasicCall function-call form;
      // INSTR (Oracle library) is positional and accepts the optional start arg.
      List<SqlNode> swapped = new ArrayList<>();
      swapped.add(args.get(1));
      swapped.add(args.get(0));
      if (args.size() == 3) swapped.add(args.get(2));
      return new SqlBasicCall(org.apache.calcite.sql.fun.SqlLibraryOperators.INSTR, swapped, POS);
    }
    if (fnName.equals("split") && args.size() == 2) {
      // PPL semantics: split(str, '') returns each character as a separate element.
      // Calcite's std SPLIT(str, '') returns a single-element array with the whole string.
      // Mirror PPLFuncImpTable's logic: CASE WHEN delim='' THEN REGEXP_EXTRACT_ALL(str, '.')
      //                                                     ELSE SPLIT(str, delim)
      SqlNode str = args.get(0);
      SqlNode delim = args.get(1);
      SqlNode emptyLit = SqlLiteral.createCharString("", POS);
      SqlNode dotLit = SqlLiteral.createCharString(".", POS);
      SqlNode isEmpty = new SqlBasicCall(SqlStdOperatorTable.EQUALS, List.of(delim, emptyLit), POS);
      SqlNode splitChars =
          new SqlBasicCall(
              org.apache.calcite.sql.fun.SqlLibraryOperators.REGEXP_EXTRACT_ALL,
              List.of(str, dotLit),
              POS);
      SqlNode normalSplit =
          new SqlBasicCall(
              org.apache.calcite.sql.fun.SqlLibraryOperators.SPLIT, List.of(str, delim), POS);
      SqlNodeList whens = new SqlNodeList(POS);
      whens.add(isEmpty);
      SqlNodeList thens = new SqlNodeList(POS);
      thens.add(splitChars);
      return new org.apache.calcite.sql.fun.SqlCase(POS, null, whens, thens, normalSplit);
    }
    if (fnName.equals("isblank") && args.size() == 1) {
      // PPL semantics: NULL or only whitespace. v2 emits `arg IS NULL OR IS_EMPTY(TRIM(BOTH ' '
      // FROM arg))` via RexBuilder, but at the SqlNode level the validator rejects TRIM with
      // a (SYMBOL, CHAR, CHAR) signature passed via SqlBasicCall (the keyword TRIM(BOTH ' ' FROM
      // expr) parses through grammar, not function-call shape).
      //
      // Workaround: emit `LENGTH(REPLACE(arg, ' ', '')) = 0` here, then post-convert the
      // resulting RexCall in SqlNodePlanner's post-conversion shuttle (`rewriteIsBlankToTrimEmpty`)
      // to v2's `IS_EMPTY(TRIM(BOTH, ' ', arg))` shape.
      SqlNode a = args.get(0);
      SqlNode replaced =
          new SqlBasicCall(
              SqlStdOperatorTable.REPLACE,
              List.of(
                  a, SqlLiteral.createCharString(" ", POS), SqlLiteral.createCharString("", POS)),
              POS);
      SqlNode lenZero =
          new SqlBasicCall(
              SqlStdOperatorTable.EQUALS,
              List.of(
                  new SqlBasicCall(
                      org.apache.calcite.sql.fun.SqlLibraryOperators.LENGTH,
                      List.of(replaced),
                      POS),
                  intLiteral(0)),
              POS);
      return new SqlBasicCall(
          SqlStdOperatorTable.OR,
          List.of(new SqlBasicCall(SqlStdOperatorTable.IS_NULL, List.of(a), POS), lenZero),
          POS);
    }
    if (fnName.equals("ispresent") && args.size() == 1) {
      return new SqlBasicCall(SqlStdOperatorTable.IS_NOT_NULL, args, POS);
    }
    if (fnName.equals("typeof") && args.size() == 1) {
      // PPL `typeof(expr)` returns the legacy/PPL type name of expr as a literal string.
      // The v2 path computes this in PPLFuncImpTable at build time
      // (`builder.makeLiteral(getLegacyTypeName(arg.getType(), QueryType.PPL))`); the SqlNode
      // path doesn't go through that table, so we have to do the same expansion here.
      // Probe the validator to find the expression's type, then emit a string literal.
      String legacyName = probeTypeName(args.get(0));
      return SqlLiteral.createCharString(legacyName, POS);
    }
    if (fnName.equals("nullif") && args.size() == 2) {
      return new SqlBasicCall(SqlStdOperatorTable.NULLIF, args, POS);
    }
    // PPL json_object('k', v, 'k2', v2, ...) → JSON_OBJECT(NULL ON NULL, 'k', v, ...).
    // Calcite's JSON_OBJECT requires the leading null-behavior flag.
    // PPL semantic: nested json_object values are serialized as JSON strings (not nested objects)
    // — testJsonObject asserts {"outer":"{\"inner\":...}"} not {"outer":{"inner":...}}.
    // Wrap each value (odd-indexed in user args) that comes from a nested json_object/json_array
    // call with CAST AS VARCHAR to force string serialization.
    if (fnName.equals("json_object")) {
      List<SqlNode> jsonArgs = new ArrayList<>();
      jsonArgs.add(
          SqlLiteral.createSymbol(
              org.apache.calcite.sql.SqlJsonConstructorNullClause.NULL_ON_NULL, POS));
      List<UnresolvedExpression> userArgs = f.getFuncArgs();
      for (int i = 0; i < args.size(); i++) {
        SqlNode a = args.get(i);
        boolean isValuePos = (i % 2) == 1;
        boolean nestedJsonCtor =
            i < userArgs.size()
                && userArgs.get(i) instanceof Function fn
                && (fn.getFuncName().equalsIgnoreCase("json_object")
                    || fn.getFuncName().equalsIgnoreCase("json_array"));
        if (isValuePos && nestedJsonCtor) {
          a =
              new SqlBasicCall(
                  SqlStdOperatorTable.CAST,
                  List.of(
                      a,
                      new org.apache.calcite.sql.SqlDataTypeSpec(
                          new org.apache.calcite.sql.SqlBasicTypeNameSpec(
                              org.apache.calcite.sql.type.SqlTypeName.VARCHAR, POS),
                          POS)),
                  POS);
        }
        jsonArgs.add(a);
      }
      return new SqlBasicCall(SqlStdOperatorTable.JSON_OBJECT, jsonArgs, POS);
    }
    // PPL json_array(v, v2, ...) → JSON_ARRAY(NULL ON NULL, v, v2, ...).
    // Same nested-stringification rule as json_object: nested json_object/json_array values are
    // serialized as JSON strings.
    if (fnName.equals("json_array")) {
      List<SqlNode> jsonArgs = new ArrayList<>();
      jsonArgs.add(
          SqlLiteral.createSymbol(
              org.apache.calcite.sql.SqlJsonConstructorNullClause.NULL_ON_NULL, POS));
      List<UnresolvedExpression> userArgs = f.getFuncArgs();
      for (int i = 0; i < args.size(); i++) {
        SqlNode a = args.get(i);
        boolean nestedJsonCtor =
            i < userArgs.size()
                && userArgs.get(i) instanceof Function fn
                && (fn.getFuncName().equalsIgnoreCase("json_object")
                    || fn.getFuncName().equalsIgnoreCase("json_array"));
        if (nestedJsonCtor) {
          a =
              new SqlBasicCall(
                  SqlStdOperatorTable.CAST,
                  List.of(
                      a,
                      new org.apache.calcite.sql.SqlDataTypeSpec(
                          new org.apache.calcite.sql.SqlBasicTypeNameSpec(
                              org.apache.calcite.sql.type.SqlTypeName.VARCHAR, POS),
                          POS)),
                  POS);
        }
        jsonArgs.add(a);
      }
      return new SqlBasicCall(SqlStdOperatorTable.JSON_ARRAY, jsonArgs, POS);
    }
    // PPL json_valid(s) → IS_JSON_VALUE(s).
    if (fnName.equals("json_valid") && args.size() == 1) {
      return new SqlBasicCall(SqlStdOperatorTable.IS_JSON_VALUE, args, POS);
    }
    // PPL json_keys: dispatch to PPLBuiltinOperators.JSON_KEYS (custom impl that returns null for
    // non-object input). Calcite's stock JSON_KEYS would otherwise be picked by the validator and
    // returns "null" (string) instead of literal null for arrays.
    if (fnName.equals("json_keys") && args.size() == 1) {
      return new SqlBasicCall(
          org.opensearch.sql.expression.function.PPLBuiltinOperators.JSON_KEYS, args, POS);
    }
    // PPL json_extract: dispatch to PPLBuiltinOperators.JSON_EXTRACT.
    if (fnName.equals("json_extract")) {
      return new SqlBasicCall(
          org.opensearch.sql.expression.function.PPLBuiltinOperators.JSON_EXTRACT, args, POS);
    }
    // PPL json_array_length: dispatch.
    if (fnName.equals("json_array_length") && args.size() == 1) {
      return new SqlBasicCall(
          org.opensearch.sql.expression.function.PPLBuiltinOperators.JSON_ARRAY_LENGTH, args, POS);
    }
    // PPL json_set/append/extend/delete — dispatch to PPL UDF variants which apply the path
    // expansion (`a{}.b` → `$.a[*].b` etc.) before calling Calcite's JsonFunctions.
    if (fnName.equals("json_set")) {
      return new SqlBasicCall(
          org.opensearch.sql.expression.function.PPLBuiltinOperators.JSON_SET, args, POS);
    }
    if (fnName.equals("json_append")) {
      return new SqlBasicCall(
          org.opensearch.sql.expression.function.PPLBuiltinOperators.JSON_APPEND, args, POS);
    }
    if (fnName.equals("json_extend")) {
      return new SqlBasicCall(
          org.opensearch.sql.expression.function.PPLBuiltinOperators.JSON_EXTEND, args, POS);
    }
    if (fnName.equals("json_delete")) {
      return new SqlBasicCall(
          org.opensearch.sql.expression.function.PPLBuiltinOperators.JSON_DELETE, args, POS);
    }
    // Niladic datetime functions — PPL parses `localtime()`, `current_date()`, etc. as Function
    // calls with empty args, but Calcite's SqlStdOperatorTable defines LOCALTIME/CURRENT_DATE as
    // niladic SqlOperators that don't accept the SqlBasicCall(empty-args) shape. Dispatch to
    // PPLBuiltinOperators' UDF variants which mirror v2's PPLFuncImpTable mappings
    // (PPLFuncImpTable.java#954-961).
    if (args.isEmpty()) {
      switch (fnName) {
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
    if (fnName.equals("coalesce")) {
      // PPL `coalesce(null, ...)` and `coalesce(nonexistent_field, ...)`: replace unresolved
      // QualifiedName operands (literal `null` parses as a Field "null"; missing fields fail
      // validator lookup) with SqlLiteral.createNull. Mirrors v2's QualifiedNameResolver
      // behavior when isInCoalesceFunction()=true (see #5175).
      java.util.List<SqlNode> coalesceArgs = new java.util.ArrayList<>(args.size());
      java.util.List<UnresolvedExpression> rawArgs = f.getFuncArgs();
      java.util.Set<String> knownFields = null;
      if (rowTypeOracle != null && currentFrom != null) {
        try {
          knownFields = new java.util.HashSet<>(deriveColumnNames(currentFrom));
        } catch (RuntimeException ignoredCoa) {
          // probe failed; fall through and trust args as-is
        }
      }
      for (int i = 0; i < args.size(); i++) {
        SqlNode argNode = args.get(i);
        UnresolvedExpression rawArg = i < rawArgs.size() ? rawArgs.get(i) : null;
        boolean replaceWithNull = false;
        QualifiedName qn = null;
        if (rawArg instanceof QualifiedName q) {
          qn = q;
        } else if (rawArg instanceof Field fld && fld.getField() instanceof QualifiedName q) {
          qn = q;
        }
        if (qn != null) {
          String name = qn.toString();
          if ("null".equalsIgnoreCase(name)) {
            replaceWithNull = true;
          } else if (knownFields != null && !knownFields.contains(name)) {
            replaceWithNull = true;
          }
        }
        coalesceArgs.add(replaceWithNull ? SqlLiteral.createNull(POS) : argNode);
      }
      return new SqlBasicCall(SqlStdOperatorTable.COALESCE, coalesceArgs, POS);
    }
    if (fnName.equals("ifnull") && args.size() == 2) {
      return new SqlBasicCall(SqlStdOperatorTable.COALESCE, args, POS);
    }
    // Cast the regex pattern to VARCHAR explicitly. Without the cast, Calcite emits a CHAR-typed
    // literal, which doesn't match v2's emission shape (REGEXP_CONTAINS($0, 'pat':VARCHAR)).
    if ((fnName.equals("regexp_match") || fnName.equals("regexp")) && args.size() == 2) {
      SqlNode pattern = args.get(1);
      if (pattern instanceof SqlLiteral lit
          && lit.getTypeName() == org.apache.calcite.sql.type.SqlTypeName.CHAR) {
        pattern = castTo(pattern, org.apache.calcite.sql.type.SqlTypeName.VARCHAR);
      }
      return new SqlBasicCall(
          org.apache.calcite.sql.fun.SqlLibraryOperators.REGEXP_CONTAINS,
          List.of(args.get(0), pattern),
          POS);
    }
    // PPL has functional names for the binary math operators (PPL grammar exposes both `+` and
    // `add`, etc.). Map them to the standard arithmetic operators directly.
    if (fnName.equals("add") && args.size() == 2) {
      if (hasStringOperand(f.getFuncArgs())) {
        return new SqlBasicCall(SqlStdOperatorTable.CONCAT, args, POS);
      }
      return new SqlBasicCall(SqlStdOperatorTable.PLUS, args, POS);
    }
    if (fnName.equals("subtract") && args.size() == 2) {
      return new SqlBasicCall(SqlStdOperatorTable.MINUS, args, POS);
    }
    if (fnName.equals("multiply") && args.size() == 2) {
      return new SqlBasicCall(SqlStdOperatorTable.MULTIPLY, args, POS);
    }
    if (fnName.equals("divide") && args.size() == 2) {
      return new SqlBasicCall(
          org.opensearch.sql.expression.function.PPLBuiltinOperators.DIVIDE, args, POS);
    }
    if (fnName.equals("modulus") && args.size() == 2) {
      return new SqlBasicCall(
          org.opensearch.sql.expression.function.PPLBuiltinOperators.MOD, args, POS);
    }
    if (fnName.equals("mod") && args.size() == 2) {
      // PPL MOD has semantics for divide-by-zero (returns NULL) and wider-type promotion that
      // SqlStdOperatorTable.MOD doesn't match. Bind directly to PPLBuiltinOperators.MOD.
      return new SqlBasicCall(
          org.opensearch.sql.expression.function.PPLBuiltinOperators.MOD, args, POS);
    }
    if (fnName.equals("atan") && args.size() == 2) {
      // Two-arg atan(y, x) is ATAN2 in standard SQL.
      return new SqlBasicCall(SqlStdOperatorTable.ATAN2, args, POS);
    }
    // PPL log(base, x) — Calcite's LOG signature is LOG(x, base). Swap operand order. Single-arg
    // log(x) is the natural log; resolve through normal name lookup against SqlLibraryOperators.
    if (fnName.equals("log") && args.size() == 2) {
      return new SqlBasicCall(
          org.apache.calcite.sql.fun.SqlLibraryOperators.LOG,
          List.of(args.get(1), args.get(0)),
          POS);
    }
    // PPL conv(num, fromBase, toBase) accepts a numeric first arg. Bind directly to
    // PPLBuiltinOperators.CONV (the real ConvFunction UDF) — going through name lookup ends up
    // matching SqlStdOperatorTable.CONVERT (charset conversion), which signals an
    // UnsupportedCharsetException at validate time.
    if (fnName.equals("conv") && args.size() == 3) {
      // Cast first arg to VARCHAR (PPL accepts numeric input). Use SqlUnresolvedFunction with
      // a PPL-specific lookup name to avoid clashing with SqlStdOperatorTable.CONVERT (which
      // has charset semantics and triggers UnsupportedCharsetException). The chained operator
      // table will resolve "PPL_CONV" to PPLBuiltinOperators.CONV via the case-insensitive
      // matcher; PPLBuiltinOperators registers CONV under name "CONVERT" though, so the
      // simplest reliable path here is to bind the SqlBasicCall directly to the operator
      // instance with no name lookup involved at convert-to-rex time.
      org.opensearch.sql.expression.function.PPLBuiltinOperators.instance(); // ensure init
      return new SqlBasicCall(
          org.opensearch.sql.expression.function.PPLBuiltinOperators.CONV,
          List.of(
              castTo(args.get(0), org.apache.calcite.sql.type.SqlTypeName.VARCHAR),
              args.get(1),
              args.get(2)),
          POS);
    }
    if (fnName.equals("like")) {
      // v2 always passes a literal '\' as the escape character so that PPL queries like
      // `Like(x, '\\%test wildcard%')` can use `\%` to match a literal percent. Mirror that.
      SqlNode escape = SqlLiteral.createCharString("\\", POS);
      if (args.size() == 3) {
        SqlNode third = args.get(2);
        boolean caseSensitive =
            !(third instanceof SqlLiteral lit) || !Boolean.FALSE.equals(lit.getValue());
        SqlOperator likeOp =
            caseSensitive
                ? SqlStdOperatorTable.LIKE
                : org.apache.calcite.sql.fun.SqlLibraryOperators.ILIKE;
        return new SqlBasicCall(likeOp, List.of(args.get(0), args.get(1), escape), POS);
      }
      if (args.size() == 2) {
        boolean caseSensitive =
            !Boolean.TRUE.equals(org.opensearch.sql.calcite.CalcitePlanContext.isLegacyPreferred());
        SqlOperator likeOp =
            caseSensitive
                ? SqlStdOperatorTable.LIKE
                : org.apache.calcite.sql.fun.SqlLibraryOperators.ILIKE;
        return new SqlBasicCall(likeOp, List.of(args.get(0), args.get(1), escape), POS);
      }
    }
    // PPL's keyword-form ILIKE / NOT LIKE / NOT ILIKE: append the literal '\' escape char like v2
    // so backslash-escaped wildcards work consistently. The 2-arg path goes through the
    // arithmeticOperator dispatch above; intercept here before the operator-name lookup falls
    // through to the std operator-table case branch (which would emit the 2-arg form without
    // escape, breaking the OpenSearch pushdown's wildcard-with-escape semantics).
    if ((fnName.equals("ilike")
            || fnName.equals("not like")
            || fnName.equals("not_like")
            || fnName.equals("not ilike")
            || fnName.equals("not_ilike"))
        && args.size() == 2) {
      SqlOperator likeOp =
          switch (fnName) {
            case "ilike" -> org.apache.calcite.sql.fun.SqlLibraryOperators.ILIKE;
            case "not like", "not_like" -> SqlStdOperatorTable.NOT_LIKE;
            case "not ilike", "not_ilike" ->
                org.apache.calcite.sql.fun.SqlLibraryOperators.NOT_ILIKE;
            default -> SqlStdOperatorTable.LIKE;
          };
      return new SqlBasicCall(
          likeOp, List.of(args.get(0), args.get(1), SqlLiteral.createCharString("\\", POS)), POS);
    }
    SqlOperator builtin =
        switch (fnName) {
          case "is null", "isnull" -> SqlStdOperatorTable.IS_NULL;
          case "is not null", "isnotnull", "is_not_null" -> SqlStdOperatorTable.IS_NOT_NULL;
          case "like" -> SqlStdOperatorTable.LIKE;
          case "not like", "not_like" -> SqlStdOperatorTable.NOT_LIKE;
          case "ilike" -> org.apache.calcite.sql.fun.SqlLibraryOperators.ILIKE;
          case "not ilike", "not_ilike" -> org.apache.calcite.sql.fun.SqlLibraryOperators.NOT_ILIKE;
          case "regexp_match", "regexp" ->
              org.apache.calcite.sql.fun.SqlLibraryOperators.REGEXP_CONTAINS;
          case "between" -> SqlStdOperatorTable.BETWEEN;
          case "not between", "not_between" -> SqlStdOperatorTable.NOT_BETWEEN;
          case "mod", "modulus", "modulusfunction" -> SqlStdOperatorTable.MOD;
          case "abs" -> SqlStdOperatorTable.ABS;
          case "ceil", "ceiling" -> SqlStdOperatorTable.CEIL;
          case "floor" -> SqlStdOperatorTable.FLOOR;
          case "round" -> SqlStdOperatorTable.ROUND;
          case "trunc", "truncate" -> SqlStdOperatorTable.TRUNCATE;
          case "exp" -> SqlStdOperatorTable.EXP;
          case "ln" -> SqlStdOperatorTable.LN;
          case "log10" -> SqlStdOperatorTable.LOG10;
          case "sqrt" -> SqlStdOperatorTable.SQRT;
          case "pi" -> SqlStdOperatorTable.PI;
          case "rand" -> SqlStdOperatorTable.RAND;
          case "pow", "power" -> SqlStdOperatorTable.POWER;
          case "lower" -> SqlStdOperatorTable.LOWER;
          case "upper" -> SqlStdOperatorTable.UPPER;
          case "trim" -> SqlStdOperatorTable.TRIM;
          // PPL concat is variadic: concat(a, b, c, ...). Calcite's std CONCAT is binary `||`;
          // use SqlLibraryOperators.CONCAT_FUNCTION which accepts arbitrary arity.
          case "concat" -> org.apache.calcite.sql.fun.SqlLibraryOperators.CONCAT_FUNCTION;
          case "substring", "substr" -> SqlStdOperatorTable.SUBSTRING;
          case "replace" -> SqlStdOperatorTable.REPLACE;
          // Date-part extractors: use PPLBuiltinOperators' UDFs (which handle OpenSearch
          // EXPR_DATE/EXPR_TIMESTAMP user-defined types) rather than Calcite's standard
          // YEAR/QUARTER/MONTH/HOUR/MINUTE/SECOND operators. The standard ones rewrite to
          // EXTRACT(<unit> FROM <datetime>), but EXTRACT only accepts Calcite's built-in
          // DATETIME types — EXPR_DATE and EXPR_TIMESTAMP are unrecognized as DATETIME.
          case "year" -> org.opensearch.sql.expression.function.PPLBuiltinOperators.YEAR;
          case "quarter" -> org.opensearch.sql.expression.function.PPLBuiltinOperators.QUARTER;
          case "month" -> org.opensearch.sql.expression.function.PPLBuiltinOperators.MONTH;
          case "dayofyear", "day_of_year" ->
              org.opensearch.sql.expression.function.PPLBuiltinOperators.DAY_OF_YEAR;
          case "dayofmonth", "day_of_month", "day" ->
              org.opensearch.sql.expression.function.PPLBuiltinOperators.DAY;
          case "dayofweek", "day_of_week" ->
              org.opensearch.sql.expression.function.PPLBuiltinOperators.DAY_OF_WEEK;
          case "hour" -> org.opensearch.sql.expression.function.PPLBuiltinOperators.HOUR;
          case "minute" -> org.opensearch.sql.expression.function.PPLBuiltinOperators.MINUTE;
          case "week" -> org.opensearch.sql.expression.function.PPLBuiltinOperators.WEEK;
          case "second" -> org.opensearch.sql.expression.function.PPLBuiltinOperators.SECOND;
          // Date-arithmetic UDFs that share names with Calcite-standard operators (which expect
          // INTERVAL qualifiers as the first arg, not strings). Bind directly to PPL UDFs so the
          // validator's name lookup doesn't pick the standard variant.
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
          case "weekday" -> org.opensearch.sql.expression.function.PPLBuiltinOperators.WEEKDAY;
          case "yearweek" -> org.opensearch.sql.expression.function.PPLBuiltinOperators.YEARWEEK;
          case "weekofyear" -> org.opensearch.sql.expression.function.PPLBuiltinOperators.WEEK;
          default -> null;
        };
    if (builtin != null) {
      return new SqlBasicCall(builtin, args, POS);
    }
    // Some PPL function names don't match the SQL operator name. The full mapping lives in
    // PPLFuncImpTable, but the SqlNode path goes through the validator's name lookup which
    // doesn't consult that table. Translate the well-known mismatches here so the validator
    // can find the operator under its standard name.
    String resolvedName = resolveFunctionName(f.getFuncName());
    // Defer function resolution to the validator. The validator is configured case-insensitive
    // (in SqlNodePlanner) so PPL function names resolve regardless of how the UDF was registered.
    return new SqlBasicCall(
        new org.apache.calcite.sql.SqlUnresolvedFunction(
            new SqlIdentifier(resolvedName, POS),
            /* returnTypeInference */ null,
            /* operandTypeInference */ null,
            /* operandTypeChecker */ null,
            /* paramTypes */ null,
            org.apache.calcite.sql.SqlFunctionCategory.USER_DEFINED_FUNCTION),
        args,
        POS);
  }

  /**
   * PPL function names that bind to a different Calcite operator name in PPLFuncImpTable. Keep this
   * in sync with {@code PPLFuncImpTable.registerOperator(POW, SqlStdOperatorTable.POWER)} style
   * mappings — anywhere a PPL builtin doesn't share the SqlStdOperatorTable name.
   */
  private static String resolveFunctionName(String pplName) {
    return switch (pplName.toLowerCase()) {
      case "pow" -> "POWER";
      case "length" -> "CHAR_LENGTH";
      case "ifnull" -> "COALESCE";
      case "locate" -> "POSITION";
      case "signum" -> "SIGN";
      case "addfunction" -> "PLUS";
      case "multiplyfunction" -> "MULTIPLY";
      case "json_valid" -> "IS_JSON_VALUE";
      // PPL multi-value array helpers — registered on PPLFuncImpTable but with different
      // operator names than what PPL parses. Map to the operator names that resolve in our
      // chained operator table (PPLBuiltinOperators + SqlStdOperatorTable + BIG_QUERY library).
      case "mvjoin" -> "ARRAY_JOIN";
      case "mvcount" -> "ARRAY_LENGTH";
      case "mvsort" -> "SORT_ARRAY";
      case "mvdedup" -> "ARRAY_DISTINCT";
      case "mvcontains" -> "ARRAY_CONTAINS";
      case "mvslice" -> "ARRAY_SLICE";
      // PPL mvappend is variadic (mvappend(a, b, c, ...)) and concatenates arrays/scalars;
      // Calcite's std ARRAY_APPEND is fixed 2-arg. Use the PPL UDF "mvappend" name which the
      // validator looks up against PPLBuiltinOperators.MVAPPEND.
      case "mvappend" -> "mvappend";
      case "mvmap" -> "transform"; // PPLFuncImpTable maps mvmap → PPLBuiltinOperators.TRANSFORM
      case "split" -> "SPLIT";
      case "isblank" -> "IS_BLANK";
      case "isempty" -> "IS_EMPTY";
      case "ispresent" -> "IS_PRESENT";
      // Date function aliases.
      case "month_of_year" -> "MONTH";
      case "week_of_year" -> "WEEK";
      case "hour_of_day" -> "HOUR";
      case "minute_of_hour" -> "MINUTE";
      case "second_of_minute" -> "SECOND";
      case "day_of_month" -> "DAY";
      case "timediff" -> "TIME_DIFF";
      default -> pplName;
    };
  }

  /**
   * Returns true when any argument expression is statically known to be a string — a STRING-typed
   * Literal or a CAST(... AS string). Recurses through nested `+` calls so chains like `'A' + name
   * + ', '` are detected even when only the leftmost operand is a literal.
   */
  private boolean hasStringOperand(List<UnresolvedExpression> args) {
    for (UnresolvedExpression a : args) {
      if (isStringExpr(a)) {
        return true;
      }
    }
    return false;
  }

  private boolean isStringExpr(UnresolvedExpression e) {
    if (e instanceof org.opensearch.sql.ast.expression.Literal lit) {
      return lit.getType() == org.opensearch.sql.ast.expression.DataType.STRING;
    }
    if (e instanceof org.opensearch.sql.ast.expression.Cast c) {
      try {
        return c.getDataType() == org.opensearch.sql.ast.expression.DataType.STRING;
      } catch (RuntimeException ignored) {
        return false;
      }
    }
    if (e instanceof Function fn && "+".equals(fn.getFuncName())) {
      return hasStringOperand(fn.getFuncArgs());
    }
    return false;
  }

  /**
   * PPL parses arithmetic operators as Function nodes with names like "+"/"-"/etc. Map those to
   * Calcite operators directly; everything else goes through the validator's name lookup.
   */
  private SqlOperator arithmeticOperator(String name) {
    return switch (name) {
      case "+" -> SqlStdOperatorTable.PLUS;
      case "-" -> SqlStdOperatorTable.MINUS;
      case "*" -> SqlStdOperatorTable.MULTIPLY;
      // PPL semantics: x/0 returns NULL, not an exception. v2's registerDivideFunction picks
      // PPLBuiltinOperators.DIVIDE when legacy syntax is preferred (the default for OpenSearch
      // PPL); BigQuery's SAFE_DIVIDE returns NULL when DECIMAL precision overflows, which
      // diverges from PPL's expected behavior on simple expressions like `22 / 7.0`. Use the
      // legacy DIVIDE operator to match v2's default.
      case "/" -> org.opensearch.sql.expression.function.PPLBuiltinOperators.DIVIDE;
      case "%" -> org.opensearch.sql.expression.function.PPLBuiltinOperators.MOD;
      default -> null;
    };
  }

  // -- Helpers ---------------------------------------------------------------

  private static SqlNode intLiteral(int v) {
    return SqlLiteral.createExactNumeric(Integer.toString(v), POS);
  }

  private static SqlNode approxNumeric(Object v) {
    BigDecimal bd = (v instanceof BigDecimal b) ? b : new BigDecimal(v.toString());
    String approx = bd.toString();
    if (!approx.contains("E") && !approx.contains("e")) {
      approx = bd.toPlainString() + "E0";
    }
    return SqlLiteral.createApproxNumeric(approx, POS);
  }

  private static SqlNode castTo(SqlNode value, org.apache.calcite.sql.type.SqlTypeName tn) {
    org.apache.calcite.sql.SqlDataTypeSpec spec =
        new org.apache.calcite.sql.SqlDataTypeSpec(
            new org.apache.calcite.sql.SqlBasicTypeNameSpec(tn, POS), POS);
    return new SqlBasicCall(SqlStdOperatorTable.CAST, List.of(value, spec), POS);
  }

  private static SqlNodeList starList() {
    SqlNodeList l = new SqlNodeList(POS);
    l.add(SqlIdentifier.star(POS));
    return l;
  }

  private static SqlNode asAlias(SqlNode expr, String alias) {
    return new SqlBasicCall(SqlStdOperatorTable.AS, List.of(expr, quotedId(alias)), POS);
  }

  /**
   * Build a single-component SqlIdentifier whose component position is quoted. Calcite's
   * SqlValidator.makeNullaryCall converts unquoted bare identifiers matching system function names
   * (USER, CURRENT_USER, LOCALTIME, ...) into niladic function calls — shadowing column references
   * with the same name. Marking the component position as quoted bypasses that lookup.
   * SqlIdentifier#isComponentQuoted reads componentPositions, so the parser-position argument alone
   * isn't enough; we have to supply componentPositions explicitly.
   */
  static SqlIdentifier quotedId(String name) {
    return new SqlIdentifier(
        java.util.Collections.singletonList(name), null, POS, java.util.List.of(QPOS));
  }

  /**
   * Returns the simple column name that a SELECT-list entry exposes, or null when the entry is an
   * arbitrary expression. Recognizes plain identifiers and AS-aliased calls.
   */
  private static String identifierName(SqlNode n) {
    if (n instanceof SqlIdentifier id) {
      if (id.isStar()) {
        return null;
      }
      return id.names.get(id.names.size() - 1);
    }
    if (n instanceof SqlBasicCall call && call.getOperator() == SqlStdOperatorTable.AS) {
      List<SqlNode> ops = call.getOperandList();
      if (ops.size() >= 2 && ops.get(1) instanceof SqlIdentifier alias) {
        return alias.names.get(alias.names.size() - 1);
      }
    }
    return null;
  }

  /**
   * Build a multi-part identifier suitable for a table reference (FROM clause). For a name like
   * `schema.table`, the validator interprets each part as a catalog level.
   */
  private static SqlIdentifier qualifiedNameToIdentifier(QualifiedName qn) {
    // Quote each component so identifiers containing hyphens (common in OpenSearch index
    // names like `opensearch-sql_test_index_*`) survive the SQL validator's identifier rules.
    java.util.List<org.apache.calcite.sql.parser.SqlParserPos> componentPositions =
        new java.util.ArrayList<>();
    for (int i = 0; i < qn.getParts().size(); i++) componentPositions.add(QPOS);
    return new SqlIdentifier(qn.getParts(), null, POS, componentPositions);
  }

  /**
   * Build a column-reference identifier from a PPL QualifiedName.
   *
   * <p>PPL parses {@code obj.sub} as a QualifiedName with two parts. There are two interpretations:
   *
   * <ul>
   *   <li>OpenSearch indices: the column is the literal dotted name {@code "obj.sub"} (single
   *       identifier; the OpenSearch storage engine exposes nested fields under a dotted key).
   *   <li>SQL joins / table aliases: {@code "table.col"} is a 2-part navigation.
   * </ul>
   *
   * The visitor tracks whether we're inside a join scope (multiple table aliases visible) via
   * {@link #joinScope}. In join scope, dotted names emit as multi-part SQL navigation; otherwise
   * they emit as a single dotted identifier so OpenSearch nested-field lookups keep working.
   */
  /**
   * Walk a join-side AST tree looking for SubqueryAlias nodes whose alias differs from {@code
   * canonicalAlias}, and register {@code <inner> -> <canonical>} entries in {@link
   * #joinAliasSynonyms}. Only the SubqueryAlias names matching the same join side are registered so
   * column refs in projections / filters resolve to the SQL alias used in the emitted FROM clause.
   */
  /**
   * Walk down a join-side AST to find any bare-table {@link org.opensearch.sql.ast.tree.Relation}
   * nodes and return their qualified table names. Used to register table-name synonyms for the
   * default `__l`/`__r` aliases so user ON-clauses that reference the raw table name resolve
   * through the wrap.
   */
  private java.util.List<String> extractRelationTableNames(
      org.opensearch.sql.ast.tree.UnresolvedPlan plan) {
    java.util.List<String> out = new java.util.ArrayList<>();
    if (plan == null) return out;
    org.opensearch.sql.ast.tree.UnresolvedPlan current = plan;
    while (current != null) {
      if (current instanceof org.opensearch.sql.ast.tree.Relation rel) {
        for (QualifiedName qn : rel.getQualifiedNames()) {
          out.add(qn.toString());
        }
        break;
      }
      java.util.List<? extends org.opensearch.sql.ast.Node> children = current.getChild();
      if (children == null || children.isEmpty()) break;
      org.opensearch.sql.ast.Node first = children.get(0);
      if (!(first instanceof org.opensearch.sql.ast.tree.UnresolvedPlan p)) break;
      current = p;
    }
    return out;
  }

  private void registerNestedAliasSynonyms(
      org.opensearch.sql.ast.tree.UnresolvedPlan plan, String canonicalAlias) {
    if (canonicalAlias == null || plan == null) return;
    org.opensearch.sql.ast.tree.UnresolvedPlan current = plan;
    while (current != null) {
      if (current instanceof org.opensearch.sql.ast.tree.SubqueryAlias sqa) {
        String inner = sqa.getAlias();
        if (!canonicalAlias.equals(inner)) {
          joinAliasSynonyms.put(inner, canonicalAlias);
        }
      }
      java.util.List<? extends org.opensearch.sql.ast.Node> children = current.getChild();
      if (children == null || children.isEmpty()) break;
      org.opensearch.sql.ast.Node first = children.get(0);
      if (!(first instanceof org.opensearch.sql.ast.tree.UnresolvedPlan p)) break;
      current = p;
    }
  }

  /**
   * For dotted QualifiedNames (e.g. {@code info.city}, {@code doc.user.name}) where the literal
   * full name is NOT a column but the first part IS a known MAP/STRUCT column in scope, emit {@code
   * ITEM(parent, 'leaf')} (chained for deeper paths) so Calcite navigates into the struct/map.
   * Returns null when the standard dotted-literal encoding should be used (the column literally
   * exists, parent is scalar, or rowTypeOracle is unavailable).
   *
   * <p>Used by:
   *
   * <ul>
   *   <li>geoip() returns MAP&lt;VARCHAR, ANY&gt;; {@code info.city} ⇒ {@code ITEM(info, 'city')}.
   *   <li>spath input=doc unpacks JSON into a struct; {@code doc.user.name} ⇒ {@code ITEM(ITEM(doc,
   *       'user'), 'name')}.
   * </ul>
   */
  private SqlNode tryMapOrStructItemAccess(QualifiedName qn) {
    java.util.List<String> parts = qn.getParts();
    if (parts.size() < 2) return null;
    if (rowTypeOracle == null || currentState == null || currentState.from == null) return null;
    // In a join scope, the standard 2-part path `<alias>.<col>` is correct. But for 3+ parts
    // where `parts[1]` is a MAP/STRUCT column on the aliased side, the multi-part identifier
    // would emit `<alias>.<col>.<deeper>` which the validator can't resolve. Probe the in-flight
    // state for `<alias>.<parts[1]>` and, when it lands on a MAP/STRUCT column, lower the deeper
    // navigation through ITEM. Falls through to standard multi-part navigation otherwise.
    if (joinScope) {
      if (parts.size() >= 3 && isKnownJoinAlias(parts.get(0))) {
        SqlNode aliasItem = tryAliasMapItemAccess(parts);
        if (aliasItem != null) return aliasItem;
        // No alias-prefixed lowering — fall through to standard alias.col multi-part emission.
        return null;
      }
      // parts[0] isn't a known alias — fall through to regular MAP-leaf navigation below
      // (e.g. `doc.user.name` after JOIN where `doc` is a column, not an alias).
    }
    // Skip if this is a known synonym (handled elsewhere).
    if (joinAliasSynonyms.containsKey(parts.get(0))) return null;
    org.apache.calcite.rel.type.RelDataType rowType;
    try {
      SqlNode probe = currentState.toSqlNode();
      rowType = rowTypeOracle.apply(probe);
    } catch (RuntimeException e) {
      return null;
    }
    String fullName = qn.toString();
    if (rowType.getField(fullName, false, false) != null) return null;
    // Find LAST occurrence of parts[0] in the row-type — eval-extended projections (e.g.
    // `eval doc = json_extract_all(doc)`) emit `SELECT *, json_extract_all(doc) AS doc` where
    // the same name appears twice. The validator's `getField` returns the FIRST match (raw
    // VARCHAR), but PPL semantics: the eval override wins. Walk fields in reverse to pick the
    // overriding column's type.
    org.apache.calcite.rel.type.RelDataTypeField parent = null;
    java.util.List<org.apache.calcite.rel.type.RelDataTypeField> fields = rowType.getFieldList();
    for (int i = fields.size() - 1; i >= 0; i--) {
      if (fields.get(i).getName().equals(parts.get(0))) {
        parent = fields.get(i);
        break;
      }
    }
    if (parent == null) return null;
    org.apache.calcite.sql.type.SqlTypeName tn = parent.getType().getSqlTypeName();
    boolean isMap = tn == org.apache.calcite.sql.type.SqlTypeName.MAP;
    boolean isStructLike =
        tn == org.apache.calcite.sql.type.SqlTypeName.ROW || parent.getType().isStruct();
    // ANY-typed parent: element of UNNEST(ARRAY<ANY>) (e.g. mvexpand on a Nested field) loses its
    // structural type. Treat as struct-like and emit chained ITEM — the runtime ExprValue is a
    // Map and ITEM dispatches dynamically.
    boolean isAny = tn == org.apache.calcite.sql.type.SqlTypeName.ANY;
    if (!isMap && !isStructLike && !isAny) return null;
    // Two emission shapes:
    //   - STRUCT/ROW (nested): chain ITEM calls — `ITEM(ITEM(doc, 'user'), 'name')`. Each ITEM
    //     navigates one level into the struct's value type.
    //   - MAP with scalar value type (e.g. JSON_EXTRACT_ALL → MAP<VARCHAR, VARCHAR> keyed by
    //     flattened dot-paths "user.name"): single-level ITEM with the joined trailing parts as
    //     key — `ITEM(doc, 'user.name')`. Chaining would type-fail since ITEM(doc, 'user') is a
    //     scalar and a second ITEM rejects `<VARCHAR>` operand.
    if (isMap) {
      org.apache.calcite.rel.type.RelDataType valueType = parent.getType().getValueType();
      org.apache.calcite.sql.type.SqlTypeName vt =
          valueType == null ? null : valueType.getSqlTypeName();
      boolean valueIsNavigable =
          vt == org.apache.calcite.sql.type.SqlTypeName.MAP
              || vt == org.apache.calcite.sql.type.SqlTypeName.ROW
              || (valueType != null && valueType.isStruct());
      if (!valueIsNavigable) {
        String joinedKey = String.join(".", parts.subList(1, parts.size()));
        return new SqlBasicCall(
            SqlStdOperatorTable.ITEM,
            List.of(
                new SqlIdentifier(parts.get(0), POS), SqlLiteral.createCharString(joinedKey, POS)),
            POS);
      }
    }
    SqlNode acc = new SqlIdentifier(parts.get(0), POS);
    for (int i = 1; i < parts.size(); i++) {
      acc =
          new SqlBasicCall(
              SqlStdOperatorTable.ITEM,
              List.of(acc, SqlLiteral.createCharString(parts.get(i), POS)),
              POS);
    }
    return acc;
  }

  /**
   * True when {@code name} is a recognized join-side alias: a registered synonym, a default
   * `__l`/`__r`/`__l2`/`__r2` from defaultLeftAlias/defaultRightAlias, or the user-supplied
   * left/right alias set on the current Pipeline state.
   */
  private boolean isKnownJoinAlias(String name) {
    if (joinAliasSynonyms.containsValue(name)) return true;
    if (joinAliasSynonyms.containsKey(name)) return true;
    if (name.equals(activeJoinLeftAlias) || name.equals(activeJoinRightAlias)) return true;
    if (currentState != null) {
      if (name.equals(currentState.joinLeftAlias)) return true;
      if (name.equals(currentState.joinRightAlias)) return true;
    }
    if (name.startsWith("__l") || name.startsWith("__r")) return true;
    return false;
  }

  /**
   * In joinScope with parts of form `<alias>.<col>.<rest...>`, emit `ITEM(<alias>.<col>, '<rest>')`
   * when `<col>` is a MAP/STRUCT column on the side aliased as `<alias>`. Returns null when: the
   * alias isn't a join side, the `<col>` doesn't exist or is scalar, or the probe fails.
   */
  private SqlNode tryAliasMapItemAccess(java.util.List<String> parts) {
    String alias = parts.get(0);
    String col = parts.get(1);
    java.util.List<String> deeper = parts.subList(2, parts.size());
    SqlNode probeFrom = joinProbeFrom != null ? joinProbeFrom : currentState.from;
    if (probeFrom == null) return null;
    org.apache.calcite.rel.type.RelDataType rowType;
    try {
      SqlNodeList sel = new SqlNodeList(POS);
      sel.add(new SqlIdentifier(java.util.Arrays.asList(alias, col), POS));
      SqlSelect probe =
          new SqlSelect(
              POS, null, sel, probeFrom, null, null, null, null, null, null, null, null, null);
      rowType = rowTypeOracle.apply(probe);
    } catch (RuntimeException e) {
      return null;
    }
    if (rowType.getFieldList().isEmpty()) return null;
    org.apache.calcite.rel.type.RelDataType colType = rowType.getFieldList().get(0).getType();
    org.apache.calcite.sql.type.SqlTypeName tn = colType.getSqlTypeName();
    boolean isMap = tn == org.apache.calcite.sql.type.SqlTypeName.MAP;
    boolean isStructLike = tn == org.apache.calcite.sql.type.SqlTypeName.ROW || colType.isStruct();
    boolean isAny = tn == org.apache.calcite.sql.type.SqlTypeName.ANY;
    if (!isMap && !isStructLike && !isAny) return null;
    SqlNode parentRef = new SqlIdentifier(java.util.Arrays.asList(alias, col), POS);
    if (isMap) {
      // Check if the value type is navigable; if not, single-level ITEM with joined trailing key.
      org.apache.calcite.rel.type.RelDataType valueType = colType.getValueType();
      org.apache.calcite.sql.type.SqlTypeName vt =
          valueType == null ? null : valueType.getSqlTypeName();
      boolean valueIsNavigable =
          vt == org.apache.calcite.sql.type.SqlTypeName.MAP
              || vt == org.apache.calcite.sql.type.SqlTypeName.ROW
              || (valueType != null && valueType.isStruct());
      if (!valueIsNavigable) {
        String joinedKey = String.join(".", deeper);
        return new SqlBasicCall(
            SqlStdOperatorTable.ITEM,
            List.of(parentRef, SqlLiteral.createCharString(joinedKey, POS)),
            POS);
      }
    }
    SqlNode acc = parentRef;
    for (String d : deeper) {
      acc =
          new SqlBasicCall(
              SqlStdOperatorTable.ITEM, List.of(acc, SqlLiteral.createCharString(d, POS)), POS);
    }
    return acc;
  }

  private SqlIdentifier qualifiedNameToFieldIdentifier(QualifiedName qn) {
    if (joinScope && qn.getParts().size() > 1) {
      // Multi-part identifier (table.col); the validator's makeNullaryCall only treats simple
      // (single-name) unquoted identifiers as niladic function candidates, so qualified
      // navigation is safe to leave unquoted.
      java.util.List<String> parts = qn.getParts();
      String canonical = joinAliasSynonyms.get(parts.get(0));
      java.util.List<String> finalParts;
      if (canonical != null) {
        finalParts = new java.util.ArrayList<>(parts);
        finalParts.set(0, canonical);
      } else {
        finalParts = parts;
      }
      // Quote each component so identifiers containing hyphens (common in OpenSearch index
      // names like `opensearch-sql_test_index_state_country`) survive the SQL validator's
      // identifier rules unscathed.
      java.util.List<org.apache.calcite.sql.parser.SqlParserPos> componentPositions =
          new java.util.ArrayList<>();
      for (int i = 0; i < finalParts.size(); i++) componentPositions.add(QPOS);
      return new SqlIdentifier(finalParts, null, POS, componentPositions);
    }
    return quotedId(qn.toString());
  }

  private static String letName(Let let) {
    UnresolvedExpression inner = let.getVar().getField();
    if (inner instanceof QualifiedName qn) {
      return qn.toString();
    }
    return inner.toString();
  }
}
