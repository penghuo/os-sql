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
import org.opensearch.sql.ast.tree.Expand;
import org.opensearch.sql.ast.tree.FillNull;
import org.opensearch.sql.ast.tree.Filter;
import org.opensearch.sql.ast.tree.Flatten;
import org.opensearch.sql.ast.tree.GraphLookup;
import org.opensearch.sql.ast.tree.Head;
import org.opensearch.sql.ast.tree.Join;
import org.opensearch.sql.ast.tree.Limit;
import org.opensearch.sql.ast.tree.Lookup;
import org.opensearch.sql.ast.tree.Multisearch;
import org.opensearch.sql.ast.tree.MvCombine;
import org.opensearch.sql.ast.tree.NoMv;
import org.opensearch.sql.ast.tree.Parse;
import org.opensearch.sql.ast.tree.Patterns;
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
 * <p>This is the sole PPL→SqlNode visitor on the OpenSearch plugin's PPL execution path (wired in
 * {@link org.opensearch.sql.executor.QueryService#analyze}). It also backs {@link
 * org.opensearch.sql.api.UnifiedQueryPlanner}'s AST-based planning strategy.
 */
public class PPLToSqlNodeVisitor extends AbstractNodeVisitor<SqlNode, Frame> {

  private static final SqlParserPos POS = SqlParserPos.ZERO;

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
          "array",
          "mvappend",
          "mvjoin",
          "array_join");

  /** Resolves a table qualified name (e.g. {@code ["my_index"]}) to its column names. */
  final Function<List<String>, List<String>> tableFields;

  /**
   * Resolves a table qualified name to its full {@link org.apache.calcite.rel.type.RelDataType}.
   * Optional — {@code null} when the visitor was constructed without a row-type oracle (legacy
   * single-arg ctor for tests). Visitors that need column types (e.g. UDT detection in {@link
   * #visitAppend}) fall through to type-agnostic emission when this is {@code null}.
   */
  final Function<List<String>, org.apache.calcite.rel.type.RelDataType> tableRowType;

  /**
   * The Frame currently in scope at expression-translation time. Set by {@link #translate} and
   * swapped by {@link #visitJoin} during the right walk so {@link #expr} and {@link #toIdentifier}
   * can apply alias synonyms recorded by upstream {@code visitSubqueryAlias} chain collapse or
   * {@code applyExplicitAlias} alias displacement. Threading the Frame through every {@code expr()}
   * call would cascade through many helpers; a transient pointer is simpler and the visit is
   * single-threaded.
   */
  Frame exprFrame;

  /** Expression-to-SqlNode visitor. Holds back-ref to this; access {@link #exprFrame} via outer. */
  final SqlExpressionVisitor rex = new SqlExpressionVisitor(this);

  public PPLToSqlNodeVisitor(Function<List<String>, List<String>> tableFields) {
    this(tableFields, null);
  }

  public PPLToSqlNodeVisitor(
      Function<List<String>, List<String>> tableFields,
      Function<List<String>, org.apache.calcite.rel.type.RelDataType> tableRowType) {
    this.tableFields = tableFields;
    this.tableRowType = tableRowType;
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
  static UnresolvedPlan stripImplicitMetaProjects(UnresolvedPlan plan) {
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
    // Union has N children (datasets); the generic unary descent below is wrong for Union
    // because Union.attach(child) prepends to datasets, turning a 1-dataset Union into a
    // 2-dataset Union and silently swallowing the "Union requires at least two datasets"
    // validation. Skip Union entirely — datasets are stripped per-branch in visitUnion.
    if (plan instanceof Union) {
      return plan;
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
    populateColumnUdt(parts, frame);
    return SqlBuilder.relation(parts, lookupTableFields(parts), frame);
  }

  /**
   * Populate {@link Frame#columnUdt} from the catalog row type for the table at {@code parts}. Each
   * column whose type is a PPL UDT ({@code EXPR_TIMESTAMP/DATE/TIME/IP}) gets recorded with its
   * lowercase UDT root so downstream UNION-pad sites can preserve the UDT.
   */
  private void populateColumnUdt(List<String> parts, Frame frame) {
    if (tableRowType == null) return;
    org.apache.calcite.rel.type.RelDataType rowType;
    try {
      rowType = tableRowType.apply(parts);
    } catch (Exception ignore) {
      return;
    }
    if (rowType == null || !rowType.isStruct()) return;
    for (org.apache.calcite.rel.type.RelDataTypeField field : rowType.getFieldList()) {
      String udt = udtRootName(field.getType());
      if (udt != null) {
        frame.columnUdt.put(field.getName(), udt);
      } else {
        DataType prim = primitiveDataTypeOf(field.getType());
        if (prim != null) {
          frame.columnPrimitiveType.put(field.getName(), prim);
        }
        // Track legacy type name for typeof() — TINYINT / SMALLINT / INT / BIGINT differ.
        org.apache.calcite.sql.type.SqlTypeName stn = field.getType().getSqlTypeName();
        String legacy =
            switch (stn) {
              case TINYINT -> "TINYINT";
              case SMALLINT -> "SMALLINT";
              case INTEGER -> "INT";
              case BIGINT -> "BIGINT";
              case FLOAT, REAL -> "FLOAT";
              case DOUBLE -> "DOUBLE";
              case DECIMAL -> "DOUBLE";
              case BOOLEAN -> "BOOLEAN";
              case VARCHAR, CHAR -> "STRING";
              case BINARY, VARBINARY -> "BINARY";
              case MAP -> "STRUCT";
              case GEOMETRY -> "GEO_POINT";
              default -> null;
            };
        if (legacy != null) {
          frame.columnLegacyTypeName.put(field.getName(), legacy);
        } else {
          // Fall back to OpenSearchDataType.legacyTypeName when SqlTypeName doesn't carry it
          // (e.g., GEO_POINT, NESTED stored as ARRAY/ANY/etc).
          String full = field.getType().getFullTypeString().toUpperCase(java.util.Locale.ROOT);
          if (full.contains("GEO_POINT") || full.contains("GEOPOINT")) {
            frame.columnLegacyTypeName.put(field.getName(), "GEO_POINT");
          }
        }
      }
      if (field.getType().getSqlTypeName() == org.apache.calcite.sql.type.SqlTypeName.MAP) {
        frame.mapColumns.add(field.getName());
      }
      // Track ARRAY-typed columns (OpenSearch `type: nested` mappings appear as ARRAY in
      // Calcite's row type). detectNestedAggregationPattern uses this to throw v2's documented
      // "Cannot execute nested aggregation on" error when an aggregate's dotted argument has an
      // array-rooted parent (true nested-type, not plain object/properties).
      if (field.getType().getSqlTypeName() == org.apache.calcite.sql.type.SqlTypeName.ARRAY) {
        frame.arrayRootedFields.add(field.getName());
      }
    }
  }

  /**
   * Detect the nested-aggregation pattern: an agg over a dotted leaf whose first part is itself a
   * group key. e.g. {@code stats min(address.area) by address}. Throws PPL's documented runtime
   * error at translation time so the user sees the documented message instead of a confusing "Table
   * not found" from the SQL validator.
   */
  private static void detectNestedAggregationPattern(Aggregation node, Frame frame) {
    java.util.Set<String> groupKeyNames = new java.util.LinkedHashSet<>();
    for (UnresolvedExpression g : node.getGroupExprList()) {
      UnresolvedExpression core = (g instanceof Alias a) ? a.getDelegated() : g;
      QualifiedName qn = null;
      if (core instanceof QualifiedName q) qn = q;
      else if (core instanceof Field f && f.getField() instanceof QualifiedName q) qn = q;
      if (qn != null && qn.getParts().size() == 1) {
        groupKeyNames.add(qn.toString());
      }
    }
    // Pre-agg complexity: any non-trivial pipe upstream of the aggregation prevents the
    // OpenSearch nested-aggregation pushdown rule from firing. v2's CalciteRelNodeVisitor sets
    // `hadPreAggComplexity` when state.where/evalExtended/projectionReplaced/orderBy/fetch are
    // populated. Mirror by walking the AST chain from the aggregation child downward: if we
    // pass through anything other than a bare Relation (or its SubqueryAlias/AllFieldsExcludeMeta
    // wrapper), there's pre-agg complexity. Specifically, Head/Limit upstream triggers this
    // (see testNestedAggExplainWhenPushdownNotApplied: `... | head 10000 | stats ...`).
    boolean hadPreAggComplexity = hasPreAggComplexity(node);
    for (UnresolvedExpression aggExpr : node.getAggExprList()) {
      UnresolvedExpression core = (aggExpr instanceof Alias a) ? a.getDelegated() : aggExpr;
      if (!(core instanceof AggregateFunction af)) continue;
      UnresolvedExpression argField = af.getField();
      QualifiedName argQn = null;
      if (argField instanceof QualifiedName q) argQn = q;
      else if (argField instanceof Field f && f.getField() instanceof QualifiedName q) argQn = q;
      if (argQn == null || argQn.getParts().size() < 2) continue;
      // Group-key-prefix path: agg over a dotted leaf whose first part is also a group key
      // (e.g. `stats min(address.area) by address`). PPL rejects this because the leaf can't be
      // reached through the STRUCT path that the validator must traverse.
      if (groupKeyNames.contains(argQn.getParts().get(0))) {
        throw new IllegalArgumentException(
            "Cannot execute nested aggregation: aggregating on '"
                + argQn
                + "' where '"
                + argQn.getParts().get(0)
                + "' is also a group key");
      }
      // Array-rooted dotted-arg path: agg over a dotted leaf whose ROOT column is OpenSearch
      // `type: nested` (Calcite ARRAY-typed). Only reject when there's pre-agg complexity —
      // bare `stats agg(addr.x) by ...` lets the OpenSearch nested-aggregation pushdown rule
      // handle it. With `head`/`where`/`eval`/`sort` upstream, pushdown is blocked and the
      // SqlNode pipeline can't evaluate the nested-array reference. Mirror v2's
      // CalciteEnumerableNestedAggregate runtime error message.
      String rootName = argQn.getParts().get(0);
      if (hadPreAggComplexity
          && !groupKeyNames.isEmpty()
          && frame.arrayRootedFields.contains(rootName)) {
        throw new IllegalArgumentException("Cannot execute nested aggregation on " + argQn);
      }
    }
  }

  /**
   * True if the chain from {@code node}'s child down to the bare Relation contains any non-trivial
   * pipe (anything other than {@link Project} / {@link SubqueryAlias} / {@link Relation}). Mirrors
   * v2's {@code hadPreAggComplexity} flag in the prior visitor's visitAggregation.
   */
  private static boolean hasPreAggComplexity(Aggregation node) {
    if (node.getChild().isEmpty()) return false;
    org.opensearch.sql.ast.Node c = node.getChild().get(0);
    while (c != null) {
      if (c instanceof Relation) return false;
      if (c instanceof Project || c instanceof SubqueryAlias) {
        if (c.getChild() == null || c.getChild().isEmpty()) return false;
        c = (org.opensearch.sql.ast.Node) c.getChild().get(0);
        continue;
      }
      // Anything else (Filter/Eval/Sort/Limit/Head/Rename/Join/...) is "complexity".
      return true;
    }
    return false;
  }

  /**
   * Static result-type for an aggregate function call. Returns {@code null} when the agg's input
   * type is unknown statically. Mirrors PPL's documented agg semantics: count/distinct_count return
   * LONG; sum/min/max/first/last/earliest/latest/take preserve input type;
   * avg/percentile/median/var_pop/var_samp/stddev_pop/stddev_samp return DOUBLE; list/values return
   * ARRAY input which is not statically derivable.
   */
  private static DataType staticAggResultType(
      UnresolvedExpression aggExpr,
      java.util.Map<String, DataType> preAggCols,
      java.util.Map<String, DataType> preAggEvals) {
    if (!(aggExpr instanceof AggregateFunction af)) {
      // Non-AggregateFunction (eval-style scalar exposed as agg): no static type.
      return null;
    }
    String fn = af.getFuncName() == null ? "" : af.getFuncName().toLowerCase(java.util.Locale.ROOT);
    switch (fn) {
      case "count":
      case "distinct_count":
      case "distinct_count_approx":
      case "dc":
        return DataType.LONG;
      case "avg":
      case "var_pop":
      case "var_samp":
      case "stddev_pop":
      case "stddev_samp":
      case "percentile":
      case "percentile_approx":
      case "median":
        return DataType.DOUBLE;
      case "sum":
      case "min":
      case "max":
      case "first":
      case "last":
      case "earliest":
      case "latest":
      case "take":
        return staticTypeOfFromMaps(af.getField(), preAggCols, preAggEvals);
      default:
        return null;
    }
  }

  /**
   * Like {@link #staticTypeOf} but reads from explicit column- and eval-alias maps (passed by
   * caller) rather than {@link Frame#columnPrimitiveType} / {@link Frame#evalAliasTypes}. Used by
   * {@link #staticAggResultType} when the caller needs a snapshot before the frame is mutated.
   */
  private static DataType staticTypeOfFromMaps(
      UnresolvedExpression e,
      java.util.Map<String, DataType> cols,
      java.util.Map<String, DataType> evals) {
    if (e == null) return null;
    if (e instanceof Literal lit) return lit.getType();
    if (e instanceof Cast c) return c.getDataType();
    QualifiedName qn = null;
    if (e instanceof QualifiedName q) qn = q;
    else if (e instanceof Field f && f.getField() instanceof QualifiedName q) qn = q;
    if (qn != null && qn.getParts().size() == 1) {
      String name = qn.toString();
      if (evals != null && evals.containsKey(name)) return evals.get(name);
      if (cols != null && cols.containsKey(name)) return cols.get(name);
    }
    return null;
  }

  /**
   * True when {@code e} is a function call that produces a MAP-typed value at runtime. PPL's {@code
   * geoip}/{@code map_*} functions return a MAP&lt;VARCHAR, ANY&gt;. Used by visitEval to track the
   * eval alias on {@link Frame#mapColumns} so a downstream dotted ref like {@code info.city} routes
   * through the ITEM dispatch instead of the multi-part identifier path.
   */
  private static boolean isMapProducingExpr(UnresolvedExpression e) {
    if (e instanceof org.opensearch.sql.ast.expression.Function fn) {
      String name =
          fn.getFuncName() == null ? "" : fn.getFuncName().toLowerCase(java.util.Locale.ROOT);
      switch (name) {
        case "geoip":
        case "map":
        case "map_concat":
        case "map_filter":
        case "map_zip":
        case "json_extract_all":
          return true;
        default:
          return false;
      }
    }
    return false;
  }

  /** True when {@code dt} is a PPL numeric type (SHORT/INTEGER/LONG/FLOAT/DOUBLE/DECIMAL). */
  private static boolean isNumericPplType(DataType dt) {
    if (dt == null) return false;
    switch (dt) {
      case SHORT:
      case INTEGER:
      case LONG:
      case FLOAT:
      case DOUBLE:
      case DECIMAL:
        return true;
      default:
        return false;
    }
  }

  /**
   * Map a Calcite {@link org.apache.calcite.rel.type.RelDataType} to a PPL {@link DataType} for
   * primitive columns. Returns {@code null} for non-primitive types (UDT, MAP, ARRAY, etc.).
   */
  private static DataType primitiveDataTypeOf(org.apache.calcite.rel.type.RelDataType type) {
    if (type == null) return null;
    switch (type.getSqlTypeName()) {
      case BOOLEAN:
        return DataType.BOOLEAN;
      case TINYINT:
      case SMALLINT:
        return DataType.SHORT;
      case INTEGER:
        return DataType.INTEGER;
      case BIGINT:
        return DataType.LONG;
      case FLOAT:
      case REAL:
        return DataType.FLOAT;
      case DOUBLE:
        return DataType.DOUBLE;
      case DECIMAL:
        return DataType.DECIMAL;
      case VARCHAR:
      case CHAR:
        return DataType.STRING;
      default:
        return null;
    }
  }

  /**
   * Return the lowercase UDT root name ({@code timestamp/date/time/ip}) when {@code type} is a PPL
   * user-defined type, or {@code null} otherwise. Detects via class hierarchy and toString
   * inspection so we don't need a hard dependency on the UDT class name.
   */
  private static String udtRootName(org.apache.calcite.rel.type.RelDataType type) {
    if (type == null) return null;
    String name = type.getClass().getSimpleName().toLowerCase(java.util.Locale.ROOT);
    if (name.contains("timestamp")) return "timestamp";
    if (name.contains("date")) return "date";
    if (name.contains("time")) return "time";
    if (name.contains("ip")) return "ip";
    if (name.contains("binary")) return "binary";
    String full = type.getFullTypeString().toLowerCase(java.util.Locale.ROOT);
    if (full.contains("expr_binary")) return "binary";
    if (full.contains("expr_timestamp")) return "timestamp";
    if (full.contains("expr_date")) return "date";
    if (full.contains("expr_time")) return "time";
    if (full.contains("expr_ip")) return "ip";
    return null;
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
    // AS-call so qualified refs `i.col` resolve. Track the alias on the frame so downstream
    // pipes (visitFilter / visitSort / visitEval / visitAggregation) re-attach it after each
    // wrap — without re-attachment the alias dies inside the FROM subquery.
    frame.subqueryAliasName = node.getAlias();
    return SqlBuilder.aliasAs(inner, node.getAlias());
  }

  /**
   * Re-wrap the result of a pipe-wrap with the active SubqueryAlias when one is set on the frame.
   * Visitors that wrap state into {@code SELECT * FROM (...)} call this so the alias survives the
   * wrap. No-op when no alias is active.
   */
  private SqlNode reattachSubqueryAlias(SqlNode wrapped, Frame frame) {
    if (frame.subqueryAliasName == null) return wrapped;
    return SqlBuilder.aliasAs(wrapped, frame.subqueryAliasName);
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
      // Short-circuit applies only when the visible field list has no dotted-leaf-with-parent
      // duplicates — otherwise we need to wrap with an explicit projection so {@link
      // #resolveSelectedFields}'s tryToRemoveNestedFields-style filter prunes the leaf.
      if (frame.currentFields == null || !hasDottedLeafWithVisibleParent(frame.currentFields)) {
        return from;
      }
    }

    List<String> selected = resolveSelectedFields(node, frame.currentFields, frame.joinHints);

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
      String leaf = name.substring(name.lastIndexOf('.') + 1);
      boolean dotted = name.indexOf('.') >= 0;
      String prefix = dotted ? name.substring(0, name.indexOf('.')) : null;
      // Spath/eval-dotted-alias: when the full dotted name is a known eval alias (e.g.
      // `doc.user.name` from spath rewriteAsEval), emit a quoted single-part identifier so the
      // validator looks up the literal alias.
      if (dotted && frame.dottedEvalAliases.contains(name)) {
        selectList.add(
            ExpressionConverter.quotedIdentifier(java.util.Collections.singletonList(name)));
        seenLeaves.add(leaf);
        continue;
      }
      // Flat-dotted catalog column: OpenSearch flattens deeply-nested object mappings into
      // top-level dotted-name fields (e.g. `machine.os1`). When the FULL dotted name appears as
      // a visible flat column in `currentFields`, prefer this over ITEM dispatch — the field has
      // a typed RelDataTypeField in the rel rowType (string, bigint, etc), but ITEM(map, key)
      // returns ANY-typed and loses the type. Emit a quoted single-part identifier so the
      // validator resolves it to the typed column directly. Mirror toIdentifier's existing logic
      // (commit 203bcfede3) for the visitProject SELECT-list emission path.
      if (dotted
          && frame.currentFields != null
          && frame.currentFields.contains(name)
          && !frame.liveJoinAliases.contains(prefix)
          && frame.aliasSynonyms.isEmpty()) {
        selectList.add(
            ExpressionConverter.quotedIdentifier(java.util.Collections.singletonList(name)));
        seenLeaves.add(leaf);
        continue;
      }
      // ITEM dispatch when the dotted prefix is a MAP-typed column (catalog "object" mapping or
      // an eval alias bound to a MAP-producing function like geoip). Validator multi-part
      // identifier resolution would otherwise fail with "Table 'X' not found". Always alias the
      // ITEM call back to the original dotted name so {@code | fields a.b} keeps {@code a.b} as
      // the output column header.
      if (dotted && frame.mapColumns.contains(prefix)) {
        String subkey = name.substring(name.indexOf('.') + 1);
        SqlNode item =
            new SqlBasicCall(
                SqlStdOperatorTable.ITEM,
                List.of(new SqlIdentifier(prefix, POS), SqlLiteral.createCharString(subkey, POS)),
                POS);
        SqlIdentifier alias =
            new SqlIdentifier(
                java.util.Collections.singletonList(name),
                null,
                POS,
                List.of(SqlParserPos.ZERO.withQuoting(true)));
        selectList.add(new SqlBasicCall(SqlStdOperatorTable.AS, List.of(item, alias), POS));
        seenLeaves.add(leaf);
        continue;
      }
      SqlIdentifier ref = qualifyIfAmbiguous(name, frame);
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

    List<String> postFields = stripAliasPrefix(selected, frame);
    // Correctness: when child is a `SELECT * FROM <relation/join>`, pull the user-projection
    // INTO that SELECT instead of nesting a new outer SELECT. Nesting seals the join aliases
    // inside the FROM subquery — `... | sort a.age | fields a.name` would produce `SELECT a.name
    // FROM (SELECT * FROM <join> ORDER BY a.age)` and the outer SELECT can't see `a` from the
    // inner FROM (Calcite throws "Table 'a' not found"). Rewriting the inner select list keeps
    // the join aliases live in the same SELECT scope. Required by CalcitePPLJoinIT
    // (testComplex*Join,
    // testNonEquiJoin).
    SqlNode rewritten = projectIntoChildSelectStar(from, selectList);
    if (rewritten != null) {
      frame.currentFields = postFields;
      return rewritten;
    }
    return SqlBuilder.select(selectList).from(from).withFields(postFields).wrap(frame);
  }

  /**
   * If {@code from} is a {@code SELECT * FROM <inner>} (optionally wrapped in a {@link
   * org.apache.calcite.sql.SqlOrderBy}) and has no GROUP BY / HAVING, swap its select list for
   * {@code newList} in place and return the rewritten node. Returns null when the shape doesn't
   * match — caller falls back to normal wrapping.
   */
  private static SqlNode projectIntoChildSelectStar(SqlNode from, SqlNodeList newList) {
    SqlSelect select = null;
    boolean fromIsOrderBy = false;
    if (from instanceof SqlSelect s) {
      select = s;
    } else if (from instanceof org.apache.calcite.sql.SqlOrderBy ob
        && ob.query instanceof SqlSelect s) {
      select = s;
      fromIsOrderBy = true;
    }
    if (select == null) return null;
    SqlNodeList items = select.getSelectList();
    if (items == null || items.size() != 1) return null;
    SqlNode lone = items.get(0);
    if (!(lone instanceof SqlIdentifier id) || !id.isStar()) return null;
    if (select.getGroup() != null && !select.getGroup().getList().isEmpty()) return null;
    if (select.getHaving() != null) return null;
    // When the SELECT is wrapped in SqlOrderBy AND its inner FROM is itself a SqlSelect with a
    // computed (non-passthrough) projection, folding the user-projection into the inner SELECT *
    // would let Calcite trim the wide projection to only user-requested cols. Falling through to
    // a fresh outer SELECT preserves the wide projection. Fall through.
    if (fromIsOrderBy
        && select.getFrom() instanceof SqlSelect inner
        && hasComputedProjection(inner.getSelectList())) {
      return null;
    }
    select.setSelectList(newList);
    return from;
  }

  /** True when the select list contains a non-passthrough projection (e.g. eval expression). */
  private static boolean hasComputedProjection(SqlNodeList items) {
    if (items == null) return false;
    for (SqlNode item : items) {
      if (item instanceof SqlIdentifier) continue;
      if (item instanceof SqlBasicCall call && call.getOperator() == SqlStdOperatorTable.AS) {
        if (call.getOperandList().size() >= 1
            && call.getOperandList().get(0) instanceof SqlIdentifier) continue;
      }
      return true;
    }
    return false;
  }

  @Override
  public SqlNode visitHead(Head node, Frame frame) {
    SqlNode from = node.getChild().get(0).accept(this, frame);
    SqlLiteral fetch = SqlLiteral.createExactNumeric(node.getSize().toString(), POS);
    Integer fromOffset = node.getFrom();
    SqlLiteral offset =
        fromOffset != null && fromOffset > 0
            ? SqlLiteral.createExactNumeric(fromOffset.toString(), POS)
            : null;
    SqlBuilder.SelectBuilder b = SqlBuilder.select(starList()).from(from).fetch(fetch);
    if (offset != null) {
      b.offset(offset);
    }
    return b.wrap(frame);
  }

  @Override
  public SqlNode visitFilter(Filter node, Frame frame) {
    SqlNode from = node.getChild().get(0).accept(this, frame);
    SqlNode where = expr(node.getCondition());
    SqlNode wrapped = SqlBuilder.select(starList()).from(from).where(where).wrap(frame);
    return reattachSubqueryAlias(wrapped, frame);
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
    // PPL eval extends the row with N new columns. Generally `SELECT *, e1 AS a1, ..., en AS an
    // FROM <child>` — but PPL allows rebinding existing columns (`eval x = x + 1`) and forward
    // refs between lets within the same eval (`eval a = b + 1, c = a + 2`); SQL forbids both.
    // See computeEvalSelectPrefix and the mid-eval wrap loop below for how we work around them.
    List<String> visible =
        new ArrayList<>(frame.currentFields == null ? List.of() : frame.currentFields);
    Set<String> existingNames = new LinkedHashSet<>(visible);
    Set<String> rebound = computeReboundNames(node, existingNames);
    SqlNodeList items = computeEvalSelectPrefix(rebound, frame, visible);

    Set<String> aliasesInThisSelect = new LinkedHashSet<>();
    boolean wrappedMidEval = false;
    for (Let let : node.getExpressionList()) {
      String alias = let.getVar().getField().toString();
      // SQL forbids forward-reference between SELECT-list aliases. When a let references an alias
      // introduced earlier in this same eval, wrap into a new SELECT * so the previous alias
      // becomes an actual column visible to the next let.
      if (referencesAny(let.getExpression(), aliasesInThisSelect)) {
        from = SqlBuilder.select(items).from(from).withFields(visible).wrap(frame);
        items = new SqlNodeList(POS);
        items.add(SqlIdentifier.star(POS));
        aliasesInThisSelect = new LinkedHashSet<>();
        wrappedMidEval = true;
      }
      SqlNode rhs = ExpressionConverter.applyEvalRhsTransforms(expr(let.getExpression()), let);
      items.add(asAliased(rhs, alias));
      aliasesInThisSelect.add(alias);
      if (existingNames.add(alias)) {
        visible.add(alias);
      }
      updateFrameForLet(frame, alias, let, rebound);
    }
    // Peephole: when the child is a SELECT * (no GROUP BY / HAVING), append our extra eval items
    // to its select list and reuse it. Avoids nesting under a subquery that would seal upstream
    // join aliases. Skip after a mid-eval wrap — the `from` we just built is itself the wrapped
    // child, and appending more items would re-introduce the forbidden forward-reference shape.
    if (!wrappedMidEval) {
      SqlNode peep = appendEvalToChildSelectStar(from, items, visible, frame);
      if (peep != null) return peep;
    }
    return SqlBuilder.select(items).from(from).withFields(visible).wrap(frame);
  }

  /** Names of lets in {@code node} that rebind a column already visible in {@code existing}. */
  private static Set<String> computeReboundNames(Eval node, Set<String> existing) {
    Set<String> rebound = new java.util.HashSet<>();
    for (Let let : node.getExpressionList()) {
      String name = let.getVar().getField().toString();
      if (existing.contains(name)) {
        rebound.add(name);
      }
    }
    return rebound;
  }

  /**
   * Build the SELECT prefix in front of the eval's let projections.
   *
   * <ul>
   *   <li>No rebound names (or no current-fields list available): emit a single {@code *}.
   *   <li>Otherwise: expand to an explicit column list, dropping each rebound name AND its struct
   *       ancestors (e.g. rebinding the flattened {@code a.b.c} also drops {@code a} and {@code
   *       a.b}, since the lingering struct would shadow the rebind). Mutates {@code visible} in
   *       place to remove the dropped ancestors so downstream pipes don't see them.
   * </ul>
   */
  private SqlNodeList computeEvalSelectPrefix(
      Set<String> rebound, Frame frame, List<String> visible) {
    SqlNodeList items = new SqlNodeList(POS);
    if (rebound.isEmpty() || frame.currentFields == null) {
      items.add(SqlIdentifier.star(POS));
      return items;
    }
    Set<String> ancestorDrop = new java.util.HashSet<>();
    for (String r : rebound) {
      int idx = r.indexOf('.');
      while (idx >= 0) {
        ancestorDrop.add(r.substring(0, idx));
        idx = r.indexOf('.', idx + 1);
      }
    }
    for (String c : frame.currentFields) {
      if (rebound.contains(c) || ancestorDrop.contains(c)) continue;
      items.add(toIdentifier(c));
    }
    visible.removeAll(ancestorDrop);
    return items;
  }

  /**
   * Update Frame metadata maps after an eval let creates or rebinds {@code alias}. Tracks
   * downstream-visible facts that SQL emission alone can't carry through:
   *
   * <ul>
   *   <li>{@code evalAliasTypes}: static PPL type when inferable, so downstream castExpr / Compare
   *       can dispatch type-specific helpers without a validator probe.
   *   <li>{@code evalPassthroughSource}: when the let is a rename-only passthrough ({@code eval new
   *       = oldField}), record the original column so visitAggregation's doc_count optimization can
   *       substitute back. Chained passthroughs collapse to the original.
   *   <li>{@code mapColumns}: MAP-returning expressions (e.g. {@code geoip(...)}) so dotted refs
   *       like {@code info.city} route through ITEM dispatch.
   *   <li>{@code dottedEvalAliases}: alias names that themselves contain dots (e.g. spath's {@code
   *       doc.user.name}) so downstream references emit a quoted single-part identifier.
   * </ul>
   */
  private static void updateFrameForLet(Frame frame, String alias, Let let, Set<String> rebound) {
    DataType staticType = ExpressionConverter.staticTypeOf(let.getExpression(), frame);
    if (staticType != null) {
      frame.evalAliasTypes.put(alias, staticType);
    } else {
      frame.evalAliasTypes.remove(alias);
    }
    UnresolvedExpression rhsExpr = let.getExpression();
    QualifiedName rhsQn = null;
    if (rhsExpr instanceof QualifiedName q) rhsQn = q;
    else if (rhsExpr instanceof Field f && f.getField() instanceof QualifiedName q) rhsQn = q;
    if (rhsQn != null && !rhsQn.toString().equals(alias)) {
      String resolved = rhsQn.toString();
      Set<String> seen = new java.util.HashSet<>();
      while (frame.evalPassthroughSource.containsKey(resolved) && seen.add(resolved)) {
        resolved = frame.evalPassthroughSource.get(resolved);
      }
      frame.evalPassthroughSource.put(alias, resolved);
    } else {
      frame.evalPassthroughSource.remove(alias);
    }
    if (isMapProducingExpr(let.getExpression())) {
      frame.mapColumns.add(alias);
    } else if (rebound.contains(alias)) {
      frame.mapColumns.remove(alias);
    }
    if (alias.indexOf('.') >= 0) {
      frame.dottedEvalAliases.add(alias);
    }
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
    // newList must also lead with `*`. When visitEval rebinds an existing column it produces an
    // explicit list (no `*`); merging that into the child's `*` would emit both the original
    // column (via `*`) and the rebind, leaving the column ambiguous downstream (e.g.
    // `eval balance = 100 | fields balance` after `fields *` raises "Column 'balance' is
    // ambiguous"). Bail out so the caller falls back to wrapping.
    if (newList.size() < 1) return null;
    SqlNode newFirst = newList.get(0);
    if (!(newFirst instanceof SqlIdentifier ns) || !ns.isStar()) return null;
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
    // PPL `rename <map>.<path> as <alias>` — when the source is a MAP-typed column's sub-path
    // (e.g. `rename doc.user.name as username` over a MAP-typed `doc` produced by spath
    // auto-extract), emit `ITEM(doc, 'user.name') AS username` and add `username` to the visible
    // set. The map sub-path isn't in {@code frame.currentFields} so the rename loop above wouldn't
    // catch it; this post-pass handles MAP-path renames specifically.
    for (org.opensearch.sql.ast.expression.Map m : node.getRenameList()) {
      String origin = ((Field) m.getOrigin()).getField().toString();
      String target = ((Field) m.getTarget()).getField().toString();
      if (origin.contains("*") || target.contains("*")) continue;
      int dot = origin.indexOf('.');
      if (dot < 0) continue;
      String prefix = origin.substring(0, dot);
      if (!frame.mapColumns.contains(prefix)) continue;
      // Avoid double-add if the loop above already covered this column (unlikely since the
      // dotted MAP path isn't in cols, but be defensive).
      if (newVisible.contains(target)) continue;
      SqlNode item =
          new SqlBasicCall(
              SqlStdOperatorTable.ITEM,
              List.of(
                  new SqlIdentifier(prefix, POS),
                  SqlLiteral.createCharString(origin.substring(dot + 1), POS)),
              POS);
      items.add(asAliased(item, target));
      newVisible.add(target);
    }
    return SqlBuilder.select(items).from(from).withFields(newVisible).wrap(frame);
  }

  @Override
  public SqlNode visitAggregation(Aggregation node, Frame frame) {
    SqlNode from = node.getChild().get(0).accept(this, frame);
    // Detect nested-aggregation pattern AFTER walking the child so {@link Frame#arrayRootedFields}
    // (populated by visitRelation) is available for the array-rooted dotted-arg check. PPL
    // `stats min(<group>.<leaf>) by <group>` and `stats agg(<arrayRoot>.<leaf>) ...` both raise
    // PPL's documented "Cannot execute nested aggregation" error here so users see it instead
    // of a confusing "Table 'X' not found" from the validator.
    detectNestedAggregationPattern(node, frame);
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

    // Snapshot per-column type info BEFORE the aggregation clears the frame's column maps —
    // visitAggregation reuses the same Frame, but agg outputs can be statically typed from the
    // input column's type (sum/min/max preserve numeric input type; count returns LONG; avg
    // returns DOUBLE). We mirror this into the post-aggregation frame's evalAliasTypes so a
    // downstream Append/Multisearch/AppendPipe can compare per-side column types.
    java.util.Map<String, DataType> preAggColumnTypes =
        new java.util.LinkedHashMap<>(frame.columnPrimitiveType);
    java.util.Map<String, DataType> preAggEvalAliases =
        new java.util.LinkedHashMap<>(frame.evalAliasTypes);
    java.util.Map<String, DataType> postAggTypes = new java.util.LinkedHashMap<>();

    for (UnresolvedExpression aggExpr : node.getAggExprList()) {
      String alias = aggLabel(aggExpr);
      UnresolvedExpression core = (aggExpr instanceof Alias a) ? a.getDelegated() : aggExpr;
      SqlNode call = aggCall(core);
      items.add(asAliased(call, alias));
      visible.add(alias);
      DataType aggResultType = staticAggResultType(core, preAggColumnTypes, preAggEvalAliases);
      if (aggResultType != null) {
        postAggTypes.put(alias, aggResultType);
      }
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
      // Group keys preserve the input column's type. Snapshot for the post-agg frame.
      DataType groupType = staticTypeOfFromMaps(core, preAggColumnTypes, preAggEvalAliases);
      if (groupType != null) {
        postAggTypes.put(alias, groupType);
      }
    }
    // Aggregations destroy row-level collation; clear the lastOrderBy hint.
    frame.lastOrderBy = null;
    // Post-aggregation: replace per-column types with the agg-output types. Group keys retain
    // input type; aggs (sum/min/max/count/...) get their statically-derived result type. Non-
    // resolved aggs leave the entry absent (downstream Append will skip type-checking those).
    frame.columnPrimitiveType.clear();
    frame.evalAliasTypes.clear();
    frame.evalAliasTypes.putAll(postAggTypes);

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
    // doc_count optimization: when there's no GROUP BY and every agg is count(field) or
    // dc(field) with the same single field arg (no count(*) and no mixed fields), add
    // IS NOT NULL(field) so OpenSearch pushdown emits an `exists` query (uses doc_count
    // instead of value_count). Mirrors v2's CalciteRelNodeVisitor.java:1462-1483
    // (aggregateWithTrimming doc_count optimization). Run regardless of the bucket_nullable
    // branch above — when groupKeys is empty, the bucket_nullable branch produces no filter
    // and falls through here.
    if (where == null && groupKeys.isEmpty() && !node.getAggExprList().isEmpty()) {
      java.util.Set<String> distinctFields = new java.util.LinkedHashSet<>();
      boolean allEligible = true;
      for (UnresolvedExpression a : node.getAggExprList()) {
        UnresolvedExpression core = a instanceof Alias al ? al.getDelegated() : a;
        String name = null;
        UnresolvedExpression argExpr = null;
        if (core instanceof AggregateFunction af) {
          name = af.getFuncName().toLowerCase(java.util.Locale.ROOT);
          argExpr = af.getField();
        } else if (core instanceof org.opensearch.sql.ast.expression.Function f) {
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
        // Resolve through passthrough eval aliases so `count(lastname), count(name)` where
        // `eval name = lastname` collapses to a single distinct field. Mirrors v2's alias-
        // resolution which preserves the original column identity across rename-only Project
        // layers.
        String resolved = qn.toString();
        java.util.Set<String> seen = new java.util.HashSet<>();
        while (frame.evalPassthroughSource.containsKey(resolved) && seen.add(resolved)) {
          resolved = frame.evalPassthroughSource.get(resolved);
        }
        distinctFields.add(resolved);
      }
      if (allEligible && distinctFields.size() == 1) {
        String fieldName = distinctFields.iterator().next();
        where =
            new SqlBasicCall(
                SqlStdOperatorTable.IS_NOT_NULL, List.of(toIdentifier(fieldName)), POS);
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
    wrappedItems.add(asAliased(rowNum, "_row_number_rare_top_"));
    SqlNode windowSelect =
        new SqlSelect(
            POS, null, wrappedItems, aggSelect, null, null, null, null, null, null, null, null);

    SqlNode rnCheck =
        new SqlBasicCall(
            SqlStdOperatorTable.LESS_THAN_OR_EQUAL,
            List.of(
                new SqlIdentifier("_row_number_rare_top_", POS),
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
    // Type-compatibility pre-check (per field). PPL fillnull requires the replacement value's
    // type to match the field's type, otherwise the runtime cast surfaces as a confusing error
    // ("For input string ..."). When the row-type oracle is wired (Frame.columnPrimitiveType
    // populated), validate up front and emit PPL's documented error message.
    java.util.Set<String> targetFields = new java.util.LinkedHashSet<>();
    boolean implicitAllFields = forAll != null && perField.isEmpty();
    if (implicitAllFields) {
      targetFields.addAll(frame.currentFields);
    } else {
      targetFields.addAll(perField.keySet());
    }
    for (String c : targetFields) {
      UnresolvedExpression repl = perField.getOrDefault(c, forAll);
      if (repl == null) continue;
      DataType replType = ExpressionConverter.staticTypeOf(repl, frame);
      DataType fieldType = frame.columnPrimitiveType.get(c);
      if (fieldType == null && frame.evalAliasTypes.containsKey(c)) {
        fieldType = frame.evalAliasTypes.get(c);
      }
      if (replType != null && fieldType != null && !pplTypesCompatible(replType, fieldType)) {
        throw new IllegalArgumentException(
            String.format(
                "fillnull failed: replacement value type %s is not compatible with field '%s' "
                    + "(type: %s). The replacement value type must match the field type.",
                pplTypeToSqlNameForError(replType), c, pplTypeToSqlNameForError(fieldType)));
      }
    }

    SqlNodeList items = new SqlNodeList(POS);
    List<String> newVisible = new ArrayList<>(frame.currentFields);
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
    // PPL `fillnull using <map>.<path> = <repl>` over a MAP-typed prefix (e.g. spath
    // auto-extract): emit `COALESCE(ITEM(<map>, '<path>'), <repl>) AS <dotted-path>` and
    // register as a flat-dotted column. Mirrors visitReplace's MAP-path handling.
    for (java.util.Map.Entry<String, UnresolvedExpression> e : perField.entrySet()) {
      String c = e.getKey();
      if (frame.currentFields.contains(c)) continue;
      int dot = c.indexOf('.');
      if (dot <= 0 || !frame.mapColumns.contains(c.substring(0, dot))) continue;
      SqlNode item =
          new SqlBasicCall(
              SqlStdOperatorTable.ITEM,
              List.of(
                  new SqlIdentifier(c.substring(0, dot), POS),
                  SqlLiteral.createCharString(c.substring(dot + 1), POS)),
              POS);
      SqlNode coalesce =
          new SqlBasicCall(SqlStdOperatorTable.COALESCE, List.of(item, expr(e.getValue())), POS);
      items.add(asAliased(coalesce, c));
      newVisible.add(c);
      frame.dottedEvalAliases.add(c);
    }
    return SqlBuilder.select(items).from(from).withFields(newVisible).wrap(frame);
  }

  /**
   * True when {@code a} and {@code b} are PPL-BETWEEN-compatible (same family — both numeric, both
   * string, both date/time, or identical type). Used by the BETWEEN expression validator to throw
   * PPL's documented "BETWEEN expression types are incompatible" message.
   */
  static boolean pplBetweenBoundsCompatible(DataType a, DataType b) {
    if (a == b) return true;
    if (isNumericPplType(a) && isNumericPplType(b)) return true;
    return false;
  }

  /**
   * True when {@code a} and {@code b} are PPL-fillnull-compatible (numeric family or both string).
   * Used by visitFillNull to validate replacement-vs-field compatibility.
   */
  private static boolean pplTypesCompatible(DataType a, DataType b) {
    if (a == b) return true;
    boolean aNum = isNumericPplType(a);
    boolean bNum = isNumericPplType(b);
    if (aNum && bNum) return true;
    return false;
  }

  /**
   * Strict per-column type identity: PPL append/multisearch/appendpipe rejects cross-numeric
   * mismatches (BIGINT vs INTEGER) and not just cross-family mismatches. Mirrors v2's documented
   * "Unable to process column 'C' due to incompatible types" check.
   */
  private static boolean pplExactTypesCompatible(DataType a, DataType b) {
    return a == b;
  }

  /**
   * Detect type conflicts on shared columns across two UNION-ALL sides. PPL throws a documented
   * error when a column appears on both sides with statically-different types that don't widen
   * cleanly (e.g. one side BIGINT and the other DOUBLE after a `cast as double` eval).
   *
   * <p>Throws {@link IllegalArgumentException} with PPL's documented message format:
   *
   * <pre>Unable to process column 'C' due to incompatible types: [A, B]</pre>
   */
  private static void detectAppendTypeConflict(
      List<String> mainCols, List<String> subCols, Frame mainFrame, Frame subFrame) {
    java.util.Set<String> shared = new java.util.LinkedHashSet<>(mainCols);
    shared.retainAll(subCols);
    for (String c : shared) {
      DataType mainType = lookupColumnType(c, mainFrame);
      DataType subType = lookupColumnType(c, subFrame);
      if (mainType != null && subType != null && !pplExactTypesCompatible(mainType, subType)) {
        throw new IllegalArgumentException(
            String.format(
                "Unable to process column '%s' due to incompatible types: [%s, %s]",
                c, pplTypeToSqlNameForError(mainType), pplTypeToSqlNameForError(subType)));
      }
    }
  }

  /**
   * Look up a column's static type from a Frame's columnPrimitiveType (catalog-derived) or
   * evalAliasTypes (eval/agg-derived). Returns {@code null} when the column isn't tracked.
   */
  private static DataType lookupColumnType(String name, Frame frame) {
    if (frame.evalAliasTypes.containsKey(name)) return frame.evalAliasTypes.get(name);
    if (frame.columnPrimitiveType.containsKey(name)) return frame.columnPrimitiveType.get(name);
    return null;
  }

  /** Map PPL DataType to the SQL type name used in fillnull error messages. */
  static String pplTypeToSqlNameForError(DataType t) {
    switch (t) {
      case BOOLEAN:
        return "BOOLEAN";
      case SHORT:
        return "SMALLINT";
      case INTEGER:
        return "INTEGER";
      case LONG:
        return "BIGINT";
      case FLOAT:
        return "FLOAT";
      case DOUBLE:
        return "DOUBLE";
      case DECIMAL:
        return "DECIMAL";
      case STRING:
        return "VARCHAR";
      case DATE:
        return "DATE";
      case TIME:
        return "TIME";
      case TIMESTAMP:
        return "TIMESTAMP";
      case IP:
        return "IP";
      default:
        return t.name();
    }
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
      // v2 emits min_span as DOUBLE literal (e.g. 5.0E0:DOUBLE); wrap in CAST AS DOUBLE.
      SqlNode minVal = minOver(fieldRef);
      SqlNode maxVal = maxOver(fieldRef);
      SqlNode dataRange = new SqlBasicCall(SqlStdOperatorTable.MINUS, List.of(maxVal, minVal), POS);
      org.apache.calcite.sql.SqlDataTypeSpec doubleSpec =
          new org.apache.calcite.sql.SqlDataTypeSpec(
              new org.apache.calcite.sql.SqlBasicTypeNameSpec(
                  org.apache.calcite.sql.type.SqlTypeName.DOUBLE, POS),
              POS);
      SqlNode minSpanCast =
          new SqlBasicCall(
              org.apache.calcite.sql.fun.SqlStdOperatorTable.CAST,
              List.of(expr(msb.getMinspan()), doubleSpec),
              POS);
      bucketCall =
          new SqlBasicCall(
              org.opensearch.sql.expression.function.PPLBuiltinOperators.MINSPAN_BUCKET,
              List.of(fieldRef, minSpanCast, dataRange, maxVal),
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
    // (mirrors v2's emission shape). Without a row-type oracle, walk frame.currentFields. Skip:
    //   - the bin alias itself (re-emitted as the bucketCall expression)
    //   - intermediate MAP-typed columns (Frame.mapColumns); for OpenSearch nested mappings,
    //     traverseAndFlatten populates currentFields with intermediate parents like `resource`,
    //     `resource.attributes`, ... which v2's emission omits when binning a leaf path
    // Metadata fields (`_id`, `_index`, ...) are KEPT in the inner Project — v2 emits them
    // here and the implicit final-Project / user `| fields` drops them downstream.
    if (frame.currentFields == null) {
      throw new UnsupportedOperationException(
          "bin requires a known column list — call after a `| fields ...` pipe");
    }
    SqlNodeList items = new SqlNodeList(POS);
    List<String> visible = new ArrayList<>();
    for (String c : frame.currentFields) {
      if (c.equals(alias)) continue;
      if (frame.mapColumns.contains(c)) continue;
      // Dotted column names (flattened from OpenSearch nested mappings) need quoted single-part
      // identifiers so the validator looks them up literally instead of treating the prefix as
      // a table reference.
      SqlNode ref =
          c.indexOf('.') < 0
              ? toIdentifier(c)
              : ExpressionConverter.quotedIdentifier(java.util.List.of(c));
      items.add(ref);
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
    // Strip metadata fields from each side's column list (same pattern as visitMultisearch /
    // visitUnion). v2 emits per-side user-only Projects; padding with metadata in our schema
    // produces extra-wide Project layers.
    List<String> mainCols = new ArrayList<>(stripMeta(frame.currentFields));

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
    List<String> subCols = new ArrayList<>(stripMeta(subFrame.currentFields));

    // Unified column order: main's order first, then sub's columns absent on main (preserving
    // sub's order). Mirrors v2 emission shape so explain plans line up.
    List<String> unified = new ArrayList<>(mainCols);
    for (String c : subCols) {
      if (!unified.contains(c)) {
        unified.add(c);
      }
    }
    // Type-conflict pre-check: when a column appears on BOTH sides with different statically-known
    // types (and they aren't in the same numeric family), throw PPL's documented error message.
    detectAppendTypeConflict(mainCols, subCols, frame, subFrame);
    SqlNode mainPadded = padToUnifiedSchema(mainBody, mainCols, unified, subFrame.columnUdt);
    SqlNode subPadded = padToUnifiedSchema(sub, subCols, unified, frame.columnUdt);
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
    // Merge sub's UDT info into main's so downstream pipes pick up UDT columns from either side.
    for (java.util.Map.Entry<String, String> e : subFrame.columnUdt.entrySet()) {
      frame.columnUdt.putIfAbsent(e.getKey(), e.getValue());
    }
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

  private SqlNode padToUnifiedSchema(SqlNode body, List<String> present, List<String> unified) {
    return padToUnifiedSchema(body, present, unified, java.util.Map.of());
  }

  /**
   * Pad {@code body} to {@code unified}'s columns. For each column absent from {@code present},
   * emit {@code NULL AS <col>} unless the column is in {@code referenceUdt} (the OTHER side's
   * column-to-UDT map). When a UDT is known, wrap the NULL in the corresponding PPL UDF ({@code
   * TIMESTAMP}, {@code DATE}, {@code TIME}, {@code IP}) so the UDT survives the UNION ALL through
   * least-restrictive type computation.
   */
  private SqlNode padToUnifiedSchema(
      SqlNode body,
      List<String> present,
      List<String> unified,
      java.util.Map<String, String> referenceUdt) {
    Set<String> presentSet = new java.util.LinkedHashSet<>(present);
    SqlNodeList items = new SqlNodeList(POS);
    org.apache.calcite.sql.SqlDataTypeSpec varcharSpec =
        new org.apache.calcite.sql.SqlDataTypeSpec(
            new org.apache.calcite.sql.SqlBasicTypeNameSpec(
                org.apache.calcite.sql.type.SqlTypeName.VARCHAR, POS),
            POS);
    for (String c : unified) {
      if (presentSet.contains(c)) {
        items.add(toIdentifier(c));
      } else {
        String udt = referenceUdt.get(c);
        SqlNode nullPad;
        if (udt != null) {
          // CAST(NULL AS VARCHAR) keeps the UDF operand checker happy (PPL UDTs require CHARACTER
          // input); the wrapping UDF returns the UDT and propagates NULL via NullPolicy.ANY.
          SqlNode castNull =
              new SqlBasicCall(
                  org.apache.calcite.sql.fun.SqlStdOperatorTable.CAST,
                  List.of(SqlLiteral.createNull(POS), varcharSpec),
                  POS);
          org.apache.calcite.sql.SqlOperator op = ExpressionConverter.udtConstructorOpForRoot(udt);
          if (op != null) {
            nullPad = new SqlBasicCall(op, List.of(castNull), POS);
          } else {
            nullPad = SqlLiteral.createNull(POS);
          }
        } else {
          nullPad = SqlLiteral.createNull(POS);
        }
        items.add(asAliased(nullPad, c));
      }
    }
    return new SqlSelect(POS, null, items, body, null, null, null, null, null, null, null, null);
  }

  /**
   * Return the lowercase UDT root name ({@code timestamp/date/time/ip}) when {@code e} is a
   * QualifiedName/Field whose target column is in {@link Frame#columnUdt}, or {@code null}
   * otherwise.
   */
  String qualifiedNameUdt(UnresolvedExpression e) {
    if (exprFrame == null || exprFrame.columnUdt.isEmpty()) return null;
    QualifiedName qn = null;
    if (e instanceof QualifiedName q) qn = q;
    else if (e instanceof Field f && f.getField() instanceof QualifiedName q) qn = q;
    if (qn == null) return null;
    String name = qn.toString();
    String udt = exprFrame.columnUdt.get(name);
    if (udt != null) return udt;
    // Tolerate alias-qualified refs like `<alias>.host` — strip the leading alias and try again.
    int dot = name.indexOf('.');
    if (dot > 0) {
      return exprFrame.columnUdt.get(name.substring(dot + 1));
    }
    return null;
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
    detectAppendTypeConflict(mainCols, subCols, mainFrame, subFrame);
    SqlNode mainPadded = padToUnifiedSchema(mainBody, mainCols, unified, subFrame.columnUdt);
    SqlNode subPadded = padToUnifiedSchema(subBody, subCols, unified, mainFrame.columnUdt);
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
    // Merge per-side columnUdt into outer frame so downstream pipes preserve UDT info.
    frame.columnUdt.putAll(mainFrame.columnUdt);
    for (java.util.Map.Entry<String, String> e : subFrame.columnUdt.entrySet()) {
      frame.columnUdt.putIfAbsent(e.getKey(), e.getValue());
    }
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
    List<java.util.Map<String, String>> branchUdt = new ArrayList<>();
    List<Frame> branchFrames = new ArrayList<>();
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
      // Strip metadata fields from each branch's column list before padding. v2's RelBuilder
      // emits per-branch user-only Projects above the Filter; including metadata in our branch
      // schema produces an extra-wide Project layer (testExplainMultisearchBasic).
      List<String> userOnlyCols = stripMeta(subFrame.currentFields);
      branchCols.add(new ArrayList<>(userOnlyCols));
      branchUdt.add(new java.util.LinkedHashMap<>(subFrame.columnUdt));
      branchFrames.add(subFrame);
      for (String c : userOnlyCols) {
        if (!unified.contains(c)) {
          unified.add(c);
        }
      }
    }
    // Type-conflict check across pairs of branches that share a column.
    for (int i = 0; i < branchFrames.size(); i++) {
      for (int j = i + 1; j < branchFrames.size(); j++) {
        detectAppendTypeConflict(
            branchCols.get(i), branchCols.get(j), branchFrames.get(i), branchFrames.get(j));
      }
    }
    if (branchNodes.isEmpty()) {
      throw new IllegalArgumentException("Multisearch requires at least one non-empty subsearch");
    }
    // Merged-of-all-other-branches UDT map per branch — so a branch missing column X gets a
    // typed pad whenever any OTHER branch has X with a UDT type.
    java.util.Map<String, String> allUdt = new java.util.LinkedHashMap<>();
    for (java.util.Map<String, String> bm : branchUdt) {
      for (java.util.Map.Entry<String, String> e : bm.entrySet()) {
        allUdt.putIfAbsent(e.getKey(), e.getValue());
      }
    }
    SqlNode union = padToUnifiedSchema(branchNodes.get(0), branchCols.get(0), unified, allUdt);
    for (int i = 1; i < branchNodes.size(); i++) {
      SqlNode padded = padToUnifiedSchema(branchNodes.get(i), branchCols.get(i), unified, allUdt);
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
    // Propagate per-branch UDT info onto the outer frame so downstream pipes preserve UDT.
    frame.columnUdt.putAll(allUdt);
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
    //
    // When ALL stream-window aggs are earliest/latest (ARG_MIN/ARG_MAX), they carry their own
    // ordering via the second arg (the time field). v2 emits `OVER (ROWS UNBOUNDED PRECEDING)`
    // without an ORDER BY clause; mirror that by skipping the OVER ORDER BY but still synthesising
    // __stream_seq__ for outer Sort. Mirrors legacy commit d4b48e1ba1.
    boolean allArgMinMax = true;
    if (node.getWindowFunctionList() == null || node.getWindowFunctionList().isEmpty()) {
      allArgMinMax = false;
    } else {
      for (UnresolvedExpression ae : node.getWindowFunctionList()) {
        UnresolvedExpression core = (ae instanceof Alias a) ? a.getDelegated() : ae;
        if (core instanceof org.opensearch.sql.ast.expression.WindowFunction wf) {
          core = wf.getFunction();
        }
        String fname = null;
        if (core instanceof org.opensearch.sql.ast.expression.AggregateFunction af) {
          fname = af.getFuncName();
        } else if (core instanceof org.opensearch.sql.ast.expression.Function fn) {
          fname = fn.getFuncName();
        }
        if (fname == null) {
          allArgMinMax = false;
          break;
        }
        fname = fname.toLowerCase(java.util.Locale.ROOT);
        if (!fname.equals("earliest") && !fname.equals("latest")) {
          allArgMinMax = false;
          break;
        }
      }
    }
    SqlNodeList orderBy = new SqlNodeList(POS);
    SqlNode wrappedFrom = from;
    List<String> postSeqFields = frame.currentFields;
    // For allArgMinMax with no partitioning (`by`), v2 emits a bare OVER (ROWS UNBOUNDED
    // PRECEDING) without seq synthesis — ARG_MIN/ARG_MAX over the whole stream collapses to a
    // global earliest/latest, no need for stable ordering. Skip synthesis in that case.
    boolean skipSeqSynthForArgMinMax = allArgMinMax && !hasGroup && !hasReset && !useRange;
    if (skipSeqSynthForArgMinMax) {
      // No ORDER BY, no seq — leave both unset so the OVER below has no ORDER BY clause.
    } else if (frame.currentFields.contains("__stream_seq__")) {
      // Upstream streamstats already provides the seq — reuse it for stable ordering. Don't
      // re-synthesise.
      if (!allArgMinMax || hasReset || useRange) {
        orderBy.add(new SqlIdentifier("__stream_seq__", POS));
      }
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
      // For allArgMinMax (earliest/latest), the OVER itself doesn't need ORDER BY because
      // ARG_MIN/ARG_MAX carry their own ordering via the time-field arg. The synthesised
      // __stream_seq__ still flows through to a downstream Sort for stable result ordering.
      if (!allArgMinMax || hasReset || useRange) {
        orderBy.add(new SqlIdentifier("__stream_seq__", POS));
      }
    }
    // Reset support: each row's reset flag bumps a __seg_id__; cumulative aggregate restarts
    // when segment id changes. Compute __seg_id__ in a wrapping SELECT and prepend it to the
    // partition-by so the cumulative window restarts on segment boundaries.
    if (hasReset) {
      SqlNode beforeFlag =
          node.getResetBefore() != null
              ? ExpressionConverter.caseFlagOneZero(expr(node.getResetBefore()))
              : SqlLiteral.createExactNumeric("0", POS);
      SqlNode afterFlag =
          node.getResetAfter() != null
              ? ExpressionConverter.caseFlagOneZero(expr(node.getResetAfter()))
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
    //
    // Keep metadata fields here — v2's streamstats Project preserves metadata, and the
    // planner-level stripMetadataFields composes the strip into the final outer Project so
    // there's no extra layer.
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
      // For windowed AVG: emit SUM(x)/CAST(COUNT(x) AS DOUBLE) directly to bypass Calcite's
      // AvgVarianceConvertlet (matches v2's emission shape — see visitWindow for rationale).
      boolean isWindowedAvg =
          aggNode instanceof SqlBasicCall avgBc
              && avgBc.getOperator() == SqlStdOperatorTable.AVG
              && avgBc.getOperandList().size() == 1;
      SqlNode over;
      if (isWindowedAvg) {
        SqlNode arg = ((SqlBasicCall) aggNode).getOperandList().get(0);
        SqlNode sumCall = new SqlBasicCall(SqlStdOperatorTable.SUM, List.of(arg), POS);
        SqlNode sumOver = new SqlBasicCall(SqlStdOperatorTable.OVER, List.of(sumCall, window), POS);
        SqlNode countCall = new SqlBasicCall(SqlStdOperatorTable.COUNT, List.of(arg), POS);
        SqlNode countOver =
            new SqlBasicCall(SqlStdOperatorTable.OVER, List.of(countCall, window), POS);
        org.apache.calcite.sql.SqlDataTypeSpec doubleSpec =
            new org.apache.calcite.sql.SqlDataTypeSpec(
                new org.apache.calcite.sql.SqlBasicTypeNameSpec(
                    org.apache.calcite.sql.type.SqlTypeName.DOUBLE, POS),
                POS);
        SqlNode countAsDouble =
            new SqlBasicCall(SqlStdOperatorTable.CAST, List.of(countOver, doubleSpec), POS);
        over = new SqlBasicCall(SqlStdOperatorTable.DIVIDE, List.of(sumOver, countAsDouble), POS);
      } else {
        over = new SqlBasicCall(SqlStdOperatorTable.OVER, List.of(aggNode, window), POS);
      }
      // Same NULL-guard as visitWindow: VAR/STDDEV over empty/all-NULL return NULL. AVG handled
      // via SUM/CAST(COUNT) shape above (NULL division returns NULL via PPL DIVIDE semantics).
      if (aggNode instanceof SqlBasicCall bc && bc.getOperandList().size() == 1) {
        org.apache.calcite.sql.SqlOperator op = bc.getOperator();
        boolean needsZeroGuard =
            op == SqlStdOperatorTable.VAR_POP || op == SqlStdOperatorTable.STDDEV_POP;
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
    SqlBuilder.SelectBuilder rb = SqlBuilder.select(items).from(wrappedFrom).withFields(visible);
    // For allArgMinMax (earliest/latest only), we skipped the OVER's ORDER BY since
    // ARG_MIN/ARG_MAX carry their own ordering. The synthesized __stream_seq__ still needs to
    // sort the result for stable streaming order — emit an outer ORDER BY __stream_seq__ here
    // so v2's `LogicalSort(__stream_seq__) <- LogicalProject(window+seq) <- ...` shape is
    // reproduced. Skip when seq isn't in scope (no synthesis happened).
    if (allArgMinMax && !hasReset && !useRange && visible.contains("__stream_seq__")) {
      rb.orderBy(List.of(new SqlIdentifier("__stream_seq__", POS)));
    }
    SqlNode result = rb.wrap(frame);
    // Streamstats with `by` partitioning: a downstream `reverse` should flip the per-partition
    // streaming order (testStreamstatsByWithReverse). __stream_seq__ is included in postSeqFields
    // and propagated through `visible`, so it's resolvable in the outer scope. Set it as the
    // active sort on the frame after wrap so visitReverse can flip via DESC. The post-RelNode
    // `stripSyntheticSeqColumns` shuttle drops it from the user-facing output. Without `by`,
    // reverse stays a no-op per visitReverse's comment.
    if (hasGroup && postSeqFields.contains("__stream_seq__")) {
      // Wrap with NULLS_LAST so reverseSortKeys flips it (the helper only flips NULLS_*-wrapped
      // keys; bare identifiers pass through unchanged).
      SqlNode seqKey =
          new SqlBasicCall(
              SqlStdOperatorTable.NULLS_LAST,
              List.of(new SqlIdentifier("__stream_seq__", POS)),
              POS);
      frame.lastOrderBy = List.of(seqKey);
    }
    return result;
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
    // Filter metadata fields out of the projection — visitWindow's outer SELECT projects the
    // currentFields followed by the window-aggregated columns; including metadata here produces
    // an extra LogicalProject layer when the planner-level stripMetadataFields shuttle later
    // trims the row type. Mirrors the visitDedupe pattern (commit e36c34d48a).
    SqlNodeList items = new SqlNodeList(POS);
    List<String> visible = new ArrayList<>();
    for (String c : frame.currentFields) {
      if (OpenSearchConstants.METADATAFIELD_TYPE_MAP.containsKey(c)) continue;
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
      // For windowed AVG: emit SUM(x)/CAST(COUNT(x) AS DOUBLE) directly instead of relying on
      // Calcite's AvgVarianceConvertlet to expand AVG. The convertlet emits a redundant
      // `CASE(>(COUNT, 0), CAST(SUM):DOUBLE, null) / COUNT` shape; v2 emits the simpler
      // `SUM/CAST(COUNT)` form. Outer NULL-guard from partitionNotNullCheck (when present)
      // already covers the all-NULL/empty-partition case at the partition level.
      boolean isWindowedAvg =
          aggNode instanceof SqlBasicCall avgBc
              && avgBc.getOperator() == SqlStdOperatorTable.AVG
              && avgBc.getOperandList().size() == 1;
      SqlNode over;
      if (isWindowedAvg) {
        SqlNode arg = ((SqlBasicCall) aggNode).getOperandList().get(0);
        SqlNode sumCall = new SqlBasicCall(SqlStdOperatorTable.SUM, List.of(arg), POS);
        SqlNode sumOver = new SqlBasicCall(SqlStdOperatorTable.OVER, List.of(sumCall, window), POS);
        SqlNode countCall = new SqlBasicCall(SqlStdOperatorTable.COUNT, List.of(arg), POS);
        SqlNode countOver =
            new SqlBasicCall(SqlStdOperatorTable.OVER, List.of(countCall, window), POS);
        org.apache.calcite.sql.SqlDataTypeSpec doubleSpec =
            new org.apache.calcite.sql.SqlDataTypeSpec(
                new org.apache.calcite.sql.SqlBasicTypeNameSpec(
                    org.apache.calcite.sql.type.SqlTypeName.DOUBLE, POS),
                POS);
        SqlNode countAsDouble =
            new SqlBasicCall(SqlStdOperatorTable.CAST, List.of(countOver, doubleSpec), POS);
        over = new SqlBasicCall(SqlStdOperatorTable.DIVIDE, List.of(sumOver, countAsDouble), POS);
      } else {
        over = new SqlBasicCall(SqlStdOperatorTable.OVER, List.of(aggNode, window), POS);
      }
      // Calcite's standard AVG over a partition with all-NULL values returns 0 (the SUM/COUNT
      // convertlet yields 0/0 in the enumerable runtime). PPL semantics: NULL when no non-NULL
      // rows contributed. For VAR_SAMP/STDDEV_SAMP need n>1 (Bessel's correction); for
      // VAR_POP/STDDEV_POP need n>0. AVG is handled via the SUM/CAST(COUNT) shape above which
      // returns NULL naturally for divide-by-zero (PPL DIVIDE semantics).
      if (aggNode instanceof SqlBasicCall bc && bc.getOperandList().size() == 1) {
        org.apache.calcite.sql.SqlOperator op = bc.getOperator();
        boolean needsZeroGuard =
            op == SqlStdOperatorTable.VAR_POP || op == SqlStdOperatorTable.STDDEV_POP;
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

    // PPL `trendline sort <fld> sma(...)` puts the ordering BEFORE the window: the outer SELECT
    // sorts the input, then the OVER clause computes over the already-sorted stream. v2's
    // RelBuilder emits this layout (LogicalSort below the trendline Project, OVER without ORDER
    // BY). Calcite's Window operator preserves input collation, so omitting ORDER BY in OVER is
    // semantically equivalent.
    SqlNodeList windowOrderBy = new SqlNodeList(POS);
    SqlNode sortKeyForFromClause = null;
    if (node.getSortByField().isPresent()) {
      Field sortField = node.getSortByField().get();
      Sort.SortOption opt = analyzeSortOption(sortField.getFieldArgs());
      SqlNode key = expr(sortField.getField());
      if (opt.getSortOrder() == Sort.SortOrder.DESC) {
        key = new SqlBasicCall(SqlStdOperatorTable.DESC, List.of(key), POS);
      }
      sortKeyForFromClause = key;
    }

    // Wrap the child with the NULL pre-filter so the trendline columns and pre-existing columns
    // come from the same SELECT scope. When a sort key is present, layer it BELOW the filter as
    // a separate SELECT-with-ORDER-BY so the resulting plan is `Filter <- Sort <- ... <- from`,
    // matching v2's emission shape (the trendline window then sees an already-sorted, filtered
    // stream).
    SqlNode preFiltered = from;
    if (sortKeyForFromClause != null) {
      SqlNodeList sortByList = new SqlNodeList(List.of(sortKeyForFromClause), POS);
      preFiltered =
          new SqlSelect(
              POS,
              null,
              new SqlNodeList(List.of(SqlIdentifier.star(POS)), POS),
              from,
              null,
              null,
              null,
              null,
              sortByList,
              null,
              null,
              null);
    }
    SqlNode filtered =
        new SqlSelect(
            POS,
            null,
            new SqlNodeList(List.of(SqlIdentifier.star(POS)), POS),
            preFiltered,
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
          // Emit SUM(x)/CAST(COUNT(x) AS DOUBLE) directly to bypass Calcite's
          // AvgVarianceConvertlet (matches v2's SUM/CAST(COUNT) shape — see visitWindow).
          SqlNode sumCall = new SqlBasicCall(SqlStdOperatorTable.SUM, List.of(fieldRef), POS);
          SqlNode sumOver =
              new SqlBasicCall(SqlStdOperatorTable.OVER, List.of(sumCall, window), POS);
          SqlNode countCallArg =
              new SqlBasicCall(SqlStdOperatorTable.COUNT, List.of(fieldRef), POS);
          SqlNode countArgOver =
              new SqlBasicCall(SqlStdOperatorTable.OVER, List.of(countCallArg, window), POS);
          org.apache.calcite.sql.SqlDataTypeSpec doubleSpecAvg =
              new org.apache.calcite.sql.SqlDataTypeSpec(
                  new org.apache.calcite.sql.SqlBasicTypeNameSpec(
                      org.apache.calcite.sql.type.SqlTypeName.DOUBLE, POS),
                  POS);
          SqlNode countAsDouble =
              new SqlBasicCall(SqlStdOperatorTable.CAST, List.of(countArgOver, doubleSpecAvg), POS);
          windowedAgg =
              new SqlBasicCall(SqlStdOperatorTable.DIVIDE, List.of(sumOver, countAsDouble), POS);
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
    return b.wrap(frame);
  }

  @Override
  public SqlNode visitUnion(Union node, Frame frame) {
    // PPL `| union [<plan1>, <plan2>, ...]` is UNION ALL of N datasets. Use
    // NonFallbackCalciteException so the validation error surfaces to the user as a 400
    // response instead of silently falling back to the V2 engine (which would mask the bad
    // input by trying to translate it differently).
    if (node.getDatasets() == null || node.getDatasets().size() < 2) {
      throw new org.opensearch.sql.exception.NonFallbackCalciteException(
          "Union command requires at least two datasets. Provided: "
              + (node.getDatasets() == null ? 0 : node.getDatasets().size()));
    }
    List<SqlNode> branches = new ArrayList<>();
    List<List<String>> branchCols = new ArrayList<>();
    List<java.util.Map<String, String>> branchUdt = new ArrayList<>();
    List<Frame> branchFrames = new ArrayList<>();
    List<String> unified = new ArrayList<>();
    for (UnresolvedPlan ds : node.getDatasets()) {
      Frame branchFrame = new Frame();
      Frame savedExpr = this.exprFrame;
      this.exprFrame = branchFrame;
      SqlNode branch = stripImplicitMetaProjects(ds).accept(this, branchFrame);
      this.exprFrame = savedExpr;
      // PPL `| union <table1>, <table2>` resolves each side via visitRelation, which returns a
      // bare SqlIdentifier. Calcite's UNION ALL operand expects a SELECT/VALUES/SET_OP — passing
      // a bare identifier raises "Was not expecting value 'IDENTIFIER' for enumeration
      // 'org.apache.calcite.sql.SqlKind' in this context". Wrap as `SELECT * FROM <branch>`.
      if (branch instanceof SqlIdentifier) {
        SqlNodeList wrapStar = new SqlNodeList(POS);
        wrapStar.add(SqlIdentifier.star(POS));
        branch =
            new SqlSelect(
                POS, null, wrapStar, branch, null, null, null, null, null, null, null, null);
      }
      branches.add(branch);
      // Strip metadata fields from each branch's column list before padding (same pattern as
      // visitMultisearch). v2 emits per-branch user-only Projects; padding with metadata in our
      // schema produces extra-wide Project layers (testExplainUnion).
      List<String> bcols =
          branchFrame.currentFields == null
              ? new ArrayList<>()
              : new ArrayList<>(stripMeta(branchFrame.currentFields));
      branchCols.add(bcols);
      branchUdt.add(new java.util.LinkedHashMap<>(branchFrame.columnUdt));
      branchFrames.add(branchFrame);
      for (String c : bcols) {
        if (!unified.contains(c)) unified.add(c);
      }
    }
    // PPL union does NOT raise on numeric/string type-conflicts (unlike append/multisearch). The
    // unified row type is computed by Calcite's UNION ALL leastRestrictive, which widens numeric
    // and string types automatically. Skip the detectAppendTypeConflict call here.
    // Merged-of-all-other-branches UDT map so absent columns get typed pads whenever any other
    // branch has that column as a UDT.
    java.util.Map<String, String> allUdt = new java.util.LinkedHashMap<>();
    for (java.util.Map<String, String> bm : branchUdt) {
      for (java.util.Map.Entry<String, String> e : bm.entrySet()) {
        allUdt.putIfAbsent(e.getKey(), e.getValue());
      }
    }
    // Pad each branch to the unified column list so UNION ALL row-type derivation succeeds.
    // Branches whose currentFields are unknown (bare table relations without a row-type oracle
    // entry) skip padding — the validator will still raise a column-count error if schemas
    // don't align, but explicit-fields branches (the common PPL union shape) work.
    SqlNode firstPadded =
        branchCols.get(0).isEmpty()
            ? branches.get(0)
            : padToUnifiedSchema(branches.get(0), branchCols.get(0), unified, allUdt);
    SqlNode union = firstPadded;
    for (int i = 1; i < branches.size(); i++) {
      SqlNode padded =
          branchCols.get(i).isEmpty()
              ? branches.get(i)
              : padToUnifiedSchema(branches.get(i), branchCols.get(i), unified, allUdt);
      union = new SqlBasicCall(SqlStdOperatorTable.UNION_ALL, List.of(union, padded), POS);
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
    if (!unified.isEmpty()) {
      frame.currentFields = unified;
      frame.columnUdt.putAll(allUdt);
    }
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
    // VARCHAR(N) width = max(label.length, labelField.length). Mirrors v2's emission so the
    // UNION-ALL leastRestrictive type computation produces a single VARCHAR(N).
    int labelVarcharWidth = Math.max(label.length(), labelField == null ? 0 : labelField.length());

    // Wrap the main pipeline as a subquery so we can UNION ALL it with the summary row.
    SqlNodeList mainProj = new SqlNodeList(POS);
    for (String c : frame.currentFields) {
      mainProj.add(toIdentifier(c));
    }
    if (appendLabelField) {
      // Emit `CAST(NULL AS VARCHAR(N))` so the UNION's row-type derivation resolves to VARCHAR(N),
      // matching v2's typed-null emission shape.
      org.apache.calcite.sql.SqlDataTypeSpec varcharSpec =
          new org.apache.calcite.sql.SqlDataTypeSpec(
              new org.apache.calcite.sql.SqlBasicTypeNameSpec(
                  org.apache.calcite.sql.type.SqlTypeName.VARCHAR, labelVarcharWidth, POS),
              POS);
      SqlNode pad =
          new SqlBasicCall(
              SqlStdOperatorTable.CAST, List.of(SqlLiteral.createNull(POS), varcharSpec), POS);
      mainProj.add(asAliased(pad, labelField));
    }
    SqlSelect mainProjected =
        new SqlSelect(POS, null, mainProj, from, null, null, null, null, null, null, null, null);

    // Summary row: SELECT SUM(c) AS c (or NULL/label) FROM (mainProjected). When the user did
    // NOT provide an explicit field list, restrict SUM to statically-known numeric columns —
    // including a non-numeric column would surface a runtime NumberFormatException ("For input
    // string: '880 Holmes Lane'"). Falls back to summing every column when the row-type oracle
    // doesn't have type info (Frame.columnPrimitiveType empty).
    SqlNodeList summaryProj = new SqlNodeList(POS);
    for (String c : frame.currentFields) {
      boolean shouldAgg;
      if (explicitFields) {
        shouldAgg = aggFieldNames.contains(c);
      } else if (frame.columnUdt.containsKey(c)) {
        // Skip UDT columns (DATE/TIME/TIMESTAMP/IP/BINARY) — SUM(date_string) raises a
        // NumberFormatException at runtime.
        shouldAgg = false;
      } else {
        DataType dt = frame.columnPrimitiveType.get(c);
        if (dt == null && frame.evalAliasTypes.containsKey(c)) {
          dt = frame.evalAliasTypes.get(c);
        }
        // dt == null → unknown type, default to summing (legacy behaviour). dt non-null and
        // non-numeric → skip; the column shows as NULL in the summary row.
        shouldAgg = dt == null || isNumericPplType(dt);
      }
      if (shouldAgg) {
        SqlNode sum =
            new SqlBasicCall(SqlStdOperatorTable.SUM, List.of(new SqlIdentifier(c, POS)), POS);
        summaryProj.add(asAliased(sum, c));
      } else if (labelField != null && c.equals(labelField)) {
        summaryProj.add(asAliased(SqlLiteral.createCharString(label, POS), c));
      } else if (frame.columnUdt.containsKey(c)) {
        // UDT column — emit `<UDT>(CAST(NULL AS VARCHAR))` so UNION-ALL leastRestrictive type
        // computation preserves EXPR_TIMESTAMP/DATE/TIME/IP through the summary side. Bare NULL
        // would collapse the UDT to NULL/BIGINT, breaking the UNION row-type derivation.
        String udt = frame.columnUdt.get(c);
        org.apache.calcite.sql.SqlOperator udtOp = ExpressionConverter.udtConstructorOpForRoot(udt);
        if (udtOp != null) {
          org.apache.calcite.sql.SqlDataTypeSpec varcharSpec =
              new org.apache.calcite.sql.SqlDataTypeSpec(
                  new org.apache.calcite.sql.SqlBasicTypeNameSpec(
                      org.apache.calcite.sql.type.SqlTypeName.VARCHAR, POS),
                  POS);
          SqlNode castNull =
              new SqlBasicCall(
                  SqlStdOperatorTable.CAST, List.of(SqlLiteral.createNull(POS), varcharSpec), POS);
          SqlNode udtNull = new SqlBasicCall(udtOp, List.of(castNull), POS);
          summaryProj.add(asAliased(udtNull, c));
        } else {
          summaryProj.add(asAliased(SqlLiteral.createNull(POS), c));
        }
      } else {
        summaryProj.add(asAliased(SqlLiteral.createNull(POS), c));
      }
    }
    if (appendLabelField) {
      // Bare char literal — UNION-ALL leastRestrictive widens it to match the main-side typed-null.
      summaryProj.add(asAliased(SqlLiteral.createCharString(label, POS), labelField));
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
        // Implicit field list: sum every visible column. PPL's runtime would NumberFormatException
        // on a non-numeric value mid-row, so filter to statically-known numeric columns when the
        // row-type oracle is wired (Frame.columnPrimitiveType is populated by visitRelation). When
        // no oracle info is available (no rowTypeOracle, or all columns are eval/agg outputs with
        // unknown static type), fall back to the legacy "sum everything" behaviour.
        sumNames = new ArrayList<>();
        for (String c : frame.currentFields) {
          // Skip UDT columns (DATE/TIME/TIMESTAMP/IP/BINARY) — SUM(date_string) raises a
          // NumberFormatException at runtime. Only include statically-numeric or unknown.
          if (frame.columnUdt.containsKey(c)) continue;
          DataType dt = frame.columnPrimitiveType.get(c);
          if (dt == null && frame.evalAliasTypes.containsKey(c)) {
            dt = frame.evalAliasTypes.get(c);
          }
          if (dt == null || isNumericPplType(dt)) {
            sumNames.add(c);
          }
        }
        if (sumNames.isEmpty()) {
          // No statically-numeric columns. Sum everything (legacy behaviour) so a runtime cast
          // fires the same exception the user expects.
          sumNames = new ArrayList<>(frame.currentFields);
        }
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
      // VARCHAR(N) width = max(label.length, labelField.length). Mirrors v2's emission so the
      // UNION-ALL leastRestrictive type computation produces a single VARCHAR(N) for both sides.
      int labelVarcharWidth =
          Math.max(label.length(), labelField == null ? 0 : labelField.length());

      SqlNodeList mainProj = new SqlNodeList(POS);
      for (String c : frame.currentFields) {
        mainProj.add(toIdentifier(c));
      }
      if (appendLabelField) {
        org.apache.calcite.sql.SqlDataTypeSpec varcharSpec =
            new org.apache.calcite.sql.SqlDataTypeSpec(
                new org.apache.calcite.sql.SqlBasicTypeNameSpec(
                    org.apache.calcite.sql.type.SqlTypeName.VARCHAR, labelVarcharWidth, POS),
                POS);
        // Emit `CAST(NULL AS VARCHAR(N))` for the main side so UNION coerces to typed null.
        SqlNode pad =
            new SqlBasicCall(
                SqlStdOperatorTable.CAST, List.of(SqlLiteral.createNull(POS), varcharSpec), POS);
        mainProj.add(asAliased(pad, labelField));
      }
      SqlSelect mainProjected =
          new SqlSelect(POS, null, mainProj, from, null, null, null, null, null, null, null, null);

      SqlNodeList summaryProj = new SqlNodeList(POS);
      for (String c : frame.currentFields) {
        boolean shouldAgg;
        if (explicitFields) {
          shouldAgg = aggFieldNames.contains(c);
        } else {
          DataType dt = frame.columnPrimitiveType.get(c);
          if (dt == null && frame.evalAliasTypes.containsKey(c)) {
            dt = frame.evalAliasTypes.get(c);
          }
          shouldAgg = dt == null || isNumericPplType(dt);
        }
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
        // Bare char literal — validator's UNION-ALL leastRestrictive widens to VARCHAR(N)
        // matching the main-side typed-null pad.
        summaryProj.add(asAliased(SqlLiteral.createCharString(label, POS), labelField));
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
      // aligntime=latest|earliest: PPL semantics align bins to the most/least recent data point.
      // v2 emits a decomposed FROM_UNIXTIME shape using PPL's DIVIDE UDF with separate
      // value*unitSeconds factors (not a single intervalSeconds literal).
      String aligntimeStr = aligntimeAsString(aligntimeExpr);
      if ("latest".equals(aligntimeStr) || "earliest".equals(aligntimeStr)) {
        long unitSeconds = unitToSeconds(unit, 1);
        if (unitSeconds > 0) {
          return buildLatestAlignedTimeSpan(fieldRef, value, unitSeconds);
        }
      }
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

  private String aligntimeAsString(UnresolvedExpression aligntimeExpr) {
    if (!(aligntimeExpr instanceof Literal lit) || lit.getValue() == null) return null;
    return lit.getValue()
        .toString()
        .replace("'", "")
        .replace("\"", "")
        .trim()
        .toLowerCase(java.util.Locale.ROOT);
  }

  /**
   * aligntime=latest/earliest emission shape (mirrors v2's TimeSpanHelper.shouldApplyAligntime with
   * decomposed value*unit factors): {@code FROM_UNIXTIME(value * unitSeconds *
   * FLOOR(DIVIDE(DIVIDE(UNIX_TIMESTAMP(field), unitSeconds), value)))}. Uses PPL's DIVIDE UDF (not
   * std SQL /), which is what v2's RexBuilder produces.
   */
  private SqlNode buildLatestAlignedTimeSpan(SqlNode fieldRef, int value, long unitSeconds) {
    SqlNode unixSeconds =
        new SqlBasicCall(
            org.opensearch.sql.expression.function.PPLBuiltinOperators.UNIX_TIMESTAMP,
            List.of(fieldRef),
            POS);
    SqlLiteral unitLit = SqlLiteral.createExactNumeric(Long.toString(unitSeconds), POS);
    SqlLiteral valueLit = SqlLiteral.createExactNumeric(Integer.toString(value), POS);
    SqlNode div1 =
        new SqlBasicCall(
            org.opensearch.sql.expression.function.PPLBuiltinOperators.DIVIDE,
            List.of(unixSeconds, unitLit),
            POS);
    SqlNode div2 =
        new SqlBasicCall(
            org.opensearch.sql.expression.function.PPLBuiltinOperators.DIVIDE,
            List.of(div1, valueLit),
            POS);
    SqlNode floored = new SqlBasicCall(SqlStdOperatorTable.FLOOR, List.of(div2), POS);
    SqlNode mul1 = new SqlBasicCall(SqlStdOperatorTable.MULTIPLY, List.of(floored, valueLit), POS);
    SqlNode mul2 = new SqlBasicCall(SqlStdOperatorTable.MULTIPLY, List.of(mul1, unitLit), POS);
    return new SqlBasicCall(
        org.opensearch.sql.expression.function.PPLBuiltinOperators.FROM_UNIXTIME,
        List.of(mul2),
        POS);
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
    } else if (splitKey instanceof Span sp && sp.getField() instanceof QualifiedName fqn) {
      // `chart agg by <field> span=<N>` — use the underlying field name as the alias. Calcite
      // auto-suffixes with `0` (e.g. `age0`) when the alias collides with an input column,
      // matching v2's RelBuilder emission shape.
      gkAlias = fqn.toString();
    }
    SqlNode keyExpr = expr(gkCore);

    // Build the agg call, capturing alias.
    UnresolvedExpression aggExpr = node.getAggregationFunction();
    String aggAlias = aggLabel(aggExpr);
    UnresolvedExpression aggCore = (aggExpr instanceof Alias a) ? a.getDelegated() : aggExpr;
    SqlNode aggCallNode = aggCall(aggCore);

    // PPL chart drops rows where the split key is NULL (mirroring v2's nonNullGroupMask).
    // For SPAN-typed group keys, IS NOT NULL targets the underlying field (SPAN(NULL)=NULL, so
    // checking the field is equivalent and OpenSearch pushdown-friendly: emits term-not-exists
    // query rather than a wrapped script). Mirrors v2's emission shape.
    SqlNode keyNullCheckTarget = keyExpr;
    if (gkCore instanceof Span sp) {
      keyNullCheckTarget = expr(sp.getField());
    }
    SqlNode nullFilter =
        new SqlBasicCall(SqlStdOperatorTable.IS_NOT_NULL, List.of(keyNullCheckTarget), POS);
    // Also add IS NOT NULL on the agg arg when it's a single QualifiedName field — mirrors v2's
    // aggregateWithTrimming behavior so OpenSearch composite source emits an `exists` filter,
    // unlocking pushdown via doc_count.
    UnresolvedExpression aggArg = aggArgIfSingleField(aggCore);
    if (aggArg instanceof QualifiedName qn) {
      // Only add when the agg arg is a different field from the group key.
      String aggArgName = qn.toString();
      String groupKeyName = (gkCore instanceof QualifiedName g) ? g.toString() : null;
      if (groupKeyName == null || !aggArgName.equals(groupKeyName)) {
        SqlNode aggArgNotNull =
            new SqlBasicCall(SqlStdOperatorTable.IS_NOT_NULL, List.of(expr(aggArg)), POS);
        nullFilter =
            new SqlBasicCall(SqlStdOperatorTable.AND, List.of(nullFilter, aggArgNotNull), POS);
      }
    }
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
    // Wrap the SUM-OVER in COALESCE(SUM-OVER, NULL) to force the column type NULLABLE.
    // Calcite's RelDataTypeFactory marks SUM-OVER NOT NULL when the partition is guaranteed
    // non-empty (PARTITION BY a key that's also a GROUP BY key one level down). Step3's rank
    // OVER references this column in its ORDER BY collation; ProjectToWindowRule.onMatch
    // (RexProgramBuilder.RegisterInputShuttle) asserts the RexInputRef's type matches the
    // input row type — fails with "<type> vs <type> NOT NULL" because the validator-derived
    // RexInputRef captures NULLABLE while the input row says NOT NULL. COALESCE forces the
    // column type to NULLABLE consistently. Behaviorally a no-op: COALESCE(x, NULL) ≡ x.
    SqlNode yTotalNullable =
        new SqlBasicCall(
            SqlStdOperatorTable.COALESCE, List.of(yTotalOver, SqlLiteral.createNull(POS)), POS);
    SqlNodeList step2Items = new SqlNodeList(POS);
    step2Items.add(new SqlIdentifier(overName, POS));
    step2Items.add(new SqlIdentifier(byName, POS));
    step2Items.add(new SqlIdentifier(aggAlias, POS));
    step2Items.add(asAliased(yTotalNullable, "__chart_y_total__"));
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
    SqlNode pattern =
        ExpressionConverter.castStringToVarchar(node.getPattern().getValue().toString());
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
    // Recognise MAP-path targets (e.g. `replace ... IN doc.user.name` over a MAP-typed `doc`):
    // these aren't in visible/currentFields but the prefix is in frame.mapColumns. Handle them as
    // appended `ITEM(<map>, '<path>')` columns. Validate any non-map-path target is in visible.
    java.util.Set<String> mapPathTargets = new java.util.LinkedHashSet<>();
    for (String t : targets) {
      int dot = t.indexOf('.');
      if (dot > 0 && frame.mapColumns.contains(t.substring(0, dot))) {
        mapPathTargets.add(t);
      } else if (!visibleSet.contains(t)) {
        throw new IllegalArgumentException(
            "field [" + t + "] not found; input fields are: " + visible);
      }
    }
    SqlNodeList items = new SqlNodeList(POS);
    List<String> newVisible = new ArrayList<>(visible.size() + mapPathTargets.size());
    for (String c : visible) {
      SqlNode fieldRef = toIdentifier(c);
      if (targets.contains(c)) {
        items.add(asAliased(buildReplaceChain(fieldRef, node), c));
      } else {
        items.add(fieldRef);
      }
      newVisible.add(c);
    }
    for (String t : mapPathTargets) {
      int dot = t.indexOf('.');
      SqlNode fieldRef =
          new SqlBasicCall(
              SqlStdOperatorTable.ITEM,
              List.of(
                  new SqlIdentifier(t.substring(0, dot), POS),
                  SqlLiteral.createCharString(t.substring(dot + 1), POS)),
              POS);
      items.add(asAliased(buildReplaceChain(fieldRef, node), t));
      newVisible.add(t);
      // Register the materialised flat-dotted column as a dotted-eval-alias so downstream
      // visitProject emits a quoted single-part identifier (rather than re-running ITEM
      // dispatch and bypassing our REPLACE substitution).
      frame.dottedEvalAliases.add(t);
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
                    ExpressionConverter.castStringToVarchar(regexPattern),
                    ExpressionConverter.castStringToVarchar(regexReplacement)),
                POS);
      } else {
        value =
            new SqlBasicCall(
                SqlStdOperatorTable.REPLACE,
                List.of(
                    value,
                    ExpressionConverter.castStringToVarchar(patternStr),
                    ExpressionConverter.castStringToVarchar(replacementStr)),
                POS);
      }
    }
    return value;
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
  public SqlNode visitPatterns(Patterns node, Frame frame) {
    SqlNode from = node.getChild().get(0).accept(this, frame);
    org.opensearch.sql.ast.expression.PatternMethod method = node.getPatternMethod();
    org.opensearch.sql.ast.expression.PatternMode mode = node.getPatternMode();
    String aliasField = "patterns_field";
    boolean showNumbered =
        node.getShowNumberedToken() instanceof Literal lit && Boolean.TRUE.equals(lit.getValue());

    if (method != org.opensearch.sql.ast.expression.PatternMethod.SIMPLE_PATTERN
        && method != org.opensearch.sql.ast.expression.PatternMethod.BRAIN) {
      throw new UnsupportedOperationException("Unknown patterns method: " + method);
    }

    if (method == org.opensearch.sql.ast.expression.PatternMethod.SIMPLE_PATTERN) {
      return visitPatternsSimple(node, frame, from, aliasField, mode, showNumbered);
    }
    return visitPatternsBrain(node, frame, from, aliasField, mode, showNumbered);
  }

  /**
   * SIMPLE_PATTERN: REGEXP_REPLACE(source, pattern, '<*>') wrapped in a CASE for empty/NULL input.
   * The {@code show_numbered_token=true} variant additionally calls {@code PATTERN_PARSER} to
   * replace wildcards with {@code <token1>}/{@code <token2>}/... and exposes the captured tokens
   * map.
   */
  private SqlNode visitPatternsSimple(
      Patterns node,
      Frame frame,
      SqlNode from,
      String aliasField,
      org.opensearch.sql.ast.expression.PatternMode mode,
      boolean showNumbered) {
    Literal patternLit =
        node.getArguments() != null
            ? node.getArguments().get(org.opensearch.sql.common.patterns.PatternUtils.PATTERN)
            : null;
    String regex = patternLit != null ? patternLit.getValue().toString() : "";
    if (regex.isEmpty()) {
      regex = "[a-zA-Z0-9]+";
    }
    SqlNode source = expr(node.getSourceField());
    // VARCHAR-cast pattern and replacement literals so emission shows `'[a-zA-Z0-9]+':VARCHAR`
    // matching v2's RexBuilder.makeLiteral default. Bare CHAR literals would render without the
    // type annotation in the explain plan.
    org.apache.calcite.sql.SqlDataTypeSpec varcharSpecForPattern =
        new org.apache.calcite.sql.SqlDataTypeSpec(
            new org.apache.calcite.sql.SqlBasicTypeNameSpec(
                org.apache.calcite.sql.type.SqlTypeName.VARCHAR, POS),
            POS);
    SqlNode regexLit =
        new SqlBasicCall(
            SqlStdOperatorTable.CAST,
            List.of(SqlLiteral.createCharString(regex, POS), varcharSpecForPattern),
            POS);
    SqlNode placeholderLit =
        new SqlBasicCall(
            SqlStdOperatorTable.CAST,
            List.of(SqlLiteral.createCharString("<*>", POS), varcharSpecForPattern),
            POS);
    SqlNode replaced =
        new SqlBasicCall(
            org.apache.calcite.sql.fun.SqlLibraryOperators.REGEXP_REPLACE_3,
            List.of(source, regexLit, placeholderLit),
            POS);
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
    SqlNode patternsExpr =
        new org.apache.calcite.sql.fun.SqlCase(POS, null, whens, thens, replaced);

    if (mode == org.opensearch.sql.ast.expression.PatternMode.AGGREGATION) {
      return emitPatternsAggregation(
          node,
          frame,
          from,
          aliasField,
          patternsExpr, /* method */
          org.opensearch.sql.ast.expression.PatternMethod.SIMPLE_PATTERN,
          showNumbered);
    }

    // LABEL mode: extend the row with patterns_field; if show_numbered_token=true, also expose
    // the token-numbered pattern (overriding patterns_field) and the tokens map. The latter
    // requires patterns_field to be a real column, so it lives in a wrapped subquery.
    // Filter metadata fields out — including them produces an extra LogicalProject layer when
    // the planner-level stripMetadataFields shuttle later trims the row type. Mirrors
    // visitDedupe / visitWindow / visitStreamWindow.
    SqlNodeList items = new SqlNodeList(POS);
    List<String> visible =
        frame.currentFields == null ? new ArrayList<>() : new ArrayList<>(frame.currentFields);
    if (frame.currentFields == null) {
      items.add(SqlIdentifier.star(POS));
    } else {
      List<String> retained = new ArrayList<>();
      for (String name : visible) {
        if (name.equals(aliasField)) continue;
        if (OpenSearchConstants.METADATAFIELD_TYPE_MAP.containsKey(name)) continue;
        items.add(toIdentifier(name));
        retained.add(name);
      }
      visible = retained;
    }
    items.add(asAliased(patternsExpr, aliasField));
    visible.add(aliasField);
    SqlNode wrapped = SqlBuilder.select(items).from(from).withFields(visible).wrap(frame);
    if (!showNumbered) {
      return wrapped;
    }
    return wrapWithPatternParser(wrapped, frame, node, aliasField);
  }

  /**
   * BRAIN: aggregate via INTERNAL_PATTERN window function, then SAFE_CAST(ITEM(PATTERN_PARSER(...),
   * 'pattern') AS VARCHAR). For LABEL mode, the window aggregate runs over PARTITION BY (no ORDER
   * BY) and the call inlines into PATTERN_PARSER. AGGREGATION mode expands the array-of-MAP via
   * UNNEST and is deferred (legacy path uses {@code Uncollect}, requires CROSS JOIN UNNEST that
   * needs a row-type oracle to spell the inner MAP keys).
   */
  private SqlNode visitPatternsBrain(
      Patterns node,
      Frame frame,
      SqlNode from,
      String aliasField,
      org.opensearch.sql.ast.expression.PatternMode mode,
      boolean showNumbered) {
    // For AGGREGATION mode: emit the BRAIN-LABEL projection first (PATTERN_PARSER OVER ...),
    // then wrap with GROUP BY (partitionBy + patterns_field). Mirrors v2's
    // CalciteRelNodeVisitor flow for BRAIN AGG. show_numbered_token is propagated through to
    // the LABEL pass; the post-AGG re-run via PATTERN_PARSER is wired in
    // emitPatternsAggregation when needed.
    boolean aggMode = mode == org.opensearch.sql.ast.expression.PatternMode.AGGREGATION;
    // BRAIN's PATTERN_PARSER produces the `<tokenN>`-numbered placeholders directly in the
    // LABEL phase, so propagate showNumbered through. The post-AGG re-run (used by
    // SIMPLE_PATTERN AGG show_numbered=true) is unnecessary for BRAIN.
    boolean labelShowNumbered = showNumbered;
    SqlNode source = expr(node.getSourceField());
    SqlNode maxSampleCount =
        node.getPatternMaxSampleCount() != null
            ? expr(node.getPatternMaxSampleCount())
            : SqlLiteral.createExactNumeric("10", POS);
    SqlNode bufferLimit =
        node.getPatternBufferLimit() != null
            ? expr(node.getPatternBufferLimit())
            : SqlLiteral.createExactNumeric("100000", POS);
    SqlNode showNumberedNode =
        node.getShowNumberedToken() != null
            ? expr(node.getShowNumberedToken())
            : SqlLiteral.createBoolean(false, POS);
    SqlNodeList partitionBy = new SqlNodeList(POS);
    if (node.getPartitionByList() != null) {
      for (UnresolvedExpression p : node.getPartitionByList()) {
        UnresolvedExpression core = (p instanceof Alias a) ? a.getDelegated() : p;
        partitionBy.add(expr(core));
      }
    }
    SqlNode patternWindow =
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
    List<SqlNode> patternArgs = new ArrayList<>();
    patternArgs.add(source);
    patternArgs.add(maxSampleCount);
    patternArgs.add(bufferLimit);
    patternArgs.add(showNumberedNode);
    if (node.getArguments() != null) {
      List<String> brainKeys =
          node.getArguments().keySet().stream()
              .filter(
                  org.opensearch.sql.common.patterns.PatternUtils.VALID_BRAIN_PARAMETERS::contains)
              .sorted()
              .toList();
      for (String k : brainKeys) {
        patternArgs.add(ExpressionConverter.literalToSqlNode(node.getArguments().get(k)));
      }
    }
    SqlNode patternAgg =
        new SqlBasicCall(
            org.opensearch.sql.expression.function.PPLBuiltinOperators.INTERNAL_PATTERN,
            patternArgs,
            POS);
    SqlNode patternOver =
        new SqlBasicCall(SqlStdOperatorTable.OVER, List.of(patternAgg, patternWindow), POS);
    SqlNode parserCall =
        new SqlBasicCall(
            new org.apache.calcite.sql.SqlUnresolvedFunction(
                new SqlIdentifier("PATTERN_PARSER", POS),
                null,
                null,
                null,
                null,
                org.apache.calcite.sql.SqlFunctionCategory.USER_DEFINED_FUNCTION),
            List.of(expr(node.getSourceField()), patternOver, showNumberedNode),
            POS);
    SqlNode itemPattern =
        new SqlBasicCall(
            SqlStdOperatorTable.ITEM,
            List.of(parserCall, SqlLiteral.createCharString("pattern", POS)),
            POS);
    SqlNode safeCastPattern =
        new SqlBasicCall(
            org.apache.calcite.sql.fun.SqlLibraryOperators.SAFE_CAST,
            List.of(
                itemPattern,
                new org.apache.calcite.sql.SqlDataTypeSpec(
                    new org.apache.calcite.sql.SqlBasicTypeNameSpec(
                        org.apache.calcite.sql.type.SqlTypeName.VARCHAR, POS),
                    POS)),
            POS);

    SqlNodeList items = new SqlNodeList(POS);
    List<String> visible =
        frame.currentFields == null ? new ArrayList<>() : new ArrayList<>(frame.currentFields);
    if (frame.currentFields == null) {
      items.add(SqlIdentifier.star(POS));
    } else {
      List<String> retained = new ArrayList<>();
      for (String name : visible) {
        if (name.equals(aliasField) || name.equals("tokens")) continue;
        items.add(toIdentifier(name));
        retained.add(name);
      }
      visible = retained;
    }
    items.add(asAliased(safeCastPattern, aliasField));
    visible.add(aliasField);
    if (labelShowNumbered) {
      SqlNode itemTokens =
          new SqlBasicCall(
              SqlStdOperatorTable.ITEM,
              List.of(parserCall, SqlLiteral.createCharString("tokens", POS)),
              POS);
      items.add(asAliased(itemTokens, "tokens"));
      visible.add("tokens");
    }
    SqlNode labelSelect = SqlBuilder.select(items).from(from).withFields(visible).wrap(frame);
    if (!aggMode) {
      return labelSelect;
    }
    if (showNumbered) {
      // BRAIN AGG show_numbered=true: v2 emits INTERNAL_PATTERN as an AggCall (not OVER) with
      // GROUP BY partitionBy, then UNNEST the ARRAY<MAP<VARCHAR,ANY>> result and project pattern,
      // pattern_count, tokens, sample_logs from each MAP element. The LABEL-OVER path doesn't
      // model the per-group token aggregation (it produces per-row tokens, then GROUP BY drops
      // them); only the AggCall path returns the merged-per-group tokens map directly.
      return emitBrainAggShowNumbered(node, frame, from, aliasField, patternArgs);
    }
    // AGGREGATION mode: GROUP BY (partitionBy ..., patterns_field) post-LABEL. Reuse the
    // SIMPLE_PATTERN aggregator with a passthrough patternsExpr (the LABEL-emitted aliasField
    // column is already the patterns_field). Pass the actual user method (BRAIN) so the
    // showNumbered post-AGG re-run uses BRAIN's tokens format.
    // For BRAIN AGG, the LABEL phase already produced numbered tokens — skip post-AGG re-run
    // by passing showNumbered=false. For SIMPLE_PATTERN AGG, the post-AGG re-run is what
    // produces numbered tokens; that path stays in visitPatternsSimple.
    return emitPatternsAggregation(
        node,
        frame,
        labelSelect,
        aliasField,
        new SqlIdentifier(aliasField, POS),
        org.opensearch.sql.ast.expression.PatternMethod.BRAIN,
        /* showNumbered */ false);
  }

  /**
   * Emit the BRAIN AGGREGATION-mode show_numbered=true path using INTERNAL_PATTERN as an AggCall
   * (not OVER), then UNNEST the ARRAY&lt;MAP&gt; result. Mirrors v2's CalciteRelNodeVisitor BRAIN
   * AGG flow.
   *
   * <p>Shape:
   *
   * <pre>
   * SELECT s.&lt;partitionBy&gt;..., CAST(ITEM(t.elem, 'pattern') AS VARCHAR) AS &lt;aliasField&gt;,
   *        ITEM(t.elem, 'pattern_count') AS pattern_count,
   *        ITEM(t.elem, 'tokens') AS tokens,
   *        ITEM(t.elem, 'sample_logs') AS sample_logs
   * FROM   (SELECT &lt;partitionBy&gt;..., INTERNAL_PATTERN(source, max, buf, true, args)
   *         FROM   &lt;input&gt;
   *         GROUP BY &lt;partitionBy&gt;...) AS s
   *      , UNNEST(s.agg_result) AS t(elem)
   * </pre>
   *
   * @param patternArgs the validated argument list for INTERNAL_PATTERN (source, max, buf, true,
   *     [variable_count, frequency_threshold ...])
   */
  private SqlNode emitBrainAggShowNumbered(
      Patterns node, Frame frame, SqlNode from, String aliasField, List<SqlNode> patternArgs) {
    // Step 1: aggregate. SELECT partitionBy..., INTERNAL_PATTERN(...) AS agg_result GROUP BY ...
    SqlNodeList aggItems = new SqlNodeList(POS);
    List<SqlNode> groupKeys = new ArrayList<>();
    List<String> partitionByNames = new ArrayList<>();
    if (node.getPartitionByList() != null) {
      for (UnresolvedExpression p : node.getPartitionByList()) {
        UnresolvedExpression core = (p instanceof Alias a) ? a.getDelegated() : p;
        SqlNode key = expr(core);
        groupKeys.add(key);
        String name =
            (p instanceof Alias a)
                ? a.getName()
                : (core instanceof QualifiedName qn ? qn.toString() : null);
        if (name != null) {
          aggItems.add(asAliased(key, name));
          partitionByNames.add(name);
        } else {
          aggItems.add(key);
        }
      }
    }
    // Use the user-facing alias as the agg-result name to match v2's emission shape, where the
    // RelBuilder calls aggregate(agg.named(aliasField, INTERNAL_PATTERN(...))) so the Aggregate
    // rel's row type field is `aliasField` (e.g. `patterns_field`). Hardcoded internal names
    // (`__pattern_agg__`) showed up in explain output, breaking plan-shape comparisons.
    String aggResultName = aliasField;
    SqlNode patternAgg =
        new SqlBasicCall(
            org.opensearch.sql.expression.function.PPLBuiltinOperators.INTERNAL_PATTERN,
            patternArgs,
            POS);
    aggItems.add(asAliased(patternAgg, aggResultName));
    Frame aggFrame = new Frame();
    List<String> aggVisible = new ArrayList<>(partitionByNames);
    aggVisible.add(aggResultName);
    SqlNode aggSelect =
        SqlBuilder.select(aggItems)
            .from(from)
            .groupBy(groupKeys)
            .withFields(aggVisible)
            .wrap(aggFrame);
    // Step 2: UNNEST. SELECT s.<partition...>, t.elem FROM (aggSelect) AS s, UNNEST(s.<agg>) AS t
    String inputAlias = "__brain_s__";
    String unnestAlias = "__brain_t__";
    String elemName = "__brain_elem__";
    SqlNode aliasedInput = SqlBuilder.aliasAs(aggSelect, inputAlias);
    SqlNode unnestArg = new SqlIdentifier(java.util.Arrays.asList(inputAlias, aggResultName), POS);
    SqlNode unnest = new SqlBasicCall(SqlStdOperatorTable.UNNEST, List.of(unnestArg), POS);
    SqlNode aliasedUnnest =
        new SqlBasicCall(
            SqlStdOperatorTable.AS,
            List.of(unnest, new SqlIdentifier(unnestAlias, POS), new SqlIdentifier(elemName, POS)),
            POS);
    SqlNode join =
        new org.apache.calcite.sql.SqlJoin(
            POS,
            aliasedInput,
            SqlLiteral.createBoolean(false, POS),
            JoinType.COMMA.symbol(POS),
            aliasedUnnest,
            org.apache.calcite.sql.JoinConditionType.NONE.symbol(POS),
            null);
    // Step 3: outer Project — ITEM(elem, 'pattern'), 'pattern_count', 'tokens', 'sample_logs'.
    SqlNode elemRef = new SqlIdentifier(java.util.Arrays.asList(unnestAlias, elemName), POS);
    // v2's flattenParsedPattern uses RexBuilder.makeCast(... safe=true) which surfaces in
    // explain as `SAFE_CAST(arg)` (no `:TYPE` annotation — type comes from result inference).
    // Use SqlLibraryOperators.SAFE_CAST so our emission matches the same shape.
    org.apache.calcite.sql.SqlDataTypeSpec varcharSpec =
        new org.apache.calcite.sql.SqlDataTypeSpec(
            new org.apache.calcite.sql.SqlBasicTypeNameSpec(
                org.apache.calcite.sql.type.SqlTypeName.VARCHAR, POS),
            POS);
    org.apache.calcite.sql.SqlDataTypeSpec bigintSpec =
        new org.apache.calcite.sql.SqlDataTypeSpec(
            new org.apache.calcite.sql.SqlBasicTypeNameSpec(
                org.apache.calcite.sql.type.SqlTypeName.BIGINT, POS),
            POS);
    SqlNode itemPattern =
        new SqlBasicCall(
            SqlStdOperatorTable.ITEM,
            List.of(elemRef, SqlLiteral.createCharString("pattern", POS)),
            POS);
    SqlNode castPattern =
        new SqlBasicCall(
            org.apache.calcite.sql.fun.SqlLibraryOperators.SAFE_CAST,
            List.of(itemPattern, varcharSpec),
            POS);
    SqlNode itemCount =
        new SqlBasicCall(
            SqlStdOperatorTable.ITEM,
            List.of(elemRef, SqlLiteral.createCharString("pattern_count", POS)),
            POS);
    SqlNode castCount =
        new SqlBasicCall(
            org.apache.calcite.sql.fun.SqlLibraryOperators.SAFE_CAST,
            List.of(itemCount, bigintSpec),
            POS);
    // tokens / sample_logs: leave un-cast. v2's SAFE_CAST to MAP<VARCHAR, ARRAY> /
    // ARRAY<VARCHAR> would match exactly, but emitting compound SqlDataTypeSpec for these
    // tripped a validator AssertionError. Plain ITEM($elem, 'k') returns the right runtime
    // shape — only the explain output differs cosmetically (`ITEM(...)` vs `SAFE_CAST(ITEM(...))`).
    SqlNode itemTokens =
        new SqlBasicCall(
            SqlStdOperatorTable.ITEM,
            List.of(elemRef, SqlLiteral.createCharString("tokens", POS)),
            POS);
    SqlNode itemSamples =
        new SqlBasicCall(
            SqlStdOperatorTable.ITEM,
            List.of(elemRef, SqlLiteral.createCharString("sample_logs", POS)),
            POS);
    SqlNodeList outerItems = new SqlNodeList(POS);
    List<String> outerVisible = new ArrayList<>();
    for (String pn : partitionByNames) {
      outerItems.add(new SqlIdentifier(java.util.Arrays.asList(inputAlias, pn), POS));
      outerVisible.add(pn);
    }
    outerItems.add(asAliased(castPattern, aliasField));
    outerVisible.add(aliasField);
    outerItems.add(asAliased(castCount, "pattern_count"));
    outerVisible.add("pattern_count");
    outerItems.add(asAliased(itemTokens, "tokens"));
    outerVisible.add("tokens");
    outerItems.add(asAliased(itemSamples, "sample_logs"));
    outerVisible.add("sample_logs");
    return SqlBuilder.select(outerItems).from(join).withFields(outerVisible).wrap(frame);
  }

  /**
   * Wrap the given SqlNode (which exposes {@code patterns_field}) in a SELECT that overrides {@code
   * patterns_field} with the {@code <tokenN>}-numbered pattern and adds {@code tokens}. Mirrors
   * v2's flattenParsedPattern. The wrap turns the prior eval-alias into a real column referenceable
   * by PATTERN_PARSER.
   */
  private SqlNode wrapWithPatternParser(
      SqlNode inner, Frame frame, Patterns node, String aliasField) {
    SqlNode patternsFieldRef = new SqlIdentifier(aliasField, POS);
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
    SqlNode itemPattern =
        new SqlBasicCall(
            SqlStdOperatorTable.ITEM,
            List.of(parserCall, SqlLiteral.createCharString("pattern", POS)),
            POS);
    SqlNode safeCastPattern =
        new SqlBasicCall(
            org.apache.calcite.sql.fun.SqlLibraryOperators.SAFE_CAST,
            List.of(
                itemPattern,
                new org.apache.calcite.sql.SqlDataTypeSpec(
                    new org.apache.calcite.sql.SqlBasicTypeNameSpec(
                        org.apache.calcite.sql.type.SqlTypeName.VARCHAR, POS),
                    POS)),
            POS);
    SqlNode itemTokens =
        new SqlBasicCall(
            SqlStdOperatorTable.ITEM,
            List.of(parserCall, SqlLiteral.createCharString("tokens", POS)),
            POS);

    List<String> innerFields = frame.currentFields == null ? List.of() : frame.currentFields;
    SqlNodeList items = new SqlNodeList(POS);
    List<String> visible = new ArrayList<>();
    for (String c : innerFields) {
      if (c.equals(aliasField) || c.equals("tokens")) continue;
      items.add(toIdentifier(c));
      visible.add(c);
    }
    items.add(asAliased(safeCastPattern, aliasField));
    visible.add(aliasField);
    items.add(asAliased(itemTokens, "tokens"));
    visible.add("tokens");
    return SqlBuilder.select(items).from(inner).withFields(visible).wrap(frame);
  }

  /**
   * AGGREGATION mode for SIMPLE_PATTERN: GROUP BY (partitionBy ..., patterns_field) with
   * pattern_count = COUNT(*), sample_logs = TAKE(source, max_sample_count). show_numbered_token
   * additionally re-runs PATTERN_PARSER post-grouping to expose the numbered pattern + tokens map.
   */
  private SqlNode emitPatternsAggregation(
      Patterns node,
      Frame frame,
      SqlNode from,
      String aliasField,
      SqlNode patternsExpr,
      org.opensearch.sql.ast.expression.PatternMethod method,
      boolean showNumbered) {
    // Both SIMPLE_PATTERN and BRAIN flow through this aggregator. The patternsExpr passed in is
    // the patterns_field source — for SIMPLE_PATTERN it's the CASE/REGEXP_REPLACE expression
    // built upstream; for BRAIN it's a column reference to the LABEL-emitted aliasField.
    List<SqlNode> selects = new ArrayList<>();
    List<SqlNode> groupKeys = new ArrayList<>();
    List<String> visible = new ArrayList<>();
    if (node.getPartitionByList() != null) {
      for (UnresolvedExpression p : node.getPartitionByList()) {
        UnresolvedExpression core = (p instanceof Alias a) ? a.getDelegated() : p;
        SqlNode key = expr(core);
        groupKeys.add(key);
        String name =
            (p instanceof Alias a)
                ? a.getName()
                : (core instanceof QualifiedName qn ? qn.toString() : null);
        if (name != null) {
          selects.add(asAliased(key, name));
          visible.add(name);
        } else {
          selects.add(key);
        }
      }
    }
    groupKeys.add(patternsExpr);
    selects.add(asAliased(patternsExpr, aliasField));
    visible.add(aliasField);
    SqlNode countCall =
        new SqlBasicCall(SqlStdOperatorTable.COUNT, List.of(SqlIdentifier.star(POS)), POS);
    selects.add(asAliased(countCall, "pattern_count"));
    visible.add("pattern_count");
    SqlNode maxSampleCount =
        node.getPatternMaxSampleCount() != null
            ? expr(node.getPatternMaxSampleCount())
            : SqlLiteral.createExactNumeric("10", POS);
    SqlNode takeCall =
        new SqlBasicCall(
            org.opensearch.sql.expression.function.PPLBuiltinOperators.TAKE,
            List.of(expr(node.getSourceField()), maxSampleCount),
            POS);
    selects.add(asAliased(takeCall, "sample_logs"));
    visible.add("sample_logs");

    SqlNodeList items = new SqlNodeList(POS);
    for (SqlNode n : selects) {
      items.add(n);
    }
    SqlNode aggSelect =
        SqlBuilder.select(items).from(from).groupBy(groupKeys).withFields(visible).wrap(frame);
    if (!showNumbered) {
      return aggSelect;
    }
    // Re-run PATTERN_PARSER post-grouping. v2 emits the projection in order:
    // [partitionBy ..., patterns_field, pattern_count, tokens, sample_logs].
    SqlNode patternsFieldRef = new SqlIdentifier(aliasField, POS);
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
    SqlNode itemPattern =
        new SqlBasicCall(
            SqlStdOperatorTable.ITEM,
            List.of(parserCall, SqlLiteral.createCharString("pattern", POS)),
            POS);
    // SAFE_CAST mirrors v2's flattenParsedPattern emission shape (RexBuilder.makeCast safe=true).
    SqlNode castPattern =
        new SqlBasicCall(
            org.apache.calcite.sql.fun.SqlLibraryOperators.SAFE_CAST,
            List.of(
                itemPattern,
                new org.apache.calcite.sql.SqlDataTypeSpec(
                    new org.apache.calcite.sql.SqlBasicTypeNameSpec(
                        org.apache.calcite.sql.type.SqlTypeName.VARCHAR, POS),
                    POS)),
            POS);
    SqlNode itemTokens =
        new SqlBasicCall(
            SqlStdOperatorTable.ITEM,
            List.of(parserCall, SqlLiteral.createCharString("tokens", POS)),
            POS);
    SqlNodeList outerItems = new SqlNodeList(POS);
    List<String> outerVisible = new ArrayList<>();
    if (node.getPartitionByList() != null) {
      for (UnresolvedExpression p : node.getPartitionByList()) {
        UnresolvedExpression core = (p instanceof Alias a) ? a.getDelegated() : p;
        String name =
            (p instanceof Alias a)
                ? a.getName()
                : (core instanceof QualifiedName qn ? qn.toString() : null);
        if (name != null) {
          outerItems.add(toIdentifier(name));
          outerVisible.add(name);
        }
      }
    }
    outerItems.add(asAliased(castPattern, aliasField));
    outerVisible.add(aliasField);
    outerItems.add(toIdentifier("pattern_count"));
    outerVisible.add("pattern_count");
    outerItems.add(asAliased(itemTokens, "tokens"));
    outerVisible.add("tokens");
    outerItems.add(toIdentifier("sample_logs"));
    outerVisible.add("sample_logs");
    return SqlBuilder.select(outerItems).from(aggSelect).withFields(outerVisible).wrap(frame);
  }

  @Override
  public SqlNode visitNoMv(NoMv node, Frame frame) {
    // PPL `nomv <field>` rewrites to `eval <field> = coalesce(mvjoin(array_compact(<field>), '\n'),
    // '')`. Pre-walk the child so frame.currentFields reflects the upstream column list, then
    // distinguish three cases:
    //   - Field missing from currentFields: emit `SELECT *, '' AS <field>` so the column shows
    //     as empty string rather than failing validation. Mirrors PPL's runtime behaviour where
    //     nomv on a missing field returns "" instead of throwing.
    //   - Field present: build the standard rewriteAsEval emission directly using the
    //     already-walked child (avoids double-walking) so frame mutations happen exactly once.
    String fieldName = node.getField().getField().toString();
    SqlNode child = node.getChild().get(0).accept(this, frame);
    // PPL `nomv <scalar_field>` is a type error — the field is statically not an array. Throw
    // IllegalArgumentException at translation time so the user sees a 400 response with the
    // documented type-mismatch message instead of a 500 from a runtime cast failure. Detection:
    // field is in scope AND has a known non-array primitive type via columnPrimitiveType (or
    // evalAliasTypes). UDT fields (TIMESTAMP/DATE/TIME/IP) are also non-array → reject.
    if (frame.currentFields != null
        && frame.currentFields.contains(fieldName)
        && (frame.columnPrimitiveType.containsKey(fieldName)
            || frame.evalAliasTypes.containsKey(fieldName)
            || frame.columnUdt.containsKey(fieldName))) {
      throw new IllegalArgumentException(
          "nomv requires an ARRAY-typed field; got scalar field '"
              + fieldName
              + "' (cannot apply MVJOIN to non-array type)");
    }
    if (frame.currentFields == null || !frame.currentFields.contains(fieldName)) {
      SqlNodeList items = new SqlNodeList(POS);
      if (frame.currentFields != null) {
        for (String c : frame.currentFields) {
          items.add(toIdentifier(c));
        }
      } else {
        items.add(SqlIdentifier.star(POS));
      }
      items.add(asAliased(SqlLiteral.createCharString("", POS), fieldName));
      List<String> newVisible =
          new ArrayList<>(frame.currentFields == null ? List.of() : frame.currentFields);
      if (!newVisible.contains(fieldName)) newVisible.add(fieldName);
      return SqlBuilder.select(items).from(child).withFields(newVisible).wrap(frame);
    }
    // Field is in scope — build the rewrite-as-eval emission directly to avoid re-walking the
    // already-visited child. Mirrors `node.rewriteAsEval().accept(this, frame)`.
    SqlNodeList items = new SqlNodeList(POS);
    for (String c : frame.currentFields) {
      if (c.equals(fieldName)) continue;
      items.add(toIdentifier(c));
    }
    SqlNode arrayCompact =
        new SqlBasicCall(
            org.apache.calcite.sql.fun.SqlLibraryOperators.ARRAY_COMPACT,
            List.of(toIdentifier(fieldName)),
            POS);
    SqlNode mvjoin =
        new SqlBasicCall(
            org.apache.calcite.sql.fun.SqlLibraryOperators.ARRAY_JOIN,
            List.of(arrayCompact, SqlLiteral.createCharString("\n", POS)),
            POS);
    SqlNode coalesce =
        new SqlBasicCall(
            SqlStdOperatorTable.COALESCE,
            List.of(mvjoin, SqlLiteral.createCharString("", POS)),
            POS);
    items.add(asAliased(coalesce, fieldName));
    return SqlBuilder.select(items)
        .from(child)
        .withFields(new ArrayList<>(frame.currentFields))
        .wrap(frame);
  }

  @Override
  public SqlNode visitMvCombine(MvCombine node, Frame frame) {
    // PPL `mvcombine <field>` collapses rows with identical other-column values into a single
    // row whose <field> becomes an array of the combined values. Emit:
    //   SELECT <other_cols>, ARRAY_AGG(<field>) AS <field>
    //   FROM <child>
    //   GROUP BY <other_cols>
    String fieldName = node.getField().getField().toString();
    SqlNode child = node.getChild().get(0).accept(this, frame);
    if (frame.currentFields == null) {
      throw new UnsupportedOperationException(
          "mvcombine requires a known column list — call after a `| fields ...` pipe");
    }
    // PPL allows mvcombine on a MAP-leaf path (e.g. `mvcombine doc.user.name` after spath). The
    // dotted leaf is not a flat column in currentFields. Lower the leaf through ITEM(map, 'key')
    // and emit a per-row 1-element array (or null when the source is null) aliased back to the
    // dotted name. Matches v2 semantics: each scalar leaf becomes a 1-element array column.
    int firstDot = fieldName.indexOf('.');
    if (!frame.currentFields.contains(fieldName)
        && firstDot > 0
        && frame.mapColumns.contains(fieldName.substring(0, firstDot))) {
      String prefix = fieldName.substring(0, firstDot);
      String subkey = fieldName.substring(firstDot + 1);
      SqlNode itemAccess =
          new SqlBasicCall(
              SqlStdOperatorTable.ITEM,
              List.of(new SqlIdentifier(prefix, POS), SqlLiteral.createCharString(subkey, POS)),
              POS);
      SqlNodeList selectItems = new SqlNodeList(POS);
      List<String> newVisibleMap = new ArrayList<>(frame.currentFields.size());
      for (String c : frame.currentFields) {
        selectItems.add(toIdentifier(c));
        newVisibleMap.add(c);
      }
      // CASE WHEN item IS NULL THEN NULL ELSE ARRAY[item] END AS "<dotted>". Plain
      // ARRAY[item] would be a 1-element array containing NULL when the source is NULL —
      // PPL semantics surface NULL as a NULL array, not [NULL].
      SqlNodeList whens = new SqlNodeList(POS);
      whens.add(new SqlBasicCall(SqlStdOperatorTable.IS_NULL, List.of(itemAccess), POS));
      SqlNodeList thens = new SqlNodeList(POS);
      thens.add(SqlLiteral.createNull(POS));
      SqlNode arrayCtor =
          new SqlBasicCall(SqlStdOperatorTable.ARRAY_VALUE_CONSTRUCTOR, List.of(itemAccess), POS);
      SqlNode caseExpr = new org.apache.calcite.sql.fun.SqlCase(POS, null, whens, thens, arrayCtor);
      selectItems.add(asAliased(caseExpr, fieldName));
      newVisibleMap.add(fieldName);
      SqlNode mapLeafSelect =
          SqlBuilder.select(selectItems).from(child).withFields(newVisibleMap).wrap(frame);
      frame.currentFields = newVisibleMap;
      return mapLeafSelect;
    }
    if (!frame.currentFields.contains(fieldName)) {
      throw new IllegalArgumentException("Field [" + fieldName + "] not found.");
    }
    SqlNodeList items = new SqlNodeList(POS);
    List<SqlNode> groupKeys = new ArrayList<>();
    List<String> newVisible = new ArrayList<>(frame.currentFields.size());
    for (String c : frame.currentFields) {
      if (c.equals(fieldName)) continue;
      SqlIdentifier ref = toIdentifier(c);
      items.add(ref);
      groupKeys.add(ref);
      newVisible.add(c);
    }
    // ARRAY_AGG(field) FILTER (WHERE field IS NOT NULL) — drop NULLs so collected array doesn't
    // contain null entries. Mirrors v2's emission shape (legacy commit aa7a3f54ea).
    SqlNode arrayAgg =
        new SqlBasicCall(
            org.apache.calcite.sql.fun.SqlLibraryOperators.ARRAY_AGG,
            List.of(toIdentifier(fieldName)),
            POS);
    SqlNode notNullFilter =
        new SqlBasicCall(SqlStdOperatorTable.IS_NOT_NULL, List.of(toIdentifier(fieldName)), POS);
    SqlNode filteredArrayAgg =
        new SqlBasicCall(SqlStdOperatorTable.FILTER, List.of(arrayAgg, notNullFilter), POS);
    items.add(asAliased(filteredArrayAgg, fieldName));
    newVisible.add(fieldName);
    SqlNode aggSelect =
        SqlBuilder.select(items).from(child).groupBy(groupKeys).withFields(newVisible).wrap(frame);
    // Wrap with an outer `SELECT * FROM (<aggregate>)` so the planner emits the
    // top-level identity Project that v2's RelBuilder produces. Otherwise SqlToRelConverter
    // collapses the aggregate to a single LogicalAggregate without a passthrough Project.
    SqlNode outer =
        new SqlSelect(
            POS,
            null,
            new SqlNodeList(List.of(SqlIdentifier.star(POS)), POS),
            aggSelect,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null);
    frame.currentFields = newVisible;
    return outer;
  }

  @Override
  public SqlNode visitFlatten(Flatten node, Frame frame) {
    // PPL `flatten <field>` lifts struct sub-fields into top-level columns. OpenSearch flattens
    // nested fields at scan time as `<field>.<sub>` columns, so the sub-fields are already
    // present in the scan schema — we re-project them under their leaf names (or user-supplied
    // aliases). Build an explicit projection so the leaf names are exposed as flat columns and
    // the dotted-name flattened-leaf duplicates are dropped (mirrors v2's tryToRemoveNestedFields).
    SqlNode from = node.getChild().get(0).accept(this, frame);
    if (frame.currentFields == null) {
      throw new UnsupportedOperationException(
          "flatten requires a known column list — call after a `| fields ...` pipe");
    }
    String fieldName = node.getField().getField().toString();
    List<String> allCols =
        frame.currentFields.stream()
            .filter(c -> !OpenSearchConstants.METADATAFIELD_TYPE_MAP.containsKey(c))
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
    // Project: input cols (drop dotted-name children of struct parents in scope) + aliased subs.
    java.util.Set<String> all = new java.util.LinkedHashSet<>(allCols);
    SqlNodeList items = new SqlNodeList(POS);
    List<String> visible = new ArrayList<>();
    for (String c : allCols) {
      int lastDot = c.lastIndexOf('.');
      if (lastDot >= 0 && all.contains(c.substring(0, lastDot))) {
        continue;
      }
      // OpenSearch flattens nested fields under their dotted name (`message.author` is a single
      // column, not a struct path). Emit as a quoted single-part identifier so the validator
      // looks it up literally in the catalog row type instead of treating `message` as a table.
      items.add(
          c.indexOf('.') < 0 ? toIdentifier(c) : ExpressionConverter.quotedIdentifier(List.of(c)));
      visible.add(c);
    }
    for (int i = 0; i < subCols.size(); i++) {
      items.add(
          asAliased(ExpressionConverter.quotedIdentifier(List.of(subCols.get(i))), aliases.get(i)));
      visible.add(aliases.get(i));
    }
    return SqlBuilder.select(items).from(from).withFields(visible).wrap(frame);
  }

  @Override
  public SqlNode visitMvExpand(org.opensearch.sql.ast.tree.MvExpand node, Frame frame) {
    // PPL `mvexpand <field>` unnests an array column in place: the same column name continues
    // to exist post-mvexpand but holds an element rather than the array. v2's RelBuilder emits:
    //   LogicalProject(<other_user_cols>, <field>=$<unnested_pos>)
    //     LogicalCorrelate($cor0, INNER)
    //       LogicalProject(<all_input_cols>)
    //       Uncollect
    //         LogicalProject(<field>=$cor0.<field>)
    //           LogicalValues(tuples=[[{ 0 }]])
    //
    // We mirror this with `SELECT <pruned_user_cols>, t.<field> AS <field>
    // FROM <input> AS s, UNNEST(s.<field>) AS t(<field>)`. Calcite's SqlToRelConverter turns
    // implicit-LATERAL CROSS JOIN with UNNEST referencing the left side into LogicalCorrelate
    // + Uncollect.
    SqlNode input = node.getChild().get(0).accept(this, frame);
    UnresolvedExpression fieldExpr = node.getField().getField();
    if (!(fieldExpr instanceof QualifiedName fqn)) {
      throw new UnsupportedOperationException(
          "mvexpand requires a simple column reference, got: " + fieldExpr.getClass());
    }
    String fieldName = fqn.toString();
    // PPL mvexpand on a scalar (non-array) field is a no-op (passes input through unchanged).
    // Without a row-type oracle we can't introspect the field type at SqlNode construction
    // time, so use the Frame's eval-alias / column-primitive type maps as a best-effort check.
    // If the field is recorded as a non-array scalar (INTEGER, STRING, etc.), skip the
    // UNNEST emission entirely. Mirrors v2's CalciteRelNodeVisitor.visitMvExpand semantics.
    // The DataType enum tracks only scalar types (INTEGER/STRING/BOOLEAN/...). If `fieldName`
    // appears in the Frame's primitive/eval-alias type maps, it's a known scalar — mvexpand is
    // a no-op. The OpenSearch flat-dotted scan columns (skills_int, skills_not_array) populate
    // columnPrimitiveType during visitRelation; eval-emitted scalars populate evalAliasTypes.
    if (frame.columnPrimitiveType.containsKey(fieldName)
        || frame.evalAliasTypes.containsKey(fieldName)) {
      return input;
    }
    String inputAlias = "mvexpand_input";
    String unnestAlias = "mvexpand_t";
    SqlNode aliasedInput = SqlBuilder.aliasAs(input, inputAlias);
    SqlNode unnestArg = new SqlIdentifier(java.util.Arrays.asList(inputAlias, fieldName), POS);
    SqlNode unnest = new SqlBasicCall(SqlStdOperatorTable.UNNEST, List.of(unnestArg), POS);
    SqlNode aliasedUnnest =
        new SqlBasicCall(
            SqlStdOperatorTable.AS,
            List.of(unnest, new SqlIdentifier(unnestAlias, POS), new SqlIdentifier(fieldName, POS)),
            POS);
    SqlNode join =
        new org.apache.calcite.sql.SqlJoin(
            POS,
            aliasedInput,
            SqlLiteral.createBoolean(false, POS),
            JoinType.COMMA.symbol(POS),
            aliasedUnnest,
            org.apache.calcite.sql.JoinConditionType.NONE.symbol(POS),
            null);
    // Output projection mirrors v2: keep input cols (drop dotted descendants of <field> since
    // the unnested element replaces the parent struct, and drop metadata fields), then surface
    // the unnested element under the same name as the original array column.
    List<String> inputCols = frame.currentFields == null ? List.of() : frame.currentFields;
    java.util.Set<String> inputColSet = new java.util.LinkedHashSet<>(inputCols);
    SqlNodeList items = new SqlNodeList(POS);
    List<String> visible = new ArrayList<>();
    String dottedPrefix = fieldName + ".";
    for (String c : inputCols) {
      if (OpenSearchConstants.METADATAFIELD_TYPE_MAP.containsKey(c)) continue;
      if (c.equals(fieldName)) continue;
      if (c.startsWith(dottedPrefix)) continue;
      int lastDot = c.lastIndexOf('.');
      if (lastDot != -1 && inputColSet.contains(c.substring(0, lastDot))) continue;
      items.add(new SqlIdentifier(java.util.Arrays.asList(inputAlias, c), POS));
      visible.add(c);
    }
    items.add(
        asAliased(
            new SqlIdentifier(java.util.Arrays.asList(unnestAlias, fieldName), POS), fieldName));
    visible.add(fieldName);
    SqlNode result = SqlBuilder.select(items).from(join).withFields(visible).wrap(frame);
    // Register the unnested column on Frame.mapColumns so downstream dotted refs like
    // `skills.name` route through ITEM(skills, 'name') instead of multi-part identifier
    // resolution. OpenSearch arrays of nested-object documents become ARRAY of MAP at the catalog
    // level; UNNEST yields a MAP element. Without this, the validator would interpret
    // `skills.name` as `<table=skills>.<col=name>` and fail with "Table 'skills' not found".
    if (frame.arrayRootedFields.contains(fieldName)) {
      frame.mapColumns.add(fieldName);
    }
    // PPL `mvexpand <field> limit=N` caps the number of expanded rows PER source document. Apply
    // a per-input-row ROW_NUMBER() PARTITION BY <input_keys> filter to <= N. Without per-row
    // partitioning a global LIMIT would only keep N rows total across all source docs.
    if (node.getLimit() != null && node.getLimit() > 0) {
      result = applyMvExpandLimit(result, frame, node.getLimit());
    }
    return result;
  }

  /**
   * Wrap the mvexpand emission with a per-source-row LIMIT: assign ROW_NUMBER() OVER (PARTITION BY
   * <all-non-expanded-cols>) and filter rows where the rank is &le; N. Mirrors PPL's documented
   * {@code mvexpand <field> limit=N} semantics — caps expansion per source document.
   */
  private SqlNode applyMvExpandLimit(SqlNode mvexpandSelect, Frame frame, int limit) {
    // Partition by all visible cols EXCEPT the unnested field — those are stable per source row.
    String unnested = frame.currentFields.get(frame.currentFields.size() - 1);
    SqlNodeList partitionBy = new SqlNodeList(POS);
    for (String c : frame.currentFields) {
      if (c.equals(unnested)) continue;
      partitionBy.add(toIdentifier(c));
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
    String rnAlias = "_mvexpand_rn_";
    SqlNodeList innerItems = new SqlNodeList(POS);
    innerItems.add(SqlIdentifier.star(POS));
    innerItems.add(asAliased(rowNum, rnAlias));
    SqlNode innerSelect =
        new SqlSelect(
            POS, null, innerItems, mvexpandSelect, null, null, null, null, null, null, null, null);
    SqlNode rnLE =
        new SqlBasicCall(
            SqlStdOperatorTable.LESS_THAN_OR_EQUAL,
            List.of(
                new SqlIdentifier(rnAlias, POS),
                SqlLiteral.createExactNumeric(Integer.toString(limit), POS)),
            POS);
    // Outer SELECT with explicit field list (to drop the helper rn column from output).
    SqlNodeList outerItems = new SqlNodeList(POS);
    for (String c : frame.currentFields) {
      outerItems.add(toIdentifier(c));
    }
    return new SqlSelect(
        POS, null, outerItems, innerSelect, rnLE, null, null, null, null, null, null, null);
  }

  @Override
  public SqlNode visitExpand(Expand node, Frame frame) {
    // PPL `expand <field> [as <alias>]` unnests an array column: each input row becomes one row
    // per array element, with the element exposed as a struct under the alias (or the original
    // field name when no alias). Emit the LATERAL CROSS JOIN UNNEST shape.
    SqlNode input = node.getChild().get(0).accept(this, frame);
    UnresolvedExpression fieldExpr = node.getField().getField();
    if (!(fieldExpr instanceof QualifiedName fqn)) {
      throw new UnsupportedOperationException(
          "expand requires a simple column reference, got: " + fieldExpr.getClass());
    }
    String fieldName = fqn.toString();
    String alias = node.getAlias() != null ? node.getAlias() : fieldName;
    String inputAlias = "expand_input";
    String unnestAlias = "expand_t";
    SqlNode aliasedInput = SqlBuilder.aliasAs(input, inputAlias);
    SqlNode unnestArg = new SqlIdentifier(java.util.Arrays.asList(inputAlias, fieldName), POS);
    SqlNode unnest = new SqlBasicCall(SqlStdOperatorTable.UNNEST, List.of(unnestArg), POS);
    SqlNode aliasedUnnest =
        new SqlBasicCall(
            SqlStdOperatorTable.AS,
            List.of(unnest, new SqlIdentifier(unnestAlias, POS), new SqlIdentifier(alias, POS)),
            POS);
    SqlNode join =
        new org.apache.calcite.sql.SqlJoin(
            POS,
            aliasedInput,
            SqlLiteral.createBoolean(false, POS),
            JoinType.COMMA.symbol(POS),
            aliasedUnnest,
            org.apache.calcite.sql.JoinConditionType.NONE.symbol(POS),
            null);
    // Build explicit projection: input cols (drop the array source and dotted descendants whose
    // parent struct is in scope, mirroring v2's tryToRemoveNestedFields) + the unnested element
    // surfaced as `alias`.
    List<String> inputCols = frame.currentFields == null ? List.of() : frame.currentFields;
    java.util.Set<String> inputColSet = new java.util.LinkedHashSet<>(inputCols);
    SqlNodeList items = new SqlNodeList(POS);
    List<String> visible = new ArrayList<>();
    String dottedPrefix = fieldName + ".";
    for (String c : inputCols) {
      if (OpenSearchConstants.METADATAFIELD_TYPE_MAP.containsKey(c)) continue;
      // Drop the array source column — the unnested element replaces it (whether under the same
      // name or the user-supplied alias).
      if (c.equals(fieldName)) continue;
      if (c.startsWith(dottedPrefix)) continue;
      int lastDot = c.lastIndexOf('.');
      if (lastDot != -1 && inputColSet.contains(c.substring(0, lastDot))) continue;
      items.add(new SqlIdentifier(java.util.Arrays.asList(inputAlias, c), POS));
      visible.add(c);
    }
    items.add(new SqlIdentifier(java.util.Arrays.asList(unnestAlias, alias), POS));
    visible.add(alias);
    return SqlBuilder.select(items).from(join).withFields(visible).wrap(frame);
  }

  @Override
  public SqlNode visitLookup(Lookup node, Frame frame) {
    // PPL `LOOKUP <table> <map_field>[ AS <src_field>]... [{REPLACE|APPEND} <out_field>[ AS
    // <new_name>]...]` is a LEFT JOIN against the lookup table on the mapping pairs, projecting
    // selected output columns into the input row. Empty OUTPUT defaults to "all lookup-side
    // columns minus the mapping keys".
    SqlNode input = node.getChild().get(0).accept(this, frame);
    UnresolvedPlan lookupRel = node.getLookupRelation();
    if (!(lookupRel instanceof Relation rel)) {
      throw new UnsupportedOperationException("LOOKUP requires a bare relation");
    }
    List<String> lookupTableParts = rel.getTableQualifiedName().getParts();
    List<String> lookupCols = lookupTableFields(lookupTableParts);
    List<String> inputCols = frame.currentFields == null ? List.of() : frame.currentFields;
    java.util.Set<String> userCols = new java.util.LinkedHashSet<>();
    for (String c : inputCols) {
      if (!OpenSearchConstants.METADATAFIELD_TYPE_MAP.containsKey(c)) {
        userCols.add(c);
      }
    }

    java.util.Map<String, String> mapping = node.getMappingAliasMap();
    java.util.LinkedHashMap<String, String> output =
        new java.util.LinkedHashMap<>(node.getOutputAliasMap());
    if (output.isEmpty()) {
      // Default: all lookup-side columns except the mapping keys (and metadata fields).
      for (String c : lookupCols) {
        if (!mapping.containsKey(c) && !OpenSearchConstants.METADATAFIELD_TYPE_MAP.containsKey(c)) {
          output.put(c, c);
        }
      }
    }

    // Lookup-side SELECT projecting the union of (output-source cols, mapping keys), deduped.
    SqlNodeList lookupSelectList = new SqlNodeList(POS);
    java.util.Set<String> emittedLookupCols = new java.util.LinkedHashSet<>();
    for (String src : output.keySet()) {
      if (emittedLookupCols.add(src)) {
        lookupSelectList.add(toIdentifier(src));
      }
    }
    for (String key : mapping.keySet()) {
      if (emittedLookupCols.add(key)) {
        lookupSelectList.add(toIdentifier(key));
      }
    }
    SqlNode lookupRelation = SqlBuilder.relation(lookupTableParts, lookupCols, new Frame());
    SqlSelect lookupProject =
        new SqlSelect(
            POS,
            SqlNodeList.EMPTY,
            lookupSelectList,
            lookupRelation,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null);

    String inputAlias = "lookup_input";
    String lookupAlias = "lookup_t";
    SqlNode aliasedInput = SqlBuilder.aliasAs(input, inputAlias);
    SqlNode aliasedLookup = SqlBuilder.aliasAs(lookupProject, lookupAlias);

    // Build join condition. mappingAliasMap stores lookup-table column -> source column.
    // When the source column is a dotted MAP-path (e.g. `doc.user.name` over a MAP-typed `doc`
    // produced by spath auto-extract), emit `ITEM(lookup_input.doc, 'user.name')` so the
    // validator can drill the MAP value instead of failing with "Field [doc.user.name] not
    // found" via multi-part identifier resolution.
    SqlNode condition = null;
    for (java.util.Map.Entry<String, String> e : mapping.entrySet()) {
      SqlNode lookupCol = new SqlIdentifier(java.util.Arrays.asList(lookupAlias, e.getKey()), POS);
      String src = e.getValue();
      SqlNode sourceCol;
      int dot = src.indexOf('.');
      if (dot > 0 && frame.mapColumns.contains(src.substring(0, dot))) {
        SqlIdentifier mapRef =
            new SqlIdentifier(java.util.Arrays.asList(inputAlias, src.substring(0, dot)), POS);
        sourceCol =
            new SqlBasicCall(
                SqlStdOperatorTable.ITEM,
                List.of(mapRef, SqlLiteral.createCharString(src.substring(dot + 1), POS)),
                POS);
      } else {
        sourceCol = new SqlIdentifier(java.util.Arrays.asList(inputAlias, src), POS);
      }
      SqlNode eq = new SqlBasicCall(SqlStdOperatorTable.EQUALS, List.of(sourceCol, lookupCol), POS);
      condition =
          condition == null
              ? eq
              : new SqlBasicCall(SqlStdOperatorTable.AND, List.of(condition, eq), POS);
    }
    SqlNode join =
        new org.apache.calcite.sql.SqlJoin(
            POS,
            aliasedInput,
            SqlLiteral.createBoolean(false, POS),
            JoinType.LEFT.symbol(POS),
            aliasedLookup,
            org.apache.calcite.sql.JoinConditionType.ON.symbol(POS),
            condition);

    // Build final projection: input columns minus collisions, then lookup outputs.
    java.util.Set<String> targetAliases = new java.util.LinkedHashSet<>(output.values());
    boolean isAppend = node.getOutputStrategy() == Lookup.OutputStrategy.APPEND;
    SqlNodeList items = new SqlNodeList(POS);
    List<String> visible = new ArrayList<>();
    for (String c : userCols) {
      if (targetAliases.contains(c)) continue;
      items.add(new SqlIdentifier(java.util.Arrays.asList(inputAlias, c), POS));
      visible.add(c);
    }
    for (java.util.Map.Entry<String, String> e : output.entrySet()) {
      String src = e.getKey();
      String tgt = e.getValue();
      if (userCols.contains(tgt) && isAppend) {
        SqlNode coalesced =
            new SqlBasicCall(
                SqlStdOperatorTable.COALESCE,
                List.of(
                    new SqlIdentifier(java.util.Arrays.asList(inputAlias, tgt), POS),
                    new SqlIdentifier(java.util.Arrays.asList(lookupAlias, src), POS)),
                POS);
        items.add(asAliased(coalesced, tgt));
      } else {
        SqlNode lookupCol = new SqlIdentifier(java.util.Arrays.asList(lookupAlias, src), POS);
        items.add(src.equals(tgt) ? lookupCol : asAliased(lookupCol, tgt));
      }
      visible.add(tgt);
    }
    return SqlBuilder.select(items).from(join).withFields(visible).wrap(frame);
  }

  @Override
  public SqlNode visitGraphLookup(GraphLookup node, Frame frame) {
    // PPL `graphLookup` is implemented as a polymorphic table function (PTF) emitted as
    //   FROM TABLE(GRAPH_LOOKUP(TABLE(<source>), TABLE(<lookup>), startField | NULL,
    //                           fromField, toField, outputField, depthField | NULL,
    //                           maxDepth, bidirectional, supportArray, batchMode, usePIT))
    // A planner rule rewrites the LogicalTableFunctionScan into a LogicalGraphLookup.
    SqlNode sourceSqlNode;
    if (node.getStartValues() != null) {
      // Literal-start mode: SELECT 0 AS _dummy_ — one-row dummy source.
      SqlNodeList sel = new SqlNodeList(POS);
      sel.add(
          SqlStdOperatorTable.AS.createCall(
              POS, SqlLiteral.createExactNumeric("0", POS), new SqlIdentifier("_dummy_", POS)));
      sourceSqlNode =
          new SqlSelect(POS, null, sel, null, null, null, null, null, null, null, null, null);
    } else {
      if (node.getChild() == null || node.getChild().isEmpty()) {
        throw new IllegalArgumentException(
            "graphLookup field-reference start requires a piped source. Use literal start values"
                + " for top-level graphLookup.");
      }
      SqlNode innerFrom = node.getChild().get(0).accept(this, frame);
      // Non-batch mode: emit SELECT * with LIMIT 100 (no narrowing list) so the source feeds
      // the GraphLookup PTF without an intermediate metadata-strip Project — match v2's
      // emission shape (LogicalSort(fetch=100) directly under LogicalGraphLookup).
      // Batch mode: keep the narrowing list because the PTF's type signature requires the inner
      // record to carry NOT NULL nullability (batch-mode tests fail with "RecordType ... NOT
      // NULL ARRAY NOT NULL" type mismatch when SELECT * widens it).
      SqlNodeList sourceSelectList;
      if (node.isBatchMode()) {
        sourceSelectList = graphLookupNonMetaSelectList(frame.currentFields);
      } else {
        sourceSelectList = new SqlNodeList(POS);
        sourceSelectList.add(SqlIdentifier.star(POS));
      }
      sourceSqlNode =
          new SqlSelect(
              POS,
              null,
              sourceSelectList,
              innerFrom,
              null,
              null,
              null,
              null,
              null,
              null,
              null,
              SqlLiteral.createExactNumeric("100", POS),
              null);
    }
    UnresolvedPlan fromTablePlan = node.getFromTable();
    SqlNode lookupBase;
    List<String> lookupFields;
    if (fromTablePlan instanceof Relation rel) {
      List<String> parts = rel.getTableQualifiedName().getParts();
      lookupFields = lookupTableFields(parts);
      lookupBase = SqlBuilder.relation(parts, lookupFields, new Frame());
    } else {
      Frame lookupFrame = new Frame();
      lookupBase = fromTablePlan.accept(this, lookupFrame);
      lookupFields = lookupFrame.currentFields;
    }
    SqlNode lookupWhere = node.getFilter() != null ? expr(node.getFilter()) : null;
    SqlNode lookupSqlNode =
        new SqlSelect(
            POS,
            null,
            graphLookupNonMetaSelectList(lookupFields),
            lookupBase,
            lookupWhere,
            null,
            null,
            null,
            null,
            null,
            null,
            null);

    String fromFieldName = node.getFromField().getField().toString();
    String toFieldName = node.getToField().getField().toString();
    String outputFieldName = node.getAs().getField().toString();
    String depthFieldName = node.getDepthFieldName();
    Object maxDepthLitValue = node.getMaxDepth().getValue();
    int maxDepthValue =
        maxDepthLitValue instanceof Number n
            ? n.intValue()
            : Integer.parseInt(maxDepthLitValue.toString());
    boolean bidirectional = node.getDirection() == GraphLookup.Direction.BI;
    String startFieldName =
        node.getStartField() != null ? node.getStartField().getField().toString() : null;

    List<SqlNode> ptfArgs = new ArrayList<>();
    ptfArgs.add(graphLookupSetSemanticsTable(sourceSqlNode));
    ptfArgs.add(graphLookupSetSemanticsTable(lookupSqlNode));
    ptfArgs.add(
        startFieldName == null
            ? SqlLiteral.createNull(POS)
            : SqlLiteral.createCharString(startFieldName, POS));
    ptfArgs.add(SqlLiteral.createCharString(fromFieldName, POS));
    ptfArgs.add(SqlLiteral.createCharString(toFieldName, POS));
    ptfArgs.add(SqlLiteral.createCharString(outputFieldName, POS));
    ptfArgs.add(
        depthFieldName == null
            ? SqlLiteral.createNull(POS)
            : SqlLiteral.createCharString(depthFieldName, POS));
    ptfArgs.add(SqlLiteral.createExactNumeric(String.valueOf(maxDepthValue), POS));
    ptfArgs.add(SqlLiteral.createBoolean(bidirectional, POS));
    ptfArgs.add(SqlLiteral.createBoolean(node.isSupportArray(), POS));
    ptfArgs.add(SqlLiteral.createBoolean(node.isBatchMode(), POS));
    ptfArgs.add(SqlLiteral.createBoolean(node.isUsePIT(), POS));
    if (node.getStartValues() != null) {
      for (Literal lit : node.getStartValues()) {
        ptfArgs.add(ExpressionConverter.literalToSqlNode(lit));
      }
    }

    SqlNode graphLookupCall = new SqlBasicCall(GraphLookupTableFunction.INSTANCE, ptfArgs, POS);
    SqlNode tableExpr =
        new SqlBasicCall(SqlStdOperatorTable.COLLECTION_TABLE, List.of(graphLookupCall), POS);
    // Reset visible fields — the GraphLookup emits a new schema (input cols + output array col).
    // Without an oracle we leave currentFields null and let the validator's `*` expansion handle
    // downstream pipes. Drop any join-disambiguation hints from the source pipeline.
    frame.currentFields = null;
    frame.joinHints = null;
    frame.lastOrderBy = null;
    // Wrap the bare COLLECTION_TABLE in `SELECT * FROM (...)` so the validator sees a top-level
    // SELECT shape and the planner can derive the row type for downstream pipes/RelFieldTrimmer.
    SqlNodeList star = new SqlNodeList(POS);
    star.add(SqlIdentifier.star(POS));
    return SqlBuilder.select(star).from(tableExpr).wrap(frame);
  }

  /**
   * Build a non-metadata SELECT list for the graphLookup PTF source/lookup branches. Returns {@code
   * SELECT *} when no field list is known.
   */
  private static SqlNodeList graphLookupNonMetaSelectList(List<String> fields) {
    SqlNodeList list = new SqlNodeList(POS);
    if (fields == null || fields.isEmpty()) {
      list.add(SqlIdentifier.star(POS));
      return list;
    }
    for (String c : fields) {
      if (!OpenSearchConstants.METADATAFIELD_TYPE_MAP.containsKey(c)) {
        list.add(new SqlIdentifier(c, POS));
      }
    }
    if (list.isEmpty()) list.add(SqlIdentifier.star(POS));
    return list;
  }

  /**
   * Wrap a relation-valued SqlNode with SET_SEMANTICS_TABLE so the PTF treats it as a table arg.
   */
  private static SqlNode graphLookupSetSemanticsTable(SqlNode tableExpr) {
    return new SqlBasicCall(
        SqlStdOperatorTable.SET_SEMANTICS_TABLE,
        List.of(tableExpr, new SqlNodeList(POS), new SqlNodeList(POS)),
        POS);
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
    // Build the partition-field IS NOT NULL / IS NULL pre-filter (no helper col yet). v2 emits
    // this as a separate inner Filter rel BEFORE the window is added so the EnumerableWindow
    // operates on already-filtered rows. Putting it in a single AND with the bound check (as we
    // did before) collapses the two Filter rels into one — cosmetic plan-shape diff against v2.
    SqlNode partitionPred = null;
    if (keepEmpty) {
      // OR: F1 IS NULL OR F2 IS NULL OR ... — keepEmpty=true means rows with ANY null partition
      // key bypass the dedup window altogether. Without a separate inner filter this is hard to
      // express, so we fall back to the old single-Filter shape. (keepEmpty=true is rare.)
    } else {
      // AND: F1 IS NOT NULL AND F2 IS NOT NULL AND ... — applied as a separate inner Filter.
      for (SqlNode pf : partitionFields) {
        SqlNode notNull = new SqlBasicCall(SqlStdOperatorTable.IS_NOT_NULL, List.of(pf), POS);
        partitionPred =
            (partitionPred == null)
                ? notNull
                : new SqlBasicCall(SqlStdOperatorTable.AND, List.of(partitionPred, notNull), POS);
      }
    }
    SqlNode preFiltered = from;
    if (partitionPred != null) {
      // Wrap as `SELECT * FROM <from> WHERE <partitionPred>`.
      SqlNodeList passthroughStar = new SqlNodeList(POS);
      passthroughStar.add(SqlIdentifier.star(POS));
      preFiltered =
          new SqlSelect(
              POS,
              null,
              passthroughStar,
              from,
              partitionPred,
              null,
              null,
              null,
              null,
              null,
              null,
              null);
    }
    // Inner SELECT adds the ROW_NUMBER window column.
    SqlNodeList innerSelects = new SqlNodeList(POS);
    innerSelects.add(SqlIdentifier.star(POS));
    innerSelects.add(asAliased(rowNum, "_row_number_dedup_"));
    SqlNode innerSelect =
        new SqlSelect(
            POS, null, innerSelects, preFiltered, null, null, null, null, null, null, null, null);
    SqlNode boundCheck =
        new SqlBasicCall(
            SqlStdOperatorTable.LESS_THAN_OR_EQUAL,
            List.of(
                new SqlIdentifier("_row_number_dedup_", POS),
                SqlLiteral.createExactNumeric(Integer.toString(allowedDup), POS)),
            POS);
    SqlNode whereCond = boundCheck;
    if (keepEmpty) {
      // keepEmpty=true: still need to OR-in the IS-NULL conditions since we didn't pre-filter.
      // Result: (F1 IS NULL) OR ... OR (_row_number_dedup_ <= N)
      for (SqlNode pf : partitionFields) {
        whereCond =
            new SqlBasicCall(
                SqlStdOperatorTable.OR,
                List.of(new SqlBasicCall(SqlStdOperatorTable.IS_NULL, List.of(pf), POS), whereCond),
                POS);
      }
    }
    // Outer SELECT projects only the user-visible columns (excluding metadata fields and the
    // `_row_number_dedup_` helper) so neither leaks into downstream pipes. Filtering out the
    // OpenSearch metadata fields here also obviates the planner-level stripMetadataFields
    // shuttle's outer Project — which would otherwise produce an extra LogicalProject layer
    // between dedup and the implicit final `| fields *`. Updates frame.currentFields so subsequent
    // pipes see the trimmed list.
    SqlNodeList outerSelects = new SqlNodeList(POS);
    List<String> visible = frame.currentFields;
    List<String> userCols = null;
    if (visible != null && !visible.isEmpty()) {
      userCols = new ArrayList<>();
      for (String c : visible) {
        if (OpenSearchConstants.METADATAFIELD_TYPE_MAP.containsKey(c)) continue;
        outerSelects.add(toIdentifier(c));
        userCols.add(c);
      }
    }
    if (outerSelects.size() == 0) {
      outerSelects.add(SqlIdentifier.star(POS));
    } else {
      // Update frame so downstream pipes see the trimmed column list.
      frame.currentFields = userCols;
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
    return rex.aggCall(e);
  }

  /**
   * Build a Calcite aggregate call. When {@code windowed} is true, the call will be used inside an
   * OVER clause: PPL's nullable AVG/VAR/STDDEV variants have no window-context enumerable
   * implementation, so fall back to the standard SQL operators (and pair with a CASE-WHEN-COUNT
   * guard at the call site for the empty-group → NULL semantics).
   */
  private SqlNode aggCall(UnresolvedExpression e, boolean windowed) {
    return rex.aggCall(e, windowed);
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

  /**
   * Return the agg argument as an UnresolvedExpression when the agg is a single-field
   * AggregateFunction (count(field), sum(field), avg(field), etc.). Returns {@code null} for
   * count(*), multi-arg aggs, or non-AggregateFunction expressions.
   */
  private static UnresolvedExpression aggArgIfSingleField(UnresolvedExpression e) {
    UnresolvedExpression core = e instanceof Alias a ? a.getDelegated() : e;
    if (!(core instanceof AggregateFunction af)) return null;
    UnresolvedExpression field = af.getField();
    if (field instanceof org.opensearch.sql.ast.expression.AllFields) return null;
    if (field instanceof Field f) return f.getField();
    if (field instanceof QualifiedName) return field;
    return null;
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
    // Drop a reverse-implicit `@timestamp DESC` order-by when a user-explicit sort follows.
    // PPL semantics: `| reverse | sort F` should produce just `sort F` (the user override).
    // Detect via the frame flag set by visitReverse case 2; unwrap an outer SqlOrderBy to
    // expose the underlying SqlSelect for the new sort to operate on directly.
    if (frame.reverseImplicitOrderBy
        && from instanceof org.apache.calcite.sql.SqlOrderBy ob
        && ob.fetch == null
        && ob.offset == null
        && ob.query instanceof SqlSelect inner) {
      from = inner;
      frame.reverseImplicitOrderBy = false;
      frame.lastOrderBy = null;
    }
    // Correctness: when child is a `SELECT * FROM <X AS a> WHERE EXISTS(...)` (e.g. SEMI/ANTI
    // join body), wrap with SqlOrderBy directly instead of building a new SELECT around it.
    // The new SELECT would seal the alias `a` inside its FROM subquery, breaking `ORDER BY a.col`.
    // Required by CalcitePPLJoinIT (testComplexSemiJoin, testComplexAntiJoin).
    SqlNode peep = orderByOnChildSelectStar(from, orderKeys, null, fetch, frame);
    if (peep != null) return peep;
    SqlBuilder.SelectBuilder b = SqlBuilder.select(starList()).from(from).orderBy(orderKeys);
    if (fetch != null) {
      b.fetch(fetch);
    }
    return reattachSubqueryAlias(b.wrap(frame), frame);
  }

  /**
   * Install ORDER BY / FETCH / OFFSET on a child {@code SELECT *} without nesting a new outer
   * SELECT, so identifiers in {@code orderKeys} resolve against the inner FROM's aliases. Returns
   * null when the shape doesn't match.
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
  public SqlNode visitLimit(Limit node, Frame frame) {
    SqlNode from = node.getChild().get(0).accept(this, frame);
    SqlLiteral fetch = SqlLiteral.createExactNumeric(node.getLimit().toString(), POS);
    SqlLiteral offset =
        node.getOffset() != null && node.getOffset() > 0
            ? SqlLiteral.createExactNumeric(node.getOffset().toString(), POS)
            : null;
    SqlBuilder.SelectBuilder b = SqlBuilder.select(starList()).from(from).fetch(fetch);
    if (offset != null) {
      b.offset(offset);
    }
    return reattachSubqueryAlias(b.wrap(frame), frame);
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
      // When the prior sort produced an outer SqlOrderBy (no fetch/offset), unwrap it so the
      // reversed order-by replaces it directly instead of stacking. PPL semantics: reverse
      // FLIPS the prior ordering — it doesn't add a new layer on top. v2's RelBuilder produces
      // a single Sort with the flipped collation.
      if (from instanceof org.apache.calcite.sql.SqlOrderBy ob
          && ob.fetch == null
          && ob.offset == null
          && ob.query instanceof SqlSelect inner) {
        from = inner;
      }
      return SqlBuilder.select(starList()).from(from).orderBy(reversed).wrap(frame);
    }
    if (frame.currentFields != null
        && frame.currentFields.contains(OpenSearchConstants.IMPLICIT_FIELD_TIMESTAMP)) {
      SqlNode tsKey =
          new SqlBasicCall(
              SqlStdOperatorTable.DESC,
              List.of(new SqlIdentifier(OpenSearchConstants.IMPLICIT_FIELD_TIMESTAMP, POS)),
              POS);
      SqlNode result = SqlBuilder.select(starList()).from(from).orderBy(List.of(tsKey)).wrap(frame);
      // Mark the emitted order-by as reverse-implicit so a downstream user-explicit `| sort F`
      // can drop this fallback ordering (PPL semantics: user sort overrides reverse).
      frame.reverseImplicitOrderBy = true;
      return result;
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
    SqlNode rightSidePostMax = applyMaxPerPartition(node, rightSide, rightFrame);
    boolean rightWasMaxWrapped = rightSidePostMax != rightSide;
    rightSide = rightSidePostMax;
    // Strip metadata fields from the LEFT side so the join's output schema matches v2's
    // emission (no `_id`, `_index`, `_score`, `_maxscore`, `_sort`, `_routing` columns leaking
    // into the join's row type). Three safe cases:
    //   1. max=N is set (rightWasMaxWrapped) — the right side is already wrapped with
    //      strip-meta, so wrapping the left side too aligns the output schema.
    //   2. No ON-clause (joinCondition.isEmpty()) — the user wrote `| join F1 F2 ... <table>`
    //      with a join field list, so no `<table>.<col>` references are possible. Hiding the
    //      table identifier from outer scope is safe.
    //   3. Both explicit `left=l right=r` aliases are given — the ON-clause refers to the
    //      explicit aliases (l.col, r.col), NOT to the table identifiers. Wrapping with
    //      strip-meta SELECT below the alias re-application preserves alias resolution.
    // All cases additionally require the LEFT child to be a bare Relation (or
    // SubqueryAlias-wrapped Relation). Multi-join chains like testMultipleJoinsWithoutTable
    // Aliases have a Join AST as left child and stay untouched.
    UnresolvedPlan leftChild = node.getChildren().get(0);
    boolean leftIsBareRelationOrAlias =
        leftChild instanceof Relation
            || (leftChild instanceof SubqueryAlias sa
                && !sa.getChild().isEmpty()
                && sa.getChild().get(0) instanceof Relation);
    boolean noOnClause = node.getJoinCondition().isEmpty();
    boolean bothExplicitAliases =
        node.getLeftAlias().isPresent() && node.getRightAlias().isPresent();
    boolean canStripBothSides = (noOnClause || bothExplicitAliases) && leftIsBareRelationOrAlias;
    if ((rightWasMaxWrapped || canStripBothSides)
        && frame.currentFields != null
        && !frame.currentFields.isEmpty()
        && leftIsBareRelationOrAlias) {
      List<String> leftNonMeta = new ArrayList<>();
      for (String c : frame.currentFields) {
        if (!OpenSearchConstants.METADATAFIELD_TYPE_MAP.containsKey(c)) leftNonMeta.add(c);
      }
      if (!leftNonMeta.isEmpty() && leftNonMeta.size() < frame.currentFields.size()) {
        SqlNodeList stripList = new SqlNodeList(POS);
        for (String c : leftNonMeta) stripList.add(new SqlIdentifier(c, POS));
        leftSide =
            new SqlSelect(
                POS, null, stripList, leftSide, null, null, null, null, null, null, null, null);
        frame.currentFields = leftNonMeta;
      }
    }
    // Strip metadata fields from the RIGHT side when there's no ON-clause (no `<table>.<col>`
    // references possible) AND the right side wasn't already max-wrapped. Same safety
    // argument as the LEFT-side strip above for the no-ON-clause case.
    if (canStripBothSides
        && !rightWasMaxWrapped
        && rightFrame.currentFields != null
        && !rightFrame.currentFields.isEmpty()) {
      UnresolvedPlan rightChild = node.getRight();
      boolean rightIsBareRelationOrAlias =
          rightChild instanceof Relation
              || (rightChild instanceof SubqueryAlias sa
                  && !sa.getChild().isEmpty()
                  && sa.getChild().get(0) instanceof Relation);
      if (rightIsBareRelationOrAlias) {
        List<String> rightNonMeta = new ArrayList<>();
        for (String c : rightFrame.currentFields) {
          if (!OpenSearchConstants.METADATAFIELD_TYPE_MAP.containsKey(c)) rightNonMeta.add(c);
        }
        if (!rightNonMeta.isEmpty() && rightNonMeta.size() < rightFrame.currentFields.size()) {
          SqlNodeList stripList = new SqlNodeList(POS);
          for (String c : rightNonMeta) stripList.add(new SqlIdentifier(c, POS));
          rightSide =
              new SqlSelect(
                  POS, null, stripList, rightSide, null, null, null, null, null, null, null, null);
          rightFrame.currentFields = rightNonMeta;
        }
      }
    }

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
    // When a side is a bare Relation (or AS-wrapped relation) with no explicit `left=`/
    // `right=` arg, derive an effective alias from the table name FOR DOWNSTREAM JOIN HINT
    // PURPOSES ONLY (do not call applyExplicitAlias — the bare Relation already exposes the
    // table name as a queryable alias). This lets PPL's bind-bare-to-LEFT logic in
    // `qualifyIfAmbiguous` qualify ambiguous bare refs in the post-join Project. Without this,
    // `source=bank | join on f=g dog` over common cols (e.g. `age`) trips Calcite's validator
    // with "Column 'age' is ambiguous". Skip derivation when both sides resolve to the SAME
    // table name (self-join) — using the same identifier on both sides would duplicate the
    // FROM-clause relation name and tests like testJoinWithFieldListSelfJoin would fail with
    // "Duplicate relation name". v2's RelBuilder uses synthesized aliases like __l/__r in
    // those cases.
    String derivedLeft = leftAlias == null ? bareRelationAlias(node.getChildren().get(0)) : null;
    String derivedRight = rightAlias == null ? bareRelationAlias(node.getRight()) : null;
    boolean selfJoin = derivedLeft != null && derivedLeft.equals(derivedRight);
    if (!selfJoin) {
      if (derivedLeft != null) leftAlias = derivedLeft;
      if (derivedRight != null) rightAlias = derivedRight;
    }
    // The join consumes any inherited subquery alias — clear it so downstream pipes don't
    // wrap the join in `AS <stale-alias>`.
    frame.subqueryAliasName = null;
    // Track join-arg aliases on the frame so toIdentifier knows not to rewrite dotted refs
    // like `a.col` as quoted-single-part (which would break alias-qualified resolution).
    if (leftAlias != null) frame.liveJoinAliases.add(leftAlias);
    if (rightAlias != null) frame.liveJoinAliases.add(rightAlias);

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
  private SqlNode applyMaxPerPartition(Join node, SqlNode rightSide, Frame rightFrame) {
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
    // Compute the non-metadata column list for the right side. v2's RelBuilder emits explicit
    // strip-meta Projects below the row_number Project AND above the WHERE filter so the
    // join's output schema and the OpenSearch pushdown PROJECT clause match the user-visible
    // fields. Without these strip-Projects the post-conversion plan keeps metadata cols inside
    // the join's right input, breaking testJoinWithCriteriaAndMaxOption / testJoinWithFieldList
    // / etc. shape comparisons.
    List<String> nonMetaCols = null;
    if (rightFrame.currentFields != null && !rightFrame.currentFields.isEmpty()) {
      nonMetaCols = new ArrayList<>();
      for (String c : rightFrame.currentFields) {
        if (!OpenSearchConstants.METADATAFIELD_TYPE_MAP.containsKey(c)) {
          nonMetaCols.add(c);
        }
      }
      if (nonMetaCols.isEmpty()) nonMetaCols = null;
    }
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
    // Bottom: separate the strip-meta Project from the IS NOT NULL filter so Calcite preserves
    // both layers (mirrors v2's dedup-style emission). Combining them in one SELECT
    // (`SELECT non-meta FROM r WHERE IS NOT NULL(col)`) lets Calcite trim/swap and lose the
    // Project. Use TWO nested SELECTS: inner = strip-meta Project, outer = WHERE filter.
    SqlNode bottom = rightSide;
    if (nonMetaCols != null) {
      SqlNodeList stripList = new SqlNodeList(POS);
      for (String c : nonMetaCols) stripList.add(new SqlIdentifier(c, POS));
      SqlNode stripSelect =
          new SqlSelect(
              POS, null, stripList, rightSide, null, null, null, null, null, null, null, null);
      SqlNode stripAliased =
          new SqlBasicCall(
              SqlStdOperatorTable.AS,
              List.of(stripSelect, new SqlIdentifier("__max_strip__", POS)),
              POS);
      SqlNodeList allStar = new SqlNodeList(POS);
      allStar.add(SqlIdentifier.star(POS));
      // IS NOT NULL on the first partition col. v2 emits a single IS NOT NULL guard.
      SqlNode notNull =
          new SqlBasicCall(
              SqlStdOperatorTable.IS_NOT_NULL,
              List.of(new SqlIdentifier(partitionCols.get(0), POS)),
              POS);
      bottom =
          new SqlSelect(
              POS, null, allStar, stripAliased, notNull, null, null, null, null, null, null, null);
    }
    // Middle: SELECT *, ROW_NUMBER() OVER (PARTITION BY <pcol>) AS _row_number_dedup_ FROM
    // <bottom>.
    SqlNodeList innerSelects = new SqlNodeList(POS);
    innerSelects.add(SqlIdentifier.star(POS));
    innerSelects.add(asAliased(rowNum, "_row_number_dedup_"));
    SqlNode innerSelect =
        new SqlSelect(
            POS, null, innerSelects, bottom, null, null, null, null, null, null, null, null);
    SqlNode innerAliased =
        new SqlBasicCall(
            SqlStdOperatorTable.AS,
            List.of(innerSelect, new SqlIdentifier("__max_inner__", POS)),
            POS);
    // Top: outer SELECT keeping only non-meta cols (v2's emission — strip _row_number_dedup_
    // and any leaked metadata before the JOIN sees the right side). Falls back to SELECT * when
    // we don't know the non-meta col list.
    SqlNodeList outerSelects = new SqlNodeList(POS);
    if (nonMetaCols != null) {
      for (String c : nonMetaCols) outerSelects.add(new SqlIdentifier(c, POS));
    } else {
      outerSelects.add(SqlIdentifier.star(POS));
    }
    SqlNode outerWhere =
        new SqlBasicCall(
            SqlStdOperatorTable.LESS_THAN_OR_EQUAL,
            List.of(
                new SqlIdentifier("_row_number_dedup_", POS),
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
   * True when {@code fields} contains a dotted-name field whose parent (substring before the last
   * dot) is also in {@code fields}. Mirrors v2's tryToRemoveNestedFields trigger condition.
   */
  private static boolean hasDottedLeafWithVisibleParent(List<String> fields) {
    java.util.Set<String> set = new java.util.HashSet<>(fields);
    for (String c : fields) {
      int lastDot = c.lastIndexOf('.');
      if (lastDot >= 0 && set.contains(c.substring(0, lastDot))) {
        return true;
      }
    }
    return false;
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
  private List<String> resolveSelectedFields(
      Project node, List<String> incomingFields, JoinHints joinHints) {
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
      // Over an explicit-ON join the visible field list is `[bare..., l.bare..., r.bare...]`
      // (see {@link #unionFieldsWithAliasPrefixes}). The bare names are kept for downstream
      // pipes that write `| where state = '...'` (resolved via PPL's bind-bare-to-LEFT through
      // {@link Frame#joinHints}); but the implicit final `| fields *` emitted by
      // AstStatementBuilder must not double-up those columns. Drop bare names whose
      // alias-qualified counterpart is also visible; keep alias-qualified entries so each
      // side's columns appear exactly once.
      List<String> source = nonMeta;
      if (!node.isExcluded() && joinHints != null) {
        Set<String> aliasQualifiedLeaves = new LinkedHashSet<>();
        for (String c : nonMeta) {
          int dot = c.indexOf('.');
          if (dot >= 0) aliasQualifiedLeaves.add(c.substring(dot + 1));
        }
        if (!aliasQualifiedLeaves.isEmpty()) {
          List<String> dedup = new ArrayList<>();
          for (String c : nonMeta) {
            if (c.indexOf('.') < 0 && aliasQualifiedLeaves.contains(c)) continue;
            dedup.add(c);
          }
          source = dedup;
        }
      }
      // Mirror v2's tryToRemoveNestedFields: when a dotted-name field's parent is itself a
      // visible column, drop the dotted leaf. Otherwise an `eval `agent.name` = 'test'` after
      // `fields agent` would expose both `agent` (struct) and `agent.name` (string), inflating
      // the implicit `fields *` output beyond PPL's documented behaviour.
      java.util.Set<String> nonMetaSet = new java.util.LinkedHashSet<>(source);
      List<String> filtered = new ArrayList<>();
      for (String c : source) {
        int lastDot = c.lastIndexOf('.');
        if (lastDot >= 0 && nonMetaSet.contains(c.substring(0, lastDot))) {
          continue;
        }
        filtered.add(c);
      }
      return filtered;
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
  SqlNode expr(UnresolvedExpression e) {
    return rex.expr(e);
  }

  /**
   * {@code EXISTS (<subquery>)} — outer-scope columns referenced from the subquery resolve via
   * Calcite's correlated-subquery binding.
   */
  private SqlNode existsSubqueryExpr(org.opensearch.sql.ast.expression.subquery.ExistsSubquery es) {
    return rex.existsSubqueryExpr(es);
  }

  /**
   * Scalar subquery: emit the subquery's SqlSelect — Calcite treats it as a scalar expression when
   * used in a comparable context (e.g. {@code col = (subquery)}).
   */
  private SqlNode scalarSubqueryExpr(org.opensearch.sql.ast.expression.subquery.ScalarSubquery ss) {
    return rex.scalarSubqueryExpr(ss);
  }

  /**
   * PPL {@code where col in [<subquery>]} — emit {@code col IN (subSqlNode)}. The subquery is
   * walked in a fresh Frame so its own scope doesn't leak into the outer expression's frame.
   * Multi-column IN wraps the LHS as a {@code ROW(...)} so Calcite's IN operator binds against a
   * row-typed subquery output.
   */
  private SqlNode inSubqueryExpr(org.opensearch.sql.ast.expression.subquery.InSubquery is) {
    return rex.inSubqueryExpr(is);
  }

  /**
   * Translate PPL {@code multi_match(["fld1"^1.5, "fld2"^2], query="x")}'s field-list arg (parsed
   * as {@link org.opensearch.sql.ast.expression.RelevanceFieldList}) to a {@code MAP["fld1", 1.5,
   * "fld2", 2.0]} so the OpenSearch pushdown's relevance UDF receives a {@code Map<String,
   * Double>}. Cast each name to VARCHAR to avoid CHAR(N) length widening across entries.
   */
  private SqlNode relevanceFieldListExpr(org.opensearch.sql.ast.expression.RelevanceFieldList rfl) {
    return rex.relevanceFieldListExpr(rfl);
  }

  /**
   * Translate a PPL named argument {@code name=value} (used by match/match_phrase/multi_match and
   * other named-arg functions) into a {@code MAP[name, value]} entry. The pushdown layer collects
   * these into a single MAP-typed argument. String-literal values are cast to VARCHAR to avoid
   * Calcite's CHAR(N) length widening across entries (which right-pads shorter values and breaks
   * the OpenSearch pushdown's exact-string match).
   */
  private SqlNode unresolvedArgExpr(org.opensearch.sql.ast.expression.UnresolvedArgument ua) {
    return rex.unresolvedArgExpr(ua);
  }

  /**
   * Translate PPL {@code INTERVAL N <unit>} to a Calcite {@link SqlLiteral#createInterval}. The AST
   * stores the value as an {@link Literal} and the unit as an enum; map the unit to Calcite's
   * {@link org.apache.calcite.avatica.util.TimeUnit} and emit the literal with the appropriate
   * {@link org.apache.calcite.sql.SqlIntervalQualifier}. Non-literal values fall back to an
   * unresolved-function call (matches v2 emission for that rare path).
   */
  private SqlNode intervalExpr(org.opensearch.sql.ast.expression.Interval i) {
    return rex.intervalExpr(i);
  }

  /**
   * Translate {@code <field> IN (v1, v2, ...)} to a {@link SqlStdOperatorTable#IN} call. PPL's type
   * compatibility check (v2 raises SemanticCheckException for STRING vs NUMERIC mix) needs a
   * row-type oracle and is deferred — Calcite's validator coerces silently in the meantime.
   */
  private SqlNode inExpr(org.opensearch.sql.ast.expression.In in) {
    return rex.inExpr(in);
  }

  /**
   * Translate a PPL {@code Case} (CASE WHEN cond THEN val ... ELSE elseVal END) to a Calcite {@link
   * org.apache.calcite.sql.fun.SqlCase}. PPL stores the WHEN/THEN pairs as a list of {@link
   * org.opensearch.sql.ast.expression.When} nodes plus an optional ELSE expression.
   */
  private SqlNode caseExpr(org.opensearch.sql.ast.expression.Case node) {
    return rex.caseExpr(node);
  }

  /**
   * Translate {@link Span} (PPL {@code span(field, value [, unit])}) to a call on the SPAN
   * built-in. {@code SpanFunction} dispatches numeric vs time bucket via the unit operand: a NULL
   * cast to NULL type for the numeric branch, a string literal for time units.
   */
  private SqlNode spanExpr(Span sp) {
    return rex.spanExpr(sp);
  }

  /**
   * Translate a {@link Function} call. Arithmetic operators (PPL parses {@code +}/{@code -}/etc. as
   * Functions) bind to the corresponding Calcite operator; PPL's {@code +} between strings desugars
   * to {@code CONCAT}. Other named functions go through {@link
   * org.apache.calcite.sql.SqlUnresolvedFunction} so the validator's name lookup resolves them.
   */
  private SqlNode funcExpr(org.opensearch.sql.ast.expression.Function fn) {
    return rex.funcExpr(fn);
  }

  /**
   * Translate {@code CAST(expr AS type)}. Numeric/string casts use SAFE_CAST. The OpenSearch UDTs
   * (DATE/TIME/TIMESTAMP/IP) dispatch to PPL UDFs because SQL's CAST cannot represent the UDT's
   * row-type — the UDF call establishes the EXPR_* type and uses PPL's format-permissive parser.
   */
  private SqlNode castExpr(Cast c) {
    return rex.castExpr(c);
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
   * Best-effort static type derivation for an eval RHS. Returns the {@link DataType} when the
   * expression is a literal, a CAST, an arithmetic chain over already-typed column refs, or a
   * column ref whose alias type is known via {@link Frame#evalAliasTypes}. Returns null when the
   * type can't be determined statically (e.g. column ref to a scan-level field). Used to track eval
   * aliases for downstream cast/comparison decisions.
   */
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

  /**
   * Return the bare-relation table name (last part) when {@code child} is a Relation, or the inner
   * Relation's table name when wrapped in a SubqueryAlias holding a bare Relation. Returns {@code
   * null} otherwise (e.g. multi-join chains, subsearches).
   */
  private static String bareRelationAlias(UnresolvedPlan child) {
    Relation rel = null;
    if (child instanceof Relation r) {
      rel = r;
    } else if (child instanceof SubqueryAlias sa
        && !sa.getChild().isEmpty()
        && sa.getChild().get(0) instanceof Relation r) {
      rel = r;
    }
    if (rel == null) return null;
    List<String> parts = rel.getTableQualifiedName().getParts();
    if (parts == null || parts.isEmpty()) return null;
    return parts.get(parts.size() - 1);
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
    return stripAliasPrefix(names, null);
  }

  /**
   * Strip the alias prefix from each dotted name (e.g. {@code t1.col} → {@code col}). Reserved for
   * join-side projection where multiple sides expose the same leaf name. Skip stripping when the
   * prefix is a known {@link Frame#mapColumns} entry — those refer to MAP-typed columns (e.g.
   * OpenSearch nested-object scans, post-mvexpand element columns) where the dotted name IS the
   * literal flat-column name and stripping would lose the column.
   */
  private static List<String> stripAliasPrefix(List<String> names, Frame frame) {
    List<String> out = new ArrayList<>(names.size());
    Set<String> seen = new LinkedHashSet<>();
    for (String n : names) {
      int dot = n.indexOf('.');
      String stripped;
      if (dot < 0) {
        stripped = n;
      } else if (frame != null && frame.mapColumns.contains(n.substring(0, dot))) {
        // Flat-dotted ref over a MAP column — keep the full name so downstream pipes can resolve
        // it.
        stripped = n;
      } else {
        stripped = n.substring(dot + 1);
      }
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
    return rex.toIdentifier(name);
  }

  /**
   * If {@code name} is a bare column that exists on BOTH sides of a recent join, prefix it with the
   * left alias so Calcite resolves it without raising "Column 'x' is ambiguous". PPL binds bare
   * references to the LEFT side. Names that are already qualified ({@code a.col}) pass through
   * unchanged.
   */
  private SqlIdentifier qualifyIfAmbiguous(String name, Frame frame) {
    return rex.qualifyIfAmbiguous(name, frame);
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
