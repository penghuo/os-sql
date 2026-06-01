/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.sqlnode;

import org.apache.calcite.adapter.java.JavaTypeFactory;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.ViewExpanders;
import org.apache.calcite.prepare.Prepare;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOperatorTable;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.util.ChainedSqlOperatorTable;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.SqlValidatorUtil;
import org.apache.calcite.sql2rel.SqlToRelConverter;
import org.apache.calcite.sql2rel.StandardConvertletTable;
import org.apache.calcite.tools.FrameworkConfig;
import org.opensearch.sql.calcite.CalcitePlanContext;
import org.opensearch.sql.calcite.utils.CalciteToolsHelper;
import org.opensearch.sql.calcite.utils.OpenSearchTypeFactory;

/**
 * POC: drives a programmatically built PPL {@link SqlNode} through Calcite's standard pipeline:
 *
 * <ol>
 *   <li>{@link SqlValidator#validate(SqlNode)} — identifier/function resolution and type coercion;
 *   <li>{@link SqlToRelConverter#convertQuery(SqlNode, boolean, boolean)} — emit a {@link RelNode}.
 * </ol>
 *
 * <p>Uses {@link CalciteToolsHelper#withOpenSearchPrepare} so the validator/converter see the
 * OpenSearch index catalog (lazy table registration via {@code OpenSearchSchema} only works when
 * driven through {@code OpenSearchPrepareImpl}).
 */
public final class SqlNodePlanner {

  private final FrameworkConfig config;
  private final CalcitePlanContext context;

  /** Constructor used by tests that don't have a CalcitePlanContext available. */
  public SqlNodePlanner(FrameworkConfig config) {
    this(config, null);
  }

  /** Constructor used by QueryService — passes the plan context's connection through. */
  public SqlNodePlanner(FrameworkConfig config, CalcitePlanContext context) {
    this.config = config;
    this.context = context;
  }

  /** Validate then convert {@code sqlNode} into a {@link RelNode}. */
  public RelNode plan(SqlNode sqlNode) {
    return runWithPrepare((cluster, catalogReader) -> convert(cluster, catalogReader, sqlNode));
  }

  /**
   * Resolve a table qualified name (e.g. {@code ["my_index"]}) to its column names via a direct
   * catalog lookup — no validator involved.
   */
  public java.util.function.Function<java.util.List<String>, java.util.List<String>> tableFields() {
    return parts ->
        runWithPrepare(
            (cluster, catalogReader) -> {
              org.apache.calcite.prepare.Prepare.PreparingTable table =
                  catalogReader.getTable(parts);
              if (table == null) {
                throw new IllegalStateException("Table not found in catalog: " + parts);
              }
              return table.getRowType().getFieldNames();
            });
  }

  /**
   * Resolve a table qualified name to its full row type via a direct catalog lookup. Used by
   * visitors that need column types (e.g. UDT detection for UNION-pad emission), not just names.
   */
  public java.util.function.Function<
          java.util.List<String>, org.apache.calcite.rel.type.RelDataType>
      tableRowType() {
    return parts ->
        runWithPrepare(
            (cluster, catalogReader) -> {
              org.apache.calcite.prepare.Prepare.PreparingTable table =
                  catalogReader.getTable(parts);
              if (table == null) {
                throw new IllegalStateException("Table not found in catalog: " + parts);
              }
              return table.getRowType();
            });
  }

  /**
   * Build a row-type oracle suitable for {@link PplToSqlNode}'s schema-introspection-backed
   * commands. Validates a probe SqlNode in the same prepare context and returns its row type.
   */
  public java.util.function.Function<SqlNode, org.apache.calcite.rel.type.RelDataType>
      rowTypeOracle() {
    return probe ->
        runWithPrepare(
            (cluster, catalogReader) -> {
              SqlValidator probeValidator =
                  SqlValidatorUtil.newValidator(
                      buildOperatorTable(),
                      catalogReader,
                      cluster.getTypeFactory(),
                      SqlValidator.Config.DEFAULT.withTypeCoercionEnabled(true));
              SqlNode validatedProbe = probeValidator.validate(probe);
              return probeValidator.getValidatedNodeType(validatedProbe);
            });
  }

  /**
   * Deep-clone a SqlNode tree. {@link SqlNode#clone(org.apache.calcite.sql.parser.SqlParserPos)} is
   * shallow — children are shared. Calcite's validator mutates {@link
   * org.apache.calcite.sql.SqlIdentifier#names} during fullyQualify expansion. Use this helper to
   * isolate validator mutations when probing a piece of the in-flight SqlNode tree (the original
   * tree must remain pristine so it can be re-validated as part of a larger context later).
   */
  public static SqlNode deepClone(SqlNode src) {
    if (src == null) return null;
    return (SqlNode) src.accept(new DeepCloneShuttle());
  }

  private static final class DeepCloneShuttle extends org.apache.calcite.sql.util.SqlShuttle {
    @Override
    public SqlNode visit(org.apache.calcite.sql.SqlLiteral lit) {
      return lit.clone(lit.getParserPosition());
    }

    @Override
    public SqlNode visit(org.apache.calcite.sql.SqlIdentifier id) {
      java.util.List<org.apache.calcite.sql.parser.SqlParserPos> positions =
          new java.util.ArrayList<>(id.names.size());
      for (int i = 0; i < id.names.size(); i++) {
        positions.add(id.getComponentParserPosition(i));
      }
      return new org.apache.calcite.sql.SqlIdentifier(
          new java.util.ArrayList<>(id.names),
          id.getCollation(),
          id.getParserPosition(),
          positions);
    }

    @Override
    public SqlNode visit(org.apache.calcite.sql.SqlDataTypeSpec spec) {
      return spec.clone(spec.getParserPosition());
    }

    @Override
    public SqlNode visit(org.apache.calcite.sql.SqlDynamicParam p) {
      return p.clone(p.getParserPosition());
    }

    @Override
    public SqlNode visit(org.apache.calcite.sql.SqlIntervalQualifier iq) {
      return iq.clone(iq.getParserPosition());
    }

    @Override
    public SqlNode visit(org.apache.calcite.sql.SqlNodeList list) {
      org.apache.calcite.sql.SqlNodeList copy =
          new org.apache.calcite.sql.SqlNodeList(list.getParserPosition());
      for (SqlNode n : list) {
        copy.add(n == null ? null : n.accept(this));
      }
      return copy;
    }

    @Override
    public SqlNode visit(org.apache.calcite.sql.SqlCall call) {
      java.util.List<SqlNode> operands = call.getOperandList();
      java.util.List<SqlNode> copies = new java.util.ArrayList<>(operands.size());
      for (SqlNode o : operands) {
        copies.add(o == null ? null : o.accept(this));
      }
      org.apache.calcite.sql.SqlOperator op = call.getOperator();
      org.apache.calcite.sql.SqlLiteral fq =
          (call instanceof org.apache.calcite.sql.SqlBasicCall sbc)
              ? sbc.getFunctionQuantifier()
              : null;
      return op.createCall(fq, call.getParserPosition(), copies);
    }
  }

  /**
   * Walk the rel tree and rewrite each {@code LogicalTableFunctionScan(GRAPH_LOOKUP)} to a {@link
   * org.opensearch.sql.calcite.plan.rel.LogicalGraphLookup}. We use a custom shuttle (rather than
   * HepPlanner) because the parent's input-ref types must be re-derived from the new child's row
   * type — HepPlanner's {@code transformTo} doesn't propagate that.
   */
  private RelNode applyGraphLookupRule(RelNode rel) {
    return rewriteGraphLookupTfs(rel);
  }

  /**
   * Recursively walk the rel tree (children first), rewriting LTFS(GRAPH_LOOKUP) → LGL. When a
   * child's row type changes, rebuild the parent (Project / LogicalSort / etc.) so its RexInputRefs
   * point at the new child's actual type — avoiding assertion failures in downstream
   * RelShuttle.copy() invocations.
   */
  private RelNode rewriteGraphLookupTfs(RelNode rel) {
    boolean anyChildChanged = false;
    java.util.List<RelNode> newInputs = new java.util.ArrayList<>();
    for (RelNode oldChild : rel.getInputs()) {
      RelNode newChild = rewriteGraphLookupTfs(oldChild);
      if (newChild != oldChild) anyChildChanged = true;
      newInputs.add(newChild);
    }
    // Rewrite this node first (in case it's a TFS).
    RelNode self = rel;
    if (rel instanceof org.apache.calcite.rel.core.TableFunctionScan tfs
        && tfs.getCall() instanceof org.apache.calcite.rex.RexCall rexCall
        && rexCall.getOperator() instanceof GraphLookupTableFunction) {
      // Use the new (already-rewritten) inputs.
      org.apache.calcite.rel.core.TableFunctionScan refreshedTfs =
          (org.apache.calcite.rel.core.TableFunctionScan)
              tfs.copy(
                  tfs.getTraitSet(),
                  newInputs,
                  tfs.getCall(),
                  tfs.getElementType(),
                  tfs.getRowType(),
                  tfs.getColumnMappings());
      return GraphLookupTableFunctionRule.rewrite(refreshedTfs);
    }
    if (anyChildChanged) {
      // Children changed; rebuild this node carefully. Project needs its RexInputRefs re-typed
      // because the new child's row type may differ.
      if (self instanceof org.apache.calcite.rel.logical.LogicalProject proj) {
        return rebuildProject(proj, newInputs.get(0));
      }
      return self.copy(self.getTraitSet(), newInputs);
    }
    return self;
  }

  /**
   * Rebuild a Project after its child's row type changed. Re-derives each project expression's type
   * by re-creating RexInputRefs against the new child rowType (positions stay the same).
   */
  private RelNode rebuildProject(
      org.apache.calcite.rel.logical.LogicalProject proj, RelNode newChild) {
    org.apache.calcite.rex.RexBuilder rb = proj.getCluster().getRexBuilder();
    java.util.List<org.apache.calcite.rex.RexNode> newProjects = new java.util.ArrayList<>();
    for (org.apache.calcite.rex.RexNode oldExpr : proj.getProjects()) {
      newProjects.add(reTypeRex(oldExpr, newChild, rb));
    }
    return org.apache.calcite.rel.logical.LogicalProject.create(
        newChild,
        proj.getHints(),
        newProjects,
        proj.getRowType().getFieldNames(),
        proj.getVariablesSet());
  }

  /** If the rex is a RexInputRef, rebuild it against newChild's row type at the same index. */
  private org.apache.calcite.rex.RexNode reTypeRex(
      org.apache.calcite.rex.RexNode expr, RelNode newChild, org.apache.calcite.rex.RexBuilder rb) {
    if (expr instanceof org.apache.calcite.rex.RexInputRef ref) {
      // Direct construction with the CHILD's exact field type — RexBuilder.makeInputRef may
      // canonicalize/strip nullability when constructing through type coercion. We want the
      // exact stored type to match child.getRowType().getFieldList().get(index).getType()
      // because Calcite's RexChecker compares stored type byte-for-byte.
      return new org.apache.calcite.rex.RexInputRef(
          ref.getIndex(), newChild.getRowType().getFieldList().get(ref.getIndex()).getType());
    }
    return expr;
  }

  /** Compute what the project's row type would be against the (possibly new) child. */
  private org.apache.calcite.rel.type.RelDataType recomputeRowType(
      org.apache.calcite.rel.logical.LogicalProject proj, RelNode child) {
    org.apache.calcite.rex.RexBuilder rb = proj.getCluster().getRexBuilder();
    org.apache.calcite.rel.type.RelDataTypeFactory.Builder builder =
        proj.getCluster().getTypeFactory().builder();
    for (int i = 0; i < proj.getProjects().size(); i++) {
      org.apache.calcite.rex.RexNode pi = proj.getProjects().get(i);
      String name = proj.getRowType().getFieldList().get(i).getName();
      org.apache.calcite.rex.RexNode rebuilt = reTypeRex(pi, child, rb);
      builder.add(name, rebuilt.getType());
    }
    return builder.build();
  }

  private SqlOperatorTable buildOperatorTable() {
    java.util.List<SqlOperatorTable> tables = new java.util.ArrayList<>();
    // Permissive relevance UDFs come FIRST so they shadow PPLBuiltinOperators' strict
    // composite-checker variants for the `query_string`/`match`/etc. names.
    tables.add(
        new SingleOperatorTable(
            org.opensearch.sql.calcite.sqlnode.PermissiveRelevanceFunctions.QUERY_STRING));
    tables.add(
        new SingleOperatorTable(
            org.opensearch.sql.calcite.sqlnode.PermissiveRelevanceFunctions.SIMPLE_QUERY_STRING));
    tables.add(
        new SingleOperatorTable(
            org.opensearch.sql.calcite.sqlnode.PermissiveRelevanceFunctions.MATCH));
    tables.add(
        new SingleOperatorTable(
            org.opensearch.sql.calcite.sqlnode.PermissiveRelevanceFunctions.MULTI_MATCH));
    tables.add(
        new SingleOperatorTable(
            org.opensearch.sql.calcite.sqlnode.PermissiveRelevanceFunctions.MATCH_PHRASE));
    tables.add(
        new SingleOperatorTable(
            org.opensearch.sql.calcite.sqlnode.PermissiveRelevanceFunctions.MATCH_BOOL_PREFIX));
    tables.add(
        new SingleOperatorTable(
            org.opensearch.sql.calcite.sqlnode.PermissiveRelevanceFunctions.MATCH_PHRASE_PREFIX));
    // Permissive IS_EMPTY function — registered as a function (resolved by name lookup) so v2's
    // `isempty(string_field)` desugar can route through it without hitting Calcite's standard
    // postfix IS_EMPTY's collection-only operand check. A post-conversion RexShuttle rewrites
    // this back to SqlStdOperatorTable.IS_EMPTY so the explain output matches v2.
    tables.add(new SingleOperatorTable(PplToSqlNode.PERMISSIVE_IS_EMPTY));
    // GRAPH_LOOKUP polymorphic table function — used by PPL's graphLookup pipe. Validator routes
    // the call to LogicalTableFunctionScan; a planner rule rewrites that to LogicalGraphLookup.
    tables.add(new SingleOperatorTable(GraphLookupTableFunction.INSTANCE));
    if (config.getOperatorTable() != null) {
      tables.add(config.getOperatorTable());
    }
    // Include OpenSearch's dynamically-registered operators (geoip, distinct_count_approx, ...)
    // — these are registered at runtime in OpenSearchExecutionEngine.registerOpenSearchFunctions
    // when the node client is available. Without including this table, queries like
    // `eval x = geoip(...)` fail at validate time with "No match found for function signature".
    try {
      Class<?> opTableClass =
          Class.forName(
              "org.opensearch.sql.opensearch.executor.OpenSearchExecutionEngine$OperatorTable");
      java.lang.reflect.Method instance = opTableClass.getMethod("instance");
      Object openSearchOpTable = instance.invoke(null);
      if (openSearchOpTable instanceof SqlOperatorTable sot) {
        tables.add(sot);
      }
    } catch (ClassNotFoundException
        | NoSuchMethodException
        | IllegalAccessException
        | java.lang.reflect.InvocationTargetException e) {
      // OpenSearch storage layer not on the classpath; skip.
    }
    tables.add(org.opensearch.sql.expression.function.PPLBuiltinOperators.instance());
    tables.add(SqlStdOperatorTable.instance());
    // SqlLibraryOperators contains operators like SAFE_DIVIDE that PPLFuncImpTable uses for
    // PPL-specific arithmetic semantics (e.g. division by zero returns NULL).
    tables.add(
        org.apache.calcite.sql.fun.SqlLibraryOperatorTableFactory.INSTANCE.getOperatorTable(
            org.apache.calcite.sql.fun.SqlLibrary.STANDARD,
            org.apache.calcite.sql.fun.SqlLibrary.BIG_QUERY,
            org.apache.calcite.sql.fun.SqlLibrary.SPARK,
            org.apache.calcite.sql.fun.SqlLibrary.HIVE,
            org.apache.calcite.sql.fun.SqlLibrary.POSTGRESQL,
            org.apache.calcite.sql.fun.SqlLibrary.MYSQL));
    SqlOperatorTable chained =
        tables.size() == 1 ? tables.get(0) : new ChainedSqlOperatorTable(tables);
    // PPL function names are conventionally case-insensitive (`num` ≡ `NUM`). The validator's
    // default name matcher is case-sensitive, so wrap the table to retry case-insensitively when
    // the case-sensitive lookup misses.
    return new CaseInsensitiveOperatorTable(chained);
  }

  /** Single-operator table — used to register one permissive relevance UDF at table head. */
  private static final class SingleOperatorTable implements SqlOperatorTable {
    private final org.apache.calcite.sql.SqlOperator op;

    SingleOperatorTable(org.apache.calcite.sql.SqlOperator op) {
      this.op = op;
    }

    @Override
    public void lookupOperatorOverloads(
        org.apache.calcite.sql.SqlIdentifier opName,
        org.apache.calcite.sql.SqlFunctionCategory category,
        org.apache.calcite.sql.SqlSyntax syntax,
        java.util.List<org.apache.calcite.sql.SqlOperator> operatorList,
        org.apache.calcite.sql.validate.SqlNameMatcher nameMatcher) {
      if (opName.isSimple() && nameMatcher.matches(opName.getSimple(), op.getName())) {
        operatorList.add(op);
      }
    }

    @Override
    public java.util.List<org.apache.calcite.sql.SqlOperator> getOperatorList() {
      return java.util.List.of(op);
    }
  }

  /**
   * Operator table that retries lookups case-insensitively when the underlying table comes back
   * empty. PPL parses function names verbatim (lower-case in PPL source); UDFs in
   * PPLBuiltinOperators are registered with their canonical (often upper-case) names. Without this
   * wrapper, the validator can't resolve `num`/`auto`/etc.
   */
  private static final class CaseInsensitiveOperatorTable implements SqlOperatorTable {
    private final SqlOperatorTable delegate;

    CaseInsensitiveOperatorTable(SqlOperatorTable delegate) {
      this.delegate = delegate;
    }

    @Override
    public void lookupOperatorOverloads(
        org.apache.calcite.sql.SqlIdentifier opName,
        org.apache.calcite.sql.SqlFunctionCategory category,
        org.apache.calcite.sql.SqlSyntax syntax,
        java.util.List<org.apache.calcite.sql.SqlOperator> operatorList,
        org.apache.calcite.sql.validate.SqlNameMatcher nameMatcher) {
      delegate.lookupOperatorOverloads(opName, category, syntax, operatorList, nameMatcher);
      if (!operatorList.isEmpty()) {
        return;
      }
      // Retry with a case-insensitive matcher.
      delegate.lookupOperatorOverloads(
          opName,
          category,
          syntax,
          operatorList,
          org.apache.calcite.sql.validate.SqlNameMatchers.withCaseSensitive(false));
    }

    @Override
    public java.util.List<org.apache.calcite.sql.SqlOperator> getOperatorList() {
      return delegate.getOperatorList();
    }
  }

  /**
   * Run an action through the OpenSearch-aware prepare pipeline so {@code OpenSearchSchema}'s lazy
   * table registration is exercised. Falls back to a fresh JDBC connection when no
   * CalcitePlanContext was supplied (test-only path).
   */
  private <R> R runWithPrepare(
      java.util.function.BiFunction<RelOptCluster, Prepare.CatalogReader, R> action) {
    JavaTypeFactory typeFactory = OpenSearchTypeFactory.TYPE_FACTORY;
    java.sql.Connection conn =
        context != null ? context.connection : CalciteToolsHelper.connect(config, typeFactory);
    return CalciteToolsHelper.withOpenSearchPrepare(
        config,
        typeFactory,
        conn,
        (cluster, relOptSchema, rootSchema, statement) -> {
          if (!(relOptSchema instanceof Prepare.CatalogReader catalogReader)) {
            throw new IllegalStateException(
                "OpenSearch prepare expected a Prepare.CatalogReader, got: "
                    + relOptSchema.getClass().getName());
          }
          return action.apply(cluster, catalogReader);
        });
  }

  private RelNode convert(
      RelOptCluster cluster, Prepare.CatalogReader catalogReader, SqlNode sqlNode) {
    SqlValidator validator =
        SqlValidatorUtil.newValidator(
            buildOperatorTable(),
            catalogReader,
            cluster.getTypeFactory(),
            SqlValidator.Config.DEFAULT.withTypeCoercionEnabled(true));

    SqlNode validated;
    try {
      validated = validator.validate(sqlNode);
    } catch (RuntimeException e) {
      throw translateValidationError(e);
    }

    RexBuilder rexBuilder = new RexBuilder(cluster.getTypeFactory());
    RelOptCluster relCluster = RelOptCluster.create(cluster.getPlanner(), rexBuilder);
    SqlToRelConverter converter =
        new PplSqlToRelConverter(
            ViewExpanders.simpleContext(relCluster),
            validator,
            catalogReader,
            relCluster,
            StandardConvertletTable.INSTANCE,
            SqlToRelConverter.config()
                .withTrimUnusedFields(false)
                .withExpand(true)
                .withRemoveSortInSubQuery(false));

    RelRoot root;
    try {
      root = converter.convertQuery(validated, /* needsValidation */ false, /* top */ true);
    } catch (RuntimeException e) {
      throw translateValidationError(e);
    }
    RelNode rel = root.rel;
    // Rewrite LogicalTableFunctionScan(GRAPH_LOOKUP) → LogicalGraphLookup before the rest of the
    // post-conversion pipeline runs. Downstream rules expect a real LogicalGraphLookup, not the
    // PTF wrapper that the validator/converter route through.
    rel = applyGraphLookupRule(rel);
    // After conversion, traverse the rel tree and inject alias-field projections atop any
    // AliasFieldsWrappable scans. v2's CalciteRelNodeVisitor.visitRelation does this immediately
    // after the scan; doing it post-conversion on the SqlNode path achieves the same result.
    rel = wrapAliasFieldsBelow(rel);
    // The converter strips a top-level ORDER BY into RelRoot.collation; PPL preserves sort
    // through the pipe chain, so re-attach an explicit LogicalSort if needed. Re-attach BEFORE
    // stripping helper columns so the sort's collation indices stay valid.
    if (!root.collation.getFieldCollations().isEmpty()
        && !(rel instanceof org.apache.calcite.rel.core.Sort)) {
      rel = org.apache.calcite.rel.logical.LogicalSort.create(rel, root.collation, null, null);
    }
    // Filter out the internal `__stream_seq__` / `__seg_id__` / `_rn_main_` / `_rn_sub_`
    // synthesizer columns from the final output — multi-pipe streamstats keeps them visible
    // in inner subqueries so each pipe's window can ORDER BY a stable global sequence, but the
    // user-facing rows don't show these helpers.
    rel = stripSyntheticSeqColumns(rel);
    // SqlToRelConverter retains sort keys in the inner project for ORDER BY references when
    // trimUnusedFields is off, so the user-visible row type (root.fields) may be narrower than
    // the actual rel row type. Project down to root.fields when they differ — PPL's `fields`
    // command expects the explicit projection list to be the sole output columns.
    if (root.fields != null
        && !root.fields.isEmpty()
        && root.fields.size() < rel.getRowType().getFieldCount()) {
      java.util.List<org.apache.calcite.rex.RexNode> projects = new java.util.ArrayList<>();
      java.util.List<String> names = new java.util.ArrayList<>();
      org.apache.calcite.rex.RexBuilder rb = rel.getCluster().getRexBuilder();
      for (java.util.Map.Entry<Integer, String> f : root.fields) {
        projects.add(rb.makeInputRef(rel, f.getKey()));
        names.add(f.getValue());
      }
      rel =
          org.apache.calcite.rel.logical.LogicalProject.create(
              rel, java.util.Collections.emptyList(), projects, names, java.util.Set.of());
    }
    // Apply v2's SubsearchUtils-style limit insertion to IN-subqueries only. The same logic
    // theoretically applies to EXISTS subqueries, but recreating LogicalFilter under a
    // SystemLimit breaks correlation association in our converted-rel post-processing path
    // (testSimpleExistsSubqueryInFilter regresses); v2's RelBuilder-based path doesn't suffer
    // because it operates pre-conversion. Limit only IN — that fixes testSubsearchMaxOut and
    // testInCorrelatedSubqueryMaxOut without touching EXISTS.
    Integer ssLimit =
        context != null && context.sysLimit != null ? context.sysLimit.subsearchLimit() : null;
    if (ssLimit != null && ssLimit > 0) {
      rel = applySubsearchLimitForIn(rel);
    }
    // Relabel join-right LogicalSort(fetch=joinSubsearchLimit, no collation) to
    // LogicalSystemLimit(JOIN_SUBSEARCH_MAXOUT) to match v2's emission shape. v2's
    // addSysLimitForJoinSubsearch wraps the right side in a LogicalSystemLimit; the SqlNode
    // path emits FETCH which becomes plain LogicalSort. Only the explain output differs —
    // execution semantics are identical.
    Integer jsLimit =
        context != null && context.sysLimit != null ? context.sysLimit.joinSubsearchLimit() : null;
    if (jsLimit != null && jsLimit > 0) {
      rel = relabelJoinSubsearchSort(rel, jsLimit);
    }
    // Rewrite RexCalls on PERMISSIVE_IS_EMPTY (registered as a `IS_EMPTY` function) back to the
    // standard SqlStdOperatorTable.IS_EMPTY postfix operator so explain output matches v2's
    // `IS EMPTY(arg)` shape. Validation already ran (and accepted ANY operand), so the rewrite
    // is post-conversion and avoids the strict collection-only check.
    rel = rewritePermissiveIsEmpty(rel);
    // Rewrite `LENGTH(REPLACE(arg, ' ', '')) = 0` (emitted by visitFunc isblank) back to
    // v2's `IS_EMPTY(TRIM(BOTH ' ', arg))` shape. The SqlNode validator can't accept the TRIM
    // keyword-syntax form via SqlBasicCall, so we emit the equivalent LENGTH/REPLACE shape
    // and post-convert here.
    rel = rewriteIsBlankToTrimEmpty(rel);
    // Rewrite TIMESTAMP/DATE/TIME/IP UDF wrappers around NULL pads to typed NULL literals.
    // padSelectWithReference emits `TIMESTAMP(CAST(NULL AS VARCHAR))` for absent UDT columns
    // in UNION-ALL pads (so the type system carries the UDT through). v2's RexBuilder produces
    // a typed-null literal directly (`null:EXPR_TIMESTAMP VARCHAR`); rewrite our wrapper RexCall
    // back to a typed null literal of the same RexCall's return type.
    rel = rewriteUdtWrappedNullsToTypedNulls(rel);
    // Rename auto-named `$fN` columns in pre-aggregate Project (input of LogicalAggregate) when
    // the column expression is a SPAN call. v2 names this column with the user-visible alias
    // `span(field,unit)`; SqlValidator only sees the SPAN call without an explicit alias in the
    // GROUP BY context and assigns `$fN`. Mirror v2's emission shape so the explain plan and the
    // OpenSearch composite-aggregation source name match v2 (Big5 composite_date_histogram_*
    // tests compare these names).
    rel = renameSpanProjectColumn(rel);
    // Bump pre-aggregate Project field names from `$fN` to `$f<N+1>` when the parent Aggregate
    // has no group-by AND a single agg-call (count-no-group with eval-arg pattern). v2's
    // RelBuilder reserves position 0 for the agg result; SqlValidator names projection cols from
    // position 0. Limit to the single-arg case to avoid regressing multi-arg agg shapes.
    rel = bumpPreAggregateAutoNames(rel);
    // Collapse `Project(span_alias=$N) <- Filter(F on $M) <- Project(passthrough[*] + SPAN AS
    // span_alias)`
    // when the outer Project ONLY references the trailing SPAN column. This pattern comes from
    // visitAggregation's pre-wrap path. v2 emits just `Project(span_alias=SPAN($M)) <- Filter(F')
    // <- Scan`.
    // Conservative: only when SPAN is the trailing computed and outer references only that col.
    rel = collapseSpanWidePassthroughProject(rel);
    // Collapse identity-Projects sitting BETWEEN two LogicalFilter rels — these come from the
    // implicit SELECT * inserted by Pipeline.wrap() when visitFilter wraps to keep adjacent
    // filter pipes as separate Filter rels (matching v2's plan shape). The Calcite optimizer
    // doesn't strip these when trimUnusedFields is off.
    rel = trimIdentityProjectsBetweenFilters(rel);
    // Swap `Sort <- Project(passthrough)` to `Project <- Sort` so PPL `sort | fields` matches v2's
    // `LogicalProject(fields) <- LogicalSort` shape (cosmetic plan-shape alignment for
    // CalciteExplainIT and CalcitePPLBig5IT). Only swaps when the Project is a pure passthrough
    // (RexInputRef-only, no computed expressions) so the sort keys can be straightforwardly
    // re-indexed from projected-positions to input-positions.
    rel = swapSortAndPassthroughProject(rel);
    // PPL `... | sort F | head N` produces two adjacent LogicalSort rels (or after the swap
    // above, separated only by a passthrough Project that swap moved on top): an inner
    // sort-only (collation, no fetch) and an outer fetch-only (no collation). v2's RelBuilder
    // path fuses them into a single Sort(collation, fetch=N). Run AFTER swap so the swap can
    // collapse `Sort(fetch) -> Project -> Sort(collation)` to `Project -> Sort(fetch) ->
    // Sort(collation)` which is the directly-adjacent shape my fuse handles.
    rel = fuseAdjacentSorts(rel);
    // Re-run trim AFTER swap so adjacent passthrough Projects produced by the swap (e.g. a
    // user-fields Project sitting on top of an eval-extended Project that exposed a sort key)
    // get collapsed into a single composed Project.
    rel = trimIdentityProjectsBetweenFilters(rel);
    // Strip `CAST($N):INTEGER` from comparison operands when the cast wraps a plain RexInputRef.
    // SqlValidator inserts these CAST wrappers when comparing fields of different numeric types
    // (e.g., BIGINT field <> INTEGER literal); v2's RexBuilder.makeCall skips the cast since
    // Calcite's runtime tolerates the type mismatch. Match v2's emission shape.
    rel = stripIntegerCastOnInputRef(rel);
    // Demote literal type tags: SqlValidator widens integer literals to BIGINT when comparing
    // against a BIGINT field, producing `30:BIGINT` in explain output where v2's RexBuilder.literal
    // keeps `30` (INTEGER). Strip the redundant widening from comparison/Sarg-bound literals so
    // explain output matches v2.
    rel = demoteIntegerLiterals(rel);
    // Rewrite CASE(IS NOT NULL($x), CAST($x):T, replacement) back to COALESCE($x, replacement).
    // StandardConvertletTable expands COALESCE to CASE during SqlNode→Rel conversion. v2's
    // RexBuilder.makeCall(COALESCE) skips this expansion, so explain output shows COALESCE
    // directly. Match v2's emission shape with a post-conversion fold.
    rel = foldCaseToCoalesce(rel);
    // Apply RexSimplify to Filter conditions to merge `>=, <=, >, <` ranges into Sarg form
    // (matches v2's emission shape for NOT BETWEEN, IN-list, range merges). v2's RelBuilder
    // path runs through RexSimplify normalization; our SqlNode-to-Rel path emits the literal
    // comparisons unsimplified.
    rel = simplifyFilterConditions(rel);
    // When a LogicalAggregate's input chain has IS NOT NULL filters covering ALL its group keys,
    // attach the IGNORE_NULL_BUCKET hint so OpenSearch composite source uses missing_bucket=false.
    // This mirrors v2's PPLHintUtils.addIgnoreNullBucketHintToAggregate path applied when PPL
    // bucket_nullable=false (and for time-span aggregations).
    rel = addIgnoreNullBucketHint(rel);
    return RelOptUtil.propagateRelHints(rel, false);
  }

  /**
   * Add the {@code AGG_ARGS(ignoreNullBucket='true')} hint to LogicalAggregate when its input chain
   * (Project/Filter/Project ... etc.) carries an IS NOT NULL filter for every group-by field
   * reference. Equivalent to v2's PPLHintUtils.addIgnoreNullBucketHintToAggregate which is what
   * makes OpenSearch composite source emit {@code missing_bucket:false} instead of {@code
   * missing_bucket:true,missing_order:"first"}.
   */
  private RelNode addIgnoreNullBucketHint(RelNode rel) {
    org.apache.calcite.rel.RelShuttle shuttle =
        new org.apache.calcite.rel.RelHomogeneousShuttle() {
          @Override
          public RelNode visit(RelNode other) {
            RelNode visited = super.visit(other);
            if (visited instanceof org.apache.calcite.rel.logical.LogicalAggregate agg
                && !agg.getGroupSet().isEmpty()
                && !agg.getHints().stream().anyMatch(h -> h.hintName.equals("AGG_ARGS"))) {
              // Walk through Project layers above the input to map group-key positions to
              // underlying field references.
              java.util.Set<Integer> groupRefIndices = collectGroupRefIndicesAtScan(agg);
              if (groupRefIndices != null
                  && !groupRefIndices.isEmpty()
                  && allCoveredByIsNotNullFilter(agg.getInput(), groupRefIndices)) {
                org.apache.calcite.rel.hint.RelHint hint =
                    org.apache.calcite.rel.hint.RelHint.builder("AGG_ARGS")
                        .hintOption("ignoreNullBucket", "true")
                        .build();
                if (rel.getCluster().getHintStrategies()
                    == org.apache.calcite.rel.hint.HintStrategyTable.EMPTY) {
                  rel.getCluster()
                      .setHintStrategies(
                          org.apache.calcite.rel.hint.HintStrategyTable.builder()
                              .hintStrategy(
                                  "AGG_ARGS",
                                  (h, r) ->
                                      r instanceof org.apache.calcite.rel.logical.LogicalAggregate)
                              .build());
                }
                return agg.withHints(java.util.List.of(hint));
              }
            }
            return visited;
          }
        };
    return rel.accept(shuttle);
  }

  /**
   * Walk through Project layers above the aggregate's input, mapping each group-key position to its
   * underlying RexInputRef index at the bottom of the Project chain. Returns null if any group-key
   * resolves to a non-RexInputRef (computed expression) or the projection chain breaks.
   */
  private static java.util.Set<Integer> collectGroupRefIndicesAtScan(
      org.apache.calcite.rel.logical.LogicalAggregate agg) {
    java.util.Set<Integer> currentIndices = new java.util.HashSet<>(agg.getGroupSet().asList());
    RelNode input = agg.getInput();
    int safety = 8;
    while (input instanceof org.apache.calcite.rel.logical.LogicalProject proj && safety-- > 0) {
      java.util.Set<Integer> next = new java.util.HashSet<>();
      java.util.List<org.apache.calcite.rex.RexNode> projects = proj.getProjects();
      for (int idx : currentIndices) {
        if (idx < 0 || idx >= projects.size()) return null;
        org.apache.calcite.rex.RexNode e = projects.get(idx);
        if (e instanceof org.apache.calcite.rex.RexInputRef ref) {
          next.add(ref.getIndex());
        } else if (e instanceof org.apache.calcite.rex.RexCall call
            && "SPAN".equals(call.getOperator().getName())
            && !call.getOperands().isEmpty()
            && call.getOperands().get(0) instanceof org.apache.calcite.rex.RexInputRef spanArg) {
          // SPAN(field, ...) group key — resolve through to the underlying field ref.
          next.add(spanArg.getIndex());
        } else {
          return null;
        }
      }
      currentIndices = next;
      input = proj.getInput();
    }
    return currentIndices;
  }

  /**
   * True when {@code rel}'s chain contains a {@link org.apache.calcite.rel.logical.LogicalFilter}
   * whose condition is an IS NOT NULL (or AND of IS NOT NULL) over RexInputRefs covering every
   * index in {@code requiredIndices}.
   */
  private static boolean allCoveredByIsNotNullFilter(
      RelNode rel, java.util.Set<Integer> requiredIndices) {
    java.util.Set<Integer> coveredIndices = new java.util.HashSet<>();
    int safety = 8;
    while (rel != null && safety-- > 0) {
      if (rel instanceof org.apache.calcite.rel.logical.LogicalFilter filter) {
        collectIsNotNullRefs(filter.getCondition(), coveredIndices);
        if (coveredIndices.containsAll(requiredIndices)) {
          return true;
        }
        rel = filter.getInput();
      } else if (rel instanceof org.apache.calcite.rel.logical.LogicalProject proj
          && (allRexInputRefs(proj.getProjects()) || allRexInputRefsOrSpan(proj.getProjects()))) {
        // Translate covered indices through the project (forward direction): if input ref X
        // becomes output index i, an outer requirement on X cannot be checked through this
        // project trivially; so just descend if Project is identity passthrough on the relevant
        // indices. Allow SPAN(field, ...) projection too — the underlying field's IS NOT NULL
        // covers the SPAN's null check (SPAN of NULL is NULL).
        rel = proj.getInput();
      } else {
        return false;
      }
    }
    return false;
  }

  private static void collectIsNotNullRefs(
      org.apache.calcite.rex.RexNode cond, java.util.Set<Integer> out) {
    if (cond instanceof org.apache.calcite.rex.RexCall call) {
      if (call.getKind() == org.apache.calcite.sql.SqlKind.AND) {
        for (org.apache.calcite.rex.RexNode op : call.getOperands()) {
          collectIsNotNullRefs(op, out);
        }
      } else if (call.getKind() == org.apache.calcite.sql.SqlKind.IS_NOT_NULL
          && call.getOperands().size() == 1
          && call.getOperands().get(0) instanceof org.apache.calcite.rex.RexInputRef ref) {
        out.add(ref.getIndex());
      }
    }
  }

  private static boolean allRexInputRefs(java.util.List<org.apache.calcite.rex.RexNode> exprs) {
    for (org.apache.calcite.rex.RexNode e : exprs) {
      if (!(e instanceof org.apache.calcite.rex.RexInputRef)) return false;
    }
    return true;
  }

  /** Each expr is either a RexInputRef or a SPAN(RexInputRef, ...) call. */
  private static boolean allRexInputRefsOrSpan(
      java.util.List<org.apache.calcite.rex.RexNode> exprs) {
    for (org.apache.calcite.rex.RexNode e : exprs) {
      if (e instanceof org.apache.calcite.rex.RexInputRef) continue;
      if (e instanceof org.apache.calcite.rex.RexCall call
          && "SPAN".equals(call.getOperator().getName())
          && !call.getOperands().isEmpty()
          && call.getOperands().get(0) instanceof org.apache.calcite.rex.RexInputRef) continue;
      return false;
    }
    return true;
  }

  /**
   * When a {@link org.apache.calcite.rel.logical.LogicalSort} sits above a {@link
   * org.apache.calcite.rel.logical.LogicalProject}, swap them: emit the Project as outermost and
   * the Sort below it. This matches v2's `Project(fields) <- Sort` shape for `... | sort A | fields
   * cols` queries. Sort keys are re-indexed from projected-positions back to input-positions.
   *
   * <p>The swap requires either (a) the Project is passthrough (all RexInputRefs), so collation
   * keys can be re-indexed through the projection, or (b) the Sort has empty collation
   * (FETCH-only), in which case no remapping is needed because Sort references no columns from its
   * child.
   */
  /**
   * Fuse {@code LogicalSort(fetch=N) <- LogicalSort(collation=...)} into a single {@code
   * LogicalSort(collation=..., fetch=N)}. PPL `... | sort F | head N` produces two adjacent
   * LogicalSort rels because the SqlNode pipeline emits Sort and Fetch as distinct operations; v2's
   * RelBuilder fuses them. Match v2's emission shape.
   *
   * <p>Conditions: the OUTER sort must have FETCH but no collation, and the INNER sort must have
   * collation but no FETCH/OFFSET. The OFFSET on the outer carries through, and the fused Sort gets
   * the inner's collation + outer's FETCH/OFFSET.
   */
  private RelNode fuseAdjacentSorts(RelNode rel) {
    org.apache.calcite.rel.RelShuttle shuttle =
        new org.apache.calcite.rel.RelHomogeneousShuttle() {
          @Override
          public RelNode visit(RelNode other) {
            RelNode visited = super.visit(other);
            return tryFuseAdjacentSorts(visited);
          }

          @Override
          public RelNode visit(org.apache.calcite.rel.logical.LogicalSort sort) {
            // RelShuttleImpl's visit(LogicalSort) doesn't route through visit(RelNode), so we
            // need an explicit override. Process children, then attempt the fuse.
            RelNode visited = super.visit(sort);
            return tryFuseAdjacentSorts(visited);
          }
        };
    return rel.accept(shuttle);
  }

  /**
   * Rename pre-aggregate Project's `$fN` auto-names to match v2's RelBuilder naming convention. v2
   * reserves position counters for the parent Aggregate's group-by + agg-call outputs first, then
   * names computed pre-aggregate Project columns starting from `$f<group_count+agg_count>`.
   * SqlValidator names by Project position (`$f<col_index>`).
   *
   * <p>Examples:
   *
   * <ul>
   *   <li>{@code stats count() | eval x = case(...)} — 0 group + 1 agg → first computed col gets
   *       `$f1` (was `$f0`).
   *   <li>{@code stats sum(a), sum(b+1) by g} — 1 group + 2 aggs → first computed gets `$f3`.
   *   <li>{@code stats take(firstname, 2)} — 0 group + 1 agg, Project=[firstname, 2] → computed col
   *       at position 1 named `$f1` (matches; bumpBy=1 + autoCounter=0).
   * </ul>
   *
   * <p>For each computed (non-passthrough) Project col whose current name is `$fN` where N matches
   * its position in the Project's field list, rename to `$f<autoCounter + bumpBy>` where
   * autoCounter increments per computed col and bumpBy = group_count + agg_count.
   */
  private RelNode bumpPreAggregateAutoNames(RelNode rel) {
    org.apache.calcite.rel.RelShuttle shuttle =
        new org.apache.calcite.rel.RelHomogeneousShuttle() {
          @Override
          public RelNode visit(RelNode other) {
            RelNode visited = super.visit(other);
            if (!(visited instanceof org.apache.calcite.rel.logical.LogicalAggregate agg)) {
              return visited;
            }
            if (!(agg.getInput() instanceof org.apache.calcite.rel.logical.LogicalProject proj)) {
              return visited;
            }
            java.util.List<org.apache.calcite.rex.RexNode> projects = proj.getProjects();
            java.util.List<String> names = proj.getRowType().getFieldNames();
            int bumpBy = agg.getGroupSet().cardinality() + agg.getAggCallList().size();
            if (bumpBy == 0) return visited;
            java.util.List<String> newNames = new java.util.ArrayList<>(names);
            boolean changed = false;
            int autoCounter = 0;
            for (int i = 0; i < projects.size(); i++) {
              org.apache.calcite.rex.RexNode rx = projects.get(i);
              if (rx instanceof org.apache.calcite.rex.RexInputRef) continue;
              String currentName = names.get(i);
              if (!currentName.startsWith("$f")) continue;
              int n;
              try {
                n = Integer.parseInt(currentName.substring(2));
              } catch (NumberFormatException e) {
                continue;
              }
              if (n != i) continue;
              String newName = "$f" + (autoCounter + bumpBy);
              if (!newName.equals(currentName)) {
                newNames.set(i, newName);
                changed = true;
              }
              autoCounter++;
            }
            if (!changed) return visited;
            org.apache.calcite.rel.logical.LogicalProject newProj =
                org.apache.calcite.rel.logical.LogicalProject.create(
                    proj.getInput(),
                    proj.getHints(),
                    proj.getProjects(),
                    newNames,
                    proj.getVariablesSet());
            return agg.copy(agg.getTraitSet(), java.util.List.of(newProj));
          }
        };
    return rel.accept(shuttle);
  }

  /**
   * Rename auto-named `$fN` columns in the pre-aggregate Project (the input of a LogicalAggregate)
   * to v2's user-visible `span(field,unit)` alias when the column expression is a SPAN call.
   * SqlValidator assigns `$fN` to unnamed Project entries in GROUP BY context; v2's RelBuilder
   * names them via the alias. Walk through Project layers above the immediate input of the
   * Aggregate to handle the case where visitAggregation's pre-wrap inserts an additional
   * passthrough Project.
   */
  private RelNode renameSpanProjectColumn(RelNode rel) {
    org.apache.calcite.rel.RelShuttle shuttle =
        new org.apache.calcite.rel.RelHomogeneousShuttle() {
          @Override
          public RelNode visit(RelNode other) {
            RelNode visited = super.visit(other);
            if (!(visited instanceof org.apache.calcite.rel.logical.LogicalAggregate agg)) {
              return visited;
            }
            if (!(agg.getInput() instanceof org.apache.calcite.rel.logical.LogicalProject proj)) {
              return visited;
            }
            java.util.List<org.apache.calcite.rex.RexNode> projects = proj.getProjects();
            java.util.List<String> names = proj.getRowType().getFieldNames();
            java.util.List<String> newNames = new java.util.ArrayList<>(names);
            boolean changed = false;
            for (int i = 0; i < projects.size(); i++) {
              org.apache.calcite.rex.RexNode rx = projects.get(i);
              if (!(rx instanceof org.apache.calcite.rex.RexCall call)) continue;
              if (!"SPAN".equals(call.getOperator().getName())) continue;
              if (call.getOperands().size() < 2) continue;
              String currentName = names.get(i);
              if (!currentName.startsWith("$f")) continue;
              org.apache.calcite.rex.RexNode fieldOp = call.getOperands().get(0);
              String fieldName = null;
              if (fieldOp instanceof org.apache.calcite.rex.RexInputRef ref) {
                int idx = ref.getIndex();
                java.util.List<org.apache.calcite.rel.type.RelDataTypeField> inFields =
                    proj.getInput().getRowType().getFieldList();
                if (idx >= 0 && idx < inFields.size()) {
                  fieldName = inFields.get(idx).getName();
                }
              }
              if (fieldName == null) continue;
              org.apache.calcite.rex.RexNode valueOp = call.getOperands().get(1);
              String valueStr;
              if (valueOp instanceof org.apache.calcite.rex.RexLiteral vlit
                  && vlit.getValue() != null) {
                valueStr = vlit.getValue().toString();
                if (valueStr.endsWith(".0")) {
                  valueStr = valueStr.substring(0, valueStr.length() - 2);
                }
              } else {
                continue;
              }
              String unitStr = "";
              if (call.getOperands().size() >= 3) {
                org.apache.calcite.rex.RexNode unitOp = call.getOperands().get(2);
                if (unitOp instanceof org.apache.calcite.rex.RexLiteral ulit
                    && ulit.getValueAs(String.class) != null) {
                  unitStr = ulit.getValueAs(String.class);
                }
              }
              String quotedField = fieldName.startsWith("@") ? "`" + fieldName + "`" : fieldName;
              String alias = "span(" + quotedField + "," + valueStr + unitStr + ")";
              if (!alias.equals(currentName)) {
                newNames.set(i, alias);
                changed = true;
              }
            }
            if (!changed) return visited;
            org.apache.calcite.rel.logical.LogicalProject newProj =
                org.apache.calcite.rel.logical.LogicalProject.create(
                    proj.getInput(),
                    proj.getHints(),
                    proj.getProjects(),
                    newNames,
                    proj.getVariablesSet());
            return agg.copy(agg.getTraitSet(), java.util.List.of(newProj));
          }
        };
    return rel.accept(shuttle);
  }

  /** Fuse `LogicalSort(fetch=N) <- LogicalSort(collation=...)` into a single Sort if possible. */
  private static RelNode tryFuseAdjacentSorts(RelNode visited) {
    if (!(visited instanceof org.apache.calcite.rel.logical.LogicalSort outer)) {
      return visited;
    }
    if (!outer.getCollation().getFieldCollations().isEmpty()) return visited;
    if (outer.fetch == null) return visited;
    org.apache.calcite.rel.RelNode inputRel = outer.getInput();
    if (!(inputRel instanceof org.apache.calcite.rel.logical.LogicalSort inner)) {
      return visited;
    }
    if (inner.getCollation().getFieldCollations().isEmpty()) return visited;
    if (inner.fetch != null || inner.offset != null) return visited;
    return org.apache.calcite.rel.logical.LogicalSort.create(
        inner.getInput(), inner.getCollation(), outer.offset, outer.fetch);
  }

  private RelNode swapSortAndPassthroughProject(RelNode rel) {
    org.apache.calcite.rel.RelShuttle shuttle =
        new org.apache.calcite.rel.RelHomogeneousShuttle() {
          @Override
          public RelNode visit(RelNode other) {
            RelNode visited = super.visit(other);
            if (visited instanceof org.apache.calcite.rel.logical.LogicalSort sort
                && sort.getInput() instanceof org.apache.calcite.rel.logical.LogicalProject proj
                && isPassthroughProject(proj)
                // Swap when Sort has collation keys, OR when Sort has FETCH but the Project's
                // input chain has a Filter (PPL `where ... | head N` case where v2 emits
                // `Project(15) <- Sort(fetch=N) <- Filter`). The bare-FETCH-no-Filter case
                // (URL fetch_size injection in `... | fields col`) keeps Sort outside there
                // for passthrough Project; the non-passthrough Project case (e.g., fieldformat
                // or eval after head) always needs the swap to match v2's shape.
                && shouldSwapSort(sort, proj)
                // Only swap when the Project's input is NOT an Aggregate. PPL `stats ... | sort`
                // emits `Sort <- Project <- Aggregate` where Sort references projected column
                // positions; swapping would re-index Sort to Aggregate output positions which
                // changes the pushdown context order (cosmetic regression in
                // testSortWithAggregationExplain et al.).
                && !(proj.getInput() instanceof org.apache.calcite.rel.core.Aggregate)) {
              org.apache.calcite.rel.RelCollation newCollation;
              if (sort.getCollation().getFieldCollations().isEmpty()) {
                // FETCH-only Sort: no collation keys, no remapping needed.
                newCollation = sort.getCollation();
              } else {
                // Build mapping from projected col index -> input col index (passthrough only).
                java.util.List<org.apache.calcite.rex.RexNode> exprs = proj.getProjects();
                int[] projToInput = new int[exprs.size()];
                for (int i = 0; i < exprs.size(); i++) {
                  projToInput[i] = ((org.apache.calcite.rex.RexInputRef) exprs.get(i)).getIndex();
                }
                // Re-index Sort collation field indices: projected $i -> input $projToInput[i].
                org.apache.calcite.rel.RelCollation oldCol = sort.getCollation();
                java.util.List<org.apache.calcite.rel.RelFieldCollation> newFields =
                    new java.util.ArrayList<>();
                for (org.apache.calcite.rel.RelFieldCollation fc : oldCol.getFieldCollations()) {
                  int oldIdx = fc.getFieldIndex();
                  if (oldIdx < 0 || oldIdx >= projToInput.length) {
                    return visited; // Out of range — bail out, leave plan unchanged.
                  }
                  newFields.add(fc.withFieldIndex(projToInput[oldIdx]));
                }
                newCollation = org.apache.calcite.rel.RelCollations.of(newFields);
              }
              // New Sort over Project's input, new Project over the new Sort.
              org.apache.calcite.rel.logical.LogicalSort newSort =
                  org.apache.calcite.rel.logical.LogicalSort.create(
                      proj.getInput(), newCollation, sort.offset, sort.fetch);
              org.apache.calcite.rel.logical.LogicalProject newProj =
                  org.apache.calcite.rel.logical.LogicalProject.create(
                      newSort,
                      proj.getHints(),
                      proj.getProjects(),
                      proj.getRowType().getFieldNames(),
                      proj.getVariablesSet());
              return newProj;
            }
            return visited;
          }
        };
    return rel.accept(shuttle);
  }

  /** Project where every projection is a bare RexInputRef (no computed expressions). */
  private static boolean isPassthroughProject(org.apache.calcite.rel.logical.LogicalProject proj) {
    for (org.apache.calcite.rex.RexNode e : proj.getProjects()) {
      if (!(e instanceof org.apache.calcite.rex.RexInputRef)) {
        return false;
      }
    }
    return true;
  }

  /** True when any of the Project's expressions contains a RexOver (window function). */
  private static boolean projectContainsWindow(org.apache.calcite.rel.logical.LogicalProject proj) {
    for (org.apache.calcite.rex.RexNode e : proj.getProjects()) {
      if (org.apache.calcite.rex.RexOver.containsOver(e)) {
        return true;
      }
    }
    return false;
  }

  /**
   * True when the Project has at least one passthrough RexInputRef AND at least one computed
   * expression — i.e., the Project augments the input row with new columns rather than purely
   * narrowing to a single computed value. The `head N | eval/fieldformat` pattern emits this.
   */
  private static boolean isAugmentingProject(org.apache.calcite.rel.logical.LogicalProject proj) {
    boolean hasPassthrough = false;
    boolean hasComputed = false;
    for (org.apache.calcite.rex.RexNode e : proj.getProjects()) {
      if (e instanceof org.apache.calcite.rex.RexInputRef) {
        hasPassthrough = true;
      } else {
        hasComputed = true;
      }
      if (hasPassthrough && hasComputed) return true;
    }
    return false;
  }

  /**
   * Simplify Filter conditions via Calcite's RexSimplify so `>=, <=, >, <` range comparisons over
   * the same field merge into Sarg form, and `NOT BETWEEN` simplifies to `SEARCH(field, Sarg[two
   * ranges])`. v2's RelBuilder path runs through RexSimplify normalization.
   */
  private RelNode simplifyFilterConditions(RelNode rel) {
    org.apache.calcite.rel.RelShuttle shuttle =
        new org.apache.calcite.rel.RelHomogeneousShuttle() {
          @Override
          public RelNode visit(RelNode other) {
            RelNode visited = super.visit(other);
            if (visited instanceof org.apache.calcite.rel.logical.LogicalFilter filter) {
              org.apache.calcite.rex.RexNode cond = filter.getCondition();
              org.apache.calcite.rex.RexBuilder rb = filter.getCluster().getRexBuilder();
              org.apache.calcite.rex.RexSimplify simplify =
                  new org.apache.calcite.rex.RexSimplify(
                      rb,
                      org.apache.calcite.plan.RelOptPredicateList.EMPTY,
                      org.apache.calcite.rex.RexUtil.EXECUTOR);
              org.apache.calcite.rex.RexNode simplified = simplify.simplify(cond);
              // Preserve user-written order for top-level OR — RexSimplify reorders OR operands
              // lexicographically. After simplify (which may dedup operands), reorder so the
              // first occurrence in the original cond comes first.
              if (cond.getKind() == org.apache.calcite.sql.SqlKind.OR
                  && simplified.getKind() == org.apache.calcite.sql.SqlKind.OR) {
                simplified = preserveOrOperandOrder(cond, simplified, rb);
              }
              if (!simplified.equals(cond)) {
                return filter.copy(filter.getTraitSet(), filter.getInput(), simplified);
              }
            }
            return visited;
          }
        };
    return rel.accept(shuttle);
  }

  /**
   * After RexSimplify rewrites a top-level OR (e.g. dedups duplicate operands), reorder its
   * operands to follow the first-occurrence order of {@code original}'s top-level OR operands.
   * Operands not found in {@code original} are appended in their post-simplify order.
   */
  private static org.apache.calcite.rex.RexNode preserveOrOperandOrder(
      org.apache.calcite.rex.RexNode original,
      org.apache.calcite.rex.RexNode simplified,
      org.apache.calcite.rex.RexBuilder rb) {
    if (!(original instanceof org.apache.calcite.rex.RexCall origCall)
        || !(simplified instanceof org.apache.calcite.rex.RexCall simpCall)) {
      return simplified;
    }
    java.util.List<org.apache.calcite.rex.RexNode> origOps =
        flattenOr(origCall, new java.util.ArrayList<>());
    java.util.List<org.apache.calcite.rex.RexNode> simpOps = simpCall.getOperands();
    // Deduplicate origOps by .toString() so duplicates from PPL desugar don't double-count.
    java.util.LinkedHashMap<String, org.apache.calcite.rex.RexNode> origByStr =
        new java.util.LinkedHashMap<>();
    for (org.apache.calcite.rex.RexNode o : origOps) {
      origByStr.putIfAbsent(o.toString(), o);
    }
    java.util.Map<String, org.apache.calcite.rex.RexNode> simpByStr = new java.util.HashMap<>();
    for (org.apache.calcite.rex.RexNode o : simpOps) {
      simpByStr.put(o.toString(), o);
    }
    java.util.List<org.apache.calcite.rex.RexNode> reordered = new java.util.ArrayList<>();
    java.util.Set<String> placed = new java.util.HashSet<>();
    for (String origKey : origByStr.keySet()) {
      org.apache.calcite.rex.RexNode match = simpByStr.get(origKey);
      if (match != null && placed.add(origKey)) {
        reordered.add(match);
      }
    }
    // Append any simp operands not in origOps (e.g. created by simplify).
    for (org.apache.calcite.rex.RexNode op : simpOps) {
      if (placed.add(op.toString())) {
        reordered.add(op);
      }
    }
    if (reordered.size() != simpOps.size()) {
      return simplified;
    }
    if (reordered.equals(simpOps)) {
      return simplified;
    }
    return rb.makeCall(simpCall.getOperator(), reordered);
  }

  private static java.util.List<org.apache.calcite.rex.RexNode> flattenOr(
      org.apache.calcite.rex.RexCall call, java.util.List<org.apache.calcite.rex.RexNode> acc) {
    for (org.apache.calcite.rex.RexNode op : call.getOperands()) {
      if (op instanceof org.apache.calcite.rex.RexCall sub
          && sub.getKind() == org.apache.calcite.sql.SqlKind.OR) {
        flattenOr(sub, acc);
      } else {
        acc.add(op);
      }
    }
    return acc;
  }

  /**
   * Demote integer literals from BIGINT/SMALLINT/TINYINT back to INTEGER when their value fits in
   * INT range. SqlValidator widens literals to match the operand type at validation time, so `where
   * age > 30` ends up emitting `30:BIGINT` (assuming age is BIGINT). v2's RexBuilder.literal keeps
   * the natural INTEGER type, and the comparison still works because Calcite's RexBuilder tolerates
   * type-mismatched comparisons. Reverting the widening makes the explain output match v2's `30`
   * shape and produces cleaner Sarg ranges.
   */
  private RelNode demoteIntegerLiterals(RelNode rel) {
    org.apache.calcite.rex.RexShuttle rexShuttle =
        new org.apache.calcite.rex.RexShuttle() {
          @Override
          public org.apache.calcite.rex.RexNode visitLiteral(
              org.apache.calcite.rex.RexLiteral lit) {
            // Sarg literal: rebuild with INTEGER bound type when carrier is BIGINT and all
            // bounds fit in INT range. v2's RexBuilder produces `Sarg[[-10..10)]` (INTEGER
            // bounds); SqlValidator widens bounds to match the field's BIGINT type. Demote
            // so explain output matches v2's emission shape.
            if (lit.getValue() instanceof org.apache.calcite.util.Sarg<?> sarg
                && lit.getType().getSqlTypeName()
                    == org.apache.calcite.sql.type.SqlTypeName.BIGINT) {
              try {
                boolean allFit = true;
                for (Object range : sarg.rangeSet.asRanges()) {
                  com.google.common.collect.Range<?> r = (com.google.common.collect.Range<?>) range;
                  if (r.hasLowerBound()) {
                    Object v = r.lowerEndpoint();
                    if (!(v instanceof java.math.BigDecimal bd2)
                        || bd2.longValueExact() < Integer.MIN_VALUE
                        || bd2.longValueExact() > Integer.MAX_VALUE) {
                      allFit = false;
                      break;
                    }
                  }
                  if (r.hasUpperBound()) {
                    Object v = r.upperEndpoint();
                    if (!(v instanceof java.math.BigDecimal bd2)
                        || bd2.longValueExact() < Integer.MIN_VALUE
                        || bd2.longValueExact() > Integer.MAX_VALUE) {
                      allFit = false;
                      break;
                    }
                  }
                }
                if (allFit) {
                  org.apache.calcite.rex.RexBuilder rb = rel.getCluster().getRexBuilder();
                  org.apache.calcite.rel.type.RelDataType intType =
                      rb.getTypeFactory()
                          .createSqlType(org.apache.calcite.sql.type.SqlTypeName.INTEGER);
                  if (lit.getType().isNullable()) {
                    intType = rb.getTypeFactory().createTypeWithNullability(intType, true);
                  }
                  return rb.makeSearchArgumentLiteral(sarg, intType);
                }
              } catch (Exception ignored) {
                // bounds conversion failed — leave unchanged
              }
            }
            org.apache.calcite.sql.type.SqlTypeName tn = lit.getType().getSqlTypeName();
            if ((tn == org.apache.calcite.sql.type.SqlTypeName.BIGINT
                    || tn == org.apache.calcite.sql.type.SqlTypeName.SMALLINT
                    || tn == org.apache.calcite.sql.type.SqlTypeName.TINYINT)
                && lit.getValue() instanceof java.math.BigDecimal bd
                && bd.scale() <= 0) {
              try {
                long v = bd.longValueExact();
                if (v >= Integer.MIN_VALUE && v <= Integer.MAX_VALUE) {
                  org.apache.calcite.rex.RexBuilder rb = rel.getCluster().getRexBuilder();
                  org.apache.calcite.rel.type.RelDataType intType =
                      rb.getTypeFactory()
                          .createSqlType(org.apache.calcite.sql.type.SqlTypeName.INTEGER);
                  if (lit.getType().isNullable()) {
                    intType = rb.getTypeFactory().createTypeWithNullability(intType, true);
                  }
                  return rb.makeLiteral(bd, intType, false);
                }
              } catch (ArithmeticException ignored) {
                // value didn't fit — leave unchanged
              }
            }
            return super.visitLiteral(lit);
          }

          @Override
          public org.apache.calcite.rex.RexNode visitCall(org.apache.calcite.rex.RexCall call) {
            org.apache.calcite.rex.RexCall visited =
                (org.apache.calcite.rex.RexCall) super.visitCall(call);
            // Demote typed-null to null:NULL inside CASE expressions when ALL non-null result
            // branches are simple literals (integer or string). v2's RexBuilder keeps NULL
            // untyped for these patterns; SqlValidator types the null branch to match. Other
            // CASE patterns that pair typed nulls with column refs / window results (e.g.,
            // eventstats null bucket emission) keep their typed null.
            if (visited.getKind() == org.apache.calcite.sql.SqlKind.CASE) {
              boolean allNonNullAreSimpleLiterals = true;
              boolean hasTypedNull = false;
              java.util.List<org.apache.calcite.rex.RexNode> ops = visited.getOperands();
              for (int i = 0; i < ops.size(); i++) {
                boolean isResultBranch = (i % 2 == 1) || (i == ops.size() - 1 && i % 2 == 0);
                if (!isResultBranch) continue;
                org.apache.calcite.rex.RexNode op = ops.get(i);
                if (op instanceof org.apache.calcite.rex.RexLiteral l) {
                  if (l.isNull()) {
                    if (l.getType().getSqlTypeName()
                        != org.apache.calcite.sql.type.SqlTypeName.NULL) {
                      hasTypedNull = true;
                    }
                  } else {
                    org.apache.calcite.sql.type.SqlTypeName tn2 = l.getType().getSqlTypeName();
                    boolean isInt =
                        tn2 == org.apache.calcite.sql.type.SqlTypeName.INTEGER
                            || tn2 == org.apache.calcite.sql.type.SqlTypeName.BIGINT
                            || tn2 == org.apache.calcite.sql.type.SqlTypeName.SMALLINT
                            || tn2 == org.apache.calcite.sql.type.SqlTypeName.TINYINT;
                    boolean isString =
                        tn2 == org.apache.calcite.sql.type.SqlTypeName.VARCHAR
                            || tn2 == org.apache.calcite.sql.type.SqlTypeName.CHAR;
                    if (!isInt && !isString) {
                      allNonNullAreSimpleLiterals = false;
                    }
                  }
                } else {
                  allNonNullAreSimpleLiterals = false;
                }
              }
              if (hasTypedNull && allNonNullAreSimpleLiterals) {
                org.apache.calcite.rex.RexBuilder rb = rel.getCluster().getRexBuilder();
                java.util.List<org.apache.calcite.rex.RexNode> newOps = new java.util.ArrayList<>();
                for (org.apache.calcite.rex.RexNode op : ops) {
                  if (op instanceof org.apache.calcite.rex.RexLiteral l
                      && l.isNull()
                      && l.getType().getSqlTypeName()
                          != org.apache.calcite.sql.type.SqlTypeName.NULL) {
                    newOps.add(
                        rb.makeNullLiteral(
                            rb.getTypeFactory()
                                .createSqlType(org.apache.calcite.sql.type.SqlTypeName.NULL)));
                  } else {
                    newOps.add(op);
                  }
                }
                return rb.makeCall(visited.getType(), visited.getOperator(), newOps);
              }
            }
            return visited;
          }
        };
    org.apache.calcite.rel.RelShuttle relShuttle =
        new org.apache.calcite.rel.RelHomogeneousShuttle() {
          @Override
          public RelNode visit(RelNode other) {
            RelNode visited = super.visit(other);
            return visited.accept(rexShuttle);
          }
        };
    return rel.accept(relShuttle);
  }

  /**
   * Rewrite {@code CASE(IS NOT NULL($x), CAST($x):T NOT NULL, replacement)} back to {@code
   * COALESCE($x, replacement)}. StandardConvertletTable expands COALESCE to this CASE form during
   * SqlNode→Rel conversion; v2's RexBuilder.makeCall(COALESCE) skips the expansion. This
   * post-conversion fold restores v2's emission shape so explain output matches.
   */
  private RelNode foldCaseToCoalesce(RelNode rel) {
    org.apache.calcite.rex.RexShuttle rexShuttle =
        new org.apache.calcite.rex.RexShuttle() {
          @Override
          public org.apache.calcite.rex.RexNode visitCall(org.apache.calcite.rex.RexCall call) {
            org.apache.calcite.rex.RexCall visited =
                (org.apache.calcite.rex.RexCall) super.visitCall(call);
            if (visited.getKind() == org.apache.calcite.sql.SqlKind.CASE
                && visited.getOperands().size() == 3) {
              org.apache.calcite.rex.RexNode cond = visited.getOperands().get(0);
              org.apache.calcite.rex.RexNode thenBranch = visited.getOperands().get(1);
              org.apache.calcite.rex.RexNode elseBranch = visited.getOperands().get(2);
              // Match `IS NOT NULL(<expr>)` condition with the SAME <expr> (modulo CAST) in
              // the then-branch. The expr can be a column ref, a function call, etc.
              if (cond instanceof org.apache.calcite.rex.RexCall condCall
                  && condCall.getKind() == org.apache.calcite.sql.SqlKind.IS_NOT_NULL
                  && condCall.getOperands().size() == 1) {
                org.apache.calcite.rex.RexNode condInner = condCall.getOperands().get(0);
                // Match `CAST(<expr>):T NOT NULL` then-branch (or bare <expr>).
                org.apache.calcite.rex.RexNode unwrappedThen = thenBranch;
                if (thenBranch instanceof org.apache.calcite.rex.RexCall thenCall
                    && thenCall.getKind() == org.apache.calcite.sql.SqlKind.CAST
                    && thenCall.getOperands().size() == 1) {
                  unwrappedThen = thenCall.getOperands().get(0);
                }
                if (unwrappedThen.equals(condInner)) {
                  org.apache.calcite.rex.RexBuilder rb = rel.getCluster().getRexBuilder();
                  return rb.makeCall(
                      visited.getType(),
                      SqlStdOperatorTable.COALESCE,
                      java.util.List.of(unwrappedThen, elseBranch));
                }
              }
            }
            return visited;
          }
        };
    org.apache.calcite.rel.RelShuttle relShuttle =
        new org.apache.calcite.rel.RelHomogeneousShuttle() {
          @Override
          public RelNode visit(RelNode other) {
            RelNode visited = super.visit(other);
            return visited.accept(rexShuttle);
          }
        };
    return rel.accept(relShuttle);
  }

  /**
   * Decide whether Sort+Project should swap. True when Sort has at least one collation key (the
   * `... | sort A | fields B,C` case), OR when Sort has FETCH and either the Project is
   * non-passthrough (computed expressions from `eval`/`fieldformat`/`rename` that follow `head N` —
   * v2 always emits Project outside Sort here) or the Project's input chain has a Filter or another
   * Sort (the `where ... | head N` or `head M | head N` case where v2 puts the implicit
   * fields-Project outside the user `head` Sort). False for the bare-FETCH-no-Filter passthrough
   * Project case (URL fetch_size injection in `... | fields col`) — v2 keeps Sort outside there.
   */
  private static boolean shouldSwapSort(
      org.apache.calcite.rel.logical.LogicalSort sort,
      org.apache.calcite.rel.logical.LogicalProject proj) {
    // Never swap when Project contains an OVER (window function): the window must see all input
    // rows (or whatever rows the Sort lets through). Pushing Sort below the Project would
    // restrict the window's input to FETCH N rows — wrong for `patterns` and other window-bearing
    // commands.
    if (projectContainsWindow(proj)) {
      return false;
    }
    if (!sort.getCollation().getFieldCollations().isEmpty()) {
      return true;
    }
    // FETCH-only Sort. Swap when Project is "augmenting" — has BOTH passthrough RexInputRefs
    // AND at least one computed expression (pattern from `head N | eval/fieldformat newcol = expr`
    // where the Project preserves source cols + adds a computed col). Don't swap when Project
    // is purely computed with no RexInputRefs (pattern from `eval result = X | fields result |
    // head N`) — v2 emits Sort outside the narrowing Project there. Also swap when Project's
    // input chain contains a Filter or another Sort (signals user-side prior pipe), OR when
    // a passthrough Project narrows the input row (multi-col `fields a, b, c` — v2 emits the
    // narrowing Project outside the URL fetch_size Sort).
    if (sort.fetch == null) return false;
    if (isAugmentingProject(proj)) {
      return true;
    }
    if (isPassthroughProject(proj)
        && proj.getProjects().size() > 1
        && proj.getProjects().size() < proj.getInput().getRowType().getFieldCount()) {
      return true;
    }
    RelNode child = proj.getInput();
    int safety = 4;
    while (child != null && safety-- > 0) {
      if (child instanceof org.apache.calcite.rel.logical.LogicalFilter
          || child instanceof org.apache.calcite.rel.logical.LogicalSort) {
        return true;
      }
      if (child.getInputs().isEmpty()) return false;
      child = child.getInput(0);
    }
    return false;
  }

  /**
   * Remove identity LogicalProjects (1:1 inputRef passthrough, same row type) whose immediate input
   * is a LogicalFilter or LogicalUnion — these come from the implicit SELECT * inserted by
   * Pipeline.wrap() when visitFilter wraps to keep adjacent filter pipes as separate Filter rels,
   * or when visitUnion/visitAppend produce a SqlSelect over the union. The Calcite optimizer
   * doesn't strip them because trimUnusedFields is off.
   */
  private RelNode trimIdentityProjectsBetweenFilters(RelNode rel) {
    org.apache.calcite.rel.RelShuttle shuttle =
        new org.apache.calcite.rel.RelHomogeneousShuttle() {
          @Override
          public RelNode visit(RelNode other) {
            RelNode visited = super.visit(other);
            // Identity passthrough Project — drop when its input is a Filter/Union, OR when
            // it sits BELOW a LogicalSort (the wrap-Project added by `head N` flushing into
            // inner). Sort is the parent here (we visit children-first) so the parent context
            // is implicit; we can drop the identity since the parent will reconnect to the
            // input directly.
            if (visited instanceof org.apache.calcite.rel.logical.LogicalProject proj
                && (proj.getInput() instanceof org.apache.calcite.rel.logical.LogicalFilter
                    || proj.getInput() instanceof org.apache.calcite.rel.logical.LogicalUnion
                    || proj.getInput() instanceof org.apache.calcite.rel.core.TableScan
                    || proj.getInput() instanceof org.apache.calcite.rel.logical.LogicalSort)
                && isIdentityProject(proj)) {
              return proj.getInput();
            }
            // Collapse adjacent passthrough Projects (Project-on-Project) by composing the
            // outer's index mapping through the inner's. The outer must be passthrough/reorder
            // (RexInputRefs only); the inner can carry arbitrary expressions (e.g.
            // WIDTH_BUCKET emitted by visitBin). v2's RelBuilder doesn't emit two adjacent
            // Projects for fields-narrowing-after-sort/bin; we sometimes do because visitBin and
            // visitFields wrap eval-extended state into a subquery. Composing produces a single
            // Project so the AggregateIndexScanRule's `Agg > Project > Scan` pattern can match
            // (auto_date_histogram pushdown depends on this).
            if (visited instanceof org.apache.calcite.rel.logical.LogicalProject outerProj
                && outerProj.getInput()
                    instanceof org.apache.calcite.rel.logical.LogicalProject innerProj
                && allRexInputRefsAtTopLevel(outerProj.getProjects())) {
              java.util.List<org.apache.calcite.rex.RexNode> innerExprs = innerProj.getProjects();
              java.util.List<org.apache.calcite.rex.RexNode> composed = new java.util.ArrayList<>();
              for (org.apache.calcite.rex.RexNode outerExpr : outerProj.getProjects()) {
                int idx = ((org.apache.calcite.rex.RexInputRef) outerExpr).getIndex();
                if (idx < 0 || idx >= innerExprs.size()) {
                  composed = null;
                  break;
                }
                composed.add(innerExprs.get(idx));
              }
              if (composed != null) {
                return org.apache.calcite.rel.logical.LogicalProject.create(
                    innerProj.getInput(),
                    outerProj.getHints(),
                    composed,
                    outerProj.getRowType().getFieldNames(),
                    outerProj.getVariablesSet());
              }
            }
            return visited;
          }
        };
    return rel.accept(shuttle);
  }

  private static boolean allRexInputRefsAtTopLevel(
      java.util.List<org.apache.calcite.rex.RexNode> exprs) {
    for (org.apache.calcite.rex.RexNode e : exprs) {
      if (!(e instanceof org.apache.calcite.rex.RexInputRef)) return false;
    }
    return true;
  }

  /**
   * True when the project simply forwards every input field as-is (no expression rewrite, same row
   * type and same column count). The names also match.
   */
  private static boolean isIdentityProject(org.apache.calcite.rel.logical.LogicalProject proj) {
    java.util.List<org.apache.calcite.rex.RexNode> exprs = proj.getProjects();
    java.util.List<org.apache.calcite.rel.type.RelDataTypeField> inFields =
        proj.getInput().getRowType().getFieldList();
    if (exprs.size() != inFields.size()) return false;
    java.util.List<org.apache.calcite.rel.type.RelDataTypeField> outFields =
        proj.getRowType().getFieldList();
    for (int i = 0; i < exprs.size(); i++) {
      org.apache.calcite.rex.RexNode e = exprs.get(i);
      if (!(e instanceof org.apache.calcite.rex.RexInputRef ref) || ref.getIndex() != i) {
        return false;
      }
      if (!inFields.get(i).getName().equals(outFields.get(i).getName())) return false;
    }
    return true;
  }

  /** Wrap RexSubQuery's inner rel with SUBSEARCH_MAXOUT system limit (IN only). */
  private RelNode applySubsearchLimitForIn(RelNode rel) {
    org.apache.calcite.rex.RexShuttle rexShuttle =
        new org.apache.calcite.rex.RexShuttle() {
          @Override
          public org.apache.calcite.rex.RexNode visitSubQuery(
              org.apache.calcite.rex.RexSubQuery sq) {
            // Skip ScalarSubquery — returns one row anyway.
            if (sq.getKind() == org.apache.calcite.sql.SqlKind.SCALAR_QUERY) {
              return super.visitSubQuery(sq);
            }
            RelNode inner = sq.rel;
            org.opensearch.sql.calcite.utils.SubsearchUtils.SystemLimitInsertionShuttle shuttle =
                new org.opensearch.sql.calcite.utils.SubsearchUtils.SystemLimitInsertionShuttle(
                    context);
            RelNode replaced = inner.accept(shuttle);
            if (!shuttle.isCorrelatedConditionFound()) {
              // Uncorrelated subquery: top-level LogicalSystemLimit. Safe for IN/EXISTS/etc.
              // v2 uses relBuilder.literal(int) which produces INTEGER, not BIGINT — match shape.
              org.apache.calcite.rex.RexBuilder rb = inner.getCluster().getRexBuilder();
              org.apache.calcite.rel.type.RelDataType intType =
                  rb.getTypeFactory()
                      .createSqlType(org.apache.calcite.sql.type.SqlTypeName.INTEGER);
              replaced =
                  org.opensearch.sql.calcite.plan.rel.LogicalSystemLimit.create(
                      org.opensearch.sql.calcite.plan.rel.LogicalSystemLimit.SystemLimitType
                          .SUBSEARCH_MAXOUT,
                      replaced,
                      rb.makeLiteral(context.sysLimit.subsearchLimit(), intType, true));
            } else if (sq.getKind() != org.apache.calcite.sql.SqlKind.IN
                && containsCorrelatedSubquery(inner)) {
              // Nested correlated EXISTS: an outer-correlation reference would flow into
              // OpenSearch's pushdown script compiler, which can't evaluate $cor0. Leave the
              // inner subquery alone — better no-cap than wrong result. Single-level correlated
              // EXISTS goes through the shuttle (v2's split-and-rebuild Filter shape decorrelates
              // cleanly).
              return super.visitSubQuery(sq);
            }
            return sq.clone(replaced);
          }
        };
    org.apache.calcite.rel.RelShuttle relShuttle =
        new org.apache.calcite.rel.RelHomogeneousShuttle() {
          @Override
          public RelNode visit(RelNode other) {
            RelNode visited = super.visit(other);
            return visited.accept(rexShuttle);
          }
        };
    return rel.accept(relShuttle);
  }

  /** True if the rel tree contains any RexSubQuery (correlated or not). */
  private static boolean containsCorrelatedSubquery(RelNode rel) {
    boolean[] found = {false};
    org.apache.calcite.rex.RexShuttle probe =
        new org.apache.calcite.rex.RexShuttle() {
          @Override
          public org.apache.calcite.rex.RexNode visitSubQuery(
              org.apache.calcite.rex.RexSubQuery sq) {
            found[0] = true;
            return sq;
          }
        };
    org.apache.calcite.rel.RelShuttle walker =
        new org.apache.calcite.rel.RelHomogeneousShuttle() {
          @Override
          public RelNode visit(RelNode other) {
            if (found[0]) return other;
            other.accept(probe);
            return found[0] ? other : super.visit(other);
          }
        };
    rel.accept(walker);
    return found[0];
  }

  /**
   * Replace a join-right LogicalSort(fetch=joinSubsearchLimit, no collation) with a
   * LogicalSystemLimit(JOIN_SUBSEARCH_MAXOUT) so the plan-shape matches v2's
   * addSysLimitForJoinSubsearch emission. The right side of a LogicalJoin is the subsearch input;
   * the SqlNode path emits FETCH there which becomes plain LogicalSort.
   */
  private RelNode relabelJoinSubsearchSort(RelNode rel, int joinSubsearchLimit) {
    org.apache.calcite.rel.RelShuttle shuttle =
        new org.apache.calcite.rel.RelHomogeneousShuttle() {
          @Override
          public RelNode visit(RelNode other) {
            RelNode visited = super.visit(other);
            if (visited instanceof org.apache.calcite.rel.logical.LogicalJoin join) {
              RelNode right = join.getRight();
              if (right instanceof org.apache.calcite.rel.logical.LogicalSort sort
                  && sort.getCollation().getFieldCollations().isEmpty()
                  && sort.offset == null
                  && sort.fetch instanceof org.apache.calcite.rex.RexLiteral lit
                  && lit.getValueAs(Integer.class) != null
                  && lit.getValueAs(Integer.class) == joinSubsearchLimit) {
                RelNode replacement =
                    org.opensearch.sql.calcite.plan.rel.LogicalSystemLimit.create(
                        org.opensearch.sql.calcite.plan.rel.LogicalSystemLimit.SystemLimitType
                            .JOIN_SUBSEARCH_MAXOUT,
                        sort.getInput(),
                        sort.fetch);
                return join.copy(
                    join.getTraitSet(),
                    join.getCondition(),
                    join.getLeft(),
                    replacement,
                    join.getJoinType(),
                    join.isSemiJoinDone());
              }
            }
            return visited;
          }
        };
    return rel.accept(shuttle);
  }

  /**
   * Collapse the SPAN-pre-wrap pattern emitted by visitAggregation. PplToSqlNode wraps the source
   * with `SELECT *, SPAN(...) AS alias FROM scan` to materialize the SPAN as a real column. Outer
   * SQL has `SELECT count(), span_alias FROM (...) WHERE IS NOT NULL($field) GROUP BY span_alias`.
   * After conversion this becomes:
   *
   * <pre>
   *   Project(span_alias=$N) &lt;- Filter(IS NOT NULL($M)) &lt;- Project(*, SPAN($M) AS span_alias) &lt;- Scan
   * </pre>
   *
   * <p>v2's RelBuilder emits just `Project(span_alias=SPAN($M)) &lt;- Filter(IS NOT NULL($M)) &lt;-
   * Scan` — no wide passthrough Project. Collapse to match.
   *
   * <p>Strictly limited to: outer Project has exactly 1 expression that is a RexInputRef to the
   * trailing computed col; the trailing computed col is a SPAN call; the inner Project is
   * passthrough[0..N-1] + trailing SPAN. This narrow detection avoids regressing dedup/rare/top
   * tests that build similar wide-Project patterns intentionally.
   */
  private RelNode collapseSpanWidePassthroughProject(RelNode rel) {
    org.apache.calcite.rel.RelShuttle shuttle =
        new org.apache.calcite.rel.RelHomogeneousShuttle() {
          @Override
          public RelNode visit(RelNode other) {
            RelNode visited = super.visit(other);
            if (!(visited instanceof org.apache.calcite.rel.logical.LogicalProject outerProj)) {
              return visited;
            }
            // Outer Project must have all passthrough RexInputRef projections.
            if (outerProj.getProjects().isEmpty()) return visited;
            for (org.apache.calcite.rex.RexNode e : outerProj.getProjects()) {
              if (!(e instanceof org.apache.calcite.rex.RexInputRef)) return visited;
            }
            if (!(outerProj.getInput()
                instanceof org.apache.calcite.rel.logical.LogicalFilter filter)) {
              return visited;
            }
            if (!(filter.getInput()
                instanceof org.apache.calcite.rel.logical.LogicalProject innerProj)) {
              return visited;
            }
            java.util.List<org.apache.calcite.rex.RexNode> innerExprs = innerProj.getProjects();
            int innerInputArity = innerProj.getInput().getRowType().getFieldCount();
            if (innerExprs.size() != innerInputArity + 1) return visited;
            // First N inner projects must be passthrough.
            for (int i = 0; i < innerInputArity; i++) {
              org.apache.calcite.rex.RexNode e = innerExprs.get(i);
              if (!(e instanceof org.apache.calcite.rex.RexInputRef ref) || ref.getIndex() != i) {
                return visited;
              }
            }
            org.apache.calcite.rex.RexNode trailing = innerExprs.get(innerInputArity);
            // Trailing must be a SPAN call.
            if (!(trailing instanceof org.apache.calcite.rex.RexCall trailCall)) return visited;
            if (!"SPAN".equals(trailCall.getOperator().getName())) return visited;
            // Filter condition refers to inner Project cols. Inline references to trailing
            // through to the SPAN expression; rewrite Filter and Project to reference inner's
            // input directly. Other refs (passthrough cols 0..N-1) stay valid since they map
            // identically to the inner's input.
            org.apache.calcite.rex.RexShuttle inlineShuttle =
                new org.apache.calcite.rex.RexShuttle() {
                  @Override
                  public org.apache.calcite.rex.RexNode visitInputRef(
                      org.apache.calcite.rex.RexInputRef ref) {
                    if (ref.getIndex() == innerInputArity) {
                      return trailing;
                    }
                    return ref;
                  }
                };
            org.apache.calcite.rex.RexNode newCondition =
                filter.getCondition().accept(inlineShuttle);
            org.apache.calcite.rel.logical.LogicalFilter newFilter =
                org.apache.calcite.rel.logical.LogicalFilter.create(
                    innerProj.getInput(), newCondition);
            // Map outer Project expressions: each RexInputRef pointing at the trailing col gets
            // inlined to SPAN; refs to passthrough cols stay as-is (their indices match the
            // inner's input).
            java.util.List<org.apache.calcite.rex.RexNode> newProjects =
                new java.util.ArrayList<>();
            for (org.apache.calcite.rex.RexNode e : outerProj.getProjects()) {
              org.apache.calcite.rex.RexInputRef ref = (org.apache.calcite.rex.RexInputRef) e;
              if (ref.getIndex() == innerInputArity) {
                newProjects.add(trailing);
              } else {
                newProjects.add(ref);
              }
            }
            return org.apache.calcite.rel.logical.LogicalProject.create(
                newFilter,
                outerProj.getHints(),
                newProjects,
                outerProj.getRowType().getFieldNames(),
                outerProj.getVariablesSet());
          }
        };
    return rel.accept(shuttle);
  }

  /**
   * Strip {@code CAST($N):INTEGER} wrappers around plain {@link org.apache.calcite.rex.RexInputRef}
   * operands of comparison operators. SqlValidator inserts these casts when comparing fields of
   * different numeric types (e.g., BIGINT field {@code <>} INTEGER literal). v2's
   * RexBuilder.makeCall skips the cast since Calcite's runtime tolerates numeric type mismatch in
   * comparisons. Strip to match v2's emission shape.
   */
  private RelNode stripIntegerCastOnInputRef(RelNode rel) {
    org.apache.calcite.rex.RexShuttle rexShuttle =
        new org.apache.calcite.rex.RexShuttle() {
          @Override
          public org.apache.calcite.rex.RexNode visitCall(org.apache.calcite.rex.RexCall call) {
            org.apache.calcite.rex.RexCall visited =
                (org.apache.calcite.rex.RexCall) super.visitCall(call);
            org.apache.calcite.sql.SqlKind kind = visited.getKind();
            if (kind != org.apache.calcite.sql.SqlKind.EQUALS
                && kind != org.apache.calcite.sql.SqlKind.NOT_EQUALS
                && kind != org.apache.calcite.sql.SqlKind.LESS_THAN
                && kind != org.apache.calcite.sql.SqlKind.LESS_THAN_OR_EQUAL
                && kind != org.apache.calcite.sql.SqlKind.GREATER_THAN
                && kind != org.apache.calcite.sql.SqlKind.GREATER_THAN_OR_EQUAL) {
              return visited;
            }
            java.util.List<org.apache.calcite.rex.RexNode> ops = visited.getOperands();
            java.util.List<org.apache.calcite.rex.RexNode> newOps = new java.util.ArrayList<>();
            boolean changed = false;
            for (org.apache.calcite.rex.RexNode op : ops) {
              if (op instanceof org.apache.calcite.rex.RexCall castCall
                  && castCall.getKind() == org.apache.calcite.sql.SqlKind.CAST
                  && (castCall.getType().getSqlTypeName()
                          == org.apache.calcite.sql.type.SqlTypeName.INTEGER
                      || castCall.getType().getSqlTypeName()
                          == org.apache.calcite.sql.type.SqlTypeName.BIGINT
                      || castCall.getType().getSqlTypeName()
                          == org.apache.calcite.sql.type.SqlTypeName.SMALLINT
                      || castCall.getType().getSqlTypeName()
                          == org.apache.calcite.sql.type.SqlTypeName.TINYINT)
                  && castCall.getOperands().size() == 1
                  && castCall.getOperands().get(0) instanceof org.apache.calcite.rex.RexInputRef) {
                newOps.add(castCall.getOperands().get(0));
                changed = true;
              } else {
                newOps.add(op);
              }
            }
            if (!changed) return visited;
            return rel.getCluster()
                .getRexBuilder()
                .makeCall(visited.getType(), visited.getOperator(), newOps);
          }
        };
    org.apache.calcite.rel.RelShuttle relShuttle =
        new org.apache.calcite.rel.RelHomogeneousShuttle() {
          @Override
          public RelNode visit(RelNode other) {
            RelNode visited = super.visit(other);
            return visited.accept(rexShuttle);
          }
        };
    return rel.accept(relShuttle);
  }

  /**
   * Rewrite UDT-wrapping NULL pads to typed NULL literals. {@code padSelectWithReference} emits
   * {@code TIMESTAMP(CAST(NULL AS VARCHAR))} for absent EXPR_TIMESTAMP/DATE/TIME/IP columns in
   * UNION-ALL pads to preserve the UDT through the union's least-restrictive type computation. v2's
   * RexBuilder emits a typed-null literal directly. This shuttle detects the wrapper RexCall
   * (TIMESTAMP/DATE/TIME/IP UDF whose sole operand is a NULL or CAST-NULL literal) and replaces it
   * with {@code makeNullLiteral} of the call's return type, matching v2's emission shape.
   */
  private RelNode rewriteUdtWrappedNullsToTypedNulls(RelNode rel) {
    org.apache.calcite.rex.RexShuttle rexShuttle =
        new org.apache.calcite.rex.RexShuttle() {
          @Override
          public org.apache.calcite.rex.RexNode visitCall(org.apache.calcite.rex.RexCall call) {
            org.apache.calcite.rex.RexCall visited =
                (org.apache.calcite.rex.RexCall) super.visitCall(call);
            String opName = visited.getOperator().getName();
            if (!("TIMESTAMP".equals(opName)
                || "DATE".equals(opName)
                || "TIME".equals(opName)
                || "IP".equals(opName))) {
              return visited;
            }
            if (visited.getOperands().size() != 1) return visited;
            // Return type must be a UDT; if it's plain VARCHAR, we'd lose information.
            if (!(visited.getType()
                instanceof org.opensearch.sql.calcite.type.AbstractExprRelDataType<?>)) {
              return visited;
            }
            org.apache.calcite.rex.RexNode operand = visited.getOperands().get(0);
            // Unwrap CAST(NULL AS T) — Calcite often expresses NULL with explicit type cast.
            if (operand instanceof org.apache.calcite.rex.RexCall castCall
                && castCall.getKind() == org.apache.calcite.sql.SqlKind.CAST
                && castCall.getOperands().size() == 1) {
              operand = castCall.getOperands().get(0);
            }
            if (!(operand instanceof org.apache.calcite.rex.RexLiteral lit)) return visited;
            if (lit.getValue() != null) return visited; // not a NULL literal
            return rel.getCluster().getRexBuilder().makeNullLiteral(visited.getType());
          }
        };
    org.apache.calcite.rel.RelShuttle relShuttle =
        new org.apache.calcite.rel.RelHomogeneousShuttle() {
          @Override
          public RelNode visit(RelNode other) {
            RelNode visited = super.visit(other);
            return visited.accept(rexShuttle);
          }
        };
    return rel.accept(relShuttle);
  }

  /**
   * Rewrite the LENGTH(REPLACE(arg, ' ', '')) = 0 pattern (emitted by visitFunc for isblank) into
   * v2's IS_EMPTY(TRIM(BOTH ' ', arg)) shape. The SqlNode validator rejects the TRIM keyword syntax
   * when called via SqlBasicCall (signature mismatch on (SYMBOL, CHAR, CHAR)), so the desugar
   * happens at the SqlNode level and is rewritten here at the Rex level where TRIM accepts the
   * (FLAG, chars, expr) operand shape directly.
   */
  private RelNode rewriteIsBlankToTrimEmpty(RelNode rel) {
    org.apache.calcite.rex.RexShuttle rexShuttle =
        new org.apache.calcite.rex.RexShuttle() {
          @Override
          public org.apache.calcite.rex.RexNode visitCall(org.apache.calcite.rex.RexCall call) {
            org.apache.calcite.rex.RexCall visited =
                (org.apache.calcite.rex.RexCall) super.visitCall(call);
            // Match: =(LENGTH(REPLACE($x, ' ', '')), 0)
            if (visited.getKind() != org.apache.calcite.sql.SqlKind.EQUALS
                || visited.getOperands().size() != 2) {
              return visited;
            }
            org.apache.calcite.rex.RexNode lhs = visited.getOperands().get(0);
            org.apache.calcite.rex.RexNode rhs = visited.getOperands().get(1);
            if (!(lhs instanceof org.apache.calcite.rex.RexCall lenCall)) return visited;
            org.apache.calcite.sql.SqlOperator lenOp = lenCall.getOperator();
            if (lenOp != org.apache.calcite.sql.fun.SqlLibraryOperators.LENGTH
                && lenOp != SqlStdOperatorTable.CHAR_LENGTH
                && lenOp != SqlStdOperatorTable.CHARACTER_LENGTH) {
              return visited;
            }
            if (lenCall.getOperands().size() != 1) return visited;
            org.apache.calcite.rex.RexNode lenArg = lenCall.getOperands().get(0);
            if (!(lenArg instanceof org.apache.calcite.rex.RexCall replCall)) return visited;
            if (replCall.getOperator() != SqlStdOperatorTable.REPLACE) return visited;
            if (replCall.getOperands().size() != 3) return visited;
            org.apache.calcite.rex.RexNode replArg = replCall.getOperands().get(0);
            org.apache.calcite.rex.RexNode space = replCall.getOperands().get(1);
            org.apache.calcite.rex.RexNode empty = replCall.getOperands().get(2);
            if (!(space instanceof org.apache.calcite.rex.RexLiteral spaceLit)) return visited;
            if (!(empty instanceof org.apache.calcite.rex.RexLiteral emptyLit)) return visited;
            if (!(rhs instanceof org.apache.calcite.rex.RexLiteral zeroLit)) return visited;
            String spaceStr = spaceLit.getValueAs(String.class);
            String emptyStr = emptyLit.getValueAs(String.class);
            Number zero = zeroLit.getValueAs(Number.class);
            if (!" ".equals(spaceStr)
                || !"".equals(emptyStr)
                || zero == null
                || zero.intValue() != 0) {
              return visited;
            }
            // Rewrite to IS_EMPTY(TRIM(BOTH, ' ', $x)).
            org.apache.calcite.rex.RexBuilder rb = rel.getCluster().getRexBuilder();
            org.apache.calcite.rex.RexNode trimmed =
                rb.makeCall(
                    SqlStdOperatorTable.TRIM,
                    rb.makeFlag(org.apache.calcite.sql.fun.SqlTrimFunction.Flag.BOTH),
                    rb.makeLiteral(" "),
                    replArg);
            return rb.makeCall(SqlStdOperatorTable.IS_EMPTY, trimmed);
          }
        };
    org.apache.calcite.rel.RelShuttle relShuttle =
        new org.apache.calcite.rel.RelHomogeneousShuttle() {
          @Override
          public RelNode visit(RelNode other) {
            RelNode visited = super.visit(other);
            return visited.accept(rexShuttle);
          }
        };
    return rel.accept(relShuttle);
  }

  /**
   * Rewrite RexCalls on {@link PplToSqlNode#PERMISSIVE_IS_EMPTY} to the standard {@link
   * SqlStdOperatorTable#IS_EMPTY} postfix operator. The PERMISSIVE_IS_EMPTY function is used at the
   * SqlValidator stage to bypass the collection-only operand-type check; this shuttle converts the
   * resulting RexCall to the standard operator so explain output matches v2.
   */
  private RelNode rewritePermissiveIsEmpty(RelNode rel) {
    org.apache.calcite.rex.RexShuttle rexShuttle =
        new org.apache.calcite.rex.RexShuttle() {
          @Override
          public org.apache.calcite.rex.RexNode visitCall(org.apache.calcite.rex.RexCall call) {
            org.apache.calcite.rex.RexCall visited =
                (org.apache.calcite.rex.RexCall) super.visitCall(call);
            if (visited.getOperator() == PplToSqlNode.PERMISSIVE_IS_EMPTY) {
              return rel.getCluster()
                  .getRexBuilder()
                  .makeCall(SqlStdOperatorTable.IS_EMPTY, visited.getOperands());
            }
            return visited;
          }
        };
    org.apache.calcite.rel.RelShuttle relShuttle =
        new org.apache.calcite.rel.RelHomogeneousShuttle() {
          @Override
          public RelNode visit(RelNode other) {
            RelNode visited = super.visit(other);
            return visited.accept(rexShuttle);
          }
        };
    return rel.accept(relShuttle);
  }

  /**
   * Map Calcite validator exceptions to PPL's existing IllegalArgumentException("Field [X] not
   * found.") shape so callers (and tests) that pattern-match on the v2 message keep working. Only
   * rewrites the column-not-found case; everything else is rethrown unchanged.
   */
  private static RuntimeException translateValidationError(RuntimeException original) {
    // Calcite's validator wraps Exception causes in CalciteContextException. If a downstream
    // ErrorReport (e.g. IndexNotFoundException → ErrorReport.code(INDEX_NOT_FOUND) from the
    // OpenSearch storage layer) bubbles up, unwrap and re-throw it so its code/stage survive
    // the StageErrorHandler.executeStage rewrap (otherwise a fresh ErrorReport is built with
    // default code=UNKNOWN).
    Throwable t = original;
    while (t != null) {
      if (t instanceof org.opensearch.sql.common.error.ErrorReport er) {
        return er;
      }
      t = t.getCause();
    }
    String msg = original.getMessage();
    if (msg == null) {
      return original;
    }
    java.util.regex.Matcher m =
        java.util.regex.Pattern.compile("Column '([^']+)' not found").matcher(msg);
    if (m.find()) {
      String col = m.group(1);
      // Wrap in ErrorReport carrying ErrorCode.FIELD_NOT_FOUND so the response surfaces a
      // specific code (CalciteErrorReportStageIT.testFieldNotFoundIncludesErrorCode asserts the
      // code is not "UNKNOWN"). Mirrors v2's QualifiedNameResolver.getNotFoundException.
      return org.opensearch.sql.common.error.ErrorReport.wrap(
              new IllegalArgumentException("Field [" + col + "] not found.", original))
          .code(org.opensearch.sql.common.error.ErrorCode.FIELD_NOT_FOUND)
          .build();
    }
    return original;
  }

  /**
   * Walk the rel tree and replace each {@link
   * org.opensearch.sql.calcite.plan.AliasFieldsWrappable}-aware scan with a Project that surfaces
   * alias-field columns. Mirrors v2's CalciteRelNodeVisitor.visitRelation post-scan wrap. The scan
   * layer's getAliasMapping() returns aliasName→originalName entries.
   */
  private RelNode wrapAliasFieldsBelow(RelNode rel) {
    if (rel instanceof org.opensearch.sql.calcite.plan.AliasFieldsWrappable wrappable) {
      java.util.Map<String, String> mapping = wrappable.getAliasMapping();
      if (mapping == null || mapping.isEmpty()) return rel;
      // Build a Project: SELECT *, original AS alias FROM scan — but the scan's row type
      // already includes the alias columns (since OpenSearchTypeFactory.convertSchema now
      // exposes them). We need to OVERWRITE those alias columns with original→alias references.
      java.util.List<org.apache.calcite.rex.RexNode> projects = new java.util.ArrayList<>();
      java.util.List<String> names = new java.util.ArrayList<>();
      org.apache.calcite.rex.RexBuilder rb = rel.getCluster().getRexBuilder();
      java.util.List<org.apache.calcite.rel.type.RelDataTypeField> fields =
          rel.getRowType().getFieldList();
      for (org.apache.calcite.rel.type.RelDataTypeField f : fields) {
        String name = f.getName();
        String original = mapping.get(name);
        if (original != null) {
          // Find original field's index in the rowtype.
          int origIdx = -1;
          for (int i = 0; i < fields.size(); i++) {
            if (fields.get(i).getName().equals(original)) {
              origIdx = i;
              break;
            }
          }
          if (origIdx >= 0) {
            projects.add(rb.makeInputRef(rel, origIdx));
          } else {
            projects.add(rb.makeInputRef(rel, f.getIndex()));
          }
        } else {
          projects.add(rb.makeInputRef(rel, f.getIndex()));
        }
        names.add(name);
      }
      return org.apache.calcite.rel.logical.LogicalProject.create(
          rel, java.util.Collections.emptyList(), projects, names, java.util.Set.of());
    }
    java.util.List<RelNode> newInputs = new java.util.ArrayList<>(rel.getInputs().size());
    boolean changed = false;
    for (RelNode child : rel.getInputs()) {
      RelNode wrapped = wrapAliasFieldsBelow(child);
      newInputs.add(wrapped);
      if (wrapped != child) changed = true;
    }
    return changed ? rel.copy(rel.getTraitSet(), newInputs) : rel;
  }

  /**
   * Drop `__stream_seq__` / `__seg_id__` from the top-level row type. These are internal helpers
   * used by the streamstats translation; user-facing output should never expose them.
   */
  private RelNode stripSyntheticSeqColumns(RelNode rel) {
    java.util.List<org.apache.calcite.rel.type.RelDataTypeField> fields =
        rel.getRowType().getFieldList();
    java.util.List<org.apache.calcite.rex.RexNode> projects = new java.util.ArrayList<>();
    java.util.List<String> names = new java.util.ArrayList<>();
    org.apache.calcite.rex.RexBuilder rb = rel.getCluster().getRexBuilder();
    boolean anyHidden = false;
    for (org.apache.calcite.rel.type.RelDataTypeField f : fields) {
      String n = f.getName();
      if ("__stream_seq__".equals(n)
          || "__seg_id__".equals(n)
          || "_rn_main_".equals(n)
          || "_rn_sub_".equals(n)
          || "_dummy_".equals(n)
          || "__join_max_rn__".equals(n)
          || "__mvexpand_rn__".equals(n)) {
        anyHidden = true;
        continue;
      }
      projects.add(rb.makeInputRef(rel, f.getIndex()));
      names.add(n);
    }
    if (!anyHidden) return rel;
    // Avoid stripping all columns — Calcite's RelFieldTrimmer can't handle an empty rowType
    // (Mappings.create asserts source >= target). Keep the helper column when no user-facing
    // columns remain (e.g., empty-mapping index source); the response formatter hides it.
    if (projects.isEmpty()) return rel;
    return org.apache.calcite.rel.logical.LogicalProject.create(
        rel, java.util.Collections.emptyList(), projects, names, java.util.Set.of());
  }
}
