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

  private SqlOperatorTable buildOperatorTable() {
    java.util.List<SqlOperatorTable> tables = new java.util.ArrayList<>();
    if (config.getOperatorTable() != null) {
      tables.add(config.getOperatorTable());
    }
    tables.add(org.opensearch.sql.expression.function.PPLBuiltinOperators.instance());
    tables.add(SqlStdOperatorTable.instance());
    // SqlLibraryOperators contains operators like SAFE_DIVIDE that PPLFuncImpTable uses for
    // PPL-specific arithmetic semantics (e.g. division by zero returns NULL).
    tables.add(
        org.apache.calcite.sql.fun.SqlLibraryOperatorTableFactory.INSTANCE.getOperatorTable(
            org.apache.calcite.sql.fun.SqlLibrary.STANDARD,
            org.apache.calcite.sql.fun.SqlLibrary.BIG_QUERY));
    return tables.size() == 1 ? tables.get(0) : new ChainedSqlOperatorTable(tables);
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
        new SqlToRelConverter(
            ViewExpanders.simpleContext(relCluster),
            validator,
            catalogReader,
            relCluster,
            StandardConvertletTable.INSTANCE,
            SqlToRelConverter.config().withTrimUnusedFields(false).withExpand(false));

    RelRoot root;
    try {
      root = converter.convertQuery(validated, /* needsValidation */ false, /* top */ true);
    } catch (RuntimeException e) {
      throw translateValidationError(e);
    }
    RelNode rel = root.rel;
    // The converter strips a top-level ORDER BY into RelRoot.collation; PPL preserves sort
    // through the pipe chain, so re-attach an explicit LogicalSort if needed.
    if (!root.collation.getFieldCollations().isEmpty()
        && !(rel instanceof org.apache.calcite.rel.core.Sort)) {
      rel = org.apache.calcite.rel.logical.LogicalSort.create(rel, root.collation, null, null);
    }
    return RelOptUtil.propagateRelHints(rel, false);
  }

  /**
   * Map Calcite validator exceptions to PPL's existing IllegalArgumentException("Field [X] not
   * found.") shape so callers (and tests) that pattern-match on the v2 message keep working. Only
   * rewrites the column-not-found case; everything else is rethrown unchanged.
   */
  private static RuntimeException translateValidationError(RuntimeException original) {
    String msg = original.getMessage();
    if (msg == null) {
      return original;
    }
    java.util.regex.Matcher m =
        java.util.regex.Pattern.compile("Column '([^']+)' not found").matcher(msg);
    if (m.find()) {
      String col = m.group(1);
      return new IllegalArgumentException("Field [" + col + "] not found.", original);
    }
    return original;
  }
}
