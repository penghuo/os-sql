/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.sqlnode;

import java.util.Properties;
import org.apache.calcite.adapter.java.JavaTypeFactory;
import org.apache.calcite.config.CalciteConnectionConfigImpl;
import org.apache.calcite.config.CalciteConnectionProperty;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.ViewExpanders;
import org.apache.calcite.prepare.CalciteCatalogReader;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOperatorTable;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.util.ChainedSqlOperatorTable;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.SqlValidatorUtil;
import org.apache.calcite.sql2rel.SqlToRelConverter;
import org.apache.calcite.sql2rel.StandardConvertletTable;
import org.apache.calcite.tools.FrameworkConfig;
import org.apache.calcite.tools.Frameworks;

/**
 * POC: drives a programmatically built PPL {@link SqlNode} through Calcite's standard pipeline:
 *
 * <ol>
 *   <li>{@link SqlValidator#validate(SqlNode)} — identifier/function resolution and type coercion;
 *   <li>{@link SqlToRelConverter#convertQuery(SqlNode, boolean, boolean)} — emit a {@link RelNode}.
 * </ol>
 *
 * <p>This intentionally mirrors what {@code OpenSearchPrepareImpl} does for raw SQL today; we just
 * skip the parser since PPL produces SqlNode directly.
 */
public final class SqlNodePlanner {

  private final FrameworkConfig config;

  public SqlNodePlanner(FrameworkConfig config) {
    this.config = config;
  }

  /** Validate then convert {@code sqlNode} into a {@link RelNode}. */
  public RelNode plan(SqlNode sqlNode) {
    return Frameworks.withPrepare(
        config,
        (cluster, relOptSchema, rootSchema, statement) -> convert(cluster, rootSchema, sqlNode));
  }

  private RelNode convert(RelOptCluster cluster, SchemaPlus rootSchema, SqlNode sqlNode) {
    JavaTypeFactory typeFactory = new JavaTypeFactoryImpl(RelDataTypeSystem.DEFAULT);

    Properties props = new Properties();
    props.setProperty(CalciteConnectionProperty.CASE_SENSITIVE.camelName(), "false");
    CalciteConnectionConfigImpl ccc = new CalciteConnectionConfigImpl(props);

    SchemaPlus defaultSchema =
        config.getDefaultSchema() != null ? config.getDefaultSchema() : rootSchema;
    CalciteSchema schema = CalciteSchema.from(defaultSchema);
    CalciteCatalogReader catalogReader =
        new CalciteCatalogReader(schema.root(), schema.path(null), typeFactory, ccc);

    SqlOperatorTable operatorTable =
        config.getOperatorTable() != null
            ? new ChainedSqlOperatorTable(
                java.util.Arrays.asList(config.getOperatorTable(), SqlStdOperatorTable.instance()))
            : SqlStdOperatorTable.instance();

    SqlValidator validator =
        SqlValidatorUtil.newValidator(
            operatorTable,
            catalogReader,
            typeFactory,
            SqlValidator.Config.DEFAULT.withTypeCoercionEnabled(true));

    SqlNode validated = validator.validate(sqlNode);

    RexBuilder rexBuilder = new RexBuilder(typeFactory);
    RelOptCluster relCluster = RelOptCluster.create(cluster.getPlanner(), rexBuilder);
    SqlToRelConverter converter =
        new SqlToRelConverter(
            ViewExpanders.simpleContext(relCluster),
            validator,
            catalogReader,
            relCluster,
            StandardConvertletTable.INSTANCE,
            SqlToRelConverter.config().withTrimUnusedFields(false).withExpand(false));

    RelRoot root = converter.convertQuery(validated, /* needsValidation */ false, /* top */ true);
    RelNode rel = root.rel;
    // The converter strips a top-level ORDER BY into RelRoot.collation; PPL preserves sort
    // through the pipe chain, so re-attach an explicit LogicalSort if needed.
    if (!root.collation.getFieldCollations().isEmpty()
        && !(rel instanceof org.apache.calcite.rel.core.Sort)) {
      rel = org.apache.calcite.rel.logical.LogicalSort.create(rel, root.collation, null, null);
    }
    return RelOptUtil.propagateRelHints(rel, false);
  }
}
