/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.sqlnode;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.ViewExpanders;
import org.apache.calcite.prepare.Prepare.CatalogReader;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOperatorTable;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.validate.SqlConformanceEnum;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.SqlValidatorUtil;
import org.apache.calcite.sql2rel.SqlToRelConverter;
import org.apache.calcite.sql2rel.StandardConvertletTable;
import org.opensearch.sql.ast.tree.UnresolvedPlan;
import org.opensearch.sql.calcite.CalcitePlanContext;
import org.opensearch.sql.calcite.OpenSearchSchema;
import org.opensearch.sql.calcite.utils.CalciteToolsHelper.OpenSearchSqlToRelConverter;
import org.opensearch.sql.calcite.utils.OpenSearchTypeFactory;

/**
 * Spike: drive a PPL {@link UnresolvedPlan} through {@link PPLToSqlNodeVisitor} → {@link
 * SqlValidator} → {@link OpenSearchSqlToRelConverter}, returning a {@link RelNode}.
 *
 * <p>Validator and converter are constructed against the cluster owned by {@code
 * context.relBuilder}, so PPL-specific rules registered on that cluster's planner stay on the path.
 * No SQL string round-trip.
 */
public class PPLSqlNodePlanner {

  private final OpenSearchSchema schema;

  public PPLSqlNodePlanner(OpenSearchSchema schema) {
    this.schema = schema;
  }

  public RelNode plan(UnresolvedPlan ast, CalcitePlanContext context) {
    SqlNode sqlNode = new PPLToSqlNodeVisitor(schema).translate(ast);

    RelOptCluster cluster = context.relBuilder.getCluster();
    CatalogReader catalogReader = (CatalogReader) context.relBuilder.getRelOptSchema();

    SqlOperatorTable operatorTable = context.config.getOperatorTable();
    if (operatorTable == null) {
      operatorTable = SqlStdOperatorTable.instance();
    }

    SqlValidator.Config validatorConfig =
        SqlValidator.Config.DEFAULT
            .withConformance(SqlConformanceEnum.DEFAULT)
            .withIdentifierExpansion(true);
    SqlValidator validator =
        SqlValidatorUtil.newValidator(
            operatorTable, catalogReader, OpenSearchTypeFactory.TYPE_FACTORY, validatorConfig);

    SqlNode validated = validator.validate(sqlNode);

    SqlToRelConverter.Config converterConfig =
        SqlToRelConverter.config().withTrimUnusedFields(false).withExpand(false);
    SqlToRelConverter converter =
        new OpenSearchSqlToRelConverter(
            ViewExpanders.simpleContext(cluster),
            validator,
            catalogReader,
            cluster,
            StandardConvertletTable.INSTANCE,
            converterConfig);

    RelRoot root = converter.convertQuery(validated, /* needsValidation */ false, /* top */ true);
    return root.project();
  }
}
