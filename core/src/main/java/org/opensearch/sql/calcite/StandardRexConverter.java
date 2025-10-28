/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite;

import static org.opensearch.sql.calcite.utils.OpenSearchTypeFactory.TYPE_FACTORY;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.NoSuchElementException;
import org.apache.calcite.config.CalciteConnectionConfigImpl;
import org.apache.calcite.prepare.Prepare;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlOperatorTable;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.fun.SqlLibraryOperators;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.util.SqlOperatorTables;
import org.apache.calcite.sql.util.SqlShuttle;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.SqlValidatorUtil;
import org.apache.calcite.sql2rel.SqlToRelConverter;
import org.apache.calcite.sql2rel.StandardConvertletTable;
import org.opensearch.sql.ast.expression.Function;

public class StandardRexConverter {
  private final SqlNodeAnalyzer analyzer;
  private final SqlOperatorTable operatorTable;

  public StandardRexConverter() {
    this.analyzer = new SqlNodeAnalyzer();
    this.operatorTable = createSqlOperatorTable();
  }

  public RexNode convert(Function node, CalcitePlanContext context) {
    SqlNode sqlCall = analyzer.analyze(node, context);

    RexNode rexNode =
        toRexNode(context, false, operatorTable, TYPE_FACTORY, getCurrentRowType(context), sqlCall);
    return rexNode;
  }

  public static RexNode toRexNode(
      CalcitePlanContext context,
      boolean caseSensitive,
      SqlOperatorTable operatorTable,
      RelDataTypeFactory typeFactory,
      RelDataType rowType,
      SqlNode expr) {
    String tableName = "_table_";
    SqlSelect select0 =
        new SqlSelect(
            SqlParserPos.ZERO,
            (SqlNodeList) null,
            new SqlNodeList(Collections.singletonList(expr), SqlParserPos.ZERO),
            new SqlIdentifier("_table_", SqlParserPos.ZERO),
            (SqlNode) null,
            (SqlNodeList) null,
            (SqlNode) null,
            (SqlNodeList) null,
            (SqlNode) null,
            (SqlNodeList) null,
            (SqlNode) null,
            (SqlNode) null,
            (SqlNodeList) null);
    Prepare.CatalogReader catalogReader =
        SqlValidatorUtil.createSingleTableCatalogReader(
            caseSensitive, "_table_", typeFactory, rowType);

    final SqlValidator.Config config =
        SqlValidator.Config.DEFAULT
            .withDefaultNullCollation(CalciteConnectionConfigImpl.DEFAULT.defaultNullCollation())
            .withIdentifierExpansion(true);
    SqlValidator validator =
        SqlValidatorUtil.newValidator(operatorTable, catalogReader, typeFactory, config);

    SqlSelect select = (SqlSelect) validator.validate(select0);

    SqlToRelConverter sqlToRelConverter =
        new SqlToRelConverter(
            null,
            validator,
            catalogReader,
            context.relBuilder.getCluster(),
            StandardConvertletTable.INSTANCE);

    SqlNode exprCoercion = stripSyntheticTableName(select.getSelectList().get(0), tableName);

    RexNode r1 = sqlToRelConverter.convertExpression(exprCoercion);
    RelNode relNode = sqlToRelConverter.convertSelect(select, true);
    //    RexNode rexNode = sqlToRelConverter.convertExpression(sqlExpr);
    return null;
  }

  private static RelDataType getCurrentRowType(CalcitePlanContext context) {
    try {
      return context.relBuilder.peek().getRowType();
    } catch (IllegalArgumentException | NoSuchElementException e) {
      return TYPE_FACTORY.builder().build();
    }
  }

  private static SqlOperatorTable createSqlOperatorTable() {
    final SqlOperatorTable opTab0 =
        CalciteConnectionConfigImpl.DEFAULT.fun(
            SqlOperatorTable.class, SqlStdOperatorTable.instance());
    final List<SqlOperatorTable> list = new ArrayList<>();
    list.add(opTab0);
    return SqlOperatorTables.chain(list);
  }

  private static SqlNode stripSyntheticTableName(SqlNode node, String tableName) {
    SqlShuttle visitor =
        new SqlShuttle() {
          @Override
          public SqlNode visit(SqlIdentifier id) {
            if (id.names.size() > 1 && tableName.equals(id.names.get(0))) {
              return new SqlIdentifier(
                  id.names.subList(1, id.names.size()), id.getParserPosition());
            }
            return id;
          }

          @Override
          public SqlNode visit(SqlCall call) {
            SqlNode processed = super.visit(call);
            if (processed instanceof SqlBasicCall basicCall
                && basicCall.getOperator() == SqlStdOperatorTable.CAST) {
              SqlNode[] operands = basicCall.getOperandList().toArray(new SqlNode[0]);
              return new SqlBasicCall(
                  SqlLibraryOperators.SAFE_CAST, operands, basicCall.getParserPosition());
            }
            return processed;
          }
        };
    return node.accept(visitor);
  }
}
