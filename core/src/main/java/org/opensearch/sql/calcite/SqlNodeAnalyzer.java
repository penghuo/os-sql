/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite;

import java.util.List;
import java.util.Locale;
import java.util.Map;
import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.fun.SqlLibraryOperators;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.opensearch.sql.ast.AbstractNodeVisitor;
import org.opensearch.sql.ast.expression.DataType;
import org.opensearch.sql.ast.expression.Function;
import org.opensearch.sql.ast.expression.Literal;
import org.opensearch.sql.ast.expression.QualifiedName;
import org.opensearch.sql.ast.expression.UnresolvedExpression;

/** Analyzer that converts AST expressions into Calcite {@link SqlNode}. */
public class SqlNodeAnalyzer extends AbstractNodeVisitor<SqlNode, CalcitePlanContext> {

  private static final SqlParserPos ZERO = SqlParserPos.ZERO;

  private static final Map<String, SqlOperator> FUNCTION_OPERATOR_MAP =
      Map.of(
          "LOWER", SqlStdOperatorTable.LOWER,
          "ABS", SqlStdOperatorTable.ABS,
          "CONCAT", SqlLibraryOperators.CONCAT_FUNCTION);

  public SqlNode analyze(UnresolvedExpression node, CalcitePlanContext context) {
    return node.accept(this, context);
  }

  @Override
  public SqlNode visitFunction(Function node, CalcitePlanContext context) {
    SqlOperator operator = FUNCTION_OPERATOR_MAP.get(node.getFuncName().toUpperCase(Locale.ROOT));
    if (operator == null) {
      throw new IllegalArgumentException("Unsupported function: " + node.getFuncName());
    }
    List<UnresolvedExpression> args = node.getFuncArgs();
    SqlNode[] operands = args.stream().map(arg -> analyze(arg, context)).toArray(SqlNode[]::new);
    return new SqlBasicCall(operator, operands, ZERO);
  }

  @Override
  public SqlNode visitLiteral(Literal node, CalcitePlanContext context) {
    Object value = node.getValue();
    DataType type = node.getType();
    if (value == null) {
      return SqlLiteral.createNull(ZERO);
    }
    return switch (type) {
      case STRING -> SqlLiteral.createCharString(String.valueOf(value), ZERO);
      case INTEGER, LONG, SHORT -> SqlLiteral.createExactNumeric(
          String.valueOf(((Number) value).longValue()), ZERO);
      case DOUBLE, FLOAT, DECIMAL -> SqlLiteral.createApproxNumeric(String.valueOf(value), ZERO);
      case BOOLEAN -> SqlLiteral.createBoolean((Boolean) value, ZERO);
      default -> throw new IllegalArgumentException("Unsupported literal type: " + type);
    };
  }

  @Override
  public SqlNode visitQualifiedName(QualifiedName node, CalcitePlanContext context) {
    return new SqlIdentifier(node.getParts(), ZERO);
  }
}
