/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite;

import java.math.BigDecimal;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlBasicTypeNameSpec;
import org.apache.calcite.sql.SqlDataTypeSpec;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlTypeNameSpec;
import org.apache.calcite.sql.fun.SqlLibraryOperators;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.opensearch.sql.ast.dsl.AstDSL;
import org.opensearch.sql.ast.expression.DataType;
import org.opensearch.sql.ast.expression.Function;
import org.opensearch.sql.ast.expression.Literal;
import org.opensearch.sql.ast.expression.QualifiedName;
import org.opensearch.sql.ast.expression.UnresolvedExpression;

/**
 * Analyzer that converts Calcite {@link SqlNode} instances back into AST {@link Function}
 * expressions.
 */
public class SqlNodeToAstAnalyzer {

  private static final Map<SqlOperator, String> OPERATOR_FUNCTION_MAP =
      Map.of(
          SqlStdOperatorTable.LOWER, "lower",
          SqlStdOperatorTable.ABS, "abs",
          SqlStdOperatorTable.CAST, "cast",
          SqlLibraryOperators.CONCAT_FUNCTION, "concat",
          SqlLibraryOperators.SAFE_CAST, "safe_cast");

  /**
   * Convert the given {@link SqlNode} into an AST {@link Function}.
   *
   * @param node SqlNode to convert
   * @param context planning context (currently unused but kept for symmetry)
   * @return converted Function
   */
  public Function analyzer(SqlNode node, CalcitePlanContext context) {
    Objects.requireNonNull(node, "SqlNode cannot be null");
    if (!(node instanceof SqlBasicCall call)) {
      throw new IllegalArgumentException("Unsupported SqlNode: " + node.getClass().getSimpleName());
    }
    return (Function) visitCall(call);
  }

  private UnresolvedExpression visitCall(SqlBasicCall call) {
    SqlOperator operator = call.getOperator();
    String funcName = resolveFunctionName(operator);
    List<UnresolvedExpression> args =
        call.getOperandList().stream().map(this::convertOperand).toList();
    if ("cast".equalsIgnoreCase(funcName)) {
      return AstDSL.cast(args.get(0), (Literal) args.get(1));
    }
    return new Function(funcName, args);
  }

  private String resolveFunctionName(SqlOperator operator) {
    String funcName = OPERATOR_FUNCTION_MAP.get(operator);
    if (funcName != null) {
      return funcName;
    }
    return operator.getName().toLowerCase(Locale.ROOT);
  }

  private UnresolvedExpression convertOperand(SqlNode operand) {
    if (operand == null) {
      return new Literal(null, DataType.NULL);
    }

    if (operand instanceof SqlBasicCall call) {
      return visitCall(call);
    } else if (operand instanceof SqlLiteral literal) {
      return convertLiteral(literal);
    } else if (operand instanceof SqlIdentifier identifier) {
      return convertIdentifier(identifier);
    } else if (operand instanceof SqlDataTypeSpec dataTypeSpec) {
      return convertDataTypeSpec(dataTypeSpec);
    }
    throw new IllegalArgumentException("Unsupported operand type: " + operand.getClass());
  }

  private QualifiedName convertIdentifier(SqlIdentifier identifier) {
    return QualifiedName.of(identifier.names);
  }

  private Literal convertLiteral(SqlLiteral literal) {
    if (literal.getTypeName() == SqlTypeName.NULL || literal.getValue() == null) {
      return new Literal(null, DataType.NULL);
    }

    SqlTypeName typeName = literal.getTypeName();
    return switch (typeName) {
      case CHAR, VARCHAR -> new Literal(literal.getValueAs(String.class), DataType.STRING);
      case BOOLEAN -> new Literal(literal.getValueAs(Boolean.class), DataType.BOOLEAN);
      case FLOAT, REAL -> new Literal(
          literal.getValueAs(BigDecimal.class).floatValue(), DataType.FLOAT);
      case DOUBLE -> new Literal(
          literal.getValueAs(BigDecimal.class).doubleValue(), DataType.DOUBLE);
      case DECIMAL, INTEGER, BIGINT, SMALLINT, TINYINT -> convertExactNumeric(literal);
      default -> throw new IllegalArgumentException("Unsupported literal type: " + typeName);
    };
  }

  private Literal convertExactNumeric(SqlLiteral literal) {
    BigDecimal value = literal.getValueAs(BigDecimal.class);
    if (value.scale() > 0) {
      return new Literal(value, DataType.DECIMAL);
    }
    try {
      return new Literal(value.shortValueExact(), DataType.SHORT);
    } catch (ArithmeticException ignored) {
      // move on
    }
    try {
      return new Literal(value.intValueExact(), DataType.INTEGER);
    } catch (ArithmeticException ignored) {
      // move on
    }
    try {
      return new Literal(value.longValueExact(), DataType.LONG);
    } catch (ArithmeticException ignored) {
      // move on
    }
    return new Literal(value, DataType.DECIMAL);
  }

  private Literal convertDataTypeSpec(SqlDataTypeSpec dataTypeSpec) {
    if ("DECIMAL".equalsIgnoreCase(dataTypeSpec.getTypeName().getSimple())) {
      return AstDSL.stringLiteral("decimal");
    }
    return AstDSL.stringLiteral("string");
  }

  private String toTypeString(SqlDataTypeSpec spec) {
    SqlTypeNameSpec typeNameSpec = spec.getTypeNameSpec();
    if (typeNameSpec instanceof SqlBasicTypeNameSpec basicSpec) {
      String baseName = basicSpec.getTypeName().getSimple().toLowerCase(Locale.ROOT);
      int precision = basicSpec.getPrecision();
      int scale = basicSpec.getScale();
      if (precision >= 0) {
        if (scale >= 0) {
          return baseName + "(" + precision + "," + scale + ")";
        }
        return baseName + "(" + precision + ")";
      }
      return baseName;
    }
    return typeNameSpec.toString().toLowerCase(Locale.ROOT);
  }

  private boolean isDecimalType(SqlDataTypeSpec spec) {
    SqlTypeNameSpec typeNameSpec = spec.getTypeNameSpec();
    if (typeNameSpec instanceof SqlBasicTypeNameSpec basicSpec) {
      String simpleName = basicSpec.getTypeName().getSimple();
      try {
        SqlTypeName sqlTypeName = SqlTypeName.get(simpleName);
        if (sqlTypeName == SqlTypeName.DECIMAL) {
          return true;
        }
      } catch (IllegalArgumentException ignored) {
      }
      return "NUMERIC".equalsIgnoreCase(simpleName);
    }
    return false;
  }
}
