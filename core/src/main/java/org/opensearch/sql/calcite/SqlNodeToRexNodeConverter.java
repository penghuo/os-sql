/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite;

import java.math.BigDecimal;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;
import lombok.RequiredArgsConstructor;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlBasicTypeNameSpec;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlDataTypeSpec;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlTypeNameSpec;
import org.apache.calcite.sql.type.SqlTypeName;
import org.opensearch.sql.ast.expression.QualifiedName;

/**
 * Converter that transforms Calcite {@link SqlNode} representations into {@link RexNode} instances
 * using the {@link CalcitePlanContext}.
 */
@RequiredArgsConstructor
public class SqlNodeToRexNodeConverter {

  private final CalcitePlanContext context;

  public RexNode convert(SqlNode node) {
    Objects.requireNonNull(node, "SqlNode cannot be null");
    if (node instanceof SqlLiteral literal) {
      return convertLiteral(literal);
    } else if (node instanceof SqlCall call) {
      return convertCall(call);
    } else if (node instanceof SqlIdentifier identifier) {
      return convertIdentifier(identifier);
    }
    throw new UnsupportedOperationException("Unsupported SqlNode: " + node);
  }

  private RexNode convertCall(SqlCall call) {
    if (call.getKind() == SqlKind.SAFE_CAST && call.getOperandList().size() == 2) {
      RexNode value = convert(call.operand(0));
      SqlNode typeOperand = call.operand(1);
      if (typeOperand instanceof SqlDataTypeSpec dataTypeSpec) {
        RelDataType targetType = deriveRelDataType(dataTypeSpec);
        return context.rexBuilder.makeCast(targetType, value, true, true);
      }
    }
    List<RexNode> operands =
        call.getOperandList().stream().map(this::convert).collect(Collectors.toList());
    return context.rexBuilder.makeCall(call.getOperator(), operands);
  }

  private RexNode convertLiteral(SqlLiteral literal) {
    SqlTypeName typeName = literal.getTypeName();
    return switch (typeName) {
      case CHAR, VARCHAR -> context.rexBuilder.makeLiteral(
          literal.getValueAs(String.class),
          context.rexBuilder.getTypeFactory().createSqlType(SqlTypeName.VARCHAR),
          true);
      case BOOLEAN -> context.rexBuilder.makeLiteral(literal.getValueAs(Boolean.class));
      case DECIMAL, INTEGER, BIGINT, SMALLINT, TINYINT -> context.rexBuilder.makeExactLiteral(
          literal.getValueAs(BigDecimal.class));
      case FLOAT, REAL -> makeApproxLiteral(literal, SqlTypeName.FLOAT);
      case DOUBLE -> makeApproxLiteral(literal, SqlTypeName.DOUBLE);
      case NULL -> context.rexBuilder.makeNullLiteral(
          context.rexBuilder.getTypeFactory().createSqlType(SqlTypeName.NULL));
      default -> throw new UnsupportedOperationException(
          String.format("Unsupported SqlLiteral type: %s", typeName));
    };
  }

  private RexNode makeApproxLiteral(SqlLiteral literal, SqlTypeName targetType) {
    Object value = literal.getValue();
    BigDecimal decimalValue;
    if (value instanceof BigDecimal bigDecimal) {
      decimalValue = bigDecimal;
    } else if (value instanceof Number number) {
      decimalValue = new BigDecimal(number.toString());
    } else {
      decimalValue = new BigDecimal(literal.getValueAs(String.class));
    }
    RelDataType type = context.rexBuilder.getTypeFactory().createSqlType(targetType);
    return context.rexBuilder.makeApproxLiteral(decimalValue, type);
  }

  private RexNode convertIdentifier(SqlIdentifier identifier) {
    QualifiedName qualifiedName = QualifiedName.of(identifier.names);
    return QualifiedNameResolver.resolve(qualifiedName, context);
  }

  private RelDataType deriveRelDataType(SqlDataTypeSpec spec) {
    RelDataTypeFactory typeFactory = context.relBuilder.getTypeFactory();
    SqlTypeNameSpec typeNameSpec = spec.getTypeNameSpec();
    RelDataType relType;
    if (typeNameSpec instanceof SqlBasicTypeNameSpec basicType) {
      SqlTypeName typeName = SqlTypeName.get(basicType.getTypeName().getSimple());
      int precision = basicType.getPrecision();
      int scale = basicType.getScale();
      if (typeName.allowsPrecScale(true, true) && precision >= 0 && scale >= 0) {
        relType = typeFactory.createSqlType(typeName, precision, scale);
      } else if (typeName.allowsPrec() && precision >= 0) {
        relType = typeFactory.createSqlType(typeName, precision);
      } else {
        relType = typeFactory.createSqlType(typeName);
      }
    } else {
      throw new UnsupportedOperationException("Unsupported SqlTypeNameSpec: " + typeNameSpec);
    }
    boolean nullable = spec.getNullable() == null || spec.getNullable();
    relType = typeFactory.createTypeWithNullability(relType, nullable);
    return relType;
  }
}
