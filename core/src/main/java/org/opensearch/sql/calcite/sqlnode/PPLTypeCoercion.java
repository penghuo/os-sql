/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.sqlnode;

import javax.annotation.Nullable;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlCallBinding;
import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.type.SqlTypeUtil;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.SqlValidatorScope;
import org.apache.calcite.sql.validate.implicit.TypeCoercionFactory;
import org.apache.calcite.sql.validate.implicit.TypeCoercionImpl;
import org.opensearch.sql.calcite.type.ExprBinaryType;
import org.opensearch.sql.calcite.type.ExprDateType;
import org.opensearch.sql.calcite.type.ExprIPType;
import org.opensearch.sql.calcite.type.ExprTimeStampType;
import org.opensearch.sql.calcite.type.ExprTimeType;
import org.opensearch.sql.calcite.utils.OpenSearchTypeFactory;
import org.opensearch.sql.calcite.utils.OpenSearchTypeFactory.ExprUDT;

/**
 * PPL-flavoured type coercion. Diverges from Calcite's stock rules where PPL semantics differ:
 *
 * <ul>
 *   <li>STRING + NUMERIC → DOUBLE (Calcite default: DECIMAL(19,9))
 *   <li>DATE + TIME → TIMESTAMP
 *   <li>STRING + BINARY → BINARY
 *   <li>STRING + DATE/TIME/TIMESTAMP/IP/BOOLEAN → the typed side
 *   <li>FLOAT + FLOAT → FLOAT (Calcite default widens to DOUBLE)
 * </ul>
 *
 * <p>Mirrors the rules previously implemented in the deleted {@code CoercionUtils} but applies them
 * through Calcite's standard validator extension point.
 */
public class PPLTypeCoercion extends TypeCoercionImpl {
  public static final TypeCoercionFactory FACTORY = PPLTypeCoercion::new;

  public PPLTypeCoercion(RelDataTypeFactory typeFactory, SqlValidator validator) {
    super(typeFactory, validator);
  }

  @Override
  public @Nullable RelDataType getWiderTypeForTwo(
      RelDataType type1, RelDataType type2, boolean stringPromotion) {
    RelDataType ppl = pplWiderType(type1, type2);
    if (ppl != null) {
      return ppl;
    }
    return super.getWiderTypeForTwo(type1, type2, stringPromotion);
  }

  @Override
  public @Nullable RelDataType getTightestCommonType(
      @Nullable RelDataType type1, @Nullable RelDataType type2) {
    if (type1 != null && type2 != null) {
      RelDataType ppl = pplWiderType(type1, type2);
      if (ppl != null) {
        return ppl;
      }
    }
    return super.getTightestCommonType(type1, type2);
  }

  @Override
  public @Nullable RelDataType commonTypeForBinaryComparison(RelDataType type1, RelDataType type2) {
    RelDataType ppl = pplWiderType(type1, type2);
    if (ppl != null) {
      return ppl;
    }
    return super.commonTypeForBinaryComparison(type1, type2);
  }

  /**
   * Skip coercion when both operands of a binary comparison share a UDT class. Calcite's stock
   * coercion path tries to look up an assignment rule for {@code SqlTypeName.OTHER} and throws.
   * UDTs are already directly comparable in PPL, so do nothing.
   */
  /**
   * Skip Calcite's stock binary-comparison coercion whenever a UDT is involved on either side.
   * Stock coercion routes through {@code SqlTypeMappingRule.canApplyFrom} which throws {@code
   * AssertionError("No assign rules for OTHER defined")} on PPL UDTs (which use {@link
   * SqlTypeName#OTHER}). Comparisons with UDTs are handled by PPL UDFs (e.g. EQUALS_IP) registered
   * in PPLBuiltinOperators — those resolve through overload selection without needing operand
   * coercion.
   */
  @Override
  public boolean binaryComparisonCoercion(SqlCallBinding binding) {
    if (binding.getOperandCount() == 2) {
      RelDataType l = binding.getOperandType(0);
      RelDataType r = binding.getOperandType(1);
      boolean lUdt = l instanceof org.opensearch.sql.calcite.type.AbstractExprRelDataType<?>;
      boolean rUdt = r instanceof org.opensearch.sql.calcite.type.AbstractExprRelDataType<?>;
      if (lUdt || rUdt) {
        // Cross-datetime UDT comparison (DATE vs TIME, DATE vs TIMESTAMP, TIME vs TIMESTAMP):
        // cast both sides to TIMESTAMP. Without this, the comparison is computed on physically
        // different UDTs and always returns false. We bypass coerceOperandType (which would
        // call canApplyFrom and assert on SqlTypeName.OTHER) by syncing the binding's operand
        // types directly through the validator.
        RelDataType target = pplDatetimeCommonType(l, r);
        if (target != null) {
          forceCastOperand(binding, 0, target);
          forceCastOperand(binding, 1, target);
          return true;
        }
        // STRING vs UDT: wrap the STRING side with the corresponding UDT-producing UDF so both
        // operands have the same UDT class. PPL semantics for `host > '1.2.3.4'` (host is IP)
        // are "parse the string as IP, then compare". Same for STRING vs DATE/TIME/TIMESTAMP.
        if (lUdt && !rUdt && isString(r)) {
          org.apache.calcite.sql.SqlOperator udf = pplUdtUdf(l);
          if (udf != null) {
            forceCastOperandWithUdf(binding, 1, l, udf);
            return true;
          }
        }
        if (rUdt && !lUdt && isString(l)) {
          org.apache.calcite.sql.SqlOperator udf = pplUdtUdf(r);
          if (udf != null) {
            forceCastOperandWithUdf(binding, 0, r, udf);
            return true;
          }
        }
        // Same UDT class on both sides (or any other UDT-vs-non-datetime pair): skip stock
        // coercion. SqlTypeMappingRule.canApplyFrom asserts on SqlTypeName.OTHER; PPL UDFs
        // (EQUALS_IP, etc.) handle these comparisons directly.
        return false;
      }
    }
    return super.binaryComparisonCoercion(binding);
  }

  /**
   * Wrap the operand with the PPL UDF that produces the target UDT (TIMESTAMP/DATE/TIME/IP). This
   * preserves UDT identity through SqlToRelConverter — using {@code CAST(... AS spec)} would lose
   * it because {@link SqlTypeUtil#convertTypeToSpec} maps the UDT's inner BasicSqlType (VARCHAR
   * for date-like UDTs), and the resulting SqlNode says {@code AS VARCHAR}, not {@code AS
   * EXPR_TIMESTAMP}. The UDFs declare the right return type and the Spark dialect quotes their
   * names so they round-trip cleanly.
   */
  private void forceCastOperand(SqlCallBinding binding, int idx, RelDataType target) {
    org.apache.calcite.sql.SqlNode operand = binding.getCall().operand(idx);
    org.apache.calcite.sql.SqlOperator udf = pplUdtUdf(target);
    if (udf == null) {
      // Fallback: stock CAST. Only reached for non-UDT targets, where convertTypeToSpec is fine.
      org.apache.calcite.sql.SqlNode cast =
          org.apache.calcite.sql.fun.SqlStdOperatorTable.CAST.createCall(
              org.apache.calcite.sql.parser.SqlParserPos.ZERO,
              operand,
              org.apache.calcite.sql.type.SqlTypeUtil.convertTypeToSpec(target));
      binding.getCall().setOperand(idx, cast);
      validator.deriveType(binding.getScope(), cast);
      updateInferredType(cast, target);
      return;
    }
    org.apache.calcite.sql.SqlNode wrapped =
        udf.createCall(org.apache.calcite.sql.parser.SqlParserPos.ZERO, operand);
    binding.getCall().setOperand(idx, wrapped);
    validator.deriveType(binding.getScope(), wrapped);
    updateInferredType(wrapped, target);
  }

  /**
   * Wrap operand at index {@code idx} with the given PPL UDF. Same as {@link
   * #forceCastOperand} but for non-datetime UDTs (e.g. IP) where we already know the UDF.
   */
  private void forceCastOperandWithUdf(
      SqlCallBinding binding,
      int idx,
      RelDataType target,
      org.apache.calcite.sql.SqlOperator udf) {
    org.apache.calcite.sql.SqlNode operand = binding.getCall().operand(idx);
    org.apache.calcite.sql.SqlNode wrapped =
        udf.createCall(org.apache.calcite.sql.parser.SqlParserPos.ZERO, operand);
    binding.getCall().setOperand(idx, wrapped);
    validator.deriveType(binding.getScope(), wrapped);
    updateInferredType(wrapped, target);
  }

  private static @Nullable org.apache.calcite.sql.SqlOperator pplUdtUdf(RelDataType target) {
    if (target instanceof ExprTimeStampType) {
      return org.opensearch.sql.expression.function.PPLBuiltinOperators.TIMESTAMP;
    }
    if (target instanceof ExprDateType) {
      return org.opensearch.sql.expression.function.PPLBuiltinOperators.DATE;
    }
    if (target instanceof ExprTimeType) {
      return org.opensearch.sql.expression.function.PPLBuiltinOperators.TIME;
    }
    if (target instanceof ExprIPType) {
      return org.opensearch.sql.expression.function.PPLBuiltinOperators.IP;
    }
    return null;
  }

  /**
   * Returns TIMESTAMP if l/r are a cross-pair of PPL datetime UDTs (DATE/TIME/TIMESTAMP). Same-UDT
   * pairs return null so the caller skips coercion.
   */
  private @Nullable RelDataType pplDatetimeCommonType(RelDataType l, RelDataType r) {
    boolean lDt = isDate(l) || isTime(l) || isTimestamp(l);
    boolean rDt = isDate(r) || isTime(r) || isTimestamp(r);
    if (!lDt || !rDt) return null;
    if (l.getClass() == r.getClass()) return null; // same UDT, no widening needed
    return makeTimestamp(l.isNullable() || r.isNullable());
  }

  /**
   * UDF-call coercion path (e.g. EQUALS_IP, GREATER_IP). Skip when all UDF operands are PPL UDTs —
   * Calcite's stock coercion would try to look up assign rules for {@code SqlTypeName.OTHER}.
   */
  @Override
  public boolean userDefinedFunctionCoercion(
      SqlValidatorScope scope, SqlCall call, SqlFunction function) {
    for (int i = 0; i < call.operandCount(); i++) {
      RelDataType type = validator.deriveType(scope, call.operand(i));
      if (type instanceof org.opensearch.sql.calcite.type.AbstractExprRelDataType<?>) {
        return false;
      }
    }
    return super.userDefinedFunctionCoercion(scope, call, function);
  }

  // -------- PPL widening rules --------

  private @Nullable RelDataType pplWiderType(RelDataType l, RelDataType r) {
    // UDT identical on both sides: short-circuit so Calcite's stock coercion (which has no
    // assign rules for SqlTypeName.OTHER) is never reached.
    if (l.getClass() == r.getClass()
        && l instanceof org.opensearch.sql.calcite.type.AbstractExprRelDataType<?>) {
      return l.isNullable() == r.isNullable() ? l : factory.createTypeWithNullability(l, true);
    }

    if (isString(l) && isNumeric(r)) return makeDouble(r.isNullable() || l.isNullable());
    if (isString(r) && isNumeric(l)) return makeDouble(l.isNullable() || r.isNullable());

    if (isString(l) && isBinary(r)) return makeVarbinary(r.isNullable() || l.isNullable());
    if (isString(r) && isBinary(l)) return makeVarbinary(l.isNullable() || r.isNullable());

    // Cross-datetime comparisons widen to TIMESTAMP.
    if (isDate(l) && isTime(r)) return makeTimestamp(r.isNullable() || l.isNullable());
    if (isDate(r) && isTime(l)) return makeTimestamp(l.isNullable() || r.isNullable());
    if (isDate(l) && isTimestamp(r)) return makeTimestamp(r.isNullable() || l.isNullable());
    if (isDate(r) && isTimestamp(l)) return makeTimestamp(l.isNullable() || r.isNullable());
    if (isTime(l) && isTimestamp(r)) return makeTimestamp(r.isNullable() || l.isNullable());
    if (isTime(r) && isTimestamp(l)) return makeTimestamp(l.isNullable() || r.isNullable());

    // STRING + (DATE/TIME/TIMESTAMP/IP/BOOLEAN) → typed side
    if (isString(l) && (isDate(r) || isTime(r) || isTimestamp(r) || isIP(r) || isBoolean(r))) {
      return r;
    }
    if (isString(r) && (isDate(l) || isTime(l) || isTimestamp(l) || isIP(l) || isBoolean(l))) {
      return l;
    }

    // FLOAT + FLOAT stays FLOAT (Calcite default would widen to DOUBLE).
    if (isFloat(l) && isFloat(r)) {
      return l.isNullable() == r.isNullable() ? l : factory.createTypeWithNullability(l, true);
    }

    return null;
  }

  private static boolean isString(RelDataType t) {
    return SqlTypeUtil.isCharacter(t);
  }

  private static boolean isNumeric(RelDataType t) {
    SqlTypeName n = t.getSqlTypeName();
    if (n == null) return false;
    return SqlTypeName.NUMERIC_TYPES.contains(n);
  }

  private static boolean isFloat(RelDataType t) {
    SqlTypeName n = t.getSqlTypeName();
    return n == SqlTypeName.REAL || n == SqlTypeName.FLOAT;
  }

  private static boolean isBoolean(RelDataType t) {
    return t.getSqlTypeName() == SqlTypeName.BOOLEAN;
  }

  private static boolean isBinary(RelDataType t) {
    SqlTypeName n = t.getSqlTypeName();
    return t instanceof ExprBinaryType || n == SqlTypeName.BINARY || n == SqlTypeName.VARBINARY;
  }

  private static boolean isDate(RelDataType t) {
    return t instanceof ExprDateType || t.getSqlTypeName() == SqlTypeName.DATE;
  }

  private static boolean isTime(RelDataType t) {
    SqlTypeName n = t.getSqlTypeName();
    return t instanceof ExprTimeType
        || n == SqlTypeName.TIME
        || n == SqlTypeName.TIME_WITH_LOCAL_TIME_ZONE
        || n == SqlTypeName.TIME_TZ;
  }

  private static boolean isTimestamp(RelDataType t) {
    SqlTypeName n = t.getSqlTypeName();
    return t instanceof ExprTimeStampType
        || n == SqlTypeName.TIMESTAMP
        || n == SqlTypeName.TIMESTAMP_WITH_LOCAL_TIME_ZONE
        || n == SqlTypeName.TIMESTAMP_TZ;
  }

  private static boolean isIP(RelDataType t) {
    return t instanceof ExprIPType;
  }

  private RelDataType makeDouble(boolean nullable) {
    return factory.createTypeWithNullability(factory.createSqlType(SqlTypeName.DOUBLE), nullable);
  }

  private RelDataType makeVarbinary(boolean nullable) {
    return factory.createTypeWithNullability(
        factory.createSqlType(SqlTypeName.VARBINARY), nullable);
  }

  private RelDataType makeTimestamp(boolean nullable) {
    OpenSearchTypeFactory tf = OpenSearchTypeFactory.TYPE_FACTORY;
    return tf.createUDT(ExprUDT.EXPR_TIMESTAMP, nullable);
  }
}
