/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite;

import com.google.common.collect.ImmutableList;
import java.util.Arrays;
import java.util.List;
import org.apache.calcite.avatica.util.TimeUnit;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlIntervalQualifier;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.SqlTypeName;
import org.opensearch.sql.ast.expression.SpanUnit;

public class ExtendedRexBuilder extends RexBuilder {

  public ExtendedRexBuilder(RexBuilder rexBuilder) {
    super(rexBuilder.getTypeFactory());
  }

  /**
   * Build a RexCall without rejecting mixed operand types. Calcite's stock makeCall asks the
   * operator to {@code inferReturnType} on the operand types and throws if the operator refuses
   * (e.g. {@code +(NUMERIC, STRING)}). Visitor-side type checking is being removed in favor of
   * SqlValidator handling coercion after RelToSql, so this override falls back to the first
   * operand's type when inferReturnType fails. The loose type is harmless: RelToSqlConverter drops
   * it when writing the SQL string, and SqlValidator re-derives the real type on round-trip.
   *
   * <p>The varargs entry point {@code makeCall(SqlOperator, RexNode...)} is {@code final} on the
   * stock {@code RexBuilder} and routes through {@code makeCall(SqlParserPos, SqlOperator, List)}
   * rather than {@code makeCall(SqlOperator, List)}. Override the {@code SqlParserPos} variant so
   * both call shapes hit the fallback.
   */
  @Override
  public RexNode makeCall(SqlOperator op, List<? extends RexNode> exprs) {
    return makeCall(SqlParserPos.ZERO, op, exprs);
  }

  @Override
  public RexNode makeCall(SqlParserPos pos, SqlOperator op, List<? extends RexNode> exprs) {
    try {
      return super.makeCall(pos, op, exprs);
    } catch (RuntimeException e) {
      RelDataType fallback = exprs.get(0).getType();
      return super.makeCall(pos, fallback, op, ImmutableList.copyOf(exprs));
    }
  }

  public RexNode coalesce(RexNode... nodes) {
    return this.makeCall(SqlStdOperatorTable.COALESCE, nodes);
  }

  public RexNode equals(RexNode n1, RexNode n2) {
    return this.makeCall(SqlStdOperatorTable.EQUALS, n1, n2);
  }

  public RexNode and(RexNode left, RexNode right) {
    final RelDataType booleanType = this.getTypeFactory().createSqlType(SqlTypeName.BOOLEAN);
    return this.makeCall(booleanType, SqlStdOperatorTable.AND, List.of(left, right));
  }

  public RelDataType commonType(RexNode... nodes) {
    return this.getTypeFactory()
        .leastRestrictive(Arrays.stream(nodes).map(RexNode::getType).toList());
  }

  public SqlIntervalQualifier createIntervalUntil(SpanUnit unit) {
    TimeUnit timeUnit;
    switch (unit) {
      case MICROSECOND:
      case US:
        timeUnit = TimeUnit.MICROSECOND;
        break;
      case MILLISECOND:
      case MS:
        timeUnit = TimeUnit.MILLISECOND;
        break;
      case SECONDS:
      case SECOND:
      case SECS:
      case SEC:
      case S:
        timeUnit = TimeUnit.SECOND;
        break;
      case MINUTES:
      case MINUTE:
      case MINS:
      case MIN:
      case m:
        timeUnit = TimeUnit.MINUTE;
        break;
      case HOURS:
      case HOUR:
      case HRS:
      case HR:
      case H:
        timeUnit = TimeUnit.HOUR;
        break;
      case DAYS:
      case DAY:
      case D:
        timeUnit = TimeUnit.DAY;
        break;
      case WEEKS:
      case WEEK:
      case W:
        timeUnit = TimeUnit.WEEK;
        break;
      case MONTHS:
      case MONTH:
      case MON:
      case M:
        timeUnit = TimeUnit.MONTH;
        break;
      case QUARTERS:
      case QUARTER:
      case QTRS:
      case QTR:
      case Q:
        timeUnit = TimeUnit.QUARTER;
        break;
      case YEARS:
      case YEAR:
      case Y:
        timeUnit = TimeUnit.YEAR;
        break;
      default:
        timeUnit = TimeUnit.EPOCH;
    }
    return new SqlIntervalQualifier(timeUnit, timeUnit, SqlParserPos.ZERO);
  }

  @Override
  public RexNode makeCast(
      SqlParserPos pos,
      RelDataType type,
      RexNode exp,
      boolean matchNullability,
      boolean safe,
      RexLiteral format) {
    final org.apache.calcite.sql.type.SqlTypeName sqlType = type.getSqlTypeName();
    final RelDataType sourceType = exp.getType();
    // Tree-shape choice for boolean cast: Calcite's stock cast on string literal "1"/"0" returns
    // null because there's no implicit conversion rule. PPL/V1 semantics: cast("1"/"0" AS BOOLEAN)
    // → true/false; cast(numeric AS BOOLEAN) → numeric != 0. Wire up these cases as RexNode-shape
    // rewrites here so the caller's intent reads through.
    if (exp instanceof RexLiteral && sqlType == org.apache.calcite.sql.type.SqlTypeName.BOOLEAN) {
      RexLiteral one = (RexLiteral) makeLiteral("1", typeFactory.createSqlType(
          org.apache.calcite.sql.type.SqlTypeName.CHAR, 1));
      RexLiteral zero = (RexLiteral) makeLiteral("0", typeFactory.createSqlType(
          org.apache.calcite.sql.type.SqlTypeName.CHAR, 1));
      if (exp.equals(one)) {
        return makeLiteral(true, type);
      }
      if (exp.equals(zero)) {
        return makeLiteral(false, type);
      }
      if (org.apache.calcite.sql.type.SqlTypeUtil.isExactNumeric(sourceType)) {
        return makeCall(
            type,
            org.apache.calcite.sql.fun.SqlStdOperatorTable.NOT_EQUALS,
            com.google.common.collect.ImmutableList.of(exp, makeZeroLiteral(sourceType)));
      }
    }
    // Tree-shape choice: when asked to cast to a PPL UDT, build a call to the corresponding
    // PPL UDF instead of a stock CAST. The UDF carries the right runtime semantics for the
    // UDT (e.g. DATE → TIMESTAMP UDT promotes to start-of-day at runtime via TIMESTAMP UDF).
    // This is not coercion injection — callers (visitor, validator-side coercion) decide the
    // target type; we only choose the right RexNode shape.
    if (org.opensearch.sql.calcite.utils.OpenSearchTypeFactory.isUserDefinedType(type)) {
      if (RexLiteral.isNullLiteral(exp)) {
        return super.makeCast(pos, type, exp, matchNullability, safe, format);
      }
      if (type instanceof org.opensearch.sql.calcite.type.ExprDateType) {
        return makeCall(
            type, org.opensearch.sql.expression.function.PPLBuiltinOperators.DATE, List.of(exp));
      }
      if (type instanceof org.opensearch.sql.calcite.type.ExprTimeType) {
        return makeCall(
            type, org.opensearch.sql.expression.function.PPLBuiltinOperators.TIME, List.of(exp));
      }
      if (type instanceof org.opensearch.sql.calcite.type.ExprTimeStampType) {
        return makeCall(
            type,
            org.opensearch.sql.expression.function.PPLBuiltinOperators.TIMESTAMP,
            List.of(exp));
      }
      if (type instanceof org.opensearch.sql.calcite.type.ExprIPType) {
        if (sourceType instanceof org.opensearch.sql.calcite.type.ExprIPType) {
          return exp;
        }
        if (org.apache.calcite.sql.type.SqlTypeUtil.isCharacter(sourceType)) {
          return makeCall(
              type, org.opensearch.sql.expression.function.PPLBuiltinOperators.IP, List.of(exp));
        }
        // Throwing inside an implementor's runtime would be wrapped as a 500 error and the
        // message buried; throw here so the user sees a clear 400 error explaining the
        // type mismatch.
        throw new org.opensearch.sql.exception.ExpressionEvaluationException(
            String.format(
                java.util.Locale.ROOT,
                "Cannot convert %s to IP, only STRING and IP types are supported",
                org.opensearch.sql.calcite.utils.OpenSearchTypeFactory.convertRelDataTypeToExprType(
                    sourceType)));
      }
    }
    // Tree-shape choice: cast FROM (APPROXIMATE | DECIMAL) TO VARCHAR routes through
    // NUMBER_TO_STRING. Calcite's stock CAST goes through SqlFunctions.toString(double|BigDecimal)
    // which emits "0E0" for 0.0 and ".99" for 0.99 — non-Spark-compatible. The PPL UDF calls
    // Java's Double.toString / BigDecimal.toString and produces "0.0" / "0.99". The UDF is
    // deterministic, so RexSimplify still folds it at compile time when the operand is constant.
    if (org.apache.calcite.sql.type.SqlTypeUtil.isCharacter(type)
        && (org.apache.calcite.sql.type.SqlTypeUtil.isApproximateNumeric(sourceType)
            || org.apache.calcite.sql.type.SqlTypeUtil.isDecimal(sourceType))) {
      return makeCall(
          type,
          org.opensearch.sql.expression.function.PPLBuiltinOperators.NUMBER_TO_STRING,
          List.of(exp));
    }
    return super.makeCast(pos, type, exp, matchNullability, safe, format);
  }
}
