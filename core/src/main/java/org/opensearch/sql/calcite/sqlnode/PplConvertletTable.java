/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.sqlnode;

import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql2rel.SqlRexConvertlet;
import org.apache.calcite.sql2rel.SqlRexConvertletTable;
import org.apache.calcite.sql2rel.SqlRexContext;
import org.apache.calcite.sql2rel.StandardConvertletTable;
import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * PPL-flavoured wrapper around {@link StandardConvertletTable}. The stock convertlet, when
 * converting a binary comparison (e.g. {@code SMALLINT_field <> 0}), follows {@code
 * ComparableOperandTypeChecker.Consistency.LEAST_RESTRICTIVE} and inserts a {@code CAST(field AS
 * INTEGER)} so both operands share the least-restrictive common type. The runtime can compare
 * mixed numerics directly, so the CAST is semantically a no-op but it changes the RelNode shape
 * — pushdown plan snapshots that match against the un-CAST plan from the visitor break.
 *
 * <p>Strip the redundant CAST after {@code StandardConvertletTable} runs: if a binary comparison
 * call has a CAST operand whose source type is also numeric and not narrower than the cast
 * target's natural family, drop the CAST and re-build the call with the original operand. This
 * preserves the visitor's RelNode shape end-to-end.
 */
final class PplConvertletTable implements SqlRexConvertletTable {

  private final StandardConvertletTable delegate = StandardConvertletTable.INSTANCE;

  @Override
  public @Nullable SqlRexConvertlet get(SqlCall call) {
    final SqlRexConvertlet stock = delegate.get(call);
    if (stock == null) {
      return null;
    }
    if (!isCastUnwrapTarget(call.getKind())) {
      return stock;
    }
    return (SqlRexContext cx, SqlCall sc) -> {
      RexNode result = stock.convertCall(cx, sc);
      if (!(result instanceof RexCall)) {
        return result;
      }
      RexCall rc = (RexCall) result;
      if (!isCastUnwrapTarget(rc.getKind())) {
        return result;
      }
      java.util.List<RexNode> operands = rc.getOperands();
      java.util.List<RexNode> unwrapped = new java.util.ArrayList<>(operands.size());
      boolean changed = false;
      for (RexNode operand : operands) {
        RexNode u = unwrapNumericCast(operand);
        unwrapped.add(u);
        if (u != operand) {
          changed = true;
        }
      }
      if (!changed) {
        return result;
      }
      return cx.getRexBuilder().makeCall(rc.getType(), rc.getOperator(), unwrapped);
    };
  }

  /**
   * Operators whose operands the {@link StandardConvertletTable} wraps with redundant numeric
   * CASTs via {@code Consistency.LEAST_RESTRICTIVE}: binary comparisons (=, <>, <, <=, >, >=) and
   * arithmetic (+, -, *, /). Stripping the CAST keeps the RelNode shape close to the visitor's
   * output, which is critical for pushdown serialization size (the bin command's nested arithmetic
   * grows from ~20KB to 683KB+ when each operand is wrapped) and for snapshot tests.
   */
  private static boolean isCastUnwrapTarget(SqlKind kind) {
    switch (kind) {
      case EQUALS:
      case NOT_EQUALS:
      case LESS_THAN:
      case LESS_THAN_OR_EQUAL:
      case GREATER_THAN:
      case GREATER_THAN_OR_EQUAL:
      case PLUS:
      case MINUS:
      case TIMES:
      case DIVIDE:
        return true;
      default:
        return false;
    }
  }

  /**
   * If {@code expr} is a {@code CAST(x AS T)} where both {@code x.getType()} and {@code T} are
   * exact numeric integer types (TINYINT/SMALLINT/INTEGER/BIGINT) of the same family, return
   * {@code x}. Otherwise return {@code expr} unchanged.
   *
   * <p>The runtime compares same-family integers directly, so the cast adds no semantic value
   * for comparison and integer arithmetic. We deliberately do <em>not</em> strip casts that cross
   * exact↔approximate boundaries (BIGINT → DOUBLE, INTEGER → DECIMAL) because those casts change
   * the result of subsequent arithmetic — most importantly division: {@code BIGINT/BIGINT} is
   * integer division (truncating), {@code BIGINT/DOUBLE} is floating-point division. PPL window
   * AVG is desugared to {@code SUM(x) / CAST(COUNT(x) AS DOUBLE)} and depends on the DOUBLE cast
   * to preserve the fractional part.
   */
  private static RexNode unwrapNumericCast(RexNode expr) {
    if (!(expr instanceof RexCall)) {
      return expr;
    }
    RexCall call = (RexCall) expr;
    if (call.getOperator() != SqlStdOperatorTable.CAST || call.getOperands().size() != 1) {
      return expr;
    }
    RexNode inner = call.getOperands().get(0);
    if (org.apache.calcite.sql.type.SqlTypeUtil.isExactNumeric(inner.getType())
        && org.apache.calcite.sql.type.SqlTypeUtil.isExactNumeric(call.getType())) {
      return inner;
    }
    return expr;
  }
}
