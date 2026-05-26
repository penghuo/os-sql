/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.utils;

import static org.opensearch.sql.calcite.utils.OpenSearchTypeFactory.TYPE_FACTORY;

import java.util.List;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.SqlCallBinding;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOperandCountRange;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.SqlOperandCountRanges;
import org.apache.calcite.sql.type.SqlOperandTypeChecker;
import org.apache.calcite.sql.type.SqlTypeFamily;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.type.SqlTypeUtil;
import org.opensearch.sql.calcite.type.AbstractExprRelDataType;
import org.opensearch.sql.calcite.utils.OpenSearchTypeFactory.ExprUDT;

/**
 * Common operand type checkers for PPL functions.
 *
 * <p>Each constant is a {@link SqlOperandTypeChecker} consumed by Calcite's validator via {@link
 * org.apache.calcite.sql.validate.SqlUserDefinedFunction}. All constants are stock Calcite {@link
 * org.apache.calcite.sql.type.FamilyOperandTypeChecker} / {@link
 * org.apache.calcite.sql.type.CompositeOperandTypeChecker}.
 */
public class PPLOperandTypes {
  private PPLOperandTypes() {}

  // RelDataType constants for UDT-aware signatures (used with udt(...)).
  // UDT-backed scalar types — check by Java class identity at validate time:
  public static final RelDataType DATE_UDT = TYPE_FACTORY.createUDT(ExprUDT.EXPR_DATE);
  public static final RelDataType TIME_UDT = TYPE_FACTORY.createUDT(ExprUDT.EXPR_TIME);
  public static final RelDataType TIMESTAMP_UDT = TYPE_FACTORY.createUDT(ExprUDT.EXPR_TIMESTAMP);
  public static final RelDataType IP_UDT = TYPE_FACTORY.createUDT(ExprUDT.EXPR_IP);
  public static final RelDataType BINARY_UDT = TYPE_FACTORY.createUDT(ExprUDT.EXPR_BINARY);
  // Plain SQL scalar types — check by SqlTypeFamily containment:
  public static final RelDataType STRING_T = TYPE_FACTORY.createSqlType(SqlTypeName.VARCHAR);
  public static final RelDataType INTEGER_T = TYPE_FACTORY.createSqlType(SqlTypeName.INTEGER);

  public static final SqlOperandTypeChecker OPTIONAL_ANY =
      OperandTypes.family(SqlTypeFamily.ANY).or(OperandTypes.family());
  public static final SqlOperandTypeChecker OPTIONAL_INTEGER =
      OperandTypes.INTEGER.or(OperandTypes.family());

  public static final SqlOperandTypeChecker NUMERIC_OPTIONAL_STRING =
      OperandTypes.NUMERIC.or(OperandTypes.family(SqlTypeFamily.NUMERIC, SqlTypeFamily.CHARACTER));

  public static final SqlOperandTypeChecker ANY_OPTIONAL_INTEGER =
      OperandTypes.ANY.or(OperandTypes.family(SqlTypeFamily.ANY, SqlTypeFamily.INTEGER));
  public static final SqlOperandTypeChecker ANY_OPTIONAL_STRING =
      OperandTypes.ANY.or(OperandTypes.family(SqlTypeFamily.ANY, SqlTypeFamily.CHARACTER));
  public static final SqlOperandTypeChecker ANY_OPTIONAL_TIMESTAMP =
      OperandTypes.ANY.or(OperandTypes.family(SqlTypeFamily.ANY, SqlTypeFamily.TIMESTAMP));
  public static final SqlOperandTypeChecker STRING_STRING_STRING =
      OperandTypes.family(
          SqlTypeFamily.CHARACTER, SqlTypeFamily.CHARACTER, SqlTypeFamily.CHARACTER);
  public static final SqlOperandTypeChecker STRING_INTEGER =
      OperandTypes.family(SqlTypeFamily.CHARACTER, SqlTypeFamily.INTEGER);
  public static final SqlOperandTypeChecker STRING_STRING_INTEGER =
      OperandTypes.family(SqlTypeFamily.CHARACTER, SqlTypeFamily.CHARACTER, SqlTypeFamily.INTEGER);

  public static final SqlOperandTypeChecker STRING_OR_STRING_INTEGER =
      OperandTypes.family(SqlTypeFamily.CHARACTER)
          .or(OperandTypes.family(SqlTypeFamily.CHARACTER, SqlTypeFamily.INTEGER));

  public static final SqlOperandTypeChecker STRING_STRING_INTEGER_INTEGER =
      OperandTypes.family(
          SqlTypeFamily.CHARACTER,
          SqlTypeFamily.CHARACTER,
          SqlTypeFamily.INTEGER,
          SqlTypeFamily.INTEGER);

  public static final SqlOperandTypeChecker NUMERIC_STRING_OR_STRING_STRING =
      OperandTypes.family(SqlTypeFamily.NUMERIC, SqlTypeFamily.STRING)
          .or(OperandTypes.family(SqlTypeFamily.STRING, SqlTypeFamily.STRING));

  public static final SqlOperandTypeChecker NUMERIC_NUMERIC_OPTIONAL_NUMERIC =
      OperandTypes.NUMERIC_NUMERIC
          .or(
              OperandTypes.family(
                  SqlTypeFamily.NUMERIC, SqlTypeFamily.NUMERIC, SqlTypeFamily.NUMERIC))
          // Trailing ANY accommodates the SqlTypeName-flag arg the SqlNode path appends
          // for percentile_approx/median (used by the runtime to coerce the result back
          // to the field's input type).
          .or(OperandTypes.family(SqlTypeFamily.NUMERIC, SqlTypeFamily.NUMERIC, SqlTypeFamily.ANY))
          .or(
              OperandTypes.family(
                  SqlTypeFamily.NUMERIC,
                  SqlTypeFamily.NUMERIC,
                  SqlTypeFamily.NUMERIC,
                  SqlTypeFamily.ANY));
  public static final SqlOperandTypeChecker NUMERIC_NUMERIC_NUMERIC =
      OperandTypes.family(SqlTypeFamily.NUMERIC, SqlTypeFamily.NUMERIC, SqlTypeFamily.NUMERIC);
  public static final SqlOperandTypeChecker NUMERIC_NUMERIC_NUMERIC_NUMERIC =
      OperandTypes.family(
          SqlTypeFamily.NUMERIC,
          SqlTypeFamily.NUMERIC,
          SqlTypeFamily.NUMERIC,
          SqlTypeFamily.NUMERIC);

  public static final SqlOperandTypeChecker WIDTH_BUCKET_OPERAND =
      // 0. EXPR_TIMESTAMP UDT (SqlTypeName=VARCHAR) on CHARACTER-family operands.
      //    Listed FIRST so the validator picks this signature for EXPR_TIMESTAMP fields
      //    instead of coercing them to NUMERIC (DECIMAL via implicit CAST). Without
      //    this, the AggregateIndexScanRule's containsWidthBucketFuncOnDate predicate
      //    fails because the CAST result type isn't an EXPR_DATE/TIME/TIMESTAMP UDT,
      //    breaking auto_date_histogram pushdown for `bin <ts> bins=N | stats ...`.
      OperandTypes.family(
              SqlTypeFamily.CHARACTER,
              SqlTypeFamily.INTEGER,
              SqlTypeFamily.CHARACTER,
              SqlTypeFamily.CHARACTER)
          // 1. Numeric fields: bin age span=10
          .or(
              OperandTypes.family(
                  SqlTypeFamily.NUMERIC,
                  SqlTypeFamily.INTEGER,
                  SqlTypeFamily.NUMERIC,
                  SqlTypeFamily.NUMERIC))
          // 2. Timestamp fields with OpenSearch type system
          .or(
              OperandTypes.family(
                  SqlTypeFamily.TIMESTAMP,
                  SqlTypeFamily.INTEGER,
                  SqlTypeFamily.CHARACTER,
                  SqlTypeFamily.TIMESTAMP))
          // 3. Timestamp fields with Calcite SCOTT schema
          .or(
              OperandTypes.family(
                  SqlTypeFamily.TIMESTAMP,
                  SqlTypeFamily.INTEGER,
                  SqlTypeFamily.TIMESTAMP,
                  SqlTypeFamily.TIMESTAMP))
          // DATE field (OpenSearch)
          .or(
              OperandTypes.family(
                  SqlTypeFamily.DATE,
                  SqlTypeFamily.INTEGER,
                  SqlTypeFamily.CHARACTER,
                  SqlTypeFamily.DATE))
          // DATE field (SCOTT)
          .or(
              OperandTypes.family(
                  SqlTypeFamily.DATE,
                  SqlTypeFamily.INTEGER,
                  SqlTypeFamily.DATE,
                  SqlTypeFamily.DATE))
          // TIME field (OpenSearch)
          .or(
              OperandTypes.family(
                  SqlTypeFamily.TIME,
                  SqlTypeFamily.INTEGER,
                  SqlTypeFamily.CHARACTER,
                  SqlTypeFamily.TIME))
          // TIME field (SCOTT)
          .or(
              OperandTypes.family(
                  SqlTypeFamily.TIME,
                  SqlTypeFamily.INTEGER,
                  SqlTypeFamily.TIME,
                  SqlTypeFamily.TIME));

  public static final SqlOperandTypeChecker NUMERIC_NUMERIC_NUMERIC_NUMERIC_NUMERIC =
      OperandTypes.family(
          SqlTypeFamily.NUMERIC,
          SqlTypeFamily.NUMERIC,
          SqlTypeFamily.NUMERIC,
          SqlTypeFamily.NUMERIC,
          SqlTypeFamily.NUMERIC);
  public static final SqlOperandTypeChecker STRING_OR_INTEGER_INTEGER_INTEGER =
      OperandTypes.family(SqlTypeFamily.CHARACTER, SqlTypeFamily.INTEGER, SqlTypeFamily.INTEGER)
          .or(
              OperandTypes.family(
                  SqlTypeFamily.INTEGER, SqlTypeFamily.INTEGER, SqlTypeFamily.INTEGER));

  public static final SqlOperandTypeChecker OPTIONAL_DATE_OR_TIMESTAMP_OR_NUMERIC =
      OperandTypes.DATETIME
          .or(OperandTypes.NUMERIC)
          .or(OperandTypes.CHARACTER)
          .or(OperandTypes.family());

  public static final SqlOperandTypeChecker DATETIME_OR_STRING =
      OperandTypes.DATETIME.or(OperandTypes.CHARACTER);
  public static final SqlOperandTypeChecker TIME_OR_TIMESTAMP_OR_STRING =
      OperandTypes.CHARACTER.or(OperandTypes.TIME).or(OperandTypes.TIMESTAMP);
  public static final SqlOperandTypeChecker DATE_OR_TIMESTAMP_OR_STRING =
      OperandTypes.DATE_OR_TIMESTAMP.or(OperandTypes.CHARACTER);
  public static final SqlOperandTypeChecker DATETIME_OR_STRING_OR_INTEGER =
      OperandTypes.DATETIME.or(OperandTypes.CHARACTER).or(OperandTypes.INTEGER);

  public static final SqlOperandTypeChecker DATETIME_OPTIONAL_INTEGER =
      OperandTypes.DATETIME
          .or(OperandTypes.CHARACTER)
          .or(OperandTypes.family(SqlTypeFamily.DATETIME, SqlTypeFamily.INTEGER))
          .or(OperandTypes.family(SqlTypeFamily.CHARACTER, SqlTypeFamily.INTEGER));
  public static final SqlOperandTypeChecker ANY_DATETIME_OR_STRING =
      OperandTypes.family(SqlTypeFamily.ANY)
          .or(OperandTypes.family(SqlTypeFamily.ANY, SqlTypeFamily.DATETIME))
          .or(OperandTypes.family(SqlTypeFamily.ANY, SqlTypeFamily.STRING));

  public static final SqlOperandTypeChecker DATETIME_DATETIME =
      OperandTypes.family(SqlTypeFamily.DATETIME, SqlTypeFamily.DATETIME)
          .or(OperandTypes.family(SqlTypeFamily.DATETIME, SqlTypeFamily.CHARACTER))
          .or(OperandTypes.family(SqlTypeFamily.CHARACTER, SqlTypeFamily.DATETIME))
          .or(OperandTypes.CHARACTER_CHARACTER);
  public static final SqlOperandTypeChecker DATETIME_OR_STRING_STRING =
      OperandTypes.family(SqlTypeFamily.DATETIME, SqlTypeFamily.CHARACTER)
          .or(OperandTypes.CHARACTER_CHARACTER);
  public static final SqlOperandTypeChecker DATETIME_OR_STRING_DATETIME_OR_STRING =
      OperandTypes.CHARACTER_CHARACTER
          .or(OperandTypes.family(SqlTypeFamily.DATETIME, SqlTypeFamily.DATETIME))
          .or(OperandTypes.family(SqlTypeFamily.DATETIME, SqlTypeFamily.CHARACTER))
          .or(OperandTypes.family(SqlTypeFamily.CHARACTER, SqlTypeFamily.DATETIME));
  public static final SqlOperandTypeChecker STRING_TIMESTAMP =
      OperandTypes.family(SqlTypeFamily.CHARACTER, SqlTypeFamily.TIMESTAMP)
          .or(OperandTypes.CHARACTER_CHARACTER);
  public static final SqlOperandTypeChecker STRING_DATETIME =
      OperandTypes.family(SqlTypeFamily.CHARACTER, SqlTypeFamily.DATETIME)
          .or(OperandTypes.CHARACTER_CHARACTER);
  public static final SqlOperandTypeChecker DATETIME_INTERVAL =
      OperandTypes.DATETIME_INTERVAL.or(
          OperandTypes.family(SqlTypeFamily.CHARACTER, SqlTypeFamily.DATETIME_INTERVAL));
  public static final SqlOperandTypeChecker TIME_TIME =
      OperandTypes.family(SqlTypeFamily.TIME, SqlTypeFamily.TIME)
          .or(OperandTypes.family(SqlTypeFamily.TIME, SqlTypeFamily.CHARACTER))
          .or(OperandTypes.family(SqlTypeFamily.CHARACTER, SqlTypeFamily.TIME))
          .or(OperandTypes.CHARACTER_CHARACTER);

  public static final SqlOperandTypeChecker TIMESTAMP_OR_STRING_STRING_STRING =
      OperandTypes.family(SqlTypeFamily.TIMESTAMP, SqlTypeFamily.CHARACTER, SqlTypeFamily.CHARACTER)
          .or(
              OperandTypes.family(
                  SqlTypeFamily.CHARACTER, SqlTypeFamily.CHARACTER, SqlTypeFamily.CHARACTER));
  public static final SqlOperandTypeChecker STRING_INTEGER_DATETIME_OR_STRING =
      OperandTypes.family(SqlTypeFamily.CHARACTER, SqlTypeFamily.NUMERIC, SqlTypeFamily.CHARACTER)
          .or(
              OperandTypes.family(
                  SqlTypeFamily.CHARACTER, SqlTypeFamily.NUMERIC, SqlTypeFamily.DATETIME));
  public static final SqlOperandTypeChecker INTERVAL_DATETIME_DATETIME =
      OperandTypes.family(SqlTypeFamily.CHARACTER, SqlTypeFamily.DATETIME, SqlTypeFamily.DATETIME)
          .or(
              OperandTypes.family(
                  SqlTypeFamily.CHARACTER, SqlTypeFamily.CHARACTER, SqlTypeFamily.DATETIME))
          .or(
              OperandTypes.family(
                  SqlTypeFamily.CHARACTER, SqlTypeFamily.DATETIME, SqlTypeFamily.CHARACTER))
          .or(
              OperandTypes.family(
                  SqlTypeFamily.CHARACTER, SqlTypeFamily.CHARACTER, SqlTypeFamily.CHARACTER));

  /**
   * Operand checker for aggregates that take any single scalar operand (e.g. {@code list(field)}).
   * Resolves to {@link OperandTypes#ANY} — a single arg of any type. UDTs (EXPR_IP, EXPR_DATE) are
   * accepted because they all report {@link SqlTypeFamily#ANY} as their fallback family.
   */
  public static final SqlOperandTypeChecker ANY_SCALAR = OperandTypes.ANY;

  /**
   * Operand checker for aggregates that take a scalar operand plus an optional integer (e.g. {@code
   * values(field [, limit])}, {@code first(field [, limit])}). One- or two-arg form.
   */
  public static final SqlOperandTypeChecker ANY_SCALAR_OPTIONAL_INTEGER =
      OperandTypes.ANY.or(OperandTypes.family(SqlTypeFamily.ANY, SqlTypeFamily.INTEGER));

  /**
   * Signature for the {@code pattern} aggregate UDF (BRAIN log-pattern parser). The first four
   * operands are fixed: {@code field:STRING, max_sample_count:INTEGER, buffer_limit:INTEGER,
   * show_numbered_token:BOOLEAN}. Optional named parameters are appended in alphabetical order;
   * {@code frequency_threshold_percentage:NUMERIC} (alphabetically first) and {@code
   * variable_count_threshold:INTEGER}.
   */
  public static final SqlOperandTypeChecker PATTERN_AGG_SIGNATURE =
      OperandTypes.family(
              SqlTypeFamily.CHARACTER,
              SqlTypeFamily.INTEGER,
              SqlTypeFamily.INTEGER,
              SqlTypeFamily.BOOLEAN)
          .or(
              OperandTypes.family(
                  SqlTypeFamily.CHARACTER,
                  SqlTypeFamily.INTEGER,
                  SqlTypeFamily.INTEGER,
                  SqlTypeFamily.BOOLEAN,
                  SqlTypeFamily.NUMERIC))
          .or(
              OperandTypes.family(
                  SqlTypeFamily.CHARACTER,
                  SqlTypeFamily.INTEGER,
                  SqlTypeFamily.INTEGER,
                  SqlTypeFamily.BOOLEAN,
                  SqlTypeFamily.INTEGER))
          .or(
              OperandTypes.family(
                  SqlTypeFamily.CHARACTER,
                  SqlTypeFamily.INTEGER,
                  SqlTypeFamily.INTEGER,
                  SqlTypeFamily.BOOLEAN,
                  SqlTypeFamily.NUMERIC,
                  SqlTypeFamily.INTEGER));

  /**
   * Build a UDT-aware checker from a list of allowed {@link RelDataType} signatures.
   *
   * <p>For each signature, every operand is validated against the formal type at the same position:
   *
   * <ul>
   *   <li>If the formal type is a PPL UDT ({@link AbstractExprRelDataType}, e.g. {@link #IP_UDT},
   *       {@link #DATE_UDT}), the actual operand must be either the same UDT class OR a plain SQL
   *       type whose family matches the UDT's underlying SqlTypeName family. This allows passing
   *       string literals where IP UDT is expected (the runtime impl parses the string).
   *   <li>If the formal type is plain SQL, the actual operand's {@link SqlTypeFamily} must match
   *       the formal's family.
   * </ul>
   *
   * <p>A call validates if it matches at least one signature.
   */
  public static SqlOperandTypeChecker udt(List<List<RelDataType>> allowedSignatures) {
    return new UDTOperandTypeChecker(allowedSignatures);
  }

  /** UDT-aware operand checker. See {@link #udt}. */
  private static final class UDTOperandTypeChecker implements SqlOperandTypeChecker {
    private final List<List<RelDataType>> allowedSignatures;
    private final SqlOperandCountRange countRange;

    UDTOperandTypeChecker(List<List<RelDataType>> allowedSignatures) {
      this.allowedSignatures = allowedSignatures;
      int min = Integer.MAX_VALUE;
      int max = 0;
      for (List<RelDataType> sig : allowedSignatures) {
        min = Math.min(min, sig.size());
        max = Math.max(max, sig.size());
      }
      if (allowedSignatures.isEmpty()) {
        min = 0;
        max = 0;
      }
      this.countRange = SqlOperandCountRanges.between(min, max);
    }

    @Override
    public boolean checkOperandTypes(SqlCallBinding callBinding, boolean throwOnFailure) {
      int count = callBinding.getOperandCount();
      for (List<RelDataType> sig : allowedSignatures) {
        if (sig.size() != count) continue;
        if (matchesSignature(callBinding, sig)) {
          return true;
        }
      }
      if (throwOnFailure) {
        throw callBinding.newValidationSignatureError();
      }
      return false;
    }

    private static boolean matchesSignature(SqlCallBinding callBinding, List<RelDataType> sig) {
      for (int i = 0; i < sig.size(); i++) {
        SqlNode operand = callBinding.operand(i);
        RelDataType actual = SqlTypeUtil.deriveType(callBinding, operand);
        RelDataType formal = sig.get(i);
        if (!matchesOperand(actual, formal)) {
          return false;
        }
      }
      return true;
    }

    /**
     * Match {@code actual} against {@code formal}.
     *
     * <ul>
     *   <li>UDT formal: accept same UDT class, OR plain CHARACTER (since PPL UDTs like
     *       EXPR_IP/EXPR_DATE serialize over the wire as strings; the runtime impl parses them).
     *   <li>Plain formal: accept any operand whose family matches the formal's family.
     * </ul>
     */
    private static boolean matchesOperand(RelDataType actual, RelDataType formal) {
      if (formal instanceof AbstractExprRelDataType<?> formalUdt) {
        if (actual.getClass() == formalUdt.getClass()) {
          return true;
        }
        // PPL UDTs are string-backed at the wire level — accept CHARACTER as a stand-in.
        return actual.getSqlTypeName().getFamily() == SqlTypeFamily.CHARACTER;
      }
      // Plain formal — family-based check.
      SqlTypeFamily formalFamily = formal.getSqlTypeName().getFamily();
      SqlTypeFamily actualFamily = actual.getSqlTypeName().getFamily();
      return formalFamily != null && formalFamily == actualFamily;
    }

    @Override
    public SqlOperandCountRange getOperandCountRange() {
      return countRange;
    }

    @Override
    public String getAllowedSignatures(SqlOperator op, String opName) {
      StringBuilder sb = new StringBuilder();
      for (int i = 0; i < allowedSignatures.size(); i++) {
        if (i > 0) sb.append("|");
        sb.append('[');
        List<RelDataType> sig = allowedSignatures.get(i);
        for (int j = 0; j < sig.size(); j++) {
          if (j > 0) sb.append(',');
          sb.append(typeName(sig.get(j)));
        }
        sb.append(']');
      }
      return sb.toString();
    }

    private static String typeName(RelDataType t) {
      if (t instanceof AbstractExprRelDataType<?> udt) {
        return udt.getUdt().name();
      }
      return t.getSqlTypeName().name();
    }
  }
}
