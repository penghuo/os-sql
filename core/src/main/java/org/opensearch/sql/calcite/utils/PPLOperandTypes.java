/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.utils;

import static org.opensearch.sql.calcite.utils.OpenSearchTypeFactory.TYPE_FACTORY;

import java.util.ArrayList;
import java.util.List;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.type.CompositeOperandTypeChecker;
import org.apache.calcite.sql.type.FamilyOperandTypeChecker;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.SqlTypeFamily;
import org.apache.calcite.sql.type.SqlTypeName;
import org.opensearch.sql.calcite.utils.OpenSearchTypeFactory.ExprUDT;
import org.opensearch.sql.expression.function.UDFOperandMetadata;

/**
 * This class contains common operand types for PPL functions. They are created by either wrapping a
 * {@link FamilyOperandTypeChecker} or a {@link CompositeOperandTypeChecker} with a {@link
 * UDFOperandMetadata}.
 */
public class PPLOperandTypes {
  // This class is not meant to be instantiated.
  private PPLOperandTypes() {}

  // Convenience RelDataType constants used to express UDF signatures via wrapUDT(...).
  // UDT-backed scalar types:
  public static final RelDataType DATE_UDT = TYPE_FACTORY.createUDT(ExprUDT.EXPR_DATE);
  public static final RelDataType TIME_UDT = TYPE_FACTORY.createUDT(ExprUDT.EXPR_TIME);
  public static final RelDataType TIMESTAMP_UDT = TYPE_FACTORY.createUDT(ExprUDT.EXPR_TIMESTAMP);
  public static final RelDataType IP_UDT = TYPE_FACTORY.createUDT(ExprUDT.EXPR_IP);
  public static final RelDataType BINARY_UDT = TYPE_FACTORY.createUDT(ExprUDT.EXPR_BINARY);
  // Plain SQL scalar types:
  public static final RelDataType BYTE_T = TYPE_FACTORY.createSqlType(SqlTypeName.TINYINT);
  public static final RelDataType SHORT_T = TYPE_FACTORY.createSqlType(SqlTypeName.SMALLINT);
  public static final RelDataType INTEGER_T = TYPE_FACTORY.createSqlType(SqlTypeName.INTEGER);
  public static final RelDataType LONG_T = TYPE_FACTORY.createSqlType(SqlTypeName.BIGINT);
  public static final RelDataType FLOAT_T = TYPE_FACTORY.createSqlType(SqlTypeName.REAL);
  public static final RelDataType DOUBLE_T = TYPE_FACTORY.createSqlType(SqlTypeName.DOUBLE);
  public static final RelDataType STRING_T = TYPE_FACTORY.createSqlType(SqlTypeName.VARCHAR);
  public static final RelDataType BOOLEAN_T = TYPE_FACTORY.createSqlType(SqlTypeName.BOOLEAN);
  public static final RelDataType ANY_T = TYPE_FACTORY.createSqlType(SqlTypeName.ANY);

  /** List of all scalar type signatures (single parameter each) */
  private static final List<List<RelDataType>> SCALAR_TYPES =
      List.of(
          // Numeric types
          List.of(BYTE_T),
          List.of(SHORT_T),
          List.of(INTEGER_T),
          List.of(LONG_T),
          List.of(FLOAT_T),
          List.of(DOUBLE_T),
          // String type
          List.of(STRING_T),
          // Boolean type
          List.of(BOOLEAN_T),
          // Temporal types
          List.of(DATE_UDT),
          List.of(TIME_UDT),
          List.of(TIMESTAMP_UDT),
          // Special scalar types
          List.of(IP_UDT),
          List.of(BINARY_UDT));

  /** Helper method to create scalar types with optional integer parameter */
  private static List<List<RelDataType>> createScalarWithOptionalInteger() {
    List<List<RelDataType>> result = new ArrayList<>(SCALAR_TYPES);

    // Add scalar + integer combinations
    SCALAR_TYPES.forEach(scalarType -> result.add(List.of(scalarType.get(0), INTEGER_T)));

    return result;
  }

  public static final UDFOperandMetadata NONE = UDFOperandMetadata.wrap(OperandTypes.family());
  public static final UDFOperandMetadata OPTIONAL_ANY =
      UDFOperandMetadata.wrap(
          (CompositeOperandTypeChecker)
              OperandTypes.family(SqlTypeFamily.ANY).or(OperandTypes.family()));
  public static final UDFOperandMetadata OPTIONAL_INTEGER =
      UDFOperandMetadata.wrap(
          (CompositeOperandTypeChecker) OperandTypes.INTEGER.or(OperandTypes.family()));
  public static final UDFOperandMetadata STRING =
      UDFOperandMetadata.wrap((FamilyOperandTypeChecker) OperandTypes.CHARACTER);
  public static final UDFOperandMetadata INTEGER =
      UDFOperandMetadata.wrap((FamilyOperandTypeChecker) OperandTypes.INTEGER);
  public static final UDFOperandMetadata NUMERIC =
      UDFOperandMetadata.wrap((FamilyOperandTypeChecker) OperandTypes.NUMERIC);

  public static final UDFOperandMetadata NUMERIC_OPTIONAL_STRING =
      UDFOperandMetadata.wrap(
          (CompositeOperandTypeChecker)
              OperandTypes.NUMERIC.or(
                  OperandTypes.family(SqlTypeFamily.NUMERIC, SqlTypeFamily.CHARACTER)));

  public static final UDFOperandMetadata ANY_OPTIONAL_INTEGER =
      UDFOperandMetadata.wrap(
          (CompositeOperandTypeChecker)
              OperandTypes.ANY.or(OperandTypes.family(SqlTypeFamily.ANY, SqlTypeFamily.INTEGER)));
  public static final UDFOperandMetadata ANY_OPTIONAL_STRING =
      UDFOperandMetadata.wrap(
          (CompositeOperandTypeChecker)
              OperandTypes.ANY.or(OperandTypes.family(SqlTypeFamily.ANY, SqlTypeFamily.CHARACTER)));
  public static final UDFOperandMetadata ANY_OPTIONAL_TIMESTAMP =
      UDFOperandMetadata.wrap(
          (CompositeOperandTypeChecker)
              OperandTypes.ANY.or(OperandTypes.family(SqlTypeFamily.ANY, SqlTypeFamily.TIMESTAMP)));
  public static final UDFOperandMetadata INTEGER_INTEGER =
      UDFOperandMetadata.wrap((FamilyOperandTypeChecker) OperandTypes.INTEGER_INTEGER);
  public static final UDFOperandMetadata STRING_STRING =
      UDFOperandMetadata.wrap((FamilyOperandTypeChecker) OperandTypes.CHARACTER_CHARACTER);
  public static final UDFOperandMetadata STRING_STRING_STRING =
      UDFOperandMetadata.wrap(
          OperandTypes.family(
              SqlTypeFamily.CHARACTER, SqlTypeFamily.CHARACTER, SqlTypeFamily.CHARACTER));
  public static final UDFOperandMetadata NUMERIC_NUMERIC =
      UDFOperandMetadata.wrap((FamilyOperandTypeChecker) OperandTypes.NUMERIC_NUMERIC);
  public static final UDFOperandMetadata STRING_INTEGER =
      UDFOperandMetadata.wrap(OperandTypes.family(SqlTypeFamily.CHARACTER, SqlTypeFamily.INTEGER));
  public static final UDFOperandMetadata STRING_STRING_INTEGER =
      UDFOperandMetadata.wrap(
          OperandTypes.family(
              SqlTypeFamily.CHARACTER, SqlTypeFamily.CHARACTER, SqlTypeFamily.INTEGER));

  public static final UDFOperandMetadata STRING_OR_STRING_INTEGER =
      UDFOperandMetadata.wrap(
          (CompositeOperandTypeChecker)
              OperandTypes.family(SqlTypeFamily.CHARACTER)
                  .or(OperandTypes.family(SqlTypeFamily.CHARACTER, SqlTypeFamily.INTEGER)));

  public static final UDFOperandMetadata STRING_STRING_INTEGER_INTEGER =
      UDFOperandMetadata.wrap(
          OperandTypes.family(
              SqlTypeFamily.CHARACTER,
              SqlTypeFamily.CHARACTER,
              SqlTypeFamily.INTEGER,
              SqlTypeFamily.INTEGER));

  public static final UDFOperandMetadata NUMERIC_STRING_OR_STRING_STRING =
      UDFOperandMetadata.wrap(
          (CompositeOperandTypeChecker)
              (OperandTypes.family(SqlTypeFamily.NUMERIC, SqlTypeFamily.STRING))
                  .or(OperandTypes.family(SqlTypeFamily.STRING, SqlTypeFamily.STRING)));

  // PERCENTILE_APPROX shape:
  //   user-side : percentile_approx(value, percent[, compression])    (2 or 3 args)
  //   visitor-side after appending type-name string for result coercion:
  //               percentile_approx(value, percent, <type>)            (3 args)
  //               percentile_approx(value, percent, compression, <type>) (4 args)
  // The visitor appends a trailing string literal carrying the field's SqlTypeName name (e.g.
  // "BIGINT") so the impl can coerce its double result to the declared return type. Without
  // accepting STRING (CHARACTER family) in the trailing slot, the SqlValidator round-trip would
  // try to coerce the string to NUMERIC at runtime and throw NumberFormatException.
  public static final UDFOperandMetadata NUMERIC_NUMERIC_OPTIONAL_NUMERIC =
      UDFOperandMetadata.wrap(
          (CompositeOperandTypeChecker)
              OperandTypes.NUMERIC_NUMERIC
                  .or(
                      OperandTypes.family(
                          SqlTypeFamily.NUMERIC, SqlTypeFamily.NUMERIC, SqlTypeFamily.NUMERIC))
                  .or(
                      OperandTypes.family(
                          SqlTypeFamily.NUMERIC, SqlTypeFamily.NUMERIC, SqlTypeFamily.CHARACTER))
                  .or(
                      OperandTypes.family(
                          SqlTypeFamily.NUMERIC,
                          SqlTypeFamily.NUMERIC,
                          SqlTypeFamily.NUMERIC,
                          SqlTypeFamily.CHARACTER)));
  public static final UDFOperandMetadata NUMERIC_NUMERIC_NUMERIC =
      UDFOperandMetadata.wrap(
          OperandTypes.family(SqlTypeFamily.NUMERIC, SqlTypeFamily.NUMERIC, SqlTypeFamily.NUMERIC));
  public static final UDFOperandMetadata NUMERIC_NUMERIC_NUMERIC_NUMERIC =
      UDFOperandMetadata.wrap(
          OperandTypes.family(
              SqlTypeFamily.NUMERIC,
              SqlTypeFamily.NUMERIC,
              SqlTypeFamily.NUMERIC,
              SqlTypeFamily.NUMERIC));

  // bin / WIDTH_BUCKET operand metadata. Uses wrapUDT so PPL UDTs (EXPR_DATE, EXPR_TIME,
  // EXPR_TIMESTAMP — VARCHAR-tagged) are accepted at the SqlValidator round-trip without falling
  // into the CHARACTER → DECIMAL coercion path that would otherwise emit
  // CAST(@timestamp AS DECIMAL) and trip Primitive.charToDecimalCast at runtime.
  // Variants:
  //   - numeric path: bin age span=10
  //   - PPL UDT date/time: bin @timestamp ... — slot 2 is INTEGER, slots 3/4 are STRING (interval
  //     diff representation) or matching UDT for SCOTT-schema unit tests.
  //   - SCOTT schema date/time: bare TIMESTAMP/DATE/TIME (validator family check).
  public static final UDFOperandMetadata WIDTH_BUCKET_OPERAND =
      UDFOperandMetadata.wrapUDT(
          List.of(
              // Numeric variant.
              List.of(BYTE_T, INTEGER_T, BYTE_T, BYTE_T),
              List.of(SHORT_T, INTEGER_T, SHORT_T, SHORT_T),
              List.of(INTEGER_T, INTEGER_T, INTEGER_T, INTEGER_T),
              List.of(LONG_T, INTEGER_T, LONG_T, LONG_T),
              List.of(FLOAT_T, INTEGER_T, FLOAT_T, FLOAT_T),
              List.of(DOUBLE_T, INTEGER_T, DOUBLE_T, DOUBLE_T),
              // PPL UDT path — slot 3 is STRING (interval-as-string) or matching UDT.
              List.of(TIMESTAMP_UDT, INTEGER_T, STRING_T, TIMESTAMP_UDT),
              List.of(TIMESTAMP_UDT, INTEGER_T, TIMESTAMP_UDT, TIMESTAMP_UDT),
              List.of(DATE_UDT, INTEGER_T, STRING_T, DATE_UDT),
              List.of(DATE_UDT, INTEGER_T, DATE_UDT, DATE_UDT),
              List.of(TIME_UDT, INTEGER_T, STRING_T, TIME_UDT),
              List.of(TIME_UDT, INTEGER_T, TIME_UDT, TIME_UDT)));

  public static final UDFOperandMetadata NUMERIC_NUMERIC_NUMERIC_NUMERIC_NUMERIC =
      UDFOperandMetadata.wrap(
          OperandTypes.family(
              SqlTypeFamily.NUMERIC,
              SqlTypeFamily.NUMERIC,
              SqlTypeFamily.NUMERIC,
              SqlTypeFamily.NUMERIC,
              SqlTypeFamily.NUMERIC));
  public static final UDFOperandMetadata STRING_OR_INTEGER_INTEGER_INTEGER =
      UDFOperandMetadata.wrap(
          (CompositeOperandTypeChecker)
              OperandTypes.family(
                      SqlTypeFamily.CHARACTER, SqlTypeFamily.INTEGER, SqlTypeFamily.INTEGER)
                  .or(
                      OperandTypes.family(
                          SqlTypeFamily.INTEGER, SqlTypeFamily.INTEGER, SqlTypeFamily.INTEGER)));

  // Accepts (), (DATE_UDT|TIME_UDT|TIMESTAMP_UDT|numeric). Uses wrapUDT so PPL UDTs (which report
  // as VARCHAR/CHARACTER family, not DATETIME) are accepted directly. Without wrapUDT, the
  // validator's CompositeOperandTypeChecker would coerce CHARACTER → NUMERIC via DECIMAL and the
  // pushdown script would later try to parse a date string as DECIMAL — see UNIX_TIMESTAMP.
  public static final UDFOperandMetadata OPTIONAL_DATE_OR_TIMESTAMP_OR_NUMERIC =
      UDFOperandMetadata.wrapUDT(
          List.of(
              List.of(),
              List.of(DATE_UDT),
              List.of(TIME_UDT),
              List.of(TIMESTAMP_UDT),
              List.of(BYTE_T),
              List.of(SHORT_T),
              List.of(INTEGER_T),
              List.of(LONG_T),
              List.of(FLOAT_T),
              List.of(DOUBLE_T)));

  public static final UDFOperandMetadata DATETIME_OR_STRING =
      UDFOperandMetadata.wrap(
          (CompositeOperandTypeChecker) OperandTypes.DATETIME.or(OperandTypes.CHARACTER));
  public static final UDFOperandMetadata TIME_OR_TIMESTAMP_OR_STRING =
      UDFOperandMetadata.wrap(
          (CompositeOperandTypeChecker)
              OperandTypes.CHARACTER.or(OperandTypes.TIME).or(OperandTypes.TIMESTAMP));
  public static final UDFOperandMetadata DATE_OR_TIMESTAMP_OR_STRING =
      UDFOperandMetadata.wrap(
          (CompositeOperandTypeChecker) OperandTypes.DATE_OR_TIMESTAMP.or(OperandTypes.CHARACTER));
  public static final UDFOperandMetadata DATETIME_OR_STRING_OR_INTEGER =
      UDFOperandMetadata.wrap(
          (CompositeOperandTypeChecker)
              OperandTypes.DATETIME.or(OperandTypes.CHARACTER).or(OperandTypes.INTEGER));

  // (DATETIME) | (DATETIME, INTEGER). Uses wrapUDT for UDT acceptance through SqlValidator.
  public static final UDFOperandMetadata DATETIME_OPTIONAL_INTEGER =
      UDFOperandMetadata.wrapUDT(
          List.of(
              List.of(DATE_UDT),
              List.of(TIME_UDT),
              List.of(TIMESTAMP_UDT),
              List.of(DATE_UDT, INTEGER_T),
              List.of(TIME_UDT, INTEGER_T),
              List.of(TIMESTAMP_UDT, INTEGER_T)));
  public static final UDFOperandMetadata ANY_DATETIME_OR_STRING =
      UDFOperandMetadata.wrap(
          (CompositeOperandTypeChecker)
              OperandTypes.family(SqlTypeFamily.ANY)
                  .or(OperandTypes.family(SqlTypeFamily.ANY, SqlTypeFamily.DATETIME))
                  .or(OperandTypes.family(SqlTypeFamily.ANY, SqlTypeFamily.STRING)));

  // (DATETIME, DATETIME). Uses wrapUDT so PPL UDTs (which report as VARCHAR/CHARACTER family,
  // not DATETIME) are accepted directly. Without wrapUDT, the SqlValidator round-trip rejects
  // EXPR_DATE/EXPR_TIME/EXPR_TIMESTAMP arguments with "Cannot apply 'ADDTIME' to arguments of
  // type 'ADDTIME(<EXPR_DATE>, <EXPR_DATE>)'".
  public static final UDFOperandMetadata DATETIME_DATETIME =
      UDFOperandMetadata.wrapUDT(
          List.of(
              List.of(DATE_UDT, DATE_UDT),
              List.of(DATE_UDT, TIME_UDT),
              List.of(DATE_UDT, TIMESTAMP_UDT),
              List.of(TIME_UDT, DATE_UDT),
              List.of(TIME_UDT, TIME_UDT),
              List.of(TIME_UDT, TIMESTAMP_UDT),
              List.of(TIMESTAMP_UDT, DATE_UDT),
              List.of(TIMESTAMP_UDT, TIME_UDT),
              List.of(TIMESTAMP_UDT, TIMESTAMP_UDT)));
  public static final UDFOperandMetadata DATETIME_OR_STRING_STRING =
      UDFOperandMetadata.wrap(
          (CompositeOperandTypeChecker)
              OperandTypes.family(SqlTypeFamily.DATETIME, SqlTypeFamily.CHARACTER)
                  .or(OperandTypes.CHARACTER_CHARACTER));
  public static final UDFOperandMetadata DATETIME_OR_STRING_DATETIME_OR_STRING =
      UDFOperandMetadata.wrap(
          (CompositeOperandTypeChecker)
              OperandTypes.CHARACTER_CHARACTER
                  .or(OperandTypes.family(SqlTypeFamily.DATETIME, SqlTypeFamily.DATETIME))
                  .or(OperandTypes.family(SqlTypeFamily.DATETIME, SqlTypeFamily.CHARACTER))
                  .or(OperandTypes.family(SqlTypeFamily.CHARACTER, SqlTypeFamily.DATETIME)));
  // (STRING, TIMESTAMP). Uses wrapUDT so EXPR_TIMESTAMP UDT (which reports as VARCHAR/CHARACTER
  // family, not TIMESTAMP family) is accepted. A plain family(CHARACTER, TIMESTAMP) check would
  // reject EXPR_TIMESTAMP arguments at the SqlValidator layer.
  public static final UDFOperandMetadata STRING_TIMESTAMP =
      UDFOperandMetadata.wrapUDT(List.of(List.of(STRING_T, TIMESTAMP_UDT)));
  // (STRING, DATETIME). Uses wrapUDT so PPL UDTs are accepted at the SqlValidator round-trip.
  public static final UDFOperandMetadata STRING_DATETIME =
      UDFOperandMetadata.wrapUDT(
          List.of(
              List.of(STRING_T, DATE_UDT),
              List.of(STRING_T, TIME_UDT),
              List.of(STRING_T, TIMESTAMP_UDT)));
  // (DATETIME, INTERVAL). Uses wrapUDT so PPL UDTs (which report as VARCHAR/CHARACTER family,
  // not DATETIME) are accepted directly. Without wrapUDT, a family(DATETIME, DATETIME_INTERVAL)
  // check rejects EXPR_DATE/EXPR_TIME/EXPR_TIMESTAMP at the SqlValidator round-trip with
  // "Cannot apply 'DATE_ADD' to arguments of type 'DATE_ADD(<EXPR_TIMESTAMP>, <INTERVAL DAY>)'".
  // The second slot uses ANY_T as a wildcard since interval qualifiers vary (INTERVAL DAY,
  // INTERVAL MONTH, etc.); PPLTypeChecker.typesMatch treats ANY in the expected slot as a
  // wildcard. UDTOperandMetadata.checkOperandTypes (the validator-side check) only validates
  // arity at present, so the round-trip accepts any 2-arg shape.
  public static final UDFOperandMetadata DATETIME_INTERVAL =
      UDFOperandMetadata.wrapUDT(
          List.of(
              List.of(DATE_UDT, ANY_T),
              List.of(TIME_UDT, ANY_T),
              List.of(TIMESTAMP_UDT, ANY_T),
              List.of(STRING_T, ANY_T)));

  // (DATETIME, INTERVAL) | (DATETIME, INTEGER). Used by ADDDATE/SUBDATE which accept either.
  // Includes STRING shapes so PPL frontend accepts string-date inputs (the runtime then parses
  // and reports "unsupported format" for malformed strings — what error-message tests expect).
  public static final UDFOperandMetadata DATETIME_INTERVAL_OR_INTEGER =
      UDFOperandMetadata.wrapUDT(
          List.of(
              List.of(DATE_UDT, ANY_T),
              List.of(TIME_UDT, ANY_T),
              List.of(TIMESTAMP_UDT, ANY_T),
              List.of(STRING_T, ANY_T),
              List.of(DATE_UDT, INTEGER_T),
              List.of(TIME_UDT, INTEGER_T),
              List.of(TIMESTAMP_UDT, INTEGER_T),
              List.of(STRING_T, INTEGER_T)));
  // (TIME, TIME). Uses wrapUDT so EXPR_TIME UDT (which reports as VARCHAR/CHARACTER, not TIME)
  // is accepted directly. Without wrapUDT, the validator coerces VARCHAR→DECIMAL and the
  // pushdown emits CAST(time AS DECIMAL) which fails at runtime parsing the time string.
  public static final UDFOperandMetadata TIME_TIME =
      UDFOperandMetadata.wrapUDT(List.of(List.of(TIME_UDT, TIME_UDT)));

  public static final UDFOperandMetadata TIMESTAMP_OR_STRING_STRING_STRING =
      UDFOperandMetadata.wrap(
          (CompositeOperandTypeChecker)
              OperandTypes.family(
                      SqlTypeFamily.TIMESTAMP, SqlTypeFamily.CHARACTER, SqlTypeFamily.CHARACTER)
                  .or(
                      OperandTypes.family(
                          SqlTypeFamily.CHARACTER,
                          SqlTypeFamily.CHARACTER,
                          SqlTypeFamily.CHARACTER)));
  public static final UDFOperandMetadata STRING_INTEGER_DATETIME_OR_STRING =
      UDFOperandMetadata.wrap(
          (CompositeOperandTypeChecker)
              OperandTypes.family(
                      SqlTypeFamily.CHARACTER, SqlTypeFamily.INTEGER, SqlTypeFamily.CHARACTER)
                  .or(
                      OperandTypes.family(
                          SqlTypeFamily.CHARACTER, SqlTypeFamily.INTEGER, SqlTypeFamily.DATETIME)));
  public static final UDFOperandMetadata INTERVAL_DATETIME_DATETIME =
      UDFOperandMetadata.wrap(
          (CompositeOperandTypeChecker)
              OperandTypes.family(
                      SqlTypeFamily.CHARACTER, SqlTypeFamily.DATETIME, SqlTypeFamily.DATETIME)
                  .or(
                      OperandTypes.family(
                          SqlTypeFamily.CHARACTER, SqlTypeFamily.CHARACTER, SqlTypeFamily.DATETIME))
                  .or(
                      OperandTypes.family(
                          SqlTypeFamily.CHARACTER, SqlTypeFamily.DATETIME, SqlTypeFamily.CHARACTER))
                  .or(
                      OperandTypes.family(
                          SqlTypeFamily.CHARACTER,
                          SqlTypeFamily.CHARACTER,
                          SqlTypeFamily.CHARACTER)));

  /**
   * Operand type checker that accepts any scalar type. This includes numeric types, strings,
   * booleans, datetime types, and special scalar types like IP and BINARY. Excludes complex types
   * like arrays, structs, and maps.
   */
  public static final UDFOperandMetadata ANY_SCALAR = UDFOperandMetadata.wrapUDT(SCALAR_TYPES);

  /**
   * Operand type checker that accepts any scalar type with an optional integer argument. This is
   * used for aggregation functions that take a field and an optional limit/size parameter.
   */
  public static final UDFOperandMetadata ANY_SCALAR_OPTIONAL_INTEGER =
      UDFOperandMetadata.wrapUDT(createScalarWithOptionalInteger());
}
