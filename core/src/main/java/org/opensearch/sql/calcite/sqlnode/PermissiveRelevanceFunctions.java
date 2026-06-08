/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.sqlnode;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.SqlCallBinding;
import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOperandCountRange;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlOperatorBinding;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.type.SqlOperandCountRanges;
import org.apache.calcite.sql.type.SqlOperandTypeChecker;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.validate.SqlNameMatcher;

/**
 * Permissive variants of PPL relevance UDFs whose original {@link
 * org.apache.calcite.sql.type.CompositeOperandTypeChecker} declarations reject single-MAP operands
 * during validator-side type checks. The originals in {@code PPLBuiltinOperators} are sized for the
 * v2 RexNode-construction path which bypasses operand-type validation; the SqlNode path runs
 * through the validator, so we need a checker that accepts the call shape PPL emits (typically one
 * MAP("query", "<text>") arg).
 *
 * <p>These wrappers don't carry an implementor — they're never executed (relevance queries are only
 * meaningful when pushed down to OpenSearch). The validator just needs to accept the call and the
 * SqlToRelConverter will produce a RexCall against this operator that the OpenSearch pushdown
 * planner can recognize and rewrite.
 */
public final class PermissiveRelevanceFunctions {

  private PermissiveRelevanceFunctions() {}

  /** Operand checker that accepts any number of arguments without type-checking them. */
  private static final SqlOperandTypeChecker ANY_VARIADIC =
      new SqlOperandTypeChecker() {
        @Override
        public boolean checkOperandTypes(SqlCallBinding callBinding, boolean throwOnFailure) {
          return true;
        }

        @Override
        public SqlOperandCountRange getOperandCountRange() {
          return SqlOperandCountRanges.from(1);
        }

        @Override
        public String getAllowedSignatures(SqlOperator op, String opName) {
          return opName + "(<MAP> [, <MAP>...])";
        }
      };

  /** {@code query_string(MAP('query', text), [MAP(opt, val), ...])} — variadic MAP. */
  public static final SqlOperator QUERY_STRING = makePermissive("query_string");

  public static final SqlOperator SIMPLE_QUERY_STRING = makePermissive("simple_query_string");
  public static final SqlOperator MATCH = makePermissive("match");
  public static final SqlOperator MULTI_MATCH = makePermissive("multi_match");
  public static final SqlOperator MATCH_PHRASE = makePermissive("match_phrase");
  public static final SqlOperator MATCH_BOOL_PREFIX = makePermissive("match_bool_prefix");
  public static final SqlOperator MATCH_PHRASE_PREFIX = makePermissive("match_phrase_prefix");

  private static SqlOperator makePermissive(String name) {
    return new SqlFunction(
        name,
        SqlKind.OTHER_FUNCTION,
        ReturnTypes.BOOLEAN,
        /* operandTypeInference */ null,
        ANY_VARIADIC,
        SqlFunctionCategory.USER_DEFINED_FUNCTION) {
      @Override
      public RelDataType inferReturnType(SqlOperatorBinding opBinding) {
        return opBinding.getTypeFactory().createSqlType(SqlTypeName.BOOLEAN);
      }

      @Override
      public boolean isDeterministic() {
        return false;
      }
    };
  }

  /** Match the unresolved-function lookup with case-insensitive name matching. */
  public static SqlOperator lookupByName(
      String name, SqlNameMatcher nameMatcher, SqlNode... operands) {
    if (nameMatcher.matches(name, "query_string")) return QUERY_STRING;
    if (nameMatcher.matches(name, "simple_query_string")) return SIMPLE_QUERY_STRING;
    if (nameMatcher.matches(name, "match")) return MATCH;
    if (nameMatcher.matches(name, "multi_match")) return MULTI_MATCH;
    if (nameMatcher.matches(name, "match_phrase")) return MATCH_PHRASE;
    if (nameMatcher.matches(name, "match_bool_prefix")) return MATCH_BOOL_PREFIX;
    if (nameMatcher.matches(name, "match_phrase_prefix")) return MATCH_PHRASE_PREFIX;
    return null;
  }
}
