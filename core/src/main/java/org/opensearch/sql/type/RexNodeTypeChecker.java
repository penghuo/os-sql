/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.type;

import java.util.function.BiFunction;
import org.apache.calcite.sql.SqlOperandCountRange;
import org.apache.calcite.sql.SqlOperator;
import org.checkerframework.checker.nullness.qual.Nullable;

public interface RexNodeTypeChecker {

  boolean checkOperandTypes(RexCallBinding callBinding, boolean throwOnFailure);

  SqlOperandCountRange getOperandCountRange();

  /**
   * Returns a string describing the allowed formal signatures of a call, e.g. "SUBSTR(VARCHAR,
   * INTEGER, INTEGER)".
   *
   * @param op the operator being checked
   * @param opName name to use for the operator in case of aliasing
   * @return generated string
   */
  String getAllowedSignatures(SqlOperator op, String opName);

  /** Returns the strategy for making the arguments have consistency types. */
  default Consistency getConsistency() {
    return Consistency.NONE;
  }

  /** Returns a copy of this checker with the given signature generator. */
  default RexCallCompositeOperandTypeChecker withGenerator(
      BiFunction<SqlOperator, String, String> signatureGenerator) {
    // We should support for all subclasses but don't yet.
    throw new UnsupportedOperationException("withGenerator");
  }

  /** Returns whether the {@code i}th operand is optional. */
  default boolean isOptional(int i) {
    return false;
  }

  /**
   * Returns whether the list of parameters is fixed-length. In standard SQL, user-defined functions
   * are fixed-length.
   *
   * <p>If true, the validator should expand calls, supplying a {@code DEFAULT} value for each
   * parameter for which an argument is not supplied.
   */
  default boolean isFixedParameters() {
    return false;
  }

  /** Converts this type checker to a type inference; returns null if not possible. */
  default @Nullable RexCallTypeInference typeInference() {
    return null;
  }

  /** Strategy used to make arguments consistent. */
  enum Consistency {
    /** Do not try to make arguments consistent. */
    NONE,
    /**
     * Make arguments of consistent type using comparison semantics. Character values are implicitly
     * converted to numeric, date-time, interval or boolean.
     */
    COMPARE,
    /** Convert all arguments to the least restrictive type. */
    LEAST_RESTRICTIVE
  }
}
