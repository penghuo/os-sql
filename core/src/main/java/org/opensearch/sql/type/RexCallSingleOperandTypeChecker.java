/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.type;

import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlOperandCountRange;
import org.apache.calcite.sql.type.SqlOperandCountRanges;

public interface RexCallSingleOperandTypeChecker extends RexNodeTypeChecker {
  boolean checkSingleOperandType(
      RexCallBinding callBinding, RexNode operand, int iFormalOperand, boolean throwOnFailure);

  @Override
  default boolean checkOperandTypes(RexCallBinding callBinding, boolean throwOnFailure) {
    return checkSingleOperandType(callBinding, callBinding.operand(0), 0, throwOnFailure);
  }

  @Override
  default SqlOperandCountRange getOperandCountRange() {
    return SqlOperandCountRanges.of(1);
  }

  /** Composes this with another single-operand checker using AND. */
  default RexCallSingleOperandTypeChecker and(RexCallSingleOperandTypeChecker checker) {
    return OperandTypes.and(this, checker);
  }

  /** Composes this with another single-operand checker using OR. */
  default RexCallSingleOperandTypeChecker or(RexCallSingleOperandTypeChecker checker) {
    return OperandTypes.or(this, checker);
  }
}
