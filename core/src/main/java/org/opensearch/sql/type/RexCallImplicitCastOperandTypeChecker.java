/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.type;

import org.apache.calcite.sql.type.CompositeOperandTypeChecker;
import org.apache.calcite.sql.type.SqlTypeFamily;

public interface RexCallImplicitCastOperandTypeChecker {
  /**
   * Checks the types of an operator's all operands, but without type coercion. This is mainly used
   * as a pre-check when this checker is included as one of the rules in {@link
   * CompositeOperandTypeChecker} and the composite predicate is `OR`.
   *
   * @param callBinding description of the call to be checked
   * @param throwOnFailure whether to throw an exception if check fails (otherwise returns false in
   *     that case)
   * @return whether check succeeded
   */
  boolean checkOperandTypesWithoutTypeCoercion(RexCallBinding callBinding, boolean throwOnFailure);

  /**
   * Get the operand SqlTypeFamily of formal index {@code iFormalOperand}. This is mainly used to
   * get the operand SqlTypeFamily when this checker is included as one of the rules in {@link
   * CompositeOperandTypeChecker} and the composite predicate is `SEQUENCE`.
   *
   * @param iFormalOperand the formal operand index.
   * @return SqlTypeFamily of the operand.
   */
  SqlTypeFamily getOperandSqlTypeFamily(int iFormalOperand);
}
