/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.type;

import org.apache.calcite.rel.type.RelDataType;

public interface RexCallTypeInference {
  // ~ Methods ----------------------------------------------------------------

  /**
   * Infers any unknown operand types.
   *
   * @param callBinding description of the call being analyzed
   * @param returnType the type known or inferred for the result of the call
   * @param operandTypes receives the inferred types for all operands
   */
  void inferOperandTypes(
      RexCallBinding callBinding, RelDataType returnType, RelDataType[] operandTypes);
}
