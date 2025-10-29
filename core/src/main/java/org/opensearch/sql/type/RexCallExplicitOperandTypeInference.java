/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.type;

import com.google.common.collect.ImmutableList;
import org.apache.calcite.rel.type.RelDataType;

public class RexCallExplicitOperandTypeInference implements RexCallTypeInference {
  // ~ Instance fields --------------------------------------------------------

  private final ImmutableList<RelDataType> paramTypes;

  // ~ Constructors -----------------------------------------------------------

  /** Use {@link org.apache.calcite.sql.type.InferTypes#explicit(java.util.List)}. */
  RexCallExplicitOperandTypeInference(ImmutableList<RelDataType> paramTypes) {
    this.paramTypes = paramTypes;
  }

  // ~ Methods ----------------------------------------------------------------

  @Override
  public void inferOperandTypes(
      RexCallBinding callBinding, RelDataType returnType, RelDataType[] operandTypes) {
    if (operandTypes.length != paramTypes.size()) {
      // This call does not match the inference strategy.
      // It's likely that we're just about to give a validation error.
      // Don't make a fuss, just give up.
      return;
    }
    @SuppressWarnings("all")
    RelDataType[] unused = paramTypes.toArray(operandTypes);
  }
}
