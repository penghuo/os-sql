/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.expression.function;

import java.util.List;
import org.apache.calcite.rel.type.RelDataType;
import org.opensearch.sql.type.RexNodeTypeChecker;

public record NewCalciteFunctionSignature(
    FunctionName functionName, RexNodeTypeChecker typeChecker) {

  public boolean match(FunctionName functionName, List<RelDataType> argTypes) {
    if (!functionName.equals(this.functionName())) return false;
    // For complex type checkers (e.g., OperandTypes.COMPARABLE_UNORDERED_COMPARABLE_UNORDERED),
    // the typeChecker will be null because only simple family-based type checks are currently
    // supported.
    if (typeChecker == null) return true;
    return typeChecker.checkOperandTypes(argTypes);
  }
}
