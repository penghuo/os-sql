/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.type;

import java.util.List;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.runtime.CalciteException;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlOperator;

public class RexCallBinding {

  private final RexCallValidatorImpl validator;

  private final RexCall call;

  public RexCallBinding(RexCallValidatorImpl validator, RexCall call) {
    this.validator = validator;
    this.call = call;
  }

  public SqlOperator getOperator() {
    return call.getOperator();
  }

  public RexCall getCall() {
    return call;
  }

  public SqlKind getKind() {
    return call.getOperator().getKind();
  }

  public RexNode operand(int index) {
    return call.getOperands().get(index);
  }

  public List<RexNode> operands() {
    return call.getOperands();
  }

  public RelDataType getOperandType(int ordinal) {
    return call.getOperands().get(ordinal).getType();
  }

  public int getOperandCount() {
    return call.getOperands().size();
  }

  public RexCallValidatorImpl getValidator() {
    return validator;
  }

  public CalciteException newValidationSignatureError() {
    return new CalciteException("newValidationSignatureError", null);
  }

  public boolean isTypeCoercionEnabled() {
    return true;
  }
}
