/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.type;

import static java.util.Objects.requireNonNull;

import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.Map;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexCorrelVariable;
import org.apache.calcite.rex.RexDynamicParam;
import org.apache.calcite.rex.RexFieldAccess;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexLocalRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexOver;
import org.apache.calcite.rex.RexPatternFieldRef;
import org.apache.calcite.rex.RexRangeRef;
import org.apache.calcite.rex.RexSubQuery;
import org.apache.calcite.rex.RexTableInputRef;
import org.apache.calcite.rex.RexVisitorImpl;
import org.apache.calcite.runtime.CalciteContextException;
import org.apache.calcite.runtime.Resources;
import org.apache.calcite.sql.SqlUtil;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.validate.SqlValidatorException;
import org.opensearch.sql.type.coercion.RexCallTypeCoercion;

public class RexCallValidatorImpl {

  private final Map<RexNode, RelDataType> nodeToTypeMap = new IdentityHashMap<>();

  private final RelDataTypeFactory typeFactory;

  public RexCallValidatorImpl(RelDataTypeFactory typeFactory) {
    this.typeFactory = typeFactory;
  }

  public CalciteContextException newValidationError(Resources.ExInst<SqlValidatorException> e) {
    return SqlUtil.newContextException(SqlParserPos.ZERO, e);
  }

  public RexCallTypeCoercion getTypeCoercion() {
    return null;
  }

  public RelDataType deriveType(RexNode expr) {
    requireNonNull(expr, "expr");

    RelDataType type = nodeToTypeMap.get(expr);
    if (type != null) {
      return type;
    }

    final RelDataType namespaceType = getNamespaceType(expr);
    if (namespaceType != null) {
      return namespaceType;
    }

    type = deriveTypeImpl(expr);
    requireNonNull(type, "RexCallValidatorImpl.deriveTypeImpl returned null");
    setValidatedNodeType(expr, type);
    return type;
  }

  protected RelDataType deriveTypeImpl(RexNode operand) {
    RelDataType type = operand.accept(new DeriveRexTypeVisitor());
    return type;
  }

  protected void setValidatedNodeType(RexNode node, RelDataType type) {
    nodeToTypeMap.put(requireNonNull(node, "node"), requireNonNull(type, "type"));
  }

  public void removeValidatedNodeType(RexNode node) {
    nodeToTypeMap.remove(node);
  }

  protected RelDataType getNamespaceType(RexNode node) {
    return null;
  }

  private class DeriveRexTypeVisitor extends RexVisitorImpl<RelDataType> {
    DeriveRexTypeVisitor() {
      super(true);
    }

    @Override
    public RelDataType visitCall(RexCall call) {
      for (RexNode operand : call.getOperands()) {
        deriveType(operand);
      }
      RelDataType type = call.getType();
      if (type == null) {
        org.apache.calcite.rex.RexCallBinding binding =
            new org.apache.calcite.rex.RexCallBinding(
                typeFactory, call.getOperator(), call.getOperands(), Collections.emptyList());
        type = call.getOperator().inferReturnType(binding);
      }
      return requireNonNull(type, () -> "Unable to derive type for RexCall: " + call);
    }

    @Override
    public RelDataType visitInputRef(RexInputRef inputRef) {
      return requireNonNull(inputRef.getType(), () -> "RexInputRef has null type: " + inputRef);
    }

    @Override
    public RelDataType visitLiteral(RexLiteral literal) {
      return requireNonNull(literal.getType(), () -> "RexLiteral has null type: " + literal);
    }

    @Override
    public RelDataType visitLocalRef(RexLocalRef localRef) {
      return requireNonNull(localRef.getType(), () -> "RexLocalRef has null type: " + localRef);
    }

    @Override
    public RelDataType visitOver(RexOver over) {
      return requireNonNull(over.getType(), () -> "RexOver has null type: " + over);
    }

    @Override
    public RelDataType visitCorrelVariable(RexCorrelVariable correlVariable) {
      return requireNonNull(
          correlVariable.getType(), () -> "RexCorrelVariable has null type: " + correlVariable);
    }

    @Override
    public RelDataType visitDynamicParam(RexDynamicParam dynamicParam) {
      return requireNonNull(
          dynamicParam.getType(), () -> "RexDynamicParam has null type: " + dynamicParam);
    }

    @Override
    public RelDataType visitRangeRef(RexRangeRef rangeRef) {
      return requireNonNull(rangeRef.getType(), () -> "RexRangeRef has null type: " + rangeRef);
    }

    @Override
    public RelDataType visitFieldAccess(RexFieldAccess fieldAccess) {
      return requireNonNull(
          fieldAccess.getType(), () -> "RexFieldAccess has null type: " + fieldAccess);
    }

    @Override
    public RelDataType visitSubQuery(RexSubQuery subQuery) {
      return requireNonNull(subQuery.getType(), () -> "RexSubQuery has null type: " + subQuery);
    }

    @Override
    public RelDataType visitTableInputRef(RexTableInputRef fieldRef) {
      return requireNonNull(
          fieldRef.getType(), () -> "RexTableInputRef has null type: " + fieldRef);
    }

    @Override
    public RelDataType visitPatternFieldRef(RexPatternFieldRef fieldRef) {
      return requireNonNull(
          fieldRef.getType(), () -> "RexPatternFieldRef has null type: " + fieldRef);
    }
  }
}
