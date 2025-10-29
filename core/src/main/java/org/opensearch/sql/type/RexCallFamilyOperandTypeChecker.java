/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.type;

import static org.apache.calcite.util.Static.RESOURCE;

import com.google.common.collect.ImmutableList;
import java.util.List;
import java.util.function.Predicate;
import org.apache.calcite.linq4j.Ord;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlOperandCountRange;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlUtil;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.SqlOperandCountRanges;
import org.apache.calcite.sql.type.SqlTypeFamily;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.Util;
import org.opensearch.sql.type.coercion.RexCallTypeCoercion;

public class RexCallFamilyOperandTypeChecker
    implements RexCallSingleOperandTypeChecker, RexCallImplicitCastOperandTypeChecker {

  protected final ImmutableList<SqlTypeFamily> families;
  protected final Predicate<Integer> optional;

  // ~ Constructors -----------------------------------------------------------

  /** Package private. Create using {@link OperandTypes#family}. */
  RexCallFamilyOperandTypeChecker(List<SqlTypeFamily> families, Predicate<Integer> optional) {
    this.families = ImmutableList.copyOf(families);
    this.optional = optional;
  }

  // ~ Methods ----------------------------------------------------------------

  @Override
  public boolean isOptional(int i) {
    return optional.test(i);
  }

  @Override
  public boolean checkSingleOperandType(
      RexCallBinding callBinding, RexNode node, int iFormalOperand, boolean throwOnFailure) {
    Util.discard(iFormalOperand);
    if (families.size() != 1) {
      throw new IllegalStateException(
          "Cannot use as SqlSingleOperandTypeChecker without exactly one family");
    }
    return checkSingleOperandType(
        callBinding, node, iFormalOperand, families.get(0), throwOnFailure);
  }

  /**
   * Helper function used by {@link #checkSingleOperandType(RexCallBinding, RexNode, int, boolean)},
   * {@link #checkOperandTypesWithoutTypeCoercion(RexCallBinding, boolean)}, and {@link
   * #checkOperandTypes(RexCallBinding, boolean)}.
   */
  protected boolean checkSingleOperandType(
      RexCallBinding callBinding,
      RexNode operand,
      int iFormalOperand,
      SqlTypeFamily family,
      boolean throwOnFailure) {
    Util.discard(iFormalOperand);
    switch (family) {
      case ANY:
        final RelDataType type = callBinding.getValidator().deriveType(operand);
        SqlTypeName typeName = type.getSqlTypeName();

        if (typeName == SqlTypeName.CURSOR) {
          // We do not allow CURSOR operands, even for ANY
          if (throwOnFailure) {
            throw callBinding.newValidationSignatureError();
          }
          return false;
        }
        // fall through
      case IGNORE:
        // no need to check
        return true;
      default:
        break;
    }

    if (RexUtils.isNullLiteral(operand, false)) {
      if (callBinding.isTypeCoercionEnabled()) {
        return true;
      } else if (throwOnFailure) {
        throw callBinding.getValidator().newValidationError(RESOURCE.nullIllegal());
      } else {
        return false;
      }
    }
    RelDataType type = callBinding.getValidator().deriveType(operand);
    SqlTypeName typeName = type.getSqlTypeName();

    // Pass type checking for operators if it's of type 'ANY'.
    if (typeName.getFamily() == SqlTypeFamily.ANY) {
      return true;
    }

    if (!family.getTypeNames().contains(typeName)) {
      if (throwOnFailure) {
        throw callBinding.newValidationSignatureError();
      }
      return false;
    }
    return true;
  }

  @Override
  public boolean checkOperandTypes(RexCallBinding callBinding, boolean throwOnFailure) {
    if (families.size() != callBinding.getOperandCount()) {
      // assume this is an inapplicable sub-rule of a composite rule;
      // don't throw
      return false;
    }
    for (Ord<RexNode> op : Ord.zip(callBinding.operands())) {
      if (!checkSingleOperandType(callBinding, op.e, op.i, families.get(op.i), false)) {
        // try to coerce type if it is allowed.
        boolean coerced = false;
        if (callBinding.isTypeCoercionEnabled()) {
          RexCallTypeCoercion typeCoercion = callBinding.getValidator().getTypeCoercion();
          ImmutableList.Builder<RelDataType> builder = ImmutableList.builder();
          for (int i = 0; i < callBinding.getOperandCount(); i++) {
            builder.add(callBinding.getOperandType(i));
          }
          ImmutableList<RelDataType> dataTypes = builder.build();
          coerced = typeCoercion.builtinFunctionCoercion(callBinding, dataTypes, families);
        }
        // re-validate the new nodes type.
        for (Ord<RexNode> op1 : Ord.zip(callBinding.operands())) {
          if (!checkSingleOperandType(
              callBinding, op1.e, op1.i, families.get(op1.i), throwOnFailure)) {
            return false;
          }
        }
        return coerced;
      }
    }
    return true;
  }

  @Override
  public boolean checkOperandTypesWithoutTypeCoercion(
      RexCallBinding callBinding, boolean throwOnFailure) {
    if (families.size() != callBinding.getOperandCount()) {
      // assume this is an inapplicable sub-rule of a composite rule;
      // don't throw exception.
      return false;
    }

    for (Ord<RexNode> op : Ord.zip(callBinding.operands())) {
      if (!checkSingleOperandType(callBinding, op.e, op.i, families.get(op.i), throwOnFailure)) {
        return false;
      }
    }
    return true;
  }

  @Override
  public SqlTypeFamily getOperandSqlTypeFamily(int iFormalOperand) {
    return families.get(iFormalOperand);
  }

  @Override
  public SqlOperandCountRange getOperandCountRange() {
    final int max = families.size();
    int min = max;
    while (min > 0 && optional.test(min - 1)) {
      --min;
    }
    return SqlOperandCountRanges.between(min, max);
  }

  @Override
  public String getAllowedSignatures(SqlOperator op, String opName) {
    return SqlUtil.getAliasedSignature(op, opName, families);
  }
}
