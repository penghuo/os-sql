/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.type.coercion;

import static java.util.Objects.requireNonNull;

import java.math.BigDecimal;
import java.util.AbstractList;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.type.SqlOperandMetadata;
import org.apache.calcite.sql.type.SqlTypeFamily;
import org.apache.calcite.sql.type.SqlTypeUtil;
import org.apache.calcite.util.Util;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.opensearch.sql.type.RexCallBinding;
import org.opensearch.sql.type.RexCallValidatorImpl;
import org.opensearch.sql.type.RexUtils;

public class RexCallTypeCoercionImpl extends AbstractRexCallTypeCoercion {

  public RexCallTypeCoercionImpl(
      RelDataTypeFactory typeFactory, RexCallValidatorImpl validator, RexBuilder rexBuilder) {
    super(typeFactory, validator, rexBuilder);
  }

  /**
   * Coerces operands in binary arithmetic expressions to NUMERIC types.
   *
   * <p>For binary arithmetic operators like [+, -, *, /, %]: If the operand is VARCHAR, coerce it
   * to data type of the other operand if its data type is NUMERIC; If the other operand is DECIMAL,
   * coerce the STRING operand to max precision/scale DECIMAL.
   */
  @Override
  public boolean binaryArithmeticCoercion(RexCallBinding binding) {
    // Assume the operator has NUMERIC family operand type checker.
    SqlOperator operator = binding.getOperator();
    SqlKind kind = operator.getKind();
    boolean coerced = false;
    // Binary operator
    if (binding.getOperandCount() == 2) {
      final RelDataType type1 = binding.getOperandType(0);
      final RelDataType type2 = binding.getOperandType(1);
      // Special case for datetime + interval or datetime - interval
      if (kind == SqlKind.PLUS || kind == SqlKind.MINUS) {
        if (SqlTypeUtil.isInterval(type1) || SqlTypeUtil.isInterval(type2)) {
          return false;
        }
      }
      // Binary arithmetic operator like: + - * / %
      if (kind.belongsTo(SqlKind.BINARY_ARITHMETIC)) {
        coerced = binaryArithmeticWithStrings(binding, type1, type2);
      }
    }
    return coerced;
  }

  /** For NUMERIC and STRING operands, cast STRING to data type of the other operand. */
  protected boolean binaryArithmeticWithStrings(
      RexCallBinding binding, RelDataType left, RelDataType right) {
    // For expression "NUMERIC <OP> CHARACTER",
    // PostgreSQL and MS-SQL coerce the CHARACTER operand to NUMERIC,
    // i.e. for '9':VARCHAR(1) / 2: INT, '9' would be coerced to INTEGER,
    // while for '9':VARCHAR(1) / 3.3: DOUBLE, '9' would be coerced to DOUBLE.
    // They do not allow both CHARACTER operands for binary arithmetic operators.

    // MySQL and Oracle would coerce all the string operands to DOUBLE.

    // Keep sync with PostgreSQL and MS-SQL because their behaviors are more in
    // line with the SQL standard.
    if (SqlTypeUtil.isString(left) && SqlTypeUtil.isNumeric(right)) {
      // If the numeric operand is DECIMAL type, coerce the STRING operand to
      // max precision/scale DECIMAL.
      if (SqlTypeUtil.isDecimal(right)) {
        right = SqlTypeUtil.getMaxPrecisionScaleDecimal(factory);
      }
      return coerceOperandType(binding.getCall(), 0, right);
    } else if (SqlTypeUtil.isNumeric(left) && SqlTypeUtil.isString(right)) {
      if (SqlTypeUtil.isDecimal(left)) {
        left = SqlTypeUtil.getMaxPrecisionScaleDecimal(factory);
      }
      return coerceOperandType(binding.getCall(), 1, left);
    }
    return false;
  }

  /**
   * Coerces operands in binary comparison expressions.
   *
   * <p>Rules:
   *
   * <ul>
   *   <li>For EQUALS(=) operator: 1. If operands are BOOLEAN and NUMERIC, evaluate `1=true` and
   *       `0=false` all to be true; 2. If operands are datetime and string, do nothing because the
   *       SqlToRelConverter already makes the type coercion;
   *   <li>For binary comparison [=, &gt;, &gt;=, &lt;, &lt;=]: try to find the common type, i.e. "1
   *       &gt; '1'" will be converted to "1 &gt; 1";
   *   <li>For BETWEEN operator, find the common comparison data type of all the operands, the
   *       common type is deduced from left to right, i.e. for expression "A between B and C", finds
   *       common comparison type D between A and B then common comparison type E between D and C as
   *       the final common type.
   * </ul>
   */
  @Override
  public boolean binaryComparisonCoercion(RexCallBinding binding) {
    SqlOperator operator = binding.getOperator();
    SqlKind kind = operator.getKind();
    int operandCnt = binding.getOperandCount();
    boolean coerced = false;
    // Binary operator
    if (operandCnt == 2) {
      final RelDataType type1 = binding.getOperandType(0);
      final RelDataType type2 = binding.getOperandType(1);
      // EQUALS(=) NOT_EQUALS(<>)
      if (kind.belongsTo(SqlKind.BINARY_EQUALITY)) {
        // STRING and datetime
        coerced = dateTimeStringEquality(binding, type1, type2) || coerced;
        // BOOLEAN and NUMERIC
        // BOOLEAN and literal
        coerced = booleanEquality(binding, type1, type2) || coerced;
      }
      // Binary comparison operator like: = > >= < <=
      if (kind.belongsTo(SqlKind.BINARY_COMPARISON)) {
        final RelDataType commonType = commonTypeForBinaryComparison(type1, type2);
        if (null != commonType) {
          coerced = coerceOperandsType(binding.getCall(), commonType);
        }
      }
    }
    // Infix operator like: BETWEEN
    if (kind == SqlKind.BETWEEN) {
      final List<RelDataType> operandTypes =
          Util.range(operandCnt).stream().map(binding::getOperandType).collect(Collectors.toList());
      final RelDataType commonType = commonTypeForComparison(operandTypes);
      if (null != commonType) {
        coerced = coerceOperandsType(binding.getCall(), commonType);
      }
    }
    return coerced;
  }

  /**
   * Finds the common type for binary comparison when the size of operands {@code dataTypes} is more
   * than 2. If there are N(more than 2) operands, finds the common type between two operands from
   * left to right:
   *
   * <p>Rules:
   *
   * <pre>
   *   type1     type2    type3
   *    |         |        |
   *    +- type4 -+        |
   *         |             |
   *         +--- type5 ---+
   * </pre>
   *
   * For operand data types (type1, type2, type3), deduce the common type type4 from type1 and
   * type2, then common type type5 from type4 and type3.
   */
  protected @Nullable RelDataType commonTypeForComparison(List<RelDataType> dataTypes) {
    assert dataTypes.size() > 2;
    final RelDataType type1 = dataTypes.get(0);
    final RelDataType type2 = dataTypes.get(1);
    // No need to do type coercion if all the data types have the same type name.
    boolean allWithSameName = SqlTypeUtil.sameNamedType(type1, type2);
    for (int i = 2; i < dataTypes.size() && allWithSameName; i++) {
      allWithSameName = SqlTypeUtil.sameNamedType(dataTypes.get(i - 1), dataTypes.get(i));
    }
    if (allWithSameName) {
      return null;
    }

    RelDataType commonType = commonTypeForBinaryComparison(type1, type2);
    for (int i = 2; i < dataTypes.size() && commonType != null; i++) {
      commonType = commonTypeForBinaryComparison(commonType, dataTypes.get(i));
    }
    return commonType;
  }

  /**
   * Datetime and STRING equality: cast STRING type to datetime type, SqlToRelConverter already
   * makes the conversion but we still keep this interface overridable so user can have their custom
   * implementation.
   */
  protected boolean dateTimeStringEquality(
      RexCallBinding binding, RelDataType left, RelDataType right) {
    // REVIEW Danny 2018-05-23 we do not need to coerce type for EQUALS
    // because SqlToRelConverter already does this.
    // REVIEW Danny 2019-09-23, we should unify the coercion rules in TypeCoercion
    // instead of SqlToRelConverter.
    if (SqlTypeUtil.isCharacter(left) && SqlTypeUtil.isDatetime(right)) {
      return coerceOperandType(binding.getCall(), 0, right);
    }
    if (SqlTypeUtil.isCharacter(right) && SqlTypeUtil.isDatetime(left)) {
      return coerceOperandType(binding.getCall(), 1, left);
    }
    return false;
  }

  /**
   * Casts "BOOLEAN = NUMERIC" to "NUMERIC = NUMERIC". Expressions like 1=`expr` and 0=`expr` can be
   * simplified to `expr` and `not expr`, but this better happens in {@link
   * org.apache.calcite.rex.RexSimplify}.
   *
   * <p>There are 2 cases that need type coercion here:
   *
   * <ol>
   *   <li>Case1: `boolean expr1` = 1 or `boolean expr1` = 0, replace the numeric literal with
   *       `true` or `false` boolean literal.
   *   <li>Case2: `boolean expr1` = `numeric expr2`, replace expr1 to `1` or `0` numeric literal.
   * </ol>
   *
   * For case2, wrap the operand in a cast operator, during sql-to-rel conversion we would convert
   * expression `cast(expr1 as right)` to `case when expr1 then 1 else 0.`
   */
  protected boolean booleanEquality(RexCallBinding binding, RelDataType left, RelDataType right) {
    RexNode lNode = binding.operand(0);
    RexNode rNode = binding.operand(1);
    if (SqlTypeUtil.isNumeric(left)
        && !RexUtils.isNullLiteral(lNode, false)
        && SqlTypeUtil.isBoolean(right)) {
      // Case1: numeric literal and boolean
      if (lNode.getKind() == SqlKind.LITERAL) {
        BigDecimal val = ((RexLiteral) lNode).getValueAs(BigDecimal.class);
        if (val.compareTo(BigDecimal.ZERO) == 0) {
          RexNode lNode1 = rexBuilder.makeLiteral(false);
          binding.getCall().operands.set(0, lNode1);
          return true;
        } else {
          RexNode lNode1 = rexBuilder.makeLiteral(true);
          binding.getCall().operands.set(0, lNode1);
          return true;
        }
      }
      // Case2: boolean and numeric
      return coerceOperandType(binding.getCall(), 1, left);
    }

    if (SqlTypeUtil.isNumeric(right)
        && !RexUtils.isNullLiteral(rNode, false)
        && SqlTypeUtil.isBoolean(left)) {
      // Case1: literal numeric + boolean
      if (rNode.getKind() == SqlKind.LITERAL) {
        BigDecimal val = ((RexLiteral) rNode).getValueAs(BigDecimal.class);
        if (val.compareTo(BigDecimal.ZERO) == 0) {
          RexNode rNode1 = rexBuilder.makeLiteral(false);
          binding.getCall().operands.set(1, rNode1);
          return true;
        } else {
          RexNode rNode1 = rexBuilder.makeLiteral(true);
          binding.getCall().operands.set(1, rNode1);
          return true;
        }
      }
      // Case2: boolean + numeric
      return coerceOperandType(binding.getCall(), 0, right);
    }
    return false;
  }

  /**
   * CASE WHEN type coercion, collect all the branches types including then operands and else
   * operands to find a common type, then cast the operands to the common type when needed.
   */
  @SuppressWarnings("deprecation")
  public boolean caseWhenCoercion(RexCallBinding callBinding) {
    // TODO.
    throw new UnsupportedOperationException("caseWhenCoercion");
    //    // For sql statement like:
    //    // `case when ... then (a, b, c) when ... then (d, e, f) else (g, h, i)`
    //    // an exception throws when entering this method.
    //    SqlCase caseCall = (SqlCase) callBinding.getCall();
    //    RexNodeList thenList = caseCall.getThenOperands();
    //    List<RelDataType> argTypes = new ArrayList<RelDataType>();
    //    SqlValidatorScope scope = getScope(callBinding);
    //    for (RexNode node : thenList) {
    //      argTypes.add(
    //          validator.deriveType(
    //              node));
    //    }
    //    RexNode elseOp =
    //        requireNonNull(caseCall.getElseOperand(),
    //            () -> "getElseOperand() is null for " + caseCall);
    //    RelDataType elseOpType = validator.deriveType(elseOp);
    //    argTypes.add(elseOpType);
    //    // Entering this method means we have already got a wider type, recompute it here
    //    // just to make the interface more clear.
    //    RelDataType widerType = getWiderTypeFor(argTypes, true);
    //    if (null != widerType) {
    //      boolean coerced = false;
    //      for (int i = 0; i < thenList.size(); i++) {
    //        coerced = coerceColumnType(thenList, i, widerType) || coerced;
    //      }
    //      if (needToCast(elseOp, widerType)) {
    //        coerced = coerceOperandType(caseCall, 3, widerType)
    //            || coerced;
    //      }
    //      return coerced;
    //    }
    //    return false;
  }

  /**
   * Coerces CASE WHEN and COALESCE statement branches to a unified type. NULLIF returns the same
   * type as the first operand without return type coercion.
   */
  @Override
  public boolean caseOrEquivalentCoercion(RexCallBinding callBinding) {
    // TODO.
    throw new UnsupportedOperationException("caseWhenCoercion");
    //    if (callBinding.getCall().getKind() == SqlKind.COALESCE) {
    //      // For sql statement like: `coalesce(a, b, c)`
    //      return coalesceCoercion(callBinding);
    //    } else if (callBinding.getCall().getKind() == SqlKind.NULLIF) {
    //      // For sql statement like: `nullif(a, b)`
    //      return false;
    //    } else {
    //      assert callBinding.getCall() instanceof SqlCase;
    //      return caseWhenCoercion(callBinding);
    //    }
    //  }
    //
    //  /**
    //   * COALESCE type coercion, collect all the branches types to find a common type,
    //   * then cast the operands to the common type when needed.
    //   */
    //  private boolean coalesceCoercion(RexCallBinding callBinding) {
    //    List<RelDataType> argTypes = new ArrayList<>();
    //    SqlValidatorScope scope = getScope(callBinding);
    //    for (RexNode node : callBinding.operands()) {
    //      argTypes.add(validator.deriveType(node));
    //    }
    //    RelDataType widerType = getWiderTypeFor(argTypes, true);
    //    if (null != widerType) {
    //      return coerceOperandsType(callBinding.getCall(), widerType);
    //    }
    //    return false;
  }

  /**
   * {@inheritDoc}
   *
   * <p>STRATEGIES
   *
   * <p>With(Without) sub-query:
   *
   * <ul>
   *   <li>With sub-query: find the common type through comparing the left hand side (LHS)
   *       expression types with corresponding right hand side (RHS) expression derived from the
   *       sub-query expression's row type. Wrap the fields of the LHS and RHS in CAST operators if
   *       it is needed.
   *   <li>Without sub-query: convert the nodes of the RHS to the common type by checking all the
   *       argument types and find out the minimum common type that all the arguments can be cast
   *       to.
   * </ul>
   *
   * <p>How to find the common type:
   *
   * <ul>
   *   <li>For both struct sql types (LHS and RHS), find the common type of every LHS and RHS fields
   *       pair:
   *       <pre>
   * (field1, field2, field3)    (field4, field5, field6)
   *    |        |       |          |       |       |
   *    +--------+---type1----------+       |       |
   *             |       |                  |       |
   *             +-------+----type2---------+       |
   *                     |                          |
   *                     +-------------type3--------+
   * </pre>
   *   <li>For both basic sql types(LHS and RHS), find the common type of LHS and RHS nodes.
   * </ul>
   */
  @Override
  public boolean inOperationCoercion(RexCallBinding binding) {
    SqlOperator operator = binding.getOperator();
    if (operator.getKind() == SqlKind.IN || operator.getKind() == SqlKind.NOT_IN) {
      assert binding.getOperandCount() == 2;
      final RelDataType type1 = binding.getOperandType(0);
      final RelDataType type2 = binding.getOperandType(1);
      final RexNode node1 = binding.operand(0);
      final RexNode node2 = binding.operand(1);
      if (type1.isStruct() && type2.isStruct() && type1.getFieldCount() != type2.getFieldCount()) {
        return false;
      }
      int colCount = type1.isStruct() ? type1.getFieldCount() : 1;
      RelDataType[] argTypes = new RelDataType[2];
      argTypes[0] = type1;
      argTypes[1] = type2;
      boolean coerced = false;
      List<RelDataType> widenTypes = new ArrayList<>();
      for (int i = 0; i < colCount; i++) {
        final int i2 = i;
        List<RelDataType> columnIthTypes =
            new AbstractList<RelDataType>() {
              @Override
              public RelDataType get(int index) {
                return argTypes[index].isStruct()
                    ? argTypes[index].getFieldList().get(i2).getType()
                    : argTypes[index];
              }

              @Override
              public int size() {
                return argTypes.length;
              }
            };

        RelDataType widenType =
            commonTypeForBinaryComparison(columnIthTypes.get(0), columnIthTypes.get(1));
        if (widenType == null) {
          widenType = getTightestCommonType(columnIthTypes.get(0), columnIthTypes.get(1));
        }
        if (widenType == null) {
          // Can not find any common type, just return early.
          return false;
        }
        widenTypes.add(widenType);
      }
      // Find all the common type for RSH and LSH columns.
      assert widenTypes.size() == colCount;
      for (int i = 0; i < widenTypes.size(); i++) {
        RelDataType desired = widenTypes.get(i);
        // LSH maybe a row values or single node.
        if (node1.getKind() == SqlKind.ROW) {
          assert node1 instanceof RexCall;
          if (coerceOperandType((RexCall) node1, i, desired)) {
            updateInferredColumnType(node1, i, widenTypes.get(i));
            coerced = true;
          }
        } else {
          coerced = coerceOperandType(binding.getCall(), 0, desired) || coerced;
        }
        // RHS may be a row values expression or sub-query.
        if (node2 instanceof List) {
          final List<RexNode> node3 = (List<RexNode>) node2;
          boolean listCoerced = false;
          if (type2.isStruct()) {
            for (RexNode node : (List<RexNode>) node2) {
              assert node instanceof RexCall;
              listCoerced = coerceOperandType((RexCall) node, i, desired) || listCoerced;
            }
            if (listCoerced) {
              updateInferredColumnType(node2, i, desired);
            }
          } else {
            for (int j = 0; j < ((List<RexNode>) node2).size(); j++) {
              listCoerced = coerceColumnType(node3, j, desired) || listCoerced;
            }
            if (listCoerced) {
              updateInferredType(node2, desired);
            }
          }
          coerced = coerced || listCoerced;
        } else {
          // TODO
          throw new UnsupportedOperationException("inOperationCoercion");
          //          // Another sub-query.
          //          SqlValidatorScope scope1 = node2 instanceof SqlSelect
          //              ? validator.getSelectScope((SqlSelect) node2)
          //              : scope;
          //          coerced = rowTypeCoercion(scope1, node2, i, desired) || coerced;
        }
      }
      return coerced;
    }
    return false;
  }

  /**
   * {@inheritDoc}
   *
   * <p>STRATEGIES
   *
   * <p>To determine the common type:
   *
   * <ul>
   *   <li>When the LHS has a Simple type and RHS has a Collection type determined by {@link
   *       SqlTypeUtil#isCollection}, to find the common type of LHS's type and RHS's component
   *       type.
   *   <li>If the common type differs from the LHS type, then coerced LHS type, and the nullability
   *       of the result type remains unchanged.
   *   <li>Create a new Collection type that matches the common type, the nullability of the new
   *       type keep same as RHS's component type and RHS's collection type.
   *   <li>If this new type differs from the RHS type, adjust the RHS type as needed.
   *       <pre>
   * field1       ARRAY(field2, field3, field4)
   *    |                |       |       |
   *    |                +-------+-------+
   *    |                        |
   *    |                  component type
   *    |                        |
   *    +------common type-------+
   * </pre>
   *   <li>If LHS type and the component type of RHS are different from the common type, CAST needs
   *       to be added.
   * </ul>
   */
  @Override
  public boolean quantifyOperationCoercion(RexCallBinding binding) {
    final RelDataType type1 = binding.getOperandType(0);
    final RelDataType collectionType = binding.getOperandType(1);
    final RelDataType type2 = collectionType.getComponentType();
    requireNonNull(type2, "type2");
    final RexCall sqlCall = binding.getCall();
    final RexNode node1 = binding.operand(0);
    final RexNode node2 = binding.operand(1);
    RelDataType widenType = commonTypeForBinaryComparison(type1, type2);
    if (widenType == null) {
      widenType = getTightestCommonType(type1, type2);
    }
    if (widenType == null) {
      return false;
    }
    final RelDataType leftWidenType =
        factory.enforceTypeWithNullability(widenType, type1.isNullable());
    boolean coercedLeft = coerceOperandType(sqlCall, 0, leftWidenType);
    if (coercedLeft) {
      updateInferredType(node1, leftWidenType);
    }
    final RelDataType rightWidenType =
        factory.enforceTypeWithNullability(widenType, type2.isNullable());
    RelDataType collectionWidenType = factory.createArrayType(rightWidenType, -1);
    collectionWidenType =
        factory.enforceTypeWithNullability(collectionWidenType, collectionType.isNullable());
    boolean coercedRight = coerceOperandType(sqlCall, 1, collectionWidenType);
    if (coercedRight) {
      updateInferredType(node2, collectionWidenType);
    }
    return coercedLeft || coercedRight;
  }

  @Override
  public boolean builtinFunctionCoercion(
      RexCallBinding binding,
      List<RelDataType> operandTypes,
      List<SqlTypeFamily> expectedFamilies) {
    assert binding.getOperandCount() == operandTypes.size();
    if (!canImplicitTypeCast(operandTypes, expectedFamilies)) {
      return false;
    }
    boolean coerced = false;
    for (int i = 0; i < operandTypes.size(); i++) {
      RelDataType implicitType = implicitCast(operandTypes.get(i), expectedFamilies.get(i));
      coerced =
          null != implicitType
                  && operandTypes.get(i) != implicitType
                  && coerceOperandType(binding.getCall(), i, implicitType)
              || coerced;
    }
    return coerced;
  }

  /** Type coercion for user-defined functions (UDFs). */
  @Override
  public boolean userDefinedFunctionCoercion(RexCall call, SqlFunction function) {
    final SqlOperandMetadata operandMetadata =
        requireNonNull(
            (SqlOperandMetadata) function.getOperandTypeChecker(),
            () -> "getOperandTypeChecker is not defined for " + function);
    final List<RelDataType> paramTypes = operandMetadata.paramTypes(factory);
    boolean coerced = false;
    for (int i = 0; i < call.operandCount(); i++) {
      RexNode operand = call.operands.get(i);
      if (operand.getKind() == SqlKind.ARGUMENT_ASSIGNMENT) {
        final List<RexNode> operandList = ((RexCall) operand).operands;
        // TODO
        String name = operandList.get(1).toString();
        //        String name = ((SqlIdentifier) operandList.get(1)).getSimple();
        final List<String> paramNames = operandMetadata.paramNames();
        int formalIndex = paramNames.indexOf(name);
        if (formalIndex < 0) {
          return false;
        }
        // Column list operand type is not supported now.
        coerced = coerceOperandType((RexCall) operand, 0, paramTypes.get(formalIndex)) || coerced;
      } else {
        coerced = coerceOperandType(call, i, paramTypes.get(i)) || coerced;
      }
    }
    return coerced;
  }
}
