/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.type;

import static java.util.Objects.requireNonNull;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import java.util.AbstractList;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.function.BiFunction;
import java.util.stream.Collectors;
import org.apache.calcite.linq4j.Ord;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.SqlOperandCountRange;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.type.SqlOperandCountRanges;
import org.apache.calcite.sql.type.SqlTypeFamily;
import org.apache.calcite.util.Util;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.checkerframework.checker.nullness.qual.UnknownKeyFor;
import org.opensearch.sql.type.coercion.RexCallTypeCoercion;

public class RexCallCompositeOperandTypeChecker implements RexNodeTypeChecker {
  private final @Nullable SqlOperandCountRange range;

  // ~ Enums ------------------------------------------------------------------

  /** How operands are composed. */
  public enum Composition {
    AND,
    OR,
    SEQUENCE,
    REPEAT
  }

  // ~ Instance fields --------------------------------------------------------

  // It is not clear if @UnknownKeyFor is needed here or not, however, checkerframework inference
  // fails otherwise, see https://github.com/typetools/checker-framework/issues/4048
  protected final ImmutableList<@UnknownKeyFor ? extends RexNodeTypeChecker> allowedRules;
  protected final Composition composition;
  private final @Nullable String allowedSignatures;
  private final @Nullable BiFunction<SqlOperator, String, String> signatureGenerator;

  // ~ Constructors -----------------------------------------------------------

  /**
   * Creates a CompositeOperandTypeChecker. Outside this package, use {@link
   * RexNodeTypeChecker#and(RexNodeTypeChecker)}, {@link OperandTypes#and}, {@link OperandTypes#or}
   * and similar.
   */
  protected RexCallCompositeOperandTypeChecker(
      RexCallCompositeOperandTypeChecker.Composition composition,
      ImmutableList<? extends RexNodeTypeChecker> allowedRules,
      @Nullable String allowedSignatures,
      @Nullable BiFunction<SqlOperator, String, String> signatureGenerator,
      @Nullable SqlOperandCountRange range) {
    this.allowedRules = requireNonNull(allowedRules, "allowedRules");
    this.composition = requireNonNull(composition, "composition");
    this.allowedSignatures = allowedSignatures;
    this.signatureGenerator = signatureGenerator;
    this.range = range;
    assert (range != null) == (composition == Composition.REPEAT);
    assert allowedRules.size() + (range == null ? 0 : 1) > 1;
  }

  // ~ Methods ----------------------------------------------------------------

  @Override
  public RexCallCompositeOperandTypeChecker withGenerator(
      BiFunction<SqlOperator, String, String> signatureGenerator) {
    return this.signatureGenerator == signatureGenerator
        ? this
        : new RexCallCompositeOperandTypeChecker(
            composition, allowedRules, allowedSignatures, signatureGenerator, range);
  }

  @Override
  public boolean isOptional(int i) {
    for (RexNodeTypeChecker allowedRule : allowedRules) {
      if (allowedRule.isOptional(i)) {
        return true;
      }
    }
    return false;
  }

  public ImmutableList<? extends RexNodeTypeChecker> getRules() {
    return allowedRules;
  }

  @Override
  public String getAllowedSignatures(SqlOperator op, String opName) {
    if (allowedSignatures != null) {
      return allowedSignatures;
    }
    if (signatureGenerator != null) {
      return signatureGenerator.apply(op, opName);
    }
    if (composition == RexCallCompositeOperandTypeChecker.Composition.SEQUENCE) {
      throw new AssertionError("specify allowedSignatures or override getAllowedSignatures");
    }
    StringBuilder ret = new StringBuilder();
    for (Ord<RexNodeTypeChecker> ord : Ord.<RexNodeTypeChecker>zip(allowedRules)) {
      if (ord.i > 0) {
        ret.append(SqlOperator.NL);
      }
      ret.append(ord.e.getAllowedSignatures(op, opName));
      if (composition == RexCallCompositeOperandTypeChecker.Composition.AND) {
        break;
      }
    }
    return ret.toString();
  }

  @Override
  public SqlOperandCountRange getOperandCountRange() {
    switch (composition) {
      case REPEAT:
        return requireNonNull(range, "range");
      case SEQUENCE:
        return SqlOperandCountRanges.of(allowedRules.size());
      case AND:
      case OR:
      default:
        final List<SqlOperandCountRange> ranges =
            new AbstractList<SqlOperandCountRange>() {
              @Override
              public SqlOperandCountRange get(int index) {
                return allowedRules.get(index).getOperandCountRange();
              }

              @Override
              public int size() {
                return allowedRules.size();
              }
            };
        final int min = minMin(ranges);
        final int max = maxMax(ranges);
        SqlOperandCountRange composite =
            new SqlOperandCountRange() {
              @Override
              public boolean isValidCount(int count) {
                switch (composition) {
                  case AND:
                    for (SqlOperandCountRange range : ranges) {
                      if (!range.isValidCount(count)) {
                        return false;
                      }
                    }
                    return true;
                  case OR:
                  default:
                    for (SqlOperandCountRange range : ranges) {
                      if (range.isValidCount(count)) {
                        return true;
                      }
                    }
                    return false;
                }
              }

              @Override
              public int getMin() {
                return min;
              }

              @Override
              public int getMax() {
                return max;
              }
            };
        if (max >= 0) {
          for (int i = min; i <= max; i++) {
            if (!composite.isValidCount(i)) {
              // Composite is not a simple range. Can't simplify,
              // so return the composite.
              return composite;
            }
          }
        }
        return min == max ? SqlOperandCountRanges.of(min) : SqlOperandCountRanges.between(min, max);
    }
  }

  private static int minMin(List<SqlOperandCountRange> ranges) {
    int min = Integer.MAX_VALUE;
    for (SqlOperandCountRange range : ranges) {
      min = Math.min(min, range.getMin());
    }
    return min;
  }

  private int maxMax(List<SqlOperandCountRange> ranges) {
    int max = Integer.MIN_VALUE;
    for (SqlOperandCountRange range : ranges) {
      if (range.getMax() < 0) {
        if (composition == Composition.OR) {
          return -1;
        }
      } else {
        max = Math.max(max, range.getMax());
      }
    }
    return max;
  }

  @Override
  public boolean checkOperandTypes(RexCallBinding callBinding, boolean throwOnFailure) {
    // 1. Check eagerly for binary arithmetic expressions.
    // 2. Check the comparability.
    // 3. Check if the operands have the right type.
    if (callBinding.isTypeCoercionEnabled()) {
      final RexCallTypeCoercion typeCoercion = callBinding.getValidator().getTypeCoercion();
      typeCoercion.binaryArithmeticCoercion(callBinding);
    }
    if (check(callBinding, false)) {
      return true;
    }
    if (!throwOnFailure) {
      return false;
    }
    // Check again, to cause error to be thrown.
    switch (composition) {
      default:
        break;
      case OR:
      case SEQUENCE:
        check(callBinding, true);
    }

    // If no exception thrown, just throw a generic validation
    // signature error.
    throw callBinding.newValidationSignatureError();
  }

  private boolean check(RexCallBinding callBinding, boolean throwOnFailure) {
    switch (composition) {
      case REPEAT:
        if (!requireNonNull(range, "range").isValidCount(callBinding.getOperandCount())) {
          return false;
        }
        for (int operand : Util.range(callBinding.getOperandCount())) {
          for (RexNodeTypeChecker rule : allowedRules) {
            if (!((RexCallSingleOperandTypeChecker) rule)
                .checkSingleOperandType(
                    callBinding, callBinding.operand(operand), 0, throwOnFailure)) {
              if (callBinding.isTypeCoercionEnabled()) {
                return coerceOperands(callBinding, true);
              }
              return false;
            }
          }
        }
        return true;

      case SEQUENCE:
        if (callBinding.getOperandCount() != allowedRules.size()) {
          return false;
        }
        for (Ord<RexNodeTypeChecker> ord : Ord.<RexNodeTypeChecker>zip(allowedRules)) {
          RexNodeTypeChecker rule = ord.e;
          if (!((RexCallSingleOperandTypeChecker) rule)
              .checkSingleOperandType(
                  callBinding, callBinding.operand(ord.i), ord.i, throwOnFailure)) {
            if (callBinding.isTypeCoercionEnabled()) {
              return coerceOperands(callBinding, false);
            }
            return false;
          }
        }
        return true;

      case AND:
        for (Ord<RexNodeTypeChecker> ord : Ord.<RexNodeTypeChecker>zip(allowedRules)) {
          RexNodeTypeChecker rule = ord.e;
          if (!rule.checkOperandTypes(callBinding, throwOnFailure)) {
            // Avoid trying other rules in AND if the first one fails.
            return false;
          }
        }
        return true;

      case OR:
        // If there is an ImplicitCastOperandTypeChecker, check it without type coercion first,
        // if all check fails, try type coercion if it is allowed (default true).
        if (checkWithoutTypeCoercion(callBinding)) {
          return true;
        }
        for (Ord<RexNodeTypeChecker> ord : Ord.<RexNodeTypeChecker>zip(allowedRules)) {
          RexNodeTypeChecker rule = ord.e;
          if (rule.checkOperandTypes(callBinding, throwOnFailure)) {
            return true;
          }
        }
        return false;

      default:
        throw new AssertionError();
    }
  }

  /** Tries to coerce the operands based on the defined type families. */
  private boolean coerceOperands(RexCallBinding callBinding, boolean repeat) {
    // Type coercion for the call,
    // collect SqlTypeFamily and data type of all the operands.
    List<SqlTypeFamily> families =
        allowedRules.stream()
            .filter(r -> r instanceof RexCallImplicitCastOperandTypeChecker)
            // All the rules are SqlSingleOperandTypeChecker.
            .map(r -> ((RexCallImplicitCastOperandTypeChecker) r).getOperandSqlTypeFamily(0))
            .collect(Collectors.toList());
    if (families.size() < allowedRules.size()) {
      // Not all the checkers are ImplicitCastOperandTypeChecker, returns early.
      return false;
    }
    if (repeat) {
      assert families.size() == 1;
      families = Collections.nCopies(callBinding.getOperandCount(), families.get(0));
    }
    final List<RelDataType> operandTypes = new ArrayList<>();
    for (int i = 0; i < callBinding.getOperandCount(); i++) {
      operandTypes.add(callBinding.getOperandType(i));
    }
    RexCallTypeCoercion typeCoercion = callBinding.getValidator().getTypeCoercion();
    return typeCoercion.builtinFunctionCoercion(callBinding, operandTypes, families);
  }

  private boolean checkWithoutTypeCoercion(RexCallBinding callBinding) {
    if (!callBinding.isTypeCoercionEnabled()) {
      return false;
    }
    RexCallBinding sqlBindingWithoutTypeCoercion =
        new RexCallBinding(callBinding.getValidator(), callBinding.getCall()) {
          @Override
          public boolean isTypeCoercionEnabled() {
            return false;
          }
        };
    for (RexNodeTypeChecker rule : allowedRules) {
      if (rule.checkOperandTypes(sqlBindingWithoutTypeCoercion, false)) {
        return true;
      }
      if (rule instanceof RexCallImplicitCastOperandTypeChecker) {
        RexCallImplicitCastOperandTypeChecker rule1 = (RexCallImplicitCastOperandTypeChecker) rule;
        if (rule1.checkOperandTypesWithoutTypeCoercion(callBinding, false)) {
          return true;
        }
      }
    }
    return false;
  }

  @Override
  public @Nullable RexCallTypeInference typeInference() {
    if (composition == Composition.REPEAT) {
      RexNodeTypeChecker checker = Iterables.getOnlyElement(allowedRules);
      if (checker instanceof RexCallTypeInference) {
        final RexCallTypeInference rule = (RexCallTypeInference) checker;
        return (callBinding, returnType, operandTypes) -> {
          for (int i = 0; i < callBinding.getOperandCount(); i++) {
            final RelDataType[] operandTypes0 = new RelDataType[1];
            rule.inferOperandTypes(callBinding, returnType, operandTypes0);
            operandTypes[i] = operandTypes0[0];
          }
        };
      }
    }
    return null;
  }
}
