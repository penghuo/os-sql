/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.type;

import com.google.common.collect.ImmutableList;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.util.Util;
import org.checkerframework.checker.nullness.qual.Nullable;

public class RexCallCompositeSingleOperandTypeChecker extends RexCallCompositeOperandTypeChecker
    implements RexCallSingleOperandTypeChecker {

  // ~ Constructors -----------------------------------------------------------

  /**
   * Creates a CompositeSingleOperandTypeChecker. Outside this package, use {@link
   * RexCallSingleOperandTypeChecker#and(RexCallSingleOperandTypeChecker)}, {@link
   * org.apache.calcite.sql.type.OperandTypes#and}, {@link OperandTypes#or} and similar.
   */
  RexCallCompositeSingleOperandTypeChecker(
      RexCallCompositeOperandTypeChecker.Composition composition,
      ImmutableList<? extends RexCallSingleOperandTypeChecker> allowedRules,
      @Nullable String allowedSignatures) {
    super(composition, allowedRules, allowedSignatures, null, null);
  }

  // ~ Methods ----------------------------------------------------------------

  @SuppressWarnings("unchecked")
  @Override
  public ImmutableList<? extends RexCallSingleOperandTypeChecker> getRules() {
    return (ImmutableList<? extends RexCallSingleOperandTypeChecker>) allowedRules;
  }

  @Override
  public boolean checkSingleOperandType(
      RexCallBinding callBinding, RexNode node, int iFormalOperand, boolean throwOnFailure) {
    assert allowedRules.size() >= 1;

    final ImmutableList<? extends RexCallSingleOperandTypeChecker> rules = getRules();
    if (composition == Composition.SEQUENCE) {
      return rules
          .get(iFormalOperand)
          .checkSingleOperandType(callBinding, node, iFormalOperand, throwOnFailure);
    }

    int typeErrorCount = 0;

    boolean throwOnAndFailure = (composition == Composition.AND) && throwOnFailure;

    for (RexCallSingleOperandTypeChecker rule : rules) {
      if (!rule.checkSingleOperandType(callBinding, node, iFormalOperand, throwOnAndFailure)) {
        typeErrorCount++;
      }
    }

    boolean ret;
    switch (composition) {
      case AND:
        ret = typeErrorCount == 0;
        break;
      case OR:
        ret = typeErrorCount < allowedRules.size();
        break;
      default:
        // should never come here
        throw Util.unexpected(composition);
    }

    if (!ret && throwOnFailure) {
      // In the case of a composite OR, we want to throw an error
      // describing in more detail what the problem was, hence doing the
      // loop again.
      for (RexCallSingleOperandTypeChecker rule : rules) {
        rule.checkSingleOperandType(callBinding, node, iFormalOperand, true);
      }

      // If no exception thrown, just throw a generic validation signature
      // error.
      throw callBinding.newValidationSignatureError();
    }

    return ret;
  }
}
