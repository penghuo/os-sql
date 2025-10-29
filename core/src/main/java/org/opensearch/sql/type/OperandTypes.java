/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.type;

import com.google.common.collect.ImmutableList;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;
import org.apache.calcite.sql.type.SqlTypeFamily;
import org.checkerframework.checker.nullness.qual.Nullable;

public class OperandTypes {

  public static final RexCallSingleOperandTypeChecker INTERVAL =
      family(SqlTypeFamily.DATETIME_INTERVAL);

  public static final RexCallSingleOperandTypeChecker NUMERIC = family(SqlTypeFamily.NUMERIC);

  public static final RexCallSingleOperandTypeChecker NUMERIC_OR_INTERVAL = NUMERIC.or(INTERVAL);

  /** Creates a checker that passes if each operand is a member of a corresponding family. */
  public static RexCallFamilyOperandTypeChecker family(SqlTypeFamily... families) {
    return new RexCallFamilyOperandTypeChecker(ImmutableList.copyOf(families), i -> false);
  }

  public static RexCallSingleOperandTypeChecker or(RexCallSingleOperandTypeChecker... rules) {
    return compositeSingle(
        RexCallCompositeSingleOperandTypeChecker.Composition.OR, ImmutableList.copyOf(rules), null);
  }

  /** Creates a single-operand checker that passes if all of the rules pass. */
  public static RexCallSingleOperandTypeChecker and(RexCallSingleOperandTypeChecker... rules) {
    return compositeSingle(
        RexCallCompositeSingleOperandTypeChecker.Composition.AND,
        ImmutableList.copyOf(rules),
        null);
  }

  private static RexCallSingleOperandTypeChecker compositeSingle(
      RexCallCompositeSingleOperandTypeChecker.Composition composition,
      List<? extends RexCallSingleOperandTypeChecker> allowedRules,
      @Nullable String allowedSignatures) {
    final List<RexCallSingleOperandTypeChecker> list = new ArrayList<>(allowedRules);
    switch (composition) {
      default:
        break;
      case AND:
      case OR:
        flatten(
            list,
            c ->
                c instanceof RexCallCompositeSingleOperandTypeChecker
                        && ((RexCallCompositeSingleOperandTypeChecker) c).composition == composition
                    ? ((RexCallCompositeSingleOperandTypeChecker) c).getRules()
                    : null);
    }
    if (list.size() == 1) {
      return list.get(0);
    }
    return new RexCallCompositeSingleOperandTypeChecker(
        composition, ImmutableList.copyOf(list), allowedSignatures);
  }

  /** Helper for {@link #compositeSingle} and {@link #composite}. */
  private static <E> void flatten(List<E> list, Function<E, @Nullable List<? extends E>> expander) {
    for (int i = 0; i < list.size(); ) {
      @Nullable List<? extends E> list2 = expander.apply(list.get(i));
      if (list2 == null) {
        ++i;
      } else {
        list.remove(i);
        list.addAll(i, list2);
      }
    }
  }
}
