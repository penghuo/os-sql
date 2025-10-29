/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.type;

import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.type.CompositeSingleOperandTypeChecker;
import org.apache.calcite.sql.type.SqlOperandTypeChecker;
import org.apache.calcite.sql.type.SqlTypeName;
import org.checkerframework.checker.nullness.qual.Nullable;

public class RexUtils {

  public static RexNodeTypeChecker toRexNodeTypeChecker(SqlOperandTypeChecker operandTypeChecker) {
    if (operandTypeChecker instanceof CompositeSingleOperandTypeChecker) {
      CompositeSingleOperandTypeChecker checker =
          (CompositeSingleOperandTypeChecker) operandTypeChecker;
    }
  }

  /**
   * Returns whether a node represents the NULL value.
   *
   * <p>Examples:
   *
   * <ul>
   *   <li>For {@link SqlLiteral} Unknown, returns false.
   *   <li>For <code>CAST(NULL AS <i>type</i>)</code>, returns true if <code>
   * allowCast</code> is true, false otherwise.
   *   <li>For <code>CAST(CAST(NULL AS <i>type</i>) AS <i>type</i>))</code>, returns false.
   * </ul>
   */
  public static boolean isNullLiteral(@Nullable RexNode node, boolean allowCast) {
    if (node instanceof RexLiteral) {
      RexLiteral literal = (RexLiteral) node;
      if (literal.getTypeName() == SqlTypeName.NULL) {
        assert null == literal.getValue();
        return true;
      } else {
        // We don't regard UNKNOWN -- SqlLiteral(null,Boolean) -- as
        // NULL.
        return false;
      }
    }
    if (allowCast && node != null) {
      if (node.getKind() == SqlKind.CAST) {
        RexCall call = (RexCall) node;
        // node is "CAST(NULL as type)"
        return isNullLiteral(call.operands.get(0), false);
      }
    }
    return false;
  }

  /**
   * Returns whether a node represents the NULL value or a series of nested <code>CAST(NULL AS type)
   * </code> calls. For example: <code>isNull(CAST(CAST(NULL as INTEGER) AS VARCHAR(1)))</code>
   * returns {@code true}.
   */
  public static boolean isNull(RexNode node) {
    return isNullLiteral(node, false)
        || node.getKind() == SqlKind.CAST && isNull(((RexCall) node).operands.get(0));
  }
}
