/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.sqlnode;

import org.apache.calcite.sql.SqlCallBinding;
import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlOperandCountRange;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.InferTypes;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.type.SqlOperandCountRanges;
import org.apache.calcite.sql.type.SqlOperandTypeChecker;

/**
 * Permissive variant of {@link SqlStdOperatorTable#IS_EMPTY} for PPL's {@code isempty} function.
 *
 * <p>v2's {@code RexBuilder.makeCall(SqlStdOperatorTable.IS_EMPTY, arg)} bypasses operand-type
 * checking, but the SqlNode→SqlValidator path resolves {@code IS EMPTY} by-name to the standard
 * IS_EMPTY whose checker rejects strings (typical PPL operand). This wrapper accepts any operand; a
 * RexShuttle in {@link SqlNodePlanner} rewrites RexCalls on this operator back to {@link
 * SqlStdOperatorTable#IS_EMPTY} so the final RexCall matches v2's emission.
 */
final class PermissiveIsEmpty {

  private PermissiveIsEmpty() {}

  static final SqlOperator INSTANCE =
      new SqlFunction(
          "IS_EMPTY",
          SqlKind.OTHER_FUNCTION,
          ReturnTypes.BOOLEAN_NOT_NULL,
          InferTypes.VARCHAR_1024,
          new SqlOperandTypeChecker() {
            @Override
            public boolean checkOperandTypes(SqlCallBinding callBinding, boolean throwOnFailure) {
              return true;
            }

            @Override
            public SqlOperandCountRange getOperandCountRange() {
              return SqlOperandCountRanges.of(1);
            }

            @Override
            public String getAllowedSignatures(SqlOperator op, String opName) {
              return "<ANY> IS EMPTY";
            }
          },
          SqlFunctionCategory.USER_DEFINED_FUNCTION);
}
