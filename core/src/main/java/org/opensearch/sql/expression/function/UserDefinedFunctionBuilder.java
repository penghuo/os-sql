/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.expression.function;

import java.util.Collections;
import org.apache.calcite.schema.ImplementableFunction;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.InferTypes;
import org.apache.calcite.sql.type.SqlReturnTypeInference;
import org.apache.calcite.sql.validate.SqlUserDefinedFunction;

/**
 * The interface helps to construct a SqlUserDefinedFunction
 *
 * <p>1. getFunction - returns the implementation of the UDF
 *
 * <p>2. getReturnTypeInference - returns the return type of the UDF
 *
 * <p>3. getOperandMetadata - returns the operand metadata of the UDF. This is for checking the
 * operand when validation, default null without checking.
 */
public interface UserDefinedFunctionBuilder {

  ImplementableFunction getFunction();

  SqlReturnTypeInference getReturnTypeInference();

  UDFOperandMetadata getOperandMetadata();

  default SqlUserDefinedFunction toUDF(String functionName) {
    return toUDF(functionName, true);
  }

  /**
   * In some rare cases, we need to call out the UDF to be not deterministic to avoid Volcano
   * planner over-optimization. For example, we don't need ReduceExpressionsRule to optimize
   * relevance query UDF.
   *
   * @param functionName UDF name to be registered
   * @param isDeterministic Specified isDeterministic flag
   * @return Calcite SqlUserDefinedFunction
   */
  default SqlUserDefinedFunction toUDF(String functionName, boolean isDeterministic) {
    SqlIdentifier udfLtrimIdentifier =
        new SqlIdentifier(Collections.singletonList(functionName), null, SqlParserPos.ZERO, null);
    UDFOperandMetadata metadata = getOperandMetadata();
    return new SqlUserDefinedFunction(
        udfLtrimIdentifier,
        SqlKind.OTHER_FUNCTION,
        getReturnTypeInference(),
        InferTypes.ANY_NULLABLE,
        metadata,
        getFunction()) {
      @Override
      public boolean isDeterministic() {
        return isDeterministic;
      }

      @Override
      public SqlIdentifier getSqlIdentifier() {
        // to avoid convert to sql dialog as identifier, use keyword instead
        // check the code SqlUtil.unparseFunctionSyntax()
        return null;
      }

      /**
       * When the UDF declares no operand metadata, return a permissive count range instead of
       * {@link org.apache.calcite.util.Util#needToImplement} (the SqlOperator default). This is
       * required for the SqlValidator to be able to look up the operator during routine resolution
       * — without it, any function with {@code getOperandMetadata() == null} crashes the validator
       * on the SqlNode→RelNode path.
       */
      @Override
      public org.apache.calcite.sql.SqlOperandCountRange getOperandCountRange() {
        if (metadata != null) {
          return metadata.getOperandCountRange();
        }
        // Permissive: any arity. The actual checking happens at runtime via the implementor.
        return org.apache.calcite.sql.type.SqlOperandCountRanges.any();
      }

      /**
       * Same fallback as {@link #getOperandCountRange}: when no operand metadata is declared, the
       * SqlOperator's default {@code checkOperandTypes} throws. Accept any types here so the
       * validator's deriveType pass succeeds.
       */
      @Override
      public boolean checkOperandTypes(
          org.apache.calcite.sql.SqlCallBinding callBinding, boolean throwOnFailure) {
        if (metadata != null) {
          return metadata.checkOperandTypes(callBinding, throwOnFailure);
        }
        return true;
      }

      /**
       * Override the default {@link org.apache.calcite.sql.SqlOperator#deriveType} so the validator
       * skips the operator-table re-resolution that would otherwise replace this UDF with the
       * standard variant living under the same name (e.g. PPL's TIMESTAMPADD vs Calcite's std
       * TIMESTAMPADD). The standard impl re-resolves by name + signature, which is order-dependent
       * across the operator table chain. We already know which variant the caller wants — this is
       * THIS instance — so just validate operands and infer.
       */
      @Override
      public org.apache.calcite.rel.type.RelDataType deriveType(
          org.apache.calcite.sql.validate.SqlValidator validator,
          org.apache.calcite.sql.validate.SqlValidatorScope scope,
          org.apache.calcite.sql.SqlCall call) {
        for (org.apache.calcite.sql.SqlNode operand : call.getOperandList()) {
          validator.deriveType(scope, operand);
        }
        org.apache.calcite.rel.type.RelDataType type = validateOperands(validator, scope, call);
        type = adjustType(validator, call, type);
        org.apache.calcite.sql.validate.SqlValidatorUtil.checkCharsetAndCollateConsistentIfCharType(
            type);
        return type;
      }
    };
  }
}
