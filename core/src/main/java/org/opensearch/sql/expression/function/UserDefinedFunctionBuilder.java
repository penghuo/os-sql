/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.expression.function;

import java.util.Collections;
import java.util.List;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.schema.ImplementableFunction;
import org.apache.calcite.sql.SqlCallBinding;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlOperandCountRange;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.InferTypes;
import org.apache.calcite.sql.type.SqlOperandMetadata;
import org.apache.calcite.sql.type.SqlOperandTypeChecker;
import org.apache.calcite.sql.type.SqlReturnTypeInference;
import org.apache.calcite.sql.validate.SqlUserDefinedFunction;

/**
 * Builds a {@link SqlUserDefinedFunction} from a PPL UDF implementation.
 *
 * <p>Implementations supply three pieces:
 *
 * <ol>
 *   <li>{@link #getFunction()} — the {@link ImplementableFunction} that emits the runtime code
 *   <li>{@link #getReturnTypeInference()} — return-type inference
 *   <li>{@link #getOperandTypeChecker()} — operand type checker (validator-side); may be {@code
 *       null} to opt out of operand validation
 * </ol>
 *
 * <p>The Calcite {@link SqlUserDefinedFunction} constructor stores its operand checker in a {@code
 * SqlOperandMetadata}-typed field; if a plain {@link SqlOperandTypeChecker} is passed it silently
 * coerces to {@code null}. {@link #toUDF} therefore wraps non-metadata checkers via {@link
 * #asMetadata}.
 */
public interface UserDefinedFunctionBuilder {

  ImplementableFunction getFunction();

  SqlReturnTypeInference getReturnTypeInference();

  /** Operand type checker, or {@code null} to skip operand validation. */
  SqlOperandTypeChecker getOperandTypeChecker();

  default SqlUserDefinedFunction toUDF(String functionName) {
    return toUDF(functionName, true);
  }

  /**
   * In some rare cases we mark the UDF as non-deterministic to avoid Volcano planner
   * over-optimization. For example, the relevance query UDFs must not be folded by
   * ReduceExpressionsRule.
   *
   * @param functionName UDF name to be registered
   * @param isDeterministic Specified isDeterministic flag
   * @return Calcite SqlUserDefinedFunction
   */
  default SqlUserDefinedFunction toUDF(String functionName, boolean isDeterministic) {
    SqlIdentifier identifier =
        new SqlIdentifier(Collections.singletonList(functionName), null, SqlParserPos.ZERO, null);
    SqlOperandTypeChecker checker = getOperandTypeChecker();
    SqlOperandMetadata metadata = asMetadata(checker);
    return new SqlUserDefinedFunction(
        identifier,
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
        // Returning null suppresses identifier-style unparsing in SqlUtil.unparseFunctionSyntax,
        // so the function is rendered as a keyword call.
        return null;
      }

      /**
       * When the UDF declares no operand checker, return a permissive count range instead of {@link
       * org.apache.calcite.util.Util#needToImplement} (the SqlOperator default). The SqlValidator
       * looks up the operand count range during routine resolution, so a non-throwing default is
       * required.
       */
      @Override
      public SqlOperandCountRange getOperandCountRange() {
        if (metadata != null) {
          return metadata.getOperandCountRange();
        }
        return org.apache.calcite.sql.type.SqlOperandCountRanges.any();
      }

      /**
       * Same fallback as {@link #getOperandCountRange}: when no checker is declared, accept any
       * types so the validator's deriveType pass succeeds. The actual semantic check happens at
       * runtime via the implementor.
       */
      @Override
      public boolean checkOperandTypes(SqlCallBinding callBinding, boolean throwOnFailure) {
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
      public RelDataType deriveType(
          org.apache.calcite.sql.validate.SqlValidator validator,
          org.apache.calcite.sql.validate.SqlValidatorScope scope,
          org.apache.calcite.sql.SqlCall call) {
        for (org.apache.calcite.sql.SqlNode operand : call.getOperandList()) {
          validator.deriveType(scope, operand);
        }
        RelDataType type = validateOperands(validator, scope, call);
        type = adjustType(validator, call, type);
        org.apache.calcite.sql.validate.SqlValidatorUtil.checkCharsetAndCollateConsistentIfCharType(
            type);
        return type;
      }
    };
  }

  /**
   * Adapt a {@link SqlOperandTypeChecker} to {@link SqlOperandMetadata}. The Calcite ctors of
   * {@link SqlUserDefinedFunction} / {@link
   * org.apache.calcite.sql.validate.SqlUserDefinedAggFunction} store their operand checker in a
   * {@code SqlOperandMetadata}-typed field; passing a plain {@code SqlOperandTypeChecker} would be
   * silently coerced to {@code null}, which then breaks downstream code that calls {@code
   * getOperandTypeChecker().paramNames()} (e.g. {@code FIRST}/{@code LAST} aggregate resolution).
   * The adapter forwards count-range and operand-type checks to the inner checker; {@code
   * paramTypes} and {@code paramNames} are returned empty since UDFs don't expose explicit
   * parameter metadata.
   */
  static SqlOperandMetadata asMetadata(SqlOperandTypeChecker checker) {
    if (checker == null) {
      return null;
    }
    if (checker instanceof SqlOperandMetadata m) {
      return m;
    }
    return new CheckerMetadata(checker);
  }

  /** Adapter implementation of {@link SqlOperandMetadata}. */
  final class CheckerMetadata implements SqlOperandMetadata {
    private final SqlOperandTypeChecker inner;

    CheckerMetadata(SqlOperandTypeChecker inner) {
      this.inner = inner;
    }

    @Override
    public List<RelDataType> paramTypes(RelDataTypeFactory typeFactory) {
      return List.of();
    }

    @Override
    public List<String> paramNames() {
      return List.of();
    }

    @Override
    public boolean checkOperandTypes(SqlCallBinding callBinding, boolean throwOnFailure) {
      return inner.checkOperandTypes(callBinding, throwOnFailure);
    }

    @Override
    public SqlOperandCountRange getOperandCountRange() {
      return inner.getOperandCountRange();
    }

    @Override
    public String getAllowedSignatures(SqlOperator op, String opName) {
      return inner.getAllowedSignatures(op, opName);
    }
  }
}
