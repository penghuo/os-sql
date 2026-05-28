/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.expression.function.udf;

import com.google.common.collect.ImmutableList;
import java.util.List;
import org.apache.calcite.adapter.enumerable.NotNullImplementor;
import org.apache.calcite.adapter.enumerable.NullPolicy;
import org.apache.calcite.adapter.enumerable.RexToLixTranslator;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.sql.type.CompositeOperandTypeChecker;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.type.SqlReturnTypeInference;
import org.apache.calcite.sql.type.SqlTypeFamily;
import org.opensearch.sql.expression.function.ImplementorUDF;
import org.opensearch.sql.expression.function.UDFOperandMetadata;

public class RelevanceQueryFunction extends ImplementorUDF {

  public RelevanceQueryFunction() {
    super(new RelevanceQueryImplementor(), NullPolicy.ANY);
  }

  @Override
  public SqlReturnTypeInference getReturnTypeInference() {
    return ReturnTypes.BOOLEAN;
  }

  /*
   * The first parameter is always required (either fields or query).
   * The second parameter is query when fields are present, otherwise it's the first parameter.
   * Starting from the 3rd parameter (or 2nd when no fields), they are optional parameters for relevance queries.
   * Different query has different parameter set, which will be validated in dedicated query builder.
   * Query parameter is always required and cannot be null.
   *
   * <p>Encoding note: the operand list is variadic (1..MAX_OPERANDS MAP arguments). Calcite's
   * {@code OperandTypes.family(List, optionalPredicate)} does <em>not</em> implement variadic
   * arity in {@code checkOperandTypes} — that path requires the family list length to equal the
   * actual operand count, ignoring the {@code optional} predicate. To express "1..N MAP operands"
   * we OR together one strictly-sized {@code family(MAP repeated k times)} checker for each
   * supported arity {@code k} from 1 through {@link #MAX_OPERANDS}. PPL's own type-check wrapper
   * ({@code PPLFamilyTypeCheckerWrapper}) goes through {@code getOperandCountRange} which the
   * earlier (predicate-based) form handled correctly, so the visitor-side path already works; the
   * SqlValidator round-trip path goes through Calcite's {@code checkOperandTypes} directly and
   * needs the strictly-sized form.
   */
  @Override
  public UDFOperandMetadata getOperandMetadata() {
    return UDFOperandMetadata.wrap((CompositeOperandTypeChecker) buildVariadicMapChecker());
  }

  /** Maximum supported number of MAP operands. */
  private static final int MAX_OPERANDS = 25;

  private static org.apache.calcite.sql.type.SqlSingleOperandTypeChecker buildVariadicMapChecker() {
    org.apache.calcite.sql.type.SqlSingleOperandTypeChecker checker = familyOfMaps(1);
    for (int k = 2; k <= MAX_OPERANDS; k++) {
      checker = checker.or(familyOfMaps(k));
    }
    return checker;
  }

  private static org.apache.calcite.sql.type.FamilyOperandTypeChecker familyOfMaps(int arity) {
    ImmutableList.Builder<SqlTypeFamily> families = ImmutableList.builderWithExpectedSize(arity);
    for (int i = 0; i < arity; i++) {
      families.add(SqlTypeFamily.MAP);
    }
    return OperandTypes.family(families.build());
  }

  public static class RelevanceQueryImplementor implements NotNullImplementor {
    @Override
    public Expression implement(
        RexToLixTranslator translator, RexCall call, List<Expression> translatedOperands) {
      throw new UnsupportedOperationException(
          "Relevance search query functions are only supported when they are pushed down");
    }
  }
}
