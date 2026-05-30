/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.expression.function.jsonUDF;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.List;
import org.apache.calcite.adapter.enumerable.NotNullImplementor;
import org.apache.calcite.adapter.enumerable.NullPolicy;
import org.apache.calcite.adapter.enumerable.RexImpTable;
import org.apache.calcite.adapter.enumerable.RexToLixTranslator;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.linq4j.tree.Types;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.schema.impl.ScalarFunctionImpl;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.type.SqlReturnTypeInference;
import org.apache.calcite.sql.type.SqlTypeTransforms;
import org.opensearch.sql.calcite.utils.PPLOperandTypes;
import org.opensearch.sql.expression.function.ImplementorUDF;
import org.opensearch.sql.expression.function.UDFOperandMetadata;

/**
 * PPL {@code json_valid(json_string)} — true when the input parses as valid JSON.
 *
 * <p>Mirrors v2's {@link org.opensearch.sql.utils.JsonUtils#isValidJson} contract, which uses
 * Jackson's {@code ObjectMapper.readTree} (returns {@code MissingNode} for empty string, so empty
 * is valid). Calcite's stock {@code SqlStdOperatorTable.IS_JSON_VALUE} delegates to JsonPath /
 * Jackson {@code parse} which throws on empty input — that produces FALSE for empty string and
 * breaks the v2 contract that several JsonFunctionsIT tests assume.
 */
public class JsonValidFunctionImpl extends ImplementorUDF {
  public JsonValidFunctionImpl() {
    super(new JsonValidImplementor(), NullPolicy.NONE);
  }

  @Override
  public SqlReturnTypeInference getReturnTypeInference() {
    return ReturnTypes.BOOLEAN.andThen(SqlTypeTransforms.FORCE_NULLABLE);
  }

  @Override
  public UDFOperandMetadata getOperandMetadata() {
    return PPLOperandTypes.STRING;
  }

  public static class JsonValidImplementor implements NotNullImplementor {
    @Override
    public Expression implement(
        RexToLixTranslator translator, RexCall call, List<Expression> translatedOperands) {
      ScalarFunctionImpl function =
          (ScalarFunctionImpl)
              ScalarFunctionImpl.create(
                  Types.lookupMethod(JsonValidFunctionImpl.class, "eval", Object[].class));
      return function.getImplementor().implement(translator, call, RexImpTable.NullAs.NULL);
    }
  }

  /** Single ObjectMapper reused across calls; thread-safe per Jackson docs. */
  private static final ObjectMapper MAPPER = new ObjectMapper();

  public static Object eval(Object... args) {
    assert args.length == 1 : "json_valid accepts one argument";
    if (args[0] == null) {
      return false;
    }
    String value = String.valueOf(args[0]);
    try {
      MAPPER.readTree(value);
      return true;
    } catch (Exception e) {
      return false;
    }
  }
}
