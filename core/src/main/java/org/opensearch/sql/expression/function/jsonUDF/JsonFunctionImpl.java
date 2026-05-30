/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.expression.function.jsonUDF;

import com.fasterxml.jackson.databind.JsonNode;
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
import org.opensearch.sql.expression.function.ImplementorUDF;
import org.opensearch.sql.expression.function.UDFOperandMetadata;

/**
 * json(value) Evaluates whether the input can be parsed as JSON format. Returns the value if valid,
 * null otherwise. Argument type: ANY Return type: ANY/NULL
 */
public class JsonFunctionImpl extends ImplementorUDF {
  public JsonFunctionImpl() {
    super(new JsonImplementor(), NullPolicy.ANY);
  }

  @Override
  public SqlReturnTypeInference getReturnTypeInference() {
    return ReturnTypes.ARG0_FORCE_NULLABLE;
  }

  @Override
  public UDFOperandMetadata getOperandMetadata() {
    return UDFOperandMetadata.permissiveVariadic();
  }

  public static class JsonImplementor implements NotNullImplementor {
    @Override
    public Expression implement(
        RexToLixTranslator translator, RexCall call, List<Expression> translatedOperands) {
      ScalarFunctionImpl function =
          (ScalarFunctionImpl)
              ScalarFunctionImpl.create(
                  Types.lookupMethod(JsonFunctionImpl.class, "eval", Object[].class));
      return function.getImplementor().implement(translator, call, RexImpTable.NullAs.NULL);
    }
  }

  /** Cached ObjectMapper — thread-safe per Jackson docs. */
  private static final ObjectMapper MAPPER = new ObjectMapper();

  public static Object eval(Object... args) {
    assert args.length == 1 : "Json only accept one argument";
    if (args[0] == null) {
      return null;
    }
    String value = args[0].toString();
    // Mirror v2's `json(x)` / `cast(x as json)` contract: parse and re-serialize via Jackson
    // → Gson canonical form (no whitespace, insertion-ordered fields). Empty string maps to
    // MissingNode which we surface as SQL NULL to match the v2 "json empty string" expectation.
    try {
      JsonNode node = MAPPER.readTree(value);
      if (node == null || node.isMissingNode() || node.isNull()) {
        return null;
      }
      if (node.isTextual()) {
        return node.asText();
      }
      if (node.isInt() || node.isLong()) {
        return node.asLong();
      }
      if (node.isFloatingPointNumber() || node.isDouble()) {
        return node.asDouble();
      }
      if (node.isBoolean()) {
        return node.asBoolean();
      }
      // Object / Array — re-serialize without whitespace via Gson to match v2's output
      // contract that the JsonFunctionsIT test pins via `new Gson().toJson(Map.of(...))`
      // and the existing CalcitePPLJsonBuiltinFunctionIT.testJson which feeds an
      // already-whitespace-free input string.
      return JsonUtils.gson.toJson(MAPPER.convertValue(node, Object.class));
      // NOTE: This returns a STRING. The JsonFunctionsIT test expects JSONObject for some
      // rows; that mismatch is a test-design inconsistency between v2 (which returns parsed
      // ExprValue → JSON object in response) and Calcite (which returns a typed string column).
      // 3 of 5 JsonFunctionsIT tests now pass; the 2 remaining (test_json, test_cast_json)
      // require either changing the column return type to a complex type (would break
      // CalcitePPLJsonBuiltinFunctionIT contract) or test-side updates aligning Calcite's
      // string output. Tracked under #3436.
    } catch (Exception e) {
      return null;
    }
  }
}
