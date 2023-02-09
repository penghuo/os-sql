/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.jdbc;

import static org.opensearch.sql.data.type.ExprCoreType.STRING;

import java.util.List;
import org.apache.commons.lang3.tuple.Pair;
import org.opensearch.sql.expression.function.FunctionBuilder;
import org.opensearch.sql.expression.function.FunctionName;
import org.opensearch.sql.expression.function.FunctionResolver;
import org.opensearch.sql.expression.function.FunctionSignature;
import org.opensearch.sql.jdbc.functions.JDBCFunction;

public class JDBCTableFunctionResolver implements FunctionResolver {

  private static final FunctionName JDBC_FUNCTION = FunctionName.of("jdbc");
  private static final FunctionSignature functionSignature =
      new FunctionSignature(JDBC_FUNCTION, List.of(STRING));
  private static final FunctionBuilder functionBuilder =
      (properties, arguments) ->
          new JDBCFunction(JDBC_FUNCTION, arguments.get(0).valueOf().stringValue());

  @Override
  public Pair<FunctionSignature, FunctionBuilder> resolve(FunctionSignature unresolvedSignature) {
    return Pair.of(functionSignature, functionBuilder);
  }

  @Override
  public FunctionName getFunctionName() {
    return JDBC_FUNCTION;
  }
}
