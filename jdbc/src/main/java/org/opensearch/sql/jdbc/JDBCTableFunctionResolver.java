/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.jdbc;

import static org.opensearch.sql.data.type.ExprCoreType.STRING;

import java.util.List;
import java.util.Locale;
import java.util.Properties;
import lombok.RequiredArgsConstructor;
import org.apache.commons.lang3.tuple.Pair;
import org.opensearch.sql.datasource.model.DataSourceMetadata;
import org.opensearch.sql.exception.SemanticCheckException;
import org.opensearch.sql.expression.function.FunctionBuilder;
import org.opensearch.sql.expression.function.FunctionName;
import org.opensearch.sql.expression.function.FunctionResolver;
import org.opensearch.sql.expression.function.FunctionSignature;

@RequiredArgsConstructor
public class JDBCTableFunctionResolver implements FunctionResolver {

  private static final String URL = "url";

  private static final FunctionName JDBC_FUNCTION = FunctionName.of("jdbc");

  private static final FunctionSignature functionSignature =
      new FunctionSignature(JDBC_FUNCTION, List.of(STRING));

  private final DataSourceMetadata dataSourceMetadata;

  @Override
  public Pair<FunctionSignature, FunctionBuilder> resolve(FunctionSignature unresolvedSignature) {
    FunctionBuilder functionBuilder =
        (properties, arguments) -> {
          checkArgument(!arguments.isEmpty(), new SemanticCheckException(
              String.format(Locale.ROOT, "SQL statement is required. "
                  + "for example %s.jdbc('select * from table')", dataSourceMetadata.getName())));
          String sqlQuery = arguments.get(0).valueOf().stringValue();

          checkArgument(dataSourceMetadata.getProperties().containsKey(URL),
              new SemanticCheckException("url must be defined in datasource"));
          String url = dataSourceMetadata.getProperties().get(URL);

          Properties dataSourceProperties = new Properties();
          dataSourceProperties.putAll(dataSourceMetadata.getProperties());
          return new JDBCFunction(JDBC_FUNCTION, url, sqlQuery, dataSourceProperties);
        };
    return Pair.of(functionSignature, functionBuilder);
  }

  @Override
  public FunctionName getFunctionName() {
    return JDBC_FUNCTION;
  }

  private void checkArgument(boolean condition, RuntimeException exception) {
    if (!condition) {
      throw exception;
    }
  }
}
