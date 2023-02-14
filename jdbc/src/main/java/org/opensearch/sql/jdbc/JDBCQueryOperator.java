/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.jdbc;

import java.security.AccessController;
import java.security.PrivilegedActionException;
import java.security.PrivilegedExceptionAction;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Locale;
import java.util.Properties;
import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.executor.ExecutionEngine;
import org.opensearch.sql.storage.TableScanOperator;

@RequiredArgsConstructor
public class JDBCQueryOperator extends TableScanOperator {

  private final String url;

  private final String sqlQuery;

  private final Properties properties;

  private Connection connection;

  private Statement statement;

  private ResultSet resultSet;

  private JDBCResponseHandle jdbcResponse;

  @Override
  public String explain() {
    return String.format(Locale.ROOT, "jdbc(%s)", sqlQuery);
  }

  @SneakyThrows
  @Override
  public void open() {
    doPrivileged(
        () -> {
          Class.forName("org.apache.hive.jdbc.HiveDriver");
          connection = DriverManager.getConnection(url, properties);
          statement = connection.createStatement();
          resultSet = statement.executeQuery(sqlQuery);
          jdbcResponse = new JDBCResponseHandle(resultSet.getMetaData());
          return null;
        });
  }

  @SneakyThrows
  @Override
  public void close() {
    if (resultSet != null) {
      resultSet.close();
    }
    if (statement != null) {
      statement.close();
    }
    if (connection != null) {
      connection.close();
    }
  }

  @SneakyThrows
  @Override
  public boolean hasNext() {
    return resultSet.next();
  }

  @Override
  public ExprValue next() {
    return jdbcResponse.parse(resultSet);
  }

  /**
   * Schema is determined at query execution time.
   */
  @Override
  public ExecutionEngine.Schema schema() {
    return jdbcResponse.schema();
  }


  /**
   * Execute the operation in privileged mode.
   */
  public static <T> T doPrivileged(final PrivilegedExceptionAction<T> operation) {
    try {
      return AccessController.doPrivileged(operation);
    } catch (final PrivilegedActionException e) {
      throw new IllegalStateException("Failed to perform privileged action", e);
    }
  }
}
