/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.jdbc;


import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import org.opensearch.sql.DataSourceSchemaName;
import org.opensearch.sql.expression.function.FunctionResolver;
import org.opensearch.sql.storage.StorageEngine;
import org.opensearch.sql.storage.Table;

public class JDBCStorageEngine implements StorageEngine {

  @Override
  public Table getTable(DataSourceSchemaName dataSourceSchemaName, String name) {
    throw new UnsupportedOperationException("JDBC does not support getTable operation");
  }

  @Override
  public Collection<FunctionResolver> getFunctions() {
    return Collections.singletonList(new JDBCTableFunctionResolver());
  }
}
