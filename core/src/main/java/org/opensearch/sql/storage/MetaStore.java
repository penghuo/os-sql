/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.storage;

import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import lombok.Data;
import org.opensearch.sql.DataSourceSchemaName;
import org.opensearch.sql.ast.statement.ddl.Column;

/**
 * Todo. Testing only
 */
public class MetaStore {

  private static MetaStore INSTANCE;

  private final ConcurrentHashMap<String, TableMetaData> tableMetaDataMap =
      new ConcurrentHashMap<>();

  private MetaStore() {

  }

  public static MetaStore instance() {
    if (INSTANCE == null) {
      INSTANCE = new MetaStore();
    }
    return INSTANCE;
  }

  public void add(TableMetaData metaData) {
    tableMetaDataMap.put(metaData.getTableName(), metaData);
  }

  public TableMetaData get(String tableName) {
    return tableMetaDataMap.get(tableName);
  }

  @Data
  public static class TableMetaData {
    // todo. add datasource and schema support.
    private final String tableName;
    private final List<Column> columns;
    private final String fileFormat;
    private final String location;
  }
}
