/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite;

import java.util.HashMap;
import java.util.Map;
import lombok.AllArgsConstructor;
import lombok.Getter;
import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.impl.AbstractSchema;
import org.opensearch.sql.DataSourceSchemaName;
import org.opensearch.sql.analysis.DataSourceSchemaIdentifierNameResolver;
import org.opensearch.sql.ast.expression.QualifiedName;
import org.opensearch.sql.data.type.ExprType;
import org.opensearch.sql.datasource.DataSourceService;

@Getter
public class OpenSearchSchema extends AbstractSchema {
  public static final String OPEN_SEARCH_SCHEMA_NAME = "OpenSearch";

  private final DataSourceService dataSourceService;

  private final Map<String, Table> tableMap =
      new HashMap<>() {
        @Override
        public Table get(Object key) {
          if (!super.containsKey(key)) {
            registerTable(new QualifiedName((String) key));
          }
          return super.get(key);
        }
      };
  private Map<String, ExprType> schema;

  public OpenSearchSchema(DataSourceService dataSourceService) {
    this.dataSourceService = dataSourceService;
  }

  public OpenSearchSchema(DataSourceService dataSourceService, Map<String, ExprType> schema) {
    this.dataSourceService = dataSourceService;
    this.schema = schema;
  }

  public void registerTable(QualifiedName qualifiedName) {
    DataSourceSchemaIdentifierNameResolver nameResolver =
        new DataSourceSchemaIdentifierNameResolver(dataSourceService, qualifiedName.getParts());
    org.opensearch.sql.storage.Table table =
        dataSourceService
            .getDataSource(nameResolver.getDataSourceName())
            .getStorageEngine()
            .getTable(
                new DataSourceSchemaName(
                    nameResolver.getDataSourceName(), nameResolver.getSchemaName()),
                nameResolver.getIdentifierName(),
                schema);
    tableMap.put(qualifiedName.toString(), (org.apache.calcite.schema.Table) table);
  }
}
