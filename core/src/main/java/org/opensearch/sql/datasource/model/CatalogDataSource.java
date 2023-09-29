/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.datasource.model;

import lombok.Getter;
import org.opensearch.sql.datasource.AbstractDataSourceVisitor;
import org.opensearch.sql.datasource.conf.DataSourceProperty;

@Getter
public class CatalogDataSource extends DataSource {

  private final DataSourceProperty dataSourceProperty;

  public CatalogDataSource(String name, DataSourceType connectorType,
                           DataSourceProperty dataSourceProperty) {
    super(name, connectorType, (dataSourceSchemaName, tableName) -> {
      throw new UnsupportedOperationException("CWL storage engine is not supported.");
    });
    this.dataSourceProperty = dataSourceProperty;
  }

  public <T, C> T accept(AbstractDataSourceVisitor<T, C> visitor, C context) {
    if (getConnectorType() == DataSourceType.S3GLUE) {
      return visitor.visitGlueS3DataSource(this, context);
    } else if (getConnectorType() == DataSourceType.CLOUDWATCHLOG) {
      return visitor.visitGlueS3DataSource(this, context);
    }
    throw new UnsupportedOperationException("unsupported datasource type");
  }
}
