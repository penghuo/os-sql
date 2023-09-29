/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.datasource;

import org.opensearch.sql.datasource.model.CatalogDataSource;

public abstract class AbstractDataSourceVisitor<T, C> {

  abstract public T visitGlueS3DataSource(CatalogDataSource dataSource, C context);

  abstract public T visitCloudWatchLogDataSource(CatalogDataSource dataSource, C context);
}
