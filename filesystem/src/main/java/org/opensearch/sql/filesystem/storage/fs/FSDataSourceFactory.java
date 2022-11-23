/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.filesystem.storage.fs;

import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import org.opensearch.sql.datasource.model.DataSource;
import org.opensearch.sql.datasource.model.DataSourceMetadata;
import org.opensearch.sql.datasource.model.DataSourceType;
import org.opensearch.sql.storage.DataSourceFactory;
import org.opensearch.sql.storage.MetaStore;

@RequiredArgsConstructor
public class FSDataSourceFactory implements DataSourceFactory {

  private final MetaStore metaStore;

  @Override
  public DataSourceType getDataSourceType() {
    return DataSourceType.FILESYSTEM;
  }

  @SneakyThrows
  @Override
  public DataSource createDataSource(DataSourceMetadata metadata) {
    return new DataSource(
        metadata.getName(),
        DataSourceType.FILESYSTEM,
        new FSStorageEngine(metaStore, metadata.getProperties()));
  }
}
