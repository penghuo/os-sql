/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.filesystem.storage.fs;

import java.net.URI;
import java.util.Map;
import java.util.stream.Collectors;
import lombok.SneakyThrows;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.s3a.S3AFileSystem;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.io.compress.GzipCodec;
import org.opensearch.sql.DataSourceSchemaName;
import org.opensearch.sql.ast.statement.ddl.Column;
import org.opensearch.sql.common.utils.AccessController;
import org.opensearch.sql.storage.MetaStore;
import org.opensearch.sql.storage.StorageEngine;
import org.opensearch.sql.storage.Table;

public class FSStorageEngine implements StorageEngine {

  private final MetaStore metaStore;

  private final Configuration conf;

  public FSStorageEngine(MetaStore metaStore,
                         Map<String, String> properties) {
    this.metaStore = metaStore;
    this.conf = new Configuration();
    properties.forEach(conf::set);
  }

  @SneakyThrows
  @Override
  public Table getTable(DataSourceSchemaName dataSourceSchemaName, String tableName) {
    // todo
    MetaStore.TableMetaData tableMetaData =
        metaStore.get(dataSourceSchemaName.getDataSourceName() + "." + tableName);
    CompressionCodec codec =
        AccessController.doPrivileged(
            () -> {
              // todo, CompressionCodec
              GzipCodec gzipCodec = (GzipCodec)
                  new CompressionCodecFactory(conf).getCodecByName(tableMetaData.getFileFormat());
              gzipCodec.setConf(conf);
              return gzipCodec;
            });
    URI uri = new URI(tableMetaData.getLocation());
    Path basePath = new Path(uri);
    Map<String, String> fieldType =
        tableMetaData.getColumns().stream().collect(Collectors.toUnmodifiableMap(Column::getName,
            Column::getType));
    FileSystem fs = AccessController.doPrivileged(() -> {
      S3AFileSystem fileSystem = new S3AFileSystem();
      fileSystem.initialize(uri, conf);
      return fileSystem;
    });
    return new FSTable(fs, codec, basePath, fieldType);
  }
}
