/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.dqe.coordinator.metadata;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.metadata.MappingMetadata;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.sql.dqe.common.types.TypeMapping;
import org.opensearch.sql.dqe.coordinator.metadata.TableInfo.ColumnInfo;

public class OpenSearchMetadata {

  private final ClusterService clusterService;

  public OpenSearchMetadata(ClusterService clusterService) {
    this.clusterService = clusterService;
  }

  @SuppressWarnings("unchecked")
  public TableInfo getTableInfo(String indexName) {
    IndexMetadata indexMetadata = clusterService.state().metadata().index(indexName);
    if (indexMetadata == null) {
      throw new IllegalArgumentException("Index not found: " + indexName);
    }

    MappingMetadata mapping = indexMetadata.mapping();
    if (mapping == null) {
      return new TableInfo(indexName, List.of());
    }

    Map<String, Object> properties = (Map<String, Object>) mapping.sourceAsMap().get("properties");
    if (properties == null) {
      return new TableInfo(indexName, List.of());
    }

    List<ColumnInfo> columns = new ArrayList<>();
    for (Map.Entry<String, Object> entry : properties.entrySet()) {
      Map<String, Object> fieldProps = (Map<String, Object>) entry.getValue();
      String fieldType = (String) fieldProps.get("type");
      if (fieldType != null) {
        try {
          columns.add(
              new ColumnInfo(entry.getKey(), fieldType, TypeMapping.toTrinoType(fieldType)));
        } catch (IllegalArgumentException e) {
          // Skip unsupported field types
        }
      }
    }
    return new TableInfo(indexName, columns);
  }
}
