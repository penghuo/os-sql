/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.dqe.coordinator.metadata;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Map;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.metadata.MappingMetadata;
import org.opensearch.cluster.metadata.Metadata;
import org.opensearch.cluster.service.ClusterService;

@DisplayName("OpenSearchMetadata: cluster state → table schema")
class OpenSearchMetadataTest {

  @Test
  @DisplayName("Resolves index mapping to TableInfo with Trino types")
  void resolveIndexMapping() {
    ClusterService clusterService = mock(ClusterService.class);
    ClusterState clusterState = mock(ClusterState.class);
    Metadata metadata = mock(Metadata.class);
    IndexMetadata indexMetadata = mock(IndexMetadata.class);
    MappingMetadata mappingMetadata = mock(MappingMetadata.class);

    when(clusterService.state()).thenReturn(clusterState);
    when(clusterState.metadata()).thenReturn(metadata);
    when(metadata.index("logs")).thenReturn(indexMetadata);
    when(indexMetadata.mapping()).thenReturn(mappingMetadata);
    when(mappingMetadata.sourceAsMap())
        .thenReturn(
            Map.of(
                "properties",
                Map.of(
                    "category", Map.of("type", "keyword"),
                    "status", Map.of("type", "long"))));

    OpenSearchMetadata osMetadata = new OpenSearchMetadata(clusterService);
    TableInfo table = osMetadata.getTableInfo("logs");

    assertEquals("logs", table.indexName());
    assertEquals(2, table.columns().size());
  }

  @Test
  @DisplayName("Throws for non-existent index")
  void nonExistentIndex() {
    ClusterService clusterService = mock(ClusterService.class);
    ClusterState clusterState = mock(ClusterState.class);
    Metadata metadata = mock(Metadata.class);

    when(clusterService.state()).thenReturn(clusterState);
    when(clusterState.metadata()).thenReturn(metadata);
    when(metadata.index("missing")).thenReturn(null);

    OpenSearchMetadata osMetadata = new OpenSearchMetadata(clusterService);
    assertThrows(IllegalArgumentException.class, () -> osMetadata.getTableInfo("missing"));
  }

  @Test
  @DisplayName("Skips unsupported field types without error")
  void skipsUnsupportedTypes() {
    ClusterService clusterService = mock(ClusterService.class);
    ClusterState clusterState = mock(ClusterState.class);
    Metadata metadata = mock(Metadata.class);
    IndexMetadata indexMetadata = mock(IndexMetadata.class);
    MappingMetadata mappingMetadata = mock(MappingMetadata.class);

    when(clusterService.state()).thenReturn(clusterState);
    when(clusterState.metadata()).thenReturn(metadata);
    when(metadata.index("idx")).thenReturn(indexMetadata);
    when(indexMetadata.mapping()).thenReturn(mappingMetadata);
    when(mappingMetadata.sourceAsMap())
        .thenReturn(
            Map.of(
                "properties",
                Map.of(
                    "name", Map.of("type", "keyword"),
                    "location", Map.of("type", "geo_shape"))));

    OpenSearchMetadata osMetadata = new OpenSearchMetadata(clusterService);
    TableInfo table = osMetadata.getTableInfo("idx");

    assertEquals(1, table.columns().size());
    assertEquals("name", table.columns().get(0).name());
  }
}
