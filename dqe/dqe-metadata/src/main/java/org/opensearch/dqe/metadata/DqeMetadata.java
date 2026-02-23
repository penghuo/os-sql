/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dqe.metadata;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.metadata.MappingMetadata;
import org.opensearch.cluster.routing.IndexRoutingTable;
import org.opensearch.cluster.routing.IndexShardRoutingTable;
import org.opensearch.cluster.routing.ShardRouting;
import org.opensearch.dqe.types.DqeType;
import org.opensearch.dqe.types.mapping.OpenSearchTypeMappingResolver;
import org.opensearch.dqe.types.mapping.ResolvedField;

/**
 * Metadata service that resolves tables, columns, and shard splits from OpenSearch cluster state.
 * Provides table handle resolution (M-2), column handle extraction (M-3), shard split enumeration
 * (M-4), schema snapshots (M-5), and table statistics (M-6).
 *
 * <p>All operations read from a {@link ClusterState} snapshot, ensuring consistent metadata for the
 * lifetime of a query.
 */
public class DqeMetadata {

  private final OpenSearchTypeMappingResolver typeMappingResolver;
  private final StatisticsCache statisticsCache;

  /**
   * Creates a DqeMetadata service.
   *
   * @param typeMappingResolver resolver for OpenSearch field types to DQE types
   * @param statisticsCache cache for table statistics
   */
  public DqeMetadata(
      OpenSearchTypeMappingResolver typeMappingResolver, StatisticsCache statisticsCache) {
    this.typeMappingResolver =
        Objects.requireNonNull(typeMappingResolver, "typeMappingResolver must not be null");
    this.statisticsCache =
        Objects.requireNonNull(statisticsCache, "statisticsCache must not be null");
  }

  /**
   * Resolves a table handle from cluster state. Validates that the index exists and extracts the
   * schema generation from the index metadata.
   *
   * @param clusterState the current cluster state snapshot
   * @param schema the schema name (typically "default", ignored in Phase 1)
   * @param tableName the table/index name
   * @return resolved table handle
   * @throws DqeTableNotFoundException if the index does not exist
   */
  public DqeTableHandle getTableHandle(ClusterState clusterState, String schema, String tableName) {
    Objects.requireNonNull(clusterState, "clusterState must not be null");
    Objects.requireNonNull(tableName, "tableName must not be null");

    IndexMetadata indexMetadata = clusterState.metadata().index(tableName);
    if (indexMetadata == null) {
      throw new DqeTableNotFoundException(tableName);
    }

    long schemaGeneration = indexMetadata.getVersion();
    return new DqeTableHandle(tableName, null, List.of(tableName), schemaGeneration, null);
  }

  /**
   * Resolves all columns for a table from the index mapping. Extracts field mappings from cluster
   * state and uses the type mapping resolver to convert each field.
   *
   * @param clusterState the current cluster state snapshot
   * @param table the resolved table handle
   * @return list of column handles
   * @throws DqeTableNotFoundException if the index does not exist
   */
  @SuppressWarnings("unchecked")
  public List<DqeColumnHandle> getColumnHandles(ClusterState clusterState, DqeTableHandle table) {
    Objects.requireNonNull(clusterState, "clusterState must not be null");
    Objects.requireNonNull(table, "table must not be null");

    String indexName = table.getIndexName();
    IndexMetadata indexMetadata = clusterState.metadata().index(indexName);
    if (indexMetadata == null) {
      throw new DqeTableNotFoundException(indexName);
    }

    MappingMetadata mappingMetadata = indexMetadata.mapping();
    if (mappingMetadata == null) {
      return List.of();
    }

    Map<String, Object> sourceMap = mappingMetadata.sourceAsMap();
    Object propertiesObj = sourceMap.get("properties");
    if (!(propertiesObj instanceof Map)) {
      return List.of();
    }

    Map<String, Object> properties = (Map<String, Object>) propertiesObj;
    Map<String, ResolvedField> resolvedFields = typeMappingResolver.resolveAll(properties);

    List<DqeColumnHandle> columns = new ArrayList<>();
    for (Map.Entry<String, ResolvedField> entry : resolvedFields.entrySet()) {
      ResolvedField resolved = entry.getValue();
      String fieldPath = resolved.getFieldPath();
      String fieldName = extractLeafName(fieldPath);
      DqeType type = resolved.getType();
      boolean sortable = resolved.isSortable();
      String keywordSubField = resolved.getKeywordSubField();
      boolean isArray = resolved.isArray();

      columns.add(
          new DqeColumnHandle(fieldName, fieldPath, type, sortable, keywordSubField, isArray));
    }

    return List.copyOf(columns);
  }

  /**
   * Enumerates shard splits for a table. For each primary shard of the index, uses the {@link
   * ShardSelector} to pick one copy (primary or replica) and produces a {@link DqeShardSplit}.
   *
   * @param clusterState the current cluster state snapshot
   * @param table the resolved table handle
   * @param localNodeId the local node ID for shard locality preference
   * @return list of shard splits, one per primary shard
   * @throws DqeTableNotFoundException if the index does not exist
   * @throws DqeShardNotAvailableException if any shard has no available copy
   */
  public List<DqeShardSplit> getSplits(
      ClusterState clusterState, DqeTableHandle table, String localNodeId) {
    Objects.requireNonNull(clusterState, "clusterState must not be null");
    Objects.requireNonNull(table, "table must not be null");
    Objects.requireNonNull(localNodeId, "localNodeId must not be null");

    String indexName = table.getIndexName();
    IndexRoutingTable indexRoutingTable = clusterState.routingTable().index(indexName);
    if (indexRoutingTable == null) {
      throw new DqeTableNotFoundException(indexName);
    }

    ShardSelector selector = new ShardSelector(localNodeId);
    List<DqeShardSplit> splits = new ArrayList<>();

    for (Map.Entry<Integer, IndexShardRoutingTable> entry : indexRoutingTable.shards().entrySet()) {
      int shardId = entry.getKey();
      IndexShardRoutingTable shardRoutingTable = entry.getValue();
      ShardRouting selected = selector.select(shardRoutingTable);
      if (selected == null) {
        throw new DqeShardNotAvailableException(indexName, shardId);
      }
      splits.add(
          new DqeShardSplit(shardId, selected.currentNodeId(), indexName, selected.primary()));
    }

    return List.copyOf(splits);
  }

  /**
   * Returns table statistics from the cache, or {@link DqeTableStatistics#UNKNOWN} if not cached or
   * stale.
   *
   * @param table the resolved table handle
   * @return cached statistics or UNKNOWN
   */
  public DqeTableStatistics getStatistics(DqeTableHandle table) {
    Objects.requireNonNull(table, "table must not be null");
    return statisticsCache.get(table.getIndexName(), table.getSchemaGeneration());
  }

  /**
   * Creates an immutable schema snapshot that freezes the table handle and column handles at the
   * current point in time.
   *
   * @param clusterState the current cluster state snapshot
   * @param table the resolved table handle
   * @return frozen schema snapshot
   */
  public SchemaSnapshot createSnapshot(ClusterState clusterState, DqeTableHandle table) {
    Objects.requireNonNull(clusterState, "clusterState must not be null");
    Objects.requireNonNull(table, "table must not be null");

    List<DqeColumnHandle> columns = getColumnHandles(clusterState, table);
    return new SchemaSnapshot(table, columns, System.currentTimeMillis());
  }

  /**
   * Convenience overload for analyzer use: resolves a table handle without explicit ClusterState.
   * This method is overridden in production by the plugin layer which provides the ClusterState.
   * The default implementation throws; subclasses or test mocks must override.
   *
   * @param schema the schema name
   * @param tableName the table/index name
   * @return resolved table handle
   */
  public DqeTableHandle getTableHandle(String schema, String tableName) {
    throw new UnsupportedOperationException(
        "getTableHandle(schema, tableName) requires a DqeMetadata instance with ClusterState");
  }

  /**
   * Convenience overload for analyzer use: resolves columns without explicit ClusterState.
   *
   * @param table the resolved table handle
   * @return list of column handles
   */
  public List<DqeColumnHandle> getColumnHandles(DqeTableHandle table) {
    throw new UnsupportedOperationException(
        "getColumnHandles(table) requires a DqeMetadata instance with ClusterState");
  }

  /** Extracts the leaf field name from a dot-notation path (e.g. "address.city" -> "city"). */
  private static String extractLeafName(String fieldPath) {
    int lastDot = fieldPath.lastIndexOf('.');
    return lastDot >= 0 ? fieldPath.substring(lastDot + 1) : fieldPath;
  }
}
