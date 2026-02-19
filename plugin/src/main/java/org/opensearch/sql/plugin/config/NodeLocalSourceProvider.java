/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.plugin.config;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.MultiReader;
import org.apache.lucene.search.IndexSearcher;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.index.IndexService;
import org.opensearch.index.engine.Engine;
import org.opensearch.index.mapper.MappedFieldType;
import org.opensearch.index.mapper.MapperService;
import org.opensearch.index.shard.IndexShard;
import org.opensearch.index.shard.ShardNotFoundException;
import org.opensearch.indices.IndicesService;
import org.opensearch.sql.distributed.context.OperatorContext;
import org.opensearch.sql.distributed.lucene.ColumnMapping;
import org.opensearch.sql.distributed.lucene.LuceneFullScan;
import org.opensearch.sql.distributed.operator.Operator;
import org.opensearch.sql.distributed.operator.OperatorFactory;
import org.opensearch.sql.distributed.planner.LuceneTableScanNode;
import org.opensearch.sql.distributed.planner.RemoteSourceNode;
import org.opensearch.sql.distributed.planner.bridge.PlanNodeToOperatorConverter;

/**
 * Source operator factory provider for single-node (local) execution. Acquires IndexSearcher from
 * all local shards via IndicesService and creates LuceneFullScan operators that read directly from
 * Lucene DocValues.
 */
public class NodeLocalSourceProvider
    implements PlanNodeToOperatorConverter.SourceOperatorFactoryProvider {

  private final IndicesService indicesService;
  private final ClusterService clusterService;

  public NodeLocalSourceProvider(IndicesService indicesService, ClusterService clusterService) {
    this.indicesService = indicesService;
    this.clusterService = clusterService;
  }

  @Override
  public OperatorFactory createSourceFactory(LuceneTableScanNode node) {
    return new LocalShardScanFactory(
        indicesService, clusterService, node.getIndexName(), node.getOutputType());
  }

  @Override
  public OperatorFactory createRemoteSourceFactory(RemoteSourceNode node) {
    throw new UnsupportedOperationException(
        "Remote sources are not supported in Phase 1 single-node execution");
  }

  /**
   * Factory that creates source operators by acquiring searchers from all local shards of an index.
   * Each operator acquires its own searcher references and releases them on close.
   */
  static class LocalShardScanFactory implements OperatorFactory {
    private final IndicesService indicesService;
    private final ClusterService clusterService;
    private final String indexName;
    private final RelDataType rowType;

    LocalShardScanFactory(
        IndicesService indicesService,
        ClusterService clusterService,
        String indexName,
        RelDataType rowType) {
      this.indicesService = indicesService;
      this.clusterService = clusterService;
      this.indexName = indexName;
      this.rowType = rowType;
    }

    @Override
    public Operator createOperator(OperatorContext operatorContext) {
      try {
        IndexMetadata indexMetadata = clusterService.state().metadata().index(indexName);
        if (indexMetadata == null) {
          throw new IllegalArgumentException("Index not found: " + indexName);
        }

        IndexService indexService = indicesService.indexServiceSafe(indexMetadata.getIndex());
        int numberOfShards = indexMetadata.getNumberOfShards();

        List<Engine.Searcher> searchers = new ArrayList<>();
        List<IndexReader> readers = new ArrayList<>();

        try {
          for (int shardId = 0; shardId < numberOfShards; shardId++) {
            try {
              IndexShard shard = indexService.getShard(shardId);
              Engine.Searcher searcher = shard.acquireSearcher("distributed_query");
              searchers.add(searcher);
              readers.add(searcher.getIndexReader());
            } catch (ShardNotFoundException e) {
              // Shard not on this node — skip (only possible in multi-node clusters)
            }
          }

          if (readers.isEmpty()) {
            throw new IllegalStateException("No local shards found for index: " + indexName);
          }

          MultiReader multiReader = new MultiReader(readers.toArray(new IndexReader[0]), false);
          IndexSearcher combinedSearcher = new IndexSearcher(multiReader);
          MapperService mapperService = indexService.mapperService();
          List<ColumnMapping> columns = buildColumnMappings(rowType, mapperService);

          return new CloseableLuceneFullScan(operatorContext, combinedSearcher, columns, searchers);
        } catch (Exception e) {
          // Release any acquired searchers on failure
          for (Engine.Searcher s : searchers) {
            s.close();
          }
          throw e;
        }
      } catch (IOException e) {
        throw new UncheckedIOException("Failed to create source operator", e);
      }
    }

    @Override
    public void noMoreOperators() {}
  }

  /**
   * Extension of LuceneFullScan that releases acquired Engine.Searcher references on close. This
   * ensures proper resource cleanup when the Driver closes its operators.
   */
  static class CloseableLuceneFullScan extends LuceneFullScan {
    private final List<Engine.Searcher> searchers;

    CloseableLuceneFullScan(
        OperatorContext operatorContext,
        IndexSearcher searcher,
        List<ColumnMapping> columns,
        List<Engine.Searcher> searchers) {
      super(operatorContext, searcher, columns);
      this.searchers = searchers;
    }

    @Override
    public void close() throws Exception {
      super.close();
      for (Engine.Searcher searcher : searchers) {
        try {
          searcher.close();
        } catch (Exception e) {
          // Best-effort cleanup
        }
      }
    }
  }

  /**
   * Builds ColumnMappings using the actual OpenSearch field mapping from MapperService. This
   * ensures we use the correct DocValues type that Lucene actually stores, rather than guessing
   * from Calcite SQL types.
   *
   * <p>OpenSearch field types map to DocValues as follows:
   *
   * <ul>
   *   <li>keyword → SORTED_SET DocValues, VARIABLE_WIDTH Block
   *   <li>text → SORTED_SET DocValues (if fielddata enabled), VARIABLE_WIDTH Block
   *   <li>long, integer, short, byte, date → SORTED_NUMERIC DocValues
   *   <li>double, float, half_float, scaled_float → SORTED_NUMERIC DocValues
   *   <li>boolean → SORTED_NUMERIC DocValues (0/1)
   * </ul>
   */
  static List<ColumnMapping> buildColumnMappings(RelDataType rowType, MapperService mapperService) {
    List<ColumnMapping> columns = new ArrayList<>();
    for (RelDataTypeField field : rowType.getFieldList()) {
      int index = field.getIndex();
      String name = field.getName();

      MappedFieldType fieldType = mapperService.fieldType(name);
      ColumnMapping mapping;

      if (fieldType == null || !fieldType.hasDocValues()) {
        // Try to find a .keyword sub-field for text fields that lack DocValues
        String keywordSubField = name + ".keyword";
        MappedFieldType keywordFieldType = mapperService.fieldType(keywordSubField);
        if (keywordFieldType != null && keywordFieldType.hasDocValues()) {
          // Use the keyword sub-field to read values via DocValues
          mapping = buildMappingFromFieldType(keywordSubField, index, keywordFieldType);
        } else {
          // No DocValues available: produce all-null output (e.g., _id metadata field)
          mapping =
              new ColumnMapping(
                  name,
                  ColumnMapping.DocValuesType.NONE,
                  ColumnMapping.BlockType.VARIABLE_WIDTH,
                  index);
        }
      } else {
        mapping = buildMappingFromFieldType(name, index, fieldType);
      }
      columns.add(mapping);
    }
    return columns;
  }

  /**
   * Creates a ColumnMapping from an OpenSearch MappedFieldType using the field's actual type name
   * to determine the correct DocValues type and Block type.
   */
  private static ColumnMapping buildMappingFromFieldType(
      String name, int index, MappedFieldType fieldType) {
    String typeName = fieldType.typeName();

    return switch (typeName) {
      case "long" ->
          new ColumnMapping(
              name,
              ColumnMapping.DocValuesType.SORTED_NUMERIC,
              ColumnMapping.BlockType.LONG,
              index);
      case "integer" ->
          new ColumnMapping(
              name, ColumnMapping.DocValuesType.SORTED_NUMERIC, ColumnMapping.BlockType.INT, index);
      case "short" ->
          new ColumnMapping(
              name,
              ColumnMapping.DocValuesType.SORTED_NUMERIC,
              ColumnMapping.BlockType.SHORT,
              index);
      case "byte" ->
          new ColumnMapping(
              name,
              ColumnMapping.DocValuesType.SORTED_NUMERIC,
              ColumnMapping.BlockType.BYTE,
              index);
      case "double", "scaled_float" ->
          new ColumnMapping(
              name,
              ColumnMapping.DocValuesType.SORTED_NUMERIC,
              ColumnMapping.BlockType.DOUBLE,
              index);
      case "float", "half_float" ->
          new ColumnMapping(
              name,
              ColumnMapping.DocValuesType.SORTED_NUMERIC,
              ColumnMapping.BlockType.FLOAT,
              index);
      case "boolean" ->
          new ColumnMapping(
              name,
              ColumnMapping.DocValuesType.SORTED_NUMERIC,
              ColumnMapping.BlockType.BOOLEAN,
              index);
      case "date", "date_nanos" ->
          new ColumnMapping(
              name,
              ColumnMapping.DocValuesType.SORTED_NUMERIC,
              ColumnMapping.BlockType.LONG,
              index);
      case "keyword", "constant_keyword", "wildcard" ->
          new ColumnMapping(
              name,
              ColumnMapping.DocValuesType.SORTED_SET,
              ColumnMapping.BlockType.VARIABLE_WIDTH,
              index);
      case "text" ->
          // text fields with fielddata enabled use SORTED_SET; without fielddata, no DocValues
          new ColumnMapping(
              name,
              ColumnMapping.DocValuesType.SORTED_SET,
              ColumnMapping.BlockType.VARIABLE_WIDTH,
              index);
      case "ip" ->
          new ColumnMapping(
              name,
              ColumnMapping.DocValuesType.SORTED_SET,
              ColumnMapping.BlockType.VARIABLE_WIDTH,
              index,
              true);
      case "binary" ->
          new ColumnMapping(
              name,
              ColumnMapping.DocValuesType.BINARY,
              ColumnMapping.BlockType.VARIABLE_WIDTH,
              index);
      default ->
          // Unknown type: try SORTED_NUMERIC for numeric-like, otherwise SORTED_SET
          new ColumnMapping(
              name,
              ColumnMapping.DocValuesType.SORTED_SET,
              ColumnMapping.BlockType.VARIABLE_WIDTH,
              index);
    };
  }
}
