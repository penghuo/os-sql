/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.dqe.iceberg.transport;

import io.trino.parquet.ParquetReaderOptions;
import io.trino.spi.Page;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.type.Type;
import java.io.ByteArrayInputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.parquet.column.ColumnDescriptor;
import org.opensearch.action.ActionRequest;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.HandledTransportAction;
import org.opensearch.common.inject.Inject;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.common.io.stream.InputStreamStreamInput;
import org.opensearch.sql.dqe.iceberg.IcebergSplitInfo;
import org.opensearch.sql.dqe.iceberg.IcebergTableResolver;
import org.opensearch.sql.dqe.iceberg.ParquetPageSource;
import org.opensearch.sql.dqe.iceberg.ParquetPredicateConverter;
import org.opensearch.sql.dqe.coordinator.metadata.TableInfo;
import org.opensearch.sql.dqe.operator.Operator;
import org.opensearch.sql.dqe.planner.plan.DqePlanNode;
import org.opensearch.sql.dqe.planner.plan.TableScanNode;
import org.opensearch.sql.dqe.shard.executor.LocalExecutionPlanner;
import org.opensearch.sql.dqe.shard.transport.TransportShardExecuteAction;
import org.opensearch.tasks.Task;
import org.opensearch.transport.TransportService;

/**
 * Transport action that executes a DQE plan fragment against a single Iceberg/Parquet file split.
 * Creates a {@link ParquetPageSource} for the split, feeds it into the generic {@link
 * LocalExecutionPlanner} pipeline, and returns result Pages.
 */
public class TransportIcebergSplitExecuteAction
    extends HandledTransportAction<ActionRequest, IcebergSplitExecuteResponse> {

  private static final org.apache.logging.log4j.Logger LOG =
      org.apache.logging.log4j.LogManager.getLogger(TransportIcebergSplitExecuteAction.class);

  private final org.opensearch.cluster.service.ClusterService clusterService;

  @Inject
  public TransportIcebergSplitExecuteAction(
      TransportService transportService, ActionFilters actionFilters,
      org.opensearch.cluster.service.ClusterService clusterService) {
    super(
        IcebergSplitExecuteAction.NAME,
        transportService,
        actionFilters,
        IcebergSplitExecuteRequest::new,
        TransportShardExecuteAction.DQE_THREAD_POOL_NAME);
    this.clusterService = clusterService;
  }

  private String getWarehousePath() {
    return org.opensearch.sql.dqe.common.config.DqeSettings.ICEBERG_WAREHOUSE_PATH.get(
        clusterService.getSettings());
  }

  @Override
  protected void doExecute(
      Task task,
      ActionRequest request,
      ActionListener<IcebergSplitExecuteResponse> listener) {
    IcebergSplitExecuteRequest req = (IcebergSplitExecuteRequest) request;
    try {
      DqePlanNode plan =
          DqePlanNode.readPlanNode(
              new InputStreamStreamInput(
                  new ByteArrayInputStream(req.getSerializedPlan())));

      IcebergSplitInfo splitInfo = req.getSplitInfo();
      IcebergTableResolver resolver = new IcebergTableResolver(getWarehousePath());
      TableInfo tableInfo = resolver.resolve(splitInfo.tableName());
      Table icebergTable = resolver.loadTable(splitInfo.tableName());
      Schema icebergSchema = icebergTable.schema();

      Map<String, Type> columnTypeMap = new HashMap<>();
      for (TableInfo.ColumnInfo col : tableInfo.columns()) {
        columnTypeMap.put(col.name(), col.trinoType());
      }

      // Build scan factory that creates ParquetPageSource for the split
      ParquetReaderOptions options = new ParquetReaderOptions();
      ParquetPredicateConverter predicateConverter = new ParquetPredicateConverter(columnTypeMap);
      LocalExecutionPlanner planner =
          new LocalExecutionPlanner(
              scanNode -> {
                try {
                  // Read file schema for predicate mapping
                  java.io.File file = new java.io.File(splitInfo.filePath());
                  io.trino.filesystem.local.LocalInputFile inputFile =
                      new io.trino.filesystem.local.LocalInputFile(file);
                  io.trino.parquet.ParquetReaderOptions opts = new io.trino.parquet.ParquetReaderOptions();
                  io.trino.parquet.ParquetDataSource ds =
                      new ParquetPageSource.TrinoLocalParquetDataSource(
                          new io.trino.parquet.ParquetDataSourceId(splitInfo.filePath()), inputFile, opts);
                  org.apache.parquet.hadoop.metadata.ParquetMetadata metadata =
                      io.trino.parquet.reader.MetadataReader.readFooter(ds, java.util.Optional.empty());
                  ds.close();
                  org.apache.parquet.schema.MessageType fileSchema =
                      metadata.getFileMetaData().getSchema();

                  TupleDomain<ColumnDescriptor> predicate =
                      predicateConverter.extractPredicates(plan, fileSchema);

                  return new ParquetPageSource(
                      splitInfo.filePath(),
                      icebergSchema,
                      scanNode.getColumns(),
                      columnTypeMap,
                      options,
                      predicate);
                } catch (Exception e) {
                  throw new RuntimeException("Failed to create ParquetPageSource", e);
                }
              },
              columnTypeMap);

      Operator pipeline = plan.accept(planner, null);
      List<Page> pages = new ArrayList<>();
      Page page;
      while ((page = pipeline.processNextBatch()) != null) {
        pages.add(page);
      }
      pipeline.close();

      List<Type> columnTypes = resolveColumnTypes(plan, columnTypeMap);
      listener.onResponse(new IcebergSplitExecuteResponse(pages, columnTypes));
    } catch (Exception e) {
      listener.onFailure(e);
    }
  }

  /**
   * Execute a split locally without transport serialization. Called by the coordinator when running
   * on the same node.
   */
  public IcebergSplitExecuteResponse executeLocal(
      DqePlanNode plan, IcebergSplitInfo splitInfo) throws Exception {
    IcebergTableResolver resolver = new IcebergTableResolver(getWarehousePath());
    TableInfo tableInfo = resolver.resolve(splitInfo.tableName());
    Table icebergTable = resolver.loadTable(splitInfo.tableName());
    Schema icebergSchema = icebergTable.schema();

    Map<String, Type> columnTypeMap = new HashMap<>();
    for (TableInfo.ColumnInfo col : tableInfo.columns()) {
      columnTypeMap.put(col.name(), col.trinoType());
    }

    ParquetReaderOptions options = new ParquetReaderOptions();
    ParquetPredicateConverter predicateConverter = new ParquetPredicateConverter(columnTypeMap);
    LocalExecutionPlanner planner =
        new LocalExecutionPlanner(
            scanNode -> {
              try {
                // Read file schema for predicate mapping
                java.io.File file = new java.io.File(splitInfo.filePath());
                io.trino.filesystem.local.LocalInputFile inputFile =
                    new io.trino.filesystem.local.LocalInputFile(file);
                io.trino.parquet.ParquetReaderOptions opts = new io.trino.parquet.ParquetReaderOptions();
                io.trino.parquet.ParquetDataSource ds =
                    new ParquetPageSource.TrinoLocalParquetDataSource(
                        new io.trino.parquet.ParquetDataSourceId(splitInfo.filePath()), inputFile, opts);
                org.apache.parquet.hadoop.metadata.ParquetMetadata metadata =
                    io.trino.parquet.reader.MetadataReader.readFooter(ds, java.util.Optional.empty());
                ds.close();
                org.apache.parquet.schema.MessageType fileSchema =
                    metadata.getFileMetaData().getSchema();

                TupleDomain<ColumnDescriptor> predicate =
                    predicateConverter.extractPredicates(plan, fileSchema);

                return new ParquetPageSource(
                    splitInfo.filePath(),
                    icebergSchema,
                    scanNode.getColumns(),
                    columnTypeMap,
                    options,
                    predicate);
              } catch (Exception e) {
                throw new RuntimeException("Failed to create ParquetPageSource", e);
              }
            },
            columnTypeMap);

    Operator pipeline = plan.accept(planner, null);
    List<Page> pages = new ArrayList<>();
    Page page;
    while ((page = pipeline.processNextBatch()) != null) {
      pages.add(page);
    }
    pipeline.close();

    List<Type> columnTypes = resolveColumnTypes(plan, columnTypeMap);
    return new IcebergSplitExecuteResponse(pages, columnTypes);
  }

  /**
   * Execute a split with bucket filtering for multi-pass aggregation.
   * Only rows where hash(groupKey) % numBuckets == bucket are processed.
   */
  public IcebergSplitExecuteResponse executeLocalWithBucketFilter(
      DqePlanNode plan, IcebergSplitInfo splitInfo,
      List<Integer> groupByIndices, List<Type> allColumnTypes,
      int bucket, int numBuckets) throws Exception {
    IcebergTableResolver resolver = new IcebergTableResolver(getWarehousePath());
    TableInfo tableInfo = resolver.resolve(splitInfo.tableName());
    Table icebergTable = resolver.loadTable(splitInfo.tableName());
    Schema icebergSchema = icebergTable.schema();

    Map<String, Type> columnTypeMap = new HashMap<>();
    for (TableInfo.ColumnInfo col : tableInfo.columns()) {
      columnTypeMap.put(col.name(), col.trinoType());
    }

    ParquetReaderOptions options = new ParquetReaderOptions();
    ParquetPredicateConverter predicateConverter = new ParquetPredicateConverter(columnTypeMap);
    LocalExecutionPlanner planner =
        new LocalExecutionPlanner(
            scanNode -> {
              try {
                java.io.File file = new java.io.File(splitInfo.filePath());
                io.trino.filesystem.local.LocalInputFile inputFile =
                    new io.trino.filesystem.local.LocalInputFile(file);
                io.trino.parquet.ParquetReaderOptions opts = new io.trino.parquet.ParquetReaderOptions();
                io.trino.parquet.ParquetDataSource ds =
                    new ParquetPageSource.TrinoLocalParquetDataSource(
                        new io.trino.parquet.ParquetDataSourceId(splitInfo.filePath()), inputFile, opts);
                org.apache.parquet.hadoop.metadata.ParquetMetadata metadata =
                    io.trino.parquet.reader.MetadataReader.readFooter(ds, java.util.Optional.empty());
                ds.close();
                org.apache.parquet.schema.MessageType fileSchema =
                    metadata.getFileMetaData().getSchema();
                TupleDomain<ColumnDescriptor> predicate =
                    predicateConverter.extractPredicates(plan, fileSchema);
                Operator source = new ParquetPageSource(
                    splitInfo.filePath(), icebergSchema, scanNode.getColumns(),
                    columnTypeMap, options, predicate);
                return new org.opensearch.sql.dqe.operator.HashAggregationOperator.BucketFilterOperator(
                    source, groupByIndices, allColumnTypes, bucket, numBuckets);
              } catch (Exception e) {
                throw new RuntimeException("Failed to create ParquetPageSource", e);
              }
            },
            columnTypeMap);

    Operator pipeline = plan.accept(planner, null);
    List<Page> pages = new ArrayList<>();
    Page page;
    while ((page = pipeline.processNextBatch()) != null) {
      pages.add(page);
    }
    pipeline.close();

    List<Type> columnTypes = resolveColumnTypes(plan, columnTypeMap);
    return new IcebergSplitExecuteResponse(pages, columnTypes);
  }

  private static List<Type> resolveColumnTypes(
      DqePlanNode plan, Map<String, Type> columnTypeMap) {
    List<String> columnNames = resolveColumnNames(plan);
    List<Type> types = new ArrayList<>();

    // For aggregation nodes, resolve output types based on function semantics
    if (plan instanceof org.opensearch.sql.dqe.planner.plan.AggregationNode agg) {
      for (String key : agg.getGroupByKeys()) {
        types.add(columnTypeMap.getOrDefault(key, io.trino.spi.type.BigintType.BIGINT));
      }
      for (String func : agg.getAggregateFunctions()) {
        // Parse "funcName(colName)" to determine output type
        // MIN/MAX preserve input column type; COUNT/SUM → BIGINT
        int paren = func.indexOf('(');
        String funcName = paren > 0 ? func.substring(0, paren).toLowerCase() : func.toLowerCase();
        if (funcName.equals("min") || funcName.equals("max")) {
          // Extract column name from "min(ColName)" or "max(ColName)"
          String colName = paren > 0 && func.endsWith(")")
              ? func.substring(paren + 1, func.length() - 1).trim() : null;
          if (colName != null && columnTypeMap.containsKey(colName)) {
            types.add(columnTypeMap.get(colName));
          } else {
            types.add(io.trino.spi.type.BigintType.BIGINT);
          }
        } else {
          types.add(io.trino.spi.type.BigintType.BIGINT);
        }
      }
      return types;
    }

    for (String col : columnNames) {
      Type resolved = columnTypeMap.getOrDefault(col, io.trino.spi.type.BigintType.BIGINT);
      if (resolved == io.trino.spi.type.BigintType.BIGINT && !columnTypeMap.containsKey(col)) {
        LOG.warn("resolveColumnTypes: column '{}' not in typeMap, defaulting to BIGINT. typeMap keys: {}",
            col, columnTypeMap.keySet());
      }
      types.add(resolved);
    }
    return types;
  }

  private static List<String> resolveColumnNames(DqePlanNode node) {
    if (node instanceof TableScanNode scan) {
      return scan.getColumns();
    }
    if (node instanceof org.opensearch.sql.dqe.planner.plan.ProjectNode proj) {
      return proj.getOutputColumns();
    }
    if (node instanceof org.opensearch.sql.dqe.planner.plan.AggregationNode agg) {
      List<String> names = new ArrayList<>(agg.getGroupByKeys());
      names.addAll(agg.getAggregateFunctions());
      return names;
    }
    List<DqePlanNode> children = node.getChildren();
    if (!children.isEmpty()) {
      return resolveColumnNames(children.get(0));
    }
    return List.of();
  }
}
