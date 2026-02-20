/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.distributed.transport;

import com.google.common.util.concurrent.ListenableFuture;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.List;
import lombok.extern.log4j.Log4j2;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.MultiReader;
import org.apache.lucene.search.IndexSearcher;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.HandledTransportAction;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.inject.Inject;
import org.opensearch.core.action.ActionListener;
import org.opensearch.index.IndexService;
import org.opensearch.index.engine.Engine;
import org.opensearch.index.mapper.MappedFieldType;
import org.opensearch.index.mapper.MapperService;
import org.opensearch.index.shard.IndexShard;
import org.opensearch.indices.IndicesService;
import org.opensearch.sql.distributed.context.DriverContext;
import org.opensearch.sql.distributed.context.OperatorContext;
import org.opensearch.sql.distributed.context.PipelineContext;
import org.opensearch.sql.distributed.context.QueryContext;
import org.opensearch.sql.distributed.context.TaskContext;
import org.opensearch.sql.distributed.data.Page;
import org.opensearch.sql.distributed.data.PagesSerde;
import org.opensearch.sql.distributed.driver.Driver;
import org.opensearch.sql.distributed.driver.Pipeline;
import org.opensearch.sql.distributed.lucene.ColumnMapping;
import org.opensearch.sql.distributed.lucene.LuceneFullScan;
import org.opensearch.sql.distributed.memory.MemoryPool;
import org.opensearch.sql.distributed.operator.Operator;
import org.opensearch.sql.distributed.operator.OperatorFactory;
import org.opensearch.sql.distributed.operator.SinkOperator;
import org.opensearch.sql.distributed.planner.LuceneTableScanNode;
import org.opensearch.sql.distributed.planner.PlanNode;
import org.opensearch.sql.distributed.planner.RemoteSourceNode;
import org.opensearch.sql.distributed.planner.bridge.PlanNodeToOperatorConverter;
import org.opensearch.sql.distributed.planner.plan.StageFragment;
import org.opensearch.tasks.Task;
import org.opensearch.transport.TransportService;

/**
 * Transport action handler for executing a plan fragment on a data node. Looks up the
 * StageFragment from the in-memory {@link FragmentRegistry}, acquires IndexSearchers for the
 * requested shards, builds the operator pipeline, runs it to completion, and returns the produced
 * Pages.
 */
@Log4j2
public class TransportShardQueryAction
    extends HandledTransportAction<ShardQueryRequest, ShardQueryResponse> {

  private static final long DEFAULT_MEMORY_POOL_BYTES = 64 * 1024 * 1024; // 64 MB
  private static final long DEFAULT_QUERY_TIMEOUT_MILLIS = 30_000;
  private static final int MAX_DRIVER_ITERATIONS = 1_000_000;

  private final PagesSerde pagesSerde;
  private final IndicesService indicesService;
  private final ClusterService clusterService;

  @Inject
  public TransportShardQueryAction(
      TransportService transportService,
      ActionFilters actionFilters,
      IndicesService indicesService,
      ClusterService clusterService) {
    // Use GENERIC thread pool instead of SAME to avoid deadlock in single-node clusters.
    // With SAME, the transport action runs inline on the calling thread, which is blocked
    // waiting for the response via GatherExchange.blocked.get(). GENERIC dispatches to a
    // separate thread, allowing the caller to block while the action executes concurrently.
    super(
        ShardQueryAction.NAME,
        transportService,
        actionFilters,
        ShardQueryRequest::new,
        org.opensearch.threadpool.ThreadPool.Names.GENERIC);
    this.pagesSerde = new PagesSerde();
    this.indicesService = indicesService;
    this.clusterService = clusterService;
  }

  @Override
  protected void doExecute(
      Task task, ShardQueryRequest request, ActionListener<ShardQueryResponse> listener) {
    try {
      log.debug(
          "Executing shard query: queryId={}, stageId={}, shards={}, index={}",
          request.getQueryId(),
          request.getStageId(),
          request.getShardIds(),
          request.getIndexName());

      // Step 1: Look up the StageFragment from the in-memory registry
      StageFragment fragment =
          FragmentRegistry.get(request.getQueryId(), request.getStageId());
      if (fragment == null) {
        throw new IllegalStateException(
            "No fragment registered for queryId="
                + request.getQueryId()
                + ", stageId="
                + request.getStageId());
      }

      // Step 2: Build a source provider that reads from the specific requested shards
      ShardLocalSourceProvider sourceProvider =
          new ShardLocalSourceProvider(
              indicesService, clusterService, request.getIndexName(), request.getShardIds());

      // Step 3: Convert PlanNode tree to OperatorFactories
      List<OperatorFactory> factories =
          PlanNodeToOperatorConverter.convert(fragment.getRoot(), sourceProvider);

      // Step 4: Append a collecting sink
      List<OperatorFactory> withSink = new ArrayList<>(factories);
      withSink.add(new CollectingSinkFactory());

      // Step 5: Build context and pipeline, then run
      String queryId = request.getQueryId();
      MemoryPool pool = new MemoryPool(queryId, DEFAULT_MEMORY_POOL_BYTES);
      QueryContext queryContext = new QueryContext(queryId, pool, DEFAULT_QUERY_TIMEOUT_MILLIS);
      try {
        TaskContext taskContext = queryContext.addTaskContext(request.getStageId());
        PipelineContext pipelineContext = taskContext.addPipelineContext(0);
        Pipeline pipeline = new Pipeline(pipelineContext, withSink);

        DriverContext driverContext = pipelineContext.addDriverContext();
        Driver driver = pipeline.createDriver(driverContext);
        try {
          List<Page> outputPages = runDriverToCompletion(driver);

          ShardQueryResponse response = new ShardQueryResponse(outputPages, false, pagesSerde);
          listener.onResponse(response);
        } finally {
          driver.close();
        }
      } finally {
        queryContext.close();
      }

    } catch (Exception e) {
      log.error(
          "Shard query execution failed: queryId={}, stageId={}",
          request.getQueryId(),
          request.getStageId(),
          e);
      listener.onFailure(e);
    }
  }

  /** Runs the Driver in a cooperative loop until finished. */
  private List<Page> runDriverToCompletion(Driver driver) {
    int iterations = 0;
    while (!driver.isFinished() && iterations < MAX_DRIVER_ITERATIONS) {
      ListenableFuture<?> blocked = driver.process();
      if (!blocked.isDone()) {
        throw new IllegalStateException("Driver blocked unexpectedly during shard query execution");
      }
      iterations++;
    }
    if (!driver.isFinished()) {
      throw new IllegalStateException(
          "Driver did not finish within " + MAX_DRIVER_ITERATIONS + " iterations");
    }

    Operator sinkOp = driver.getOperators().get(driver.getOperators().size() - 1);
    if (sinkOp instanceof CollectingSink collectingSink) {
      return collectingSink.getCollectedPages();
    }
    throw new IllegalStateException("Last operator is not a CollectingSink");
  }

  /**
   * Source provider that reads from specific shards on this node (not all shards). Used by
   * TransportShardQueryAction to execute a leaf fragment against only the assigned shards.
   */
  private static class ShardLocalSourceProvider
      implements PlanNodeToOperatorConverter.SourceOperatorFactoryProvider {

    private final IndicesService indicesService;
    private final ClusterService clusterService;
    private final String indexName;
    private final List<Integer> shardIds;

    ShardLocalSourceProvider(
        IndicesService indicesService,
        ClusterService clusterService,
        String indexName,
        List<Integer> shardIds) {
      this.indicesService = indicesService;
      this.clusterService = clusterService;
      this.indexName = indexName;
      this.shardIds = shardIds;
    }

    @Override
    public OperatorFactory createSourceFactory(LuceneTableScanNode node) {
      return new ShardScanFactory(
          indicesService, clusterService, indexName, shardIds, node.getOutputType());
    }

    @Override
    public OperatorFactory createRemoteSourceFactory(RemoteSourceNode node) {
      throw new UnsupportedOperationException(
          "Remote sources are not supported in leaf stage execution");
    }
  }

  /**
   * Factory that creates LuceneFullScan operators reading from specific shards. Similar to
   * NodeLocalSourceProvider.LocalShardScanFactory but restricted to the assigned shard IDs.
   */
  private static class ShardScanFactory implements OperatorFactory {
    private final IndicesService indicesService;
    private final ClusterService clusterService;
    private final String indexName;
    private final List<Integer> shardIds;
    private final RelDataType rowType;

    ShardScanFactory(
        IndicesService indicesService,
        ClusterService clusterService,
        String indexName,
        List<Integer> shardIds,
        RelDataType rowType) {
      this.indicesService = indicesService;
      this.clusterService = clusterService;
      this.indexName = indexName;
      this.shardIds = shardIds;
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
        List<Engine.Searcher> searchers = new ArrayList<>();
        List<IndexReader> readers = new ArrayList<>();

        try {
          for (int shardId : shardIds) {
            IndexShard shard = indexService.getShard(shardId);
            Engine.Searcher searcher = shard.acquireSearcher("distributed_shard_query");
            searchers.add(searcher);
            readers.add(searcher.getIndexReader());
          }

          if (readers.isEmpty()) {
            throw new IllegalStateException(
                "No local shards found for index: " + indexName + " shards: " + shardIds);
          }

          MultiReader multiReader = new MultiReader(readers.toArray(new IndexReader[0]), false);
          IndexSearcher combinedSearcher = new IndexSearcher(multiReader);
          MapperService mapperService = indexService.mapperService();
          List<ColumnMapping> columns = buildColumnMappings(rowType, mapperService);

          return new CloseableLuceneFullScan(operatorContext, combinedSearcher, columns, searchers);
        } catch (Exception e) {
          for (Engine.Searcher s : searchers) {
            s.close();
          }
          throw e;
        }
      } catch (IOException e) {
        throw new UncheckedIOException("Failed to create source operator for shard query", e);
      }
    }

    @Override
    public void noMoreOperators() {}
  }

  /** LuceneFullScan that releases Engine.Searcher references on close. */
  private static class CloseableLuceneFullScan extends LuceneFullScan {
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
   * Builds ColumnMappings from a RelDataType using MapperService. Mirrors
   * NodeLocalSourceProvider.buildColumnMappings.
   */
  static List<ColumnMapping> buildColumnMappings(RelDataType rowType, MapperService mapperService) {
    List<ColumnMapping> columns = new ArrayList<>();
    for (RelDataTypeField field : rowType.getFieldList()) {
      int index = field.getIndex();
      String name = field.getName();

      MappedFieldType fieldType = mapperService.fieldType(name);
      ColumnMapping mapping;

      if (fieldType == null || !fieldType.hasDocValues()) {
        String keywordSubField = name + ".keyword";
        MappedFieldType keywordFieldType = mapperService.fieldType(keywordSubField);
        if (keywordFieldType != null && keywordFieldType.hasDocValues()) {
          mapping = buildMappingFromFieldType(keywordSubField, index, keywordFieldType);
        } else {
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

  private static ColumnMapping buildMappingFromFieldType(
      String name, int index, MappedFieldType fieldType) {
    String typeName = fieldType.typeName();
    return switch (typeName) {
      case "long" ->
          new ColumnMapping(
              name, ColumnMapping.DocValuesType.SORTED_NUMERIC, ColumnMapping.BlockType.LONG, index);
      case "integer" ->
          new ColumnMapping(
              name, ColumnMapping.DocValuesType.SORTED_NUMERIC, ColumnMapping.BlockType.INT, index);
      case "short" ->
          new ColumnMapping(
              name, ColumnMapping.DocValuesType.SORTED_NUMERIC, ColumnMapping.BlockType.SHORT, index);
      case "byte" ->
          new ColumnMapping(
              name, ColumnMapping.DocValuesType.SORTED_NUMERIC, ColumnMapping.BlockType.BYTE, index);
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
              name, ColumnMapping.DocValuesType.SORTED_NUMERIC, ColumnMapping.BlockType.LONG, index);
      case "keyword", "constant_keyword", "wildcard" ->
          new ColumnMapping(
              name,
              ColumnMapping.DocValuesType.SORTED_SET,
              ColumnMapping.BlockType.VARIABLE_WIDTH,
              index);
      case "text" ->
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
          new ColumnMapping(
              name,
              ColumnMapping.DocValuesType.SORTED_SET,
              ColumnMapping.BlockType.VARIABLE_WIDTH,
              index);
    };
  }

  /** Factory for creating CollectingSink operators. */
  private static class CollectingSinkFactory implements OperatorFactory {
    private boolean closed;

    @Override
    public Operator createOperator(OperatorContext operatorContext) {
      if (closed) {
        throw new IllegalStateException("Factory is already closed");
      }
      return new CollectingSink(operatorContext);
    }

    @Override
    public void noMoreOperators() {
      closed = true;
    }
  }

  /** Sink operator that collects Pages for the response. */
  private static class CollectingSink implements SinkOperator {
    private final OperatorContext operatorContext;
    private final List<Page> collected = new ArrayList<>();
    private boolean finished;

    CollectingSink(OperatorContext operatorContext) {
      this.operatorContext = operatorContext;
    }

    @Override
    public ListenableFuture<?> isBlocked() {
      return NOT_BLOCKED;
    }

    @Override
    public boolean needsInput() {
      return !finished;
    }

    @Override
    public void addInput(Page page) {
      if (page != null && page.getPositionCount() > 0) {
        collected.add(page);
      }
    }

    @Override
    public void finish() {
      finished = true;
    }

    @Override
    public boolean isFinished() {
      return finished;
    }

    @Override
    public OperatorContext getOperatorContext() {
      return operatorContext;
    }

    @Override
    public void close() {
      finished = true;
    }

    List<Page> getCollectedPages() {
      return collected;
    }
  }
}
