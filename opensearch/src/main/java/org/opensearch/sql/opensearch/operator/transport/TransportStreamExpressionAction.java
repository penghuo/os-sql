/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.operator.transport;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.action.ActionListener;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.HandledTransportAction;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.routing.ShardRouting;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.CheckedConsumer;
import org.opensearch.common.inject.Inject;
import org.opensearch.common.io.stream.StreamInput;
import org.opensearch.common.io.stream.StreamOutput;
import org.opensearch.index.IndexService;
import org.opensearch.index.engine.Engine;
import org.opensearch.index.shard.IndexShard;
import org.opensearch.indices.IndicesService;
import org.opensearch.search.internal.ContextIndexSearcher;
import org.opensearch.sql.data.model.ExprCollectionValue;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.opensearch.operator.common.SerDe;
import org.opensearch.sql.planner.physical.PhysicalPlan;
import org.opensearch.tasks.Task;
import org.opensearch.tasks.TaskId;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.TransportChannel;
import org.opensearch.transport.TransportException;
import org.opensearch.transport.TransportRequest;
import org.opensearch.transport.TransportRequestHandler;
import org.opensearch.transport.TransportResponse;
import org.opensearch.transport.TransportResponseHandler;
import org.opensearch.transport.TransportService;

public class TransportStreamExpressionAction
    extends HandledTransportAction<StreamExpressionRequest, StreamExpressionResponse> {
  private static final Logger LOG = LogManager.getLogger();

  public static final String ACTION_NAME = "n/stream/job";

  private final ClusterService clusterService;
  private final TransportService transportService;
  private final IndicesService indicesService;
  public static final String EXECUTOR = ThreadPool.Names.SEARCH;

  @Inject
  public TransportStreamExpressionAction(
      String actionName,
      ClusterService clusterService,
      IndicesService indicesService,
      TransportService transportService,
      ActionFilters actionFilters,
      String executor) {
    super(actionName, transportService, actionFilters, StreamExpressionRequest::new, executor);

    this.clusterService = clusterService;
    this.transportService = transportService;
    this.indicesService = indicesService;
    transportService.registerRequestHandler(
        ACTION_NAME,
        EXECUTOR,
        StreamExpressionRequest::new,
        new TransportRequestHandler<>() {
          @Override
          public void messageReceived(
              StreamExpressionRequest streamExpressionRequest,
              TransportChannel transportChannel,
              Task task)
              throws Exception {
            nodeExecution(SerDe.deserialize(streamExpressionRequest.getStreamExpression()),
                streamExpressionRequest.getShardRouting())
                .accept(transportChannel);
          }
        });
  }

  @Override
  protected void doExecute(
      Task task,
      StreamExpressionRequest request,
      ActionListener<StreamExpressionResponse> actionListener) {

    LOG.info("Action Request: {}", request.getStreamExpression());

    final ShardRouting shardRouting = request.getShardRouting();


    final DiscoveryNode node =
        clusterService.state().getNodes().get(shardRouting.currentNodeId());

    transportService.sendRequest(node, TransportStreamExpressionAction.ACTION_NAME,
        new StreamExpressionRequest(shardRouting,
            request.getStreamExpression()), new TransportResponseHandler<StreamExpressionResponse>() {
          @Override
          public void handleResponse(StreamExpressionResponse transportResponse) {
            actionListener.onResponse(transportResponse);
          }

          @Override
          public void handleException(TransportException e) {
            actionListener.onFailure(e);
          }

          @Override
          public String executor() {
            return TransportStreamExpressionAction.EXECUTOR;
          }

          @Override
          public StreamExpressionResponse read(StreamInput streamInput) throws IOException {
            return new StreamExpressionResponse(streamInput);
          }
        });
  }

  protected CheckedConsumer<TransportChannel, IOException> nodeExecution(PhysicalPlan plan, ShardRouting shardRouting) {
    List<ExprValue> result = new ArrayList<>();

    IndexService indexService = indicesService.indexServiceSafe(shardRouting.index());
    IndexShard shard = indexService.getShard(shardRouting.shardId().id());
    Engine.SearcherSupplier reader = shard.acquireSearcherSupplier();
    final Engine.Searcher engineSearcher = reader.acquireSearcher("search");
    try {
      ContextIndexSearcher searcher =
          new ContextIndexSearcher(
              engineSearcher.getIndexReader(),
              engineSearcher.getSimilarity(),
              engineSearcher.getQueryCache(),
              engineSearcher.getQueryCachingPolicy(),
              false);
      plan.setContext(searcher);
      plan.open();

      while (plan.hasNext()) {
        result.add(plan.next());
      }
      plan.close();
      ExprCollectionValue exprValue = new ExprCollectionValue(result);
      return channel ->
          channel.sendResponse(new StreamExpressionResponse(SerDe.serializeExprValue(exprValue)));
    } catch (IOException e) {
      throw new RuntimeException(e);
    }

  }
}
