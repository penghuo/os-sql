/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.plugin.transport;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Stream;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.action.ActionListener;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.HandledTransportAction;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.node.DiscoveryNodes;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.CheckedConsumer;
import org.opensearch.common.inject.Inject;
import org.opensearch.common.io.stream.StreamInput;
import org.opensearch.sql.plugin.streamexpression.Lang;
import org.opensearch.sql.plugin.streamexpression.Tuple;
import org.opensearch.sql.plugin.streamexpression.stream.ParallelStream;
import org.opensearch.sql.plugin.streamexpression.stream.TupleStream;
import org.opensearch.sql.plugin.streamexpression.stream.expr.DefaultStreamFactory;
import org.opensearch.tasks.Task;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.TransportChannel;
import org.opensearch.transport.TransportException;
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
  public static final String EXECUTOR = ThreadPool.Names.SEARCH;
  ;

  @Inject
  public TransportStreamExpressionAction(
      String actionName,
      ClusterService clusterService,
      TransportService transportService,
      ActionFilters actionFilters,
      String executor) {
    super(actionName, transportService, actionFilters, StreamExpressionRequest::new, executor);

    this.clusterService = clusterService;
    this.transportService = transportService;
    transportService.registerRequestHandler(
        ACTION_NAME,
        EXECUTOR,
        StreamExpressionRequest::new,
        new TransportRequestHandler<StreamExpressionRequest>() {
          @Override
          public void messageReceived(
              StreamExpressionRequest request, TransportChannel transportChannel, Task task)
              throws Exception {
            LOG.info("Handler Request: {}", request.getStreamExpression());

            TupleStream tupleStream =
                new DefaultStreamFactory().constructStream(request.getStreamExpression());
            tupleStream.execute(
                clusterService,
                transportService,
                ActionListener.wrap(
                    new CheckedConsumer<StreamExpressionResponse, Exception>() {
                      @Override
                      public void accept(StreamExpressionResponse response) throws Exception {
                        transportChannel.sendResponse(response);
                      }
                    },
                    e ->
                        TransportChannel.sendErrorResponse(
                            transportChannel, ACTION_NAME, request, e)));
          }
        });
  }

  @Override
  protected void doExecute(
      Task task,
      StreamExpressionRequest request,
      ActionListener<StreamExpressionResponse> actionListener) {
    LOG.info("Action Request: {}", request.getStreamExpression());

    TupleStream tupleStream = new ParallelStream(new Lang.LocalInputStream("return tuple"));
    tupleStream.execute(clusterService, transportService, actionListener);
  }
}
