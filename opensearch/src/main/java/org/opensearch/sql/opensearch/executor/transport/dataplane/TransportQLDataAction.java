/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.sql.opensearch.executor.transport.dataplane;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import lombok.SneakyThrows;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.action.ActionListener;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.HandledTransportAction;
import org.opensearch.client.node.NodeClient;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.inject.Inject;
import org.opensearch.common.io.stream.BytesStreamInput;
import org.opensearch.common.io.stream.StreamInput;
import org.opensearch.tasks.Task;
import org.opensearch.transport.TransportException;
import org.opensearch.transport.TransportResponseHandler;
import org.opensearch.transport.TransportService;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;

public class TransportQLDataAction extends HandledTransportAction<QLDataRequest, QLDataResponse> {

  private static final Logger LOG = LogManager.getLogger();

  private final QLDataService dataService;

  public static final String ACTION_NAME = QLDataAction.NAME + "[n]";

  public static final String EXECUTOR = "sql-worker";

  private final TransportService transportService;

  @Inject
  public TransportQLDataAction(
      String actionName,
      TransportService transportService,
      ActionFilters actionFilters,
      String executor,
      ClusterService clusterService,
      NodeClient nodeClient,
      AnnotationConfigApplicationContext applicationContext,
      QLDataService dataService) {
    super(actionName, transportService, actionFilters, QLDataRequest::new, executor);
    this.dataService = dataService;
    this.transportService = transportService;

    transportService.registerRequestHandler(
        ACTION_NAME,
        EXECUTOR,
        QLDataRequest::new,
        (req, channel, task) -> {
          try {
            channel.sendResponse(new QLDataResponse(true, dataService.pop(req.getTaskId())));
          } catch (Exception e) {
            LOG.error("task service exception.", e);
            channel.sendResponse(e);
          }
        });
  }

  @SneakyThrows
  @Override
  protected void doExecute(
      Task task, QLDataRequest request, ActionListener<QLDataResponse> actionListener) {
    transportService.sendRequest(
        new DiscoveryNode(new BytesStreamInput(request.getTaskId().getMeta().getBytes(
            StandardCharsets.UTF_8))),
        ACTION_NAME,
        request,
        new TransportResponseHandler<QLDataResponse>() {
          @Override
          public void handleResponse(QLDataResponse resp) {
            actionListener.onResponse(resp);
          }

          @Override
          public void handleException(TransportException e) {
            actionListener.onFailure(e);
          }

          @Override
          public String executor() {
            return EXECUTOR;
          }

          @Override
          public QLDataResponse read(StreamInput in) throws IOException {
            return new QLDataResponse(in);
          }
        });
  }
}
