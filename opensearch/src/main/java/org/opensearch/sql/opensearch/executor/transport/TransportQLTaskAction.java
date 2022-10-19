/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.executor.transport;

import java.io.IOException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.action.ActionListener;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.HandledTransportAction;
import org.opensearch.client.node.NodeClient;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.inject.Inject;
import org.opensearch.common.io.stream.StreamInput;
import org.opensearch.sql.executor.ExecutionEngine;
import org.opensearch.sql.opensearch.client.OpenSearchNodeClient;
import org.opensearch.sql.opensearch.executor.task.TaskService;
import org.opensearch.sql.opensearch.executor.transport.dataplane.QLDataService;
import org.opensearch.sql.planner.Planner;
import org.opensearch.tasks.Task;
import org.opensearch.transport.TransportException;
import org.opensearch.transport.TransportResponseHandler;
import org.opensearch.transport.TransportService;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;

public class TransportQLTaskAction extends HandledTransportAction<QLTaskRequest, QLTaskResponse> {

  public static final String SQL_WORKER_THREAD_POOL_NAME = "sql-worker";

  private static final Logger LOG = LogManager.getLogger();

  public static final String ACTION_NAME = "n/query/task";

  private final TaskService taskService;

  public static final String EXECUTOR = "sql-worker";

  private final TransportService transportService;

  private final ClusterService clusterService;

  @Inject
  public TransportQLTaskAction(
      String actionName,
      TransportService transportService,
      ActionFilters actionFilters,
      String executor,
      ClusterService clusterService,
      NodeClient nodeClient,
      AnnotationConfigApplicationContext applicationContext,
      QLDataService dataService) {
    super(actionName, transportService, actionFilters, QLTaskRequest::new, executor);


    this.taskService =
        new TaskService(
            new OpenSearchNodeClient(nodeClient),
            nodeClient.threadPool().executor(SQL_WORKER_THREAD_POOL_NAME),
            applicationContext.getBean(ExecutionEngine.class), dataService);
    this.transportService = transportService;
    this.clusterService = clusterService;
    taskService.init();
    transportService.registerRequestHandler(
        ACTION_NAME,
        EXECUTOR,
        QLTaskRequest::new,
        (req, channel, task) -> {
          switch (req.getTaskType()) {
            case CREATE:
              taskService.addTask(
                  req,
                  newState -> {
                    try {
                      channel.sendResponse(
                          new QLTaskResponse(taskService.getTaskExecutionInfo(req.getTaskId())));
                    } catch (IOException e) {
                      LOG.error("Transport exception");
                    }
                  });
              break;
            case UPDATE:
              LOG.info("Engine does not support UPDATE yet.");
              break;
            default:
              break;
          }
        });
  }

  @Override
  protected void doExecute(
      Task task, QLTaskRequest request, ActionListener<QLTaskResponse> actionListener) {
    transportService.sendRequest(
        request.getNode(),
        ACTION_NAME,
        request,
        new TransportResponseHandler<QLTaskResponse>() {
          @Override
          public void handleResponse(QLTaskResponse resp) {
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
          public QLTaskResponse read(StreamInput in) throws IOException {
            return new QLTaskResponse(in);
          }
        });
  }
}
