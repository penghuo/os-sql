/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.executor.transport;


import java.io.IOException;
import java.util.Collections;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.action.ActionListener;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.HandledTransportAction;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.common.inject.Inject;
import org.opensearch.common.io.stream.StreamInput;
import org.opensearch.common.io.stream.Writeable;
import org.opensearch.sql.opensearch.executor.task.TaskService;
import org.opensearch.tasks.Task;
import org.opensearch.transport.TransportException;
import org.opensearch.transport.TransportResponseHandler;
import org.opensearch.transport.TransportService;

public class TransportQLTaskAction extends HandledTransportAction<QLTaskRequest, QLTaskResponse> {

  private static final Logger LOG = LogManager.getLogger();

  public static final String ACTION_NAME = "n/query/task";

  private final TaskService taskService;

  public static final String EXECUTOR = "sql-worker";

  private final TransportService transportService;

  @Inject
  public TransportQLTaskAction(String actionName,
                               TransportService transportService,
                               ActionFilters actionFilters,
                               Writeable.Reader<QLTaskRequest> qlTaskRequestReader,
                               String executor,
                               TaskService taskService) {
    super(actionName, transportService, actionFilters, qlTaskRequestReader, executor);

    this.taskService = taskService;
    this.transportService = transportService;
    transportService.registerRequestHandler(
        ACTION_NAME,
        EXECUTOR,
        QLTaskRequest::new,
        (req, channel, task) -> {
          try {
            switch (req.getTaskType()) {
              case CREATE:
                taskService.addTask(req.getTaskPlan());
                break;
              case UPDATE:
                taskService.addSplit(Collections.emptyList());
                break;
              default:
                break;
            }
            channel.sendResponse(new QLTaskResponse(taskService.taskState(req.getTaskPlan())));
          } catch (Exception e) {
            channel.sendResponse(e);
          }
        });
  }

  @Override
  protected void doExecute(Task task, QLTaskRequest request,
                           ActionListener<QLTaskResponse> actionListener) {
    DiscoveryNode node = request.getTaskPlan().getNode();
    transportService.sendRequest(node, ACTION_NAME, request,
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
