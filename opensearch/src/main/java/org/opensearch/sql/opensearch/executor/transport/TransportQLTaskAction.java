/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.executor.transport;


import static org.opensearch.sql.opensearch.client.OpenSearchNodeClient.SQL_WORKER_THREAD_POOL_NAME;

import java.io.IOException;
import java.util.Collections;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.action.ActionListener;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.HandledTransportAction;
import org.opensearch.client.node.NodeClient;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.node.DiscoveryNodes;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.inject.Inject;
import org.opensearch.common.io.stream.StreamInput;
import org.opensearch.common.io.stream.Writeable;
import org.opensearch.sql.opensearch.client.OpenSearchNodeClient;
import org.opensearch.sql.opensearch.executor.task.TaskExecution;
import org.opensearch.sql.opensearch.executor.task.TaskNode;
import org.opensearch.sql.opensearch.executor.task.TaskService;
import org.opensearch.sql.opensearch.executor.task.TaskState;
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

//  private final ClusterState clusterState;

  private final ClusterService clusterService;

  @Inject
  public TransportQLTaskAction(String actionName,
                               TransportService transportService,
                               ActionFilters actionFilters,
                               String executor,
                               ClusterService clusterService,
                               NodeClient nodeClient) {
    super(actionName, transportService, actionFilters, QLTaskRequest::new, executor);


    this.taskService =
        new TaskService(
            new OpenSearchNodeClient(clusterService, nodeClient),
            nodeClient.threadPool().executor(SQL_WORKER_THREAD_POOL_NAME));
    this.transportService = transportService;
    this.clusterService = clusterService;
//    this.clusterState = clusterService.state();
    taskService.init();
    transportService.registerRequestHandler(
        ACTION_NAME,
        EXECUTOR,
        QLTaskRequest::new,
        (req, channel, task) -> {
            switch (req.getTaskType()) {
              case CREATE:
                taskService.addTask(req.getTaskPlan(), new TaskExecution.TaskExecutionListener() {
                  @Override
                  public void onSuccess(TaskState taskState) {
                    try {
                      channel.sendResponse(new QLTaskResponse(taskState));
                    } catch (IOException e) {
                      LOG.error("transport IO exception");
                    }
                  }

                  @Override
                  public void onFailure(TaskState taskState, Exception e) {
                    try {
                      channel.sendResponse(e);
                    } catch (IOException ioException) {
                      LOG.error("transport IO exception");
                    }
                  }
                });
                break;
              case UPDATE:
                taskService.addSplit(Collections.emptyList());
                break;
              default:
                break;
            }

        });
  }

  @Override
  protected void doExecute(Task task, QLTaskRequest request,
                           ActionListener<QLTaskResponse> actionListener) {
    TaskNode node = request.getTaskPlan().getNode();

    if (node == TaskNode.LOCAL) {
      final DiscoveryNode localNode = clusterService.state().getNodes().getLocalNode();
      transportService.sendRequest(localNode, ACTION_NAME, request,
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
}
