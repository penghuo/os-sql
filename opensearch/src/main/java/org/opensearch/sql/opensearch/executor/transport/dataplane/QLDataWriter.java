/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.sql.opensearch.executor.transport.dataplane;

import java.io.IOException;
import java.util.List;
import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.action.ActionFuture;
import org.opensearch.action.ActionListener;
import org.opensearch.client.node.NodeClient;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.executor.task.TaskId;
import org.opensearch.sql.executor.task.TaskNode;
import org.opensearch.sql.opensearch.executor.transport.QLTaskAction;
import org.opensearch.sql.opensearch.executor.transport.QLTaskRequest;
import org.opensearch.sql.opensearch.executor.transport.QLTaskResponse;
import org.opensearch.sql.opensearch.executor.transport.QLTaskType;

@RequiredArgsConstructor
public class QLDataWriter {

  private static final Logger LOG = LogManager.getLogger();

  private final QLDataService dataService;

  public void write(TaskId taskId, List<ExprValue> data) throws IOException {
    dataService.add(taskId, data);
  }
}
