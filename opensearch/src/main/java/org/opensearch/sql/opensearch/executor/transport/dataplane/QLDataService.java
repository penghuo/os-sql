/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.sql.opensearch.executor.transport.dataplane;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.executor.task.TaskId;

public class QLDataService {

  private static final Logger LOG = LogManager.getLogger();

  private Map<TaskId, List<ExprValue>> buffers = new HashMap<>();

  public synchronized List<ExprValue> pop(TaskId taskId) {
    if (buffers.containsKey(taskId)) {
      List<ExprValue> result = buffers.getOrDefault(taskId, new ArrayList<>());
      buffers.remove(taskId);
      LOG.info("pop data from task id: {}", taskId);
      return result;
    } else {
      return new ArrayList<>();
    }
  }

  public synchronized void add(TaskId sourceTaskId, List<ExprValue> values) {
    if (!buffers.containsKey(sourceTaskId)) {
      buffers.put(sourceTaskId, values);
      LOG.info("add data from task id: {}", sourceTaskId);
    } else {
      LOG.info("receive duplicate data from same task id: {}", sourceTaskId);
    }
  }
}
