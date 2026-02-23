/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dqe.execution.task;

import java.util.Map;
import org.opensearch.core.tasks.TaskId;
import org.opensearch.tasks.CancellableTask;

/** OpenSearch CancellableTask representing a DQE query for task management API integration. */
public class DqeQueryTask extends CancellableTask {

  private final String queryId;
  private final String sqlQuery;

  public DqeQueryTask(
      long id,
      String type,
      String action,
      String description,
      TaskId parentTaskId,
      Map<String, String> headers,
      String queryId,
      String sqlQuery) {
    super(id, type, action, description, parentTaskId, headers);
    this.queryId = queryId;
    this.sqlQuery = sqlQuery;
  }

  public String getQueryId() {
    return queryId;
  }

  public String getSqlQuery() {
    return sqlQuery;
  }

  @Override
  public boolean shouldCancelChildrenOnCancellation() {
    return true;
  }
}
