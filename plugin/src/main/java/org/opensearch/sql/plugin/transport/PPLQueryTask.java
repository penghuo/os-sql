/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.plugin.transport;

import java.util.Map;
import org.opensearch.core.tasks.TaskId;
import org.opensearch.sql.opensearch.executor.ExecutionStartListener;
import org.opensearch.tasks.CancellableTask;

public class PPLQueryTask extends CancellableTask implements ExecutionStartListener {

  // Set by an async query's runner; the engine calls it when the query begins executing.
  private volatile Runnable executionStartListener;

  public PPLQueryTask(
      long id,
      String type,
      String action,
      String description,
      TaskId parentTaskId,
      Map<String, String> headers) {
    super(id, type, action, description, parentTaskId, headers);
  }

  @Override
  public boolean shouldCancelChildrenOnCancellation() {
    return true;
  }

  /** Registers what to run when the query begins executing. */
  public void setExecutionStartListener(Runnable listener) {
    this.executionStartListener = listener;
  }

  @Override
  public void onExecutionStarted() {
    Runnable listener = executionStartListener;
    if (listener != null) {
      listener.run();
    }
  }
}
