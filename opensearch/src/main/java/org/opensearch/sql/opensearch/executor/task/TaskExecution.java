/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.executor.task;

import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import lombok.Getter;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.executor.StateChangeListener;
import org.opensearch.sql.executor.task.TaskExecutionInfo;
import org.opensearch.sql.executor.task.TaskExecutionState;
import org.opensearch.sql.planner.physical.PhysicalPlan;
import org.opensearch.sql.storage.splits.Split;

public class TaskExecution {

  public static ListenableFuture<?> NOT_BLOCKED = Futures.immediateFuture(null);

  private final PhysicalPlan plan;

  @Getter
  private final TaskExecutionInfo taskExecutionInfo;

  private final Split split;

  private StateChangeListener<TaskExecutionState> listener;

  public TaskExecution(PhysicalPlan plan, StateChangeListener<TaskExecutionState> listener,
                       Split split) {
    this.plan = plan;
    this.taskExecutionInfo = new TaskExecutionInfo();
    this.listener = listener;
    this.split = split;
  }

  public ListenableFuture<?> execute() {
    int rowCount = 0;

    ListenableFuture<?> blocked = isBlocked();
    if (blocked != NOT_BLOCKED) {
      return blocked;
    }
    List<ExprValue> result = new ArrayList<>();
    try {
      plan.addSplit(split);
      plan.open();

      while (plan.hasNext()) {
        result.add(plan.next());
        rowCount++;
      }
    } catch (Exception e) {
      taskExecutionInfo.setState(TaskExecutionState.FAILED);
      taskExecutionInfo.setFailureInfo(
          Optional.of(new TaskExecutionInfo.TaskExecutionFailureInfo(e, e.getMessage())));

      listener.stateChanged(TaskExecutionState.FAILED);
    } finally {
      taskExecutionInfo.setState(TaskExecutionState.FINISH);
      listener.stateChanged(TaskExecutionState.FINISH);

      plan.close();
    }
    return null;
  }

  /** plan is blocked? */
  private ListenableFuture<?> isBlocked() {
    return NOT_BLOCKED;
  }
}
