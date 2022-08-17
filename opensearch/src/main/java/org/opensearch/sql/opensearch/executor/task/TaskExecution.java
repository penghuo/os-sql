/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.executor.task;

import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;
import lombok.RequiredArgsConstructor;
import org.apache.commons.math3.analysis.function.Exp;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.planner.physical.PhysicalPlan;

public class TaskExecution {

  public static ListenableFuture<?> NOT_BLOCKED = Futures.immediateFuture(null);

  private final PhysicalPlan plan;

  private final TaskState taskState = TaskState.SCHEDULING();

  private Consumer<List<ExprValue>> resultConsumer;

  private TaskExecutionListener listener;

  public TaskExecution(PhysicalPlan plan, Consumer<List<ExprValue>> resultConsumer,
                       TaskExecutionListener listener) {
    this.plan = plan;
    this.resultConsumer = resultConsumer;
    this.listener = listener;
  }


  public ListenableFuture<?> execute() {
    int rowCount = 0;

    ListenableFuture<?> blocked = isBlocked();
    if (blocked != NOT_BLOCKED) {
      return blocked;
    }
    List<ExprValue> result = new ArrayList<>();
    try {
      plan.open();

      while (plan.hasNext()) {
        result.add(plan.next());
        rowCount++;
      }
    } catch (Exception e) {
      listener.onFailure(new TaskState(-1, -1, TaskState.TaskExecutionState.FINISH), e);
    } finally{
      listener.onSuccess(new TaskState(0, rowCount, TaskState.TaskExecutionState.FINISH));
      resultConsumer.accept(result);
      plan.close();
    }
    return null;
  }

  /**
   * plan is blocked?
   */
  private ListenableFuture<?> isBlocked() {
    return NOT_BLOCKED;
  }

  public TaskState taskState() {
    return taskState;
  }


  public interface TaskExecutionListener {

    void onSuccess(TaskState taskState);

    void onFailure(TaskState taskState, Exception e);
  }
}
