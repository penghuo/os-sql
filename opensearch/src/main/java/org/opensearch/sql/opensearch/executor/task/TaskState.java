/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.executor.task;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.Setter;

@AllArgsConstructor
@Getter
@Setter
public class TaskState {
  private int state;
  private int rowCount;
  private TaskExecutionState taskExecutionState;

  public static TaskState SCHEDULING() {
    return new TaskState(0, 0, TaskExecutionState.SCHEDULING);
  }

  public enum TaskExecutionState {
    SCHEDULING,
    RUNNING,
    FINISH,
    ERROR
  }

  public void updateTaskState(TaskState taskState) {
    this.state = taskState.state;
    this.rowCount = taskState.rowCount;
    this.taskExecutionState = taskState.getTaskExecutionState();
  }

  public boolean isFinish() {
    return taskExecutionState == TaskExecutionState.FINISH;
  }
}
