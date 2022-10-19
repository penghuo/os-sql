/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.sql.executor.stage;


import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import lombok.Setter;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.sql.executor.StateChangeListener;
import org.opensearch.sql.executor.task.TaskExecutionInfo;
import org.opensearch.sql.executor.task.TaskExecutionState;
import org.opensearch.sql.executor.task.TaskId;
import org.opensearch.sql.executor.task.Task;
import org.opensearch.sql.executor.task.TaskScheduler;
import org.opensearch.sql.planner.logical.LogicalPlan;

public class StageExecution {

  private static final Logger LOG = LogManager.getLogger();

  private final StageId stageId;

  private final LogicalPlan logicalPlan;

  private final TaskScheduler taskScheduler;

  private final StageExecutionInfo stageExecutionInfo;

  private final TaskTracker taskTracker;

  private final Map<TaskId, Task> taskMap;

  @Setter
  private Set<TaskId> sourceTaskIds;

  public StageExecution(LogicalPlan logicalPlan, TaskScheduler taskScheduler) {
    this.stageId = StageId.stageId();
    this.logicalPlan = logicalPlan;
    this.taskScheduler = taskScheduler;
    this.stageExecutionInfo = new StageExecutionInfo();
    this.taskTracker = new TaskTracker();
    this.taskMap = new HashMap<>();
  }

  public void execute() {
    List<Task> taskList = taskScheduler.schedule(logicalPlan, sourceTaskIds);

    for (Task task : taskList) {
      StateChangeListener<TaskExecutionInfo> listener = new StateChangeListener<TaskExecutionInfo>() {
        @Override
        public void stateChanged(TaskExecutionInfo taskExecutionInfo) {
          taskTracker.update(task.getTaskId(), taskExecutionInfo);
        }
      };
      taskTracker.register(task.getTaskId(), task);
      taskMap.put(task.getTaskId(), task);
      task.execute(listener);
    }
  }

  public Set<TaskId> getTaskId() {
    return taskMap.keySet();
  }

  // todo, cancel stage execution
  public void cancel() {

  }

  public void addStateChangeListener(StateChangeListener<StageExecutionState> listener) {
    stageExecutionInfo.register(listener);
  }

  public StageExecutionInfo stageExecutionInfo() {
    return stageExecutionInfo;
  }

  private class TaskTracker {

    private AtomicInteger runningTaskCount = new AtomicInteger(0);

    private AtomicInteger finishTaskCount = new AtomicInteger(0);

    public void register(TaskId taskId, Task taskPlan) {
      runningTaskCount.addAndGet(1);
    }

    public void update(TaskId taskId, TaskExecutionInfo executionInfo) {
      LOG.info("TaskId: {}, update info {}", taskId, executionInfo);
      if (TaskExecutionState.FINISH == executionInfo.getState()) {
        finishTaskCount.addAndGet(1);
      }
      if (TaskExecutionState.FAILED == executionInfo.getState()) {
        stageExecutionInfo.setState(StageExecutionState.FINISH);

        // todo. cancel all the tasks.
        return;
      }
      if (finishTaskCount.get() == runningTaskCount.get()) {
        stageExecutionInfo.setState(StageExecutionState.FINISH);
      }
    }
  }
}
