/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.executor.scheduler;

import java.util.List;
import org.opensearch.sql.common.response.ResponseListener;
import org.opensearch.sql.executor.ExecutionEngine;
import org.opensearch.sql.opensearch.executor.stage.StageExecution;
import org.opensearch.sql.opensearch.executor.stage.StageState;

public class ExecutionSchedule {

  private List<StageExecution> stageExecutionList = null;
  private ResponseListener<ExecutionEngine.QueryResponse> listener;

  public ExecutionSchedule(List<StageExecution> stageExecutionList) {
    this.stageExecutionList = stageExecutionList;
    for (StageExecution stageExecution : stageExecutionList) {
      stageExecution.addListener(
          stageState -> {
            if (stageState.stageExecutionState() == StageState.StageExecutionState.FINISH) {
              execute();
            }
          }
      );
    }
  }

  public void execute() {
    try {
      StageExecution nextStage = nextStage();
      StageScheduler stageScheduler = nextStage.createStageScheduler();
      stageScheduler.schedule();
    } catch (Exception e) {
      // listener.onFailure(e);
    } finally{
      // todo
    }
  }

  public StageExecution nextStage() {
    return null;
  }
}
