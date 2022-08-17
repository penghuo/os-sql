/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.executor.scheduler;

import java.util.List;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.sql.opensearch.executor.stage.StageExecution;

public class ExecutionSchedule {

  private static final Logger LOG = LogManager.getLogger();

  private final List<StageExecution> stageExecutionList;

  public ExecutionSchedule(List<StageExecution> stageExecutionList) {
    this.stageExecutionList = stageExecutionList;

    // add listener for each stage.
    for (StageExecution stageExecution : stageExecutionList) {
      stageExecution.addListener(
          stageState -> {
            if (stageState.isFinish()) {
              execute();
            }
          }
      );
    }
  }

  public void execute() {
    try {
      StageExecution nextStage = nextStage(stageExecutionList.size());
      if (nextStage == null) {
        LOG.info("Done. no stage execution pending schedule");
      }
      StageScheduler stageScheduler = nextStage.createStageScheduler();
      stageScheduler.schedule();
    } catch (Exception e) {
      LOG.error("stage execution", e);
      throw new RuntimeException(e);
    } finally{
      // todo
    }
  }

  public StageExecution nextStage(int index) {
    if (index < 0) {
      return null;
    }
    StageExecution stageExecution = stageExecutionList.get(index);
    if (stageExecution.getStageState().isFinish()) {
      return nextStage(index - 1);
    } else {
      return stageExecution;
    }
  }
}
