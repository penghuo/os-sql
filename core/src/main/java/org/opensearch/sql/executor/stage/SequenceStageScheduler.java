/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.sql.executor.stage;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;
import lombok.SneakyThrows;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.sql.executor.task.TaskId;

public class SequenceStageScheduler {

  private static final Logger LOG = LogManager.getLogger();

  private List<StageExecutionInfo> infos = new ArrayList<>();

  @SneakyThrows
  public void schedule(List<StageExecution> stageExecutions, Consumer<Exception> errorHandler) {
    Set<TaskId> sourceTaskIds = new HashSet<>();

    for (int i = stageExecutions.size() - 1; i >= 0; i--) {
      StageExecution stageExecution = stageExecutions.get(i);

      stageExecution.setSourceTaskIds(sourceTaskIds);

      CompletableFuture<StageExecutionState> stageDone = new CompletableFuture<>();
      stageExecution.addStateChangeListener(
          state -> {
            if (state == StageExecutionState.FAILED || state == StageExecutionState.FINISH) {
              stageDone.complete(state);
            }
          });
      stageExecution.execute();

      // sync wait.
      StageExecutionState state = stageDone.get();

      // update infos.
      infos.add(stageExecution.stageExecutionInfo());

      if (state == StageExecutionState.FAILED) {
        LOG.info("stage execution failed: {}", stageExecution);
        errorHandler.accept(
            stageExecution.stageExecutionInfo().getFailureInfo().get().getException());
        break;
      }

      LOG.info("schedule next stage");
      // continue schedule next stage.
      sourceTaskIds = stageExecution.getTaskId();
    }
  }
}
