/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.planner.stage;

import lombok.Data;

@Data
public class StageState {
  private final int status;
  private final int rowCount;
  private final StageExecutionState stageExecutionState;

  public static StageState SCHEDULING() {
    return new StageState(0, 0, StageExecutionState.SCHEDULING);
  }

  public enum StageExecutionState {
    SCHEDULING,
    RUNNING,
    FINISH
  }

  public StageExecutionState stageExecutionState() {
    return null;
  }

  public boolean isFinish() {
    return stageExecutionState == StageExecutionState.FINISH;
  }

}
