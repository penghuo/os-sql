/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.executor.stage;

public class StageState {
  public enum StageExecutionState {
    SCHEDULING,
    RUNNING,
    FINISH
  }


  public StageExecutionState stageExecutionState() {
    return null;
  }

}
