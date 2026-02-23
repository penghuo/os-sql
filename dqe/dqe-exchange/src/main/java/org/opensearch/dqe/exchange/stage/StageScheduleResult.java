/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dqe.exchange.stage;

import java.util.Objects;

/**
 * Result of dispatching a stage to data nodes.
 *
 * <p>Returned by {@link StageScheduler#scheduleStage} to indicate how many tasks were dispatched.
 */
public class StageScheduleResult {

  private final String queryId;
  private final int stageId;
  private final int dispatchedTaskCount;

  /**
   * Create a stage schedule result.
   *
   * @param queryId query identifier
   * @param stageId stage identifier
   * @param dispatchedTaskCount number of tasks dispatched to data nodes
   */
  public StageScheduleResult(String queryId, int stageId, int dispatchedTaskCount) {
    this.queryId = Objects.requireNonNull(queryId, "queryId must not be null");
    this.stageId = stageId;
    this.dispatchedTaskCount = dispatchedTaskCount;
  }

  public String getQueryId() {
    return queryId;
  }

  public int getStageId() {
    return stageId;
  }

  public int getDispatchedTaskCount() {
    return dispatchedTaskCount;
  }
}
