/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.executor.scheduler;

import java.util.List;
import lombok.RequiredArgsConstructor;
import org.opensearch.sql.opensearch.executor.stage.StageExecution;
import org.opensearch.sql.planner.splits.Split;
import org.opensearch.sql.planner.splits.SplitManager;

@RequiredArgsConstructor
public class ScaleWriterScheduler implements StageScheduler {
  private final StageExecution stageExecution;
  private final List<Split> splits;

  @Override
  public void schedule() {

  }
}
