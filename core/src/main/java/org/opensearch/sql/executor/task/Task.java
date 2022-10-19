/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.sql.executor.task;

import lombok.Getter;
import lombok.RequiredArgsConstructor;
import org.opensearch.sql.executor.StateChangeListener;
import org.opensearch.sql.planner.logical.LogicalPlan;

@RequiredArgsConstructor
public abstract class Task {

  @Getter
  protected final TaskId taskId;

  @Getter
  protected final LogicalPlan plan;

  public abstract void execute(StateChangeListener<TaskExecutionInfo> listener);

  public abstract void cancel();

  public abstract TaskExecutionInfo taskExecutionInfo();
}
