/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.executor.task;

import lombok.Getter;
import lombok.RequiredArgsConstructor;
import org.opensearch.sql.planner.logical.LogicalPlan;

public interface TaskPlan {
  TaskId getTaskId();

  LogicalPlan getPlan();
}
