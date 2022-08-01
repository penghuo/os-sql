/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.executor.task;

import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor
public class TaskState {
  enum TaskExecutionState {
    SCHEDULING,
    RUNNING,
    FINISH
  }

  private final TaskState taskState;
}
