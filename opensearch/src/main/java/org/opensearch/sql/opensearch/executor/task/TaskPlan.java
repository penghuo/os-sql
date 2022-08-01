/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.executor.task;

import lombok.Getter;
import lombok.RequiredArgsConstructor;

public interface TaskPlan {
  TaskId getTaskId();
}
