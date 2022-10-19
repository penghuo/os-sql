/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.executor.task;

import java.util.UUID;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.ToString;

@ToString
@EqualsAndHashCode
@RequiredArgsConstructor
public class TaskId {
  private final String id;

  @Getter
  private final String meta;

  public static TaskId taskId(String meta) {
    return new TaskId("task-" + UUID.randomUUID(), meta);
  }
}
