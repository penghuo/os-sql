/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.sql.executor.task;

import java.util.Optional;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.Setter;
import lombok.ToString;

@ToString
@Setter
@Getter
@RequiredArgsConstructor
public class TaskExecutionInfo {

  private TaskExecutionState state;

  private TaskExecutionStats stats;

  private Optional<TaskExecutionFailureInfo> failureInfo;

  @Getter
  @RequiredArgsConstructor
  public static class TaskExecutionFailureInfo {
    private final Exception exception;
    private final String context;
  }
}
