/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.apache.spark.status.api.v1;

import org.apache.spark.util.EnumUtil;

public enum TaskStatus {
  RUNNING,
  KILLED,
  FAILED,
  SUCCESS,
  UNKNOWN;

  public static TaskStatus fromString(String str) {
    return EnumUtil.parseIgnoreCase(TaskStatus.class, str);
  }
}
