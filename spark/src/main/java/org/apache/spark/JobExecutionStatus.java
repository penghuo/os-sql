/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.apache.spark;

import org.apache.spark.util.EnumUtil;

public enum JobExecutionStatus {
  RUNNING,
  SUCCEEDED,
  FAILED,
  UNKNOWN;

  public static JobExecutionStatus fromString(String str) {
    return EnumUtil.parseIgnoreCase(JobExecutionStatus.class, str);
  }
}
