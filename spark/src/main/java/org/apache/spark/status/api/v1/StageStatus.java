/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.apache.spark.status.api.v1;

import org.apache.spark.util.EnumUtil;

public enum StageStatus {
  ACTIVE,
  COMPLETE,
  FAILED,
  PENDING,
  SKIPPED;

  public static StageStatus fromString(String str) {
    return EnumUtil.parseIgnoreCase(StageStatus.class, str);
  }
}
