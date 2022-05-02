/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.apache.spark.status.api.v1;

import org.apache.spark.util.EnumUtil;

public enum ApplicationStatus {
  COMPLETED,
  RUNNING;

  public static ApplicationStatus fromString(String str) {
    return EnumUtil.parseIgnoreCase(ApplicationStatus.class, str);
  }

}
