/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.apache.spark.status.api.v1;

import java.util.Collections;
import java.util.HashSet;
import java.util.Locale;
import java.util.Set;
import org.apache.spark.util.EnumUtil;

public enum TaskSorting {
  ID,
  INCREASING_RUNTIME("runtime"),
  DECREASING_RUNTIME("-runtime");

  private final Set<String> alternateNames;
  TaskSorting(String... names) {
    alternateNames = new HashSet<>();
    Collections.addAll(alternateNames, names);
  }

  public static TaskSorting fromString(String str) {
    String lower = str.toLowerCase(Locale.ROOT);
    for (TaskSorting t: values()) {
      if (t.alternateNames.contains(lower)) {
        return t;
      }
    }
    return EnumUtil.parseIgnoreCase(TaskSorting.class, str);
  }

}
