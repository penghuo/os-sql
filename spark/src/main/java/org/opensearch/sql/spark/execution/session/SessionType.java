/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.spark.execution.session;

import lombok.Getter;

@Getter
public enum SessionType {
  INTERACTIVE("interactive");

  private final String sessionType;

  SessionType(String sessionType) {
    this.sessionType = sessionType;
  }

  public static SessionType fromString(String string) {
    for (SessionType value : SessionType.values()) {
      if (value.sessionType.equalsIgnoreCase(string)) {
        return value;
      }
    }
    throw new IllegalArgumentException("Invalid string: " + string);
  }
}
