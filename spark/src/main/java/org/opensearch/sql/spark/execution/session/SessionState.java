/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.spark.execution.session;

import lombok.Getter;

@Getter
public enum SessionState {
  NOT_STARTED("not_started"),
  STARTING("starting"),
  IDLE("idle"),
  BUSY("busy"),
  SHUTTING_DOWN("shutting_down"),
  DEAD("dead"),
  ERROR("error"),
  KILLED("killed");

  private final String sessionState;

  SessionState(String sessionState) {
    this.sessionState = sessionState;
  }

  public static SessionState fromString(String string) {
    for (SessionState value : SessionState.values()) {
      if (value.sessionState.equalsIgnoreCase(string)) {
        return value;
      }
    }
    throw new IllegalArgumentException("Invalid string: " + string);
  }
}
