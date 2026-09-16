/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.plan;

/** Request-scoped marker used by Calcite rules that have an async incremental physical form. */
public final class ProgressivePlanningContext {
  private static final ThreadLocal<Boolean> ACTIVE = new ThreadLocal<>();

  private ProgressivePlanningContext() {}

  public static Scope open() {
    Boolean previous = ACTIVE.get();
    ACTIVE.set(Boolean.TRUE);
    return new Scope(previous);
  }

  public static boolean isActive() {
    return Boolean.TRUE.equals(ACTIVE.get());
  }

  public static final class Scope implements AutoCloseable {
    private final Boolean previous;

    private Scope(Boolean previous) {
      this.previous = previous;
    }

    @Override
    public void close() {
      if (previous == null) {
        ACTIVE.remove();
      } else {
        ACTIVE.set(previous);
      }
    }
  }
}
