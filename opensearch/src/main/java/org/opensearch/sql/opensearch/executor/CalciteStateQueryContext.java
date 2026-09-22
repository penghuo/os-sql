/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.executor;

import java.util.List;
import java.util.function.Supplier;
import org.opensearch.sql.data.model.ExprValue;

/** Propagates the aggregation state store through background OpenSearch search threads. */
public final class CalciteStateQueryContext {
  public record Captured(StateStore store) {
    public void publish(List<ExprValue> rows) {
      store.publish(rows);
    }
  }

  public static final class Scope implements AutoCloseable {
    private final Captured previous;

    private Scope(Captured previous) {
      this.previous = previous;
    }

    @Override
    public void close() {
      restore(previous);
    }
  }

  private static final ThreadLocal<Captured> CURRENT = new ThreadLocal<>();

  private CalciteStateQueryContext() {}

  public static Scope open(StateStore store) {
    Captured previous = CURRENT.get();
    CURRENT.set(new Captured(store));
    return new Scope(previous);
  }

  public static Captured capture() {
    return CURRENT.get();
  }

  public static boolean isActive() {
    return CURRENT.get() != null;
  }

  public static <T> T withContext(Captured captured, Supplier<T> supplier) {
    Captured previous = CURRENT.get();
    try {
      restore(captured);
      return supplier.get();
    } finally {
      restore(previous);
    }
  }

  private static void restore(Captured captured) {
    if (captured == null) {
      CURRENT.remove();
    } else {
      CURRENT.set(captured);
    }
  }
}
