/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.executor;

import java.util.List;
import java.util.Objects;
import java.util.function.Supplier;
import org.opensearch.sql.data.model.ExprValue;

/**
 * Request-scoped bridge from an OpenSearch source callback to the Calcite execution root.
 *
 * <p>Background source fetches explicitly capture and restore this context because executor thread
 * pools do not inherit thread locals.
 */
public final class PartialResultContext {

  /** Receives source-native aggregation rows before root result materialization. */
  public interface Observer {
    void onAggregationSnapshot(List<ExprValue> rows);
  }

  /** Captured context that can be propagated to a worker thread. */
  public record Captured(Observer observer) {
    public Captured {
      Objects.requireNonNull(observer);
    }

    public void publishAggregationSnapshot(List<ExprValue> rows) {
      if (!rows.isEmpty()) {
        observer.onAggregationSnapshot(rows);
      }
    }
  }

  /** Restores the previous context when closed. */
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

  private PartialResultContext() {}

  public static Scope open(Observer observer) {
    Captured previous = CURRENT.get();
    CURRENT.set(new Captured(observer));
    return new Scope(previous);
  }

  public static Captured capture() {
    return CURRENT.get();
  }

  public static boolean isActive() {
    return CURRENT.get() != null;
  }

  public static void publishAggregationSnapshot(List<ExprValue> rows) {
    Captured captured = CURRENT.get();
    if (captured != null) {
      captured.publishAggregationSnapshot(rows);
    }
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
