/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.executor;

import java.util.List;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.executor.ProgressiveQueryResponseListener.QueryProgress;
import org.opensearch.tasks.CancellableTask;

/**
 * Request-scoped bridge between Calcite execution and ordinary OpenSearch search tasks.
 *
 * <p>Background search tasks explicitly capture and restore this context because executor thread
 * pools do not inherit thread locals.
 */
public final class ProgressiveQueryContext {
  private static final Logger LOG = LogManager.getLogger(ProgressiveQueryContext.class);

  /** Receives search progress and cancellation handles for the owning PPL job. */
  public interface Observer {
    void onProgress(QueryProgress progress);

    default void onAggregationSnapshot(List<ExprValue> rows) {}

    default void onCompositeAggregationSnapshot(List<ExprValue> rows) {}

    default boolean acceptsCompositeAggregationSnapshots() {
      return false;
    }

    default void onOperatorSnapshot(OperatorSnapshot snapshot) {}

    void onSearchTaskStarted(long operationId, Runnable cancelAction);

    void onSearchTaskFinished(long operationId);
  }

  /** A provisional result produced directly from one live Calcite operator state. */
  public record OperatorSnapshot(
      Object operatorIdentity,
      String operator,
      RelDataType rowType,
      List<Object[]> rows,
      long rowsConsumed,
      long stateUpdates,
      long snapshotSequence) {
    public OperatorSnapshot {
      Objects.requireNonNull(operatorIdentity);
      Objects.requireNonNull(operator);
      Objects.requireNonNull(rowType);
      rows = List.copyOf(rows);
    }
  }

  /** Captured context that may be propagated to another worker thread. */
  public static final class Captured {
    private final Observer observer;
    private final AtomicLong nextOperationId = new AtomicLong();

    private Captured(Observer observer) {
      this.observer = Objects.requireNonNull(observer);
    }

    public Observer observer() {
      return observer;
    }

    private long nextOperationId() {
      return nextOperationId.incrementAndGet();
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

  private ProgressiveQueryContext() {}

  public static Scope open(Observer observer) {
    Captured previous = CURRENT.get();
    CURRENT.set(new Captured(Objects.requireNonNull(observer)));
    return new Scope(previous);
  }

  public static Captured capture() {
    return CURRENT.get();
  }

  public static boolean isActive() {
    return CURRENT.get() != null;
  }

  /** Publishes a snapshot without replaying the source or Calcite plan. */
  public static void publishOperatorSnapshot(OperatorSnapshot snapshot) {
    Captured captured = CURRENT.get();
    if (captured == null) {
      return;
    }
    try {
      captured.observer().onOperatorSnapshot(snapshot);
    } catch (RuntimeException e) {
      LOG.warn("Failed to publish an incremental Calcite operator snapshot", e);
    }
  }

  /** Publishes all finalized buckets from completed composite aggregation pages. */
  public static void publishCompositeAggregationSnapshot(List<ExprValue> rows) {
    Captured captured = CURRENT.get();
    if (captured == null) {
      return;
    }
    try {
      captured.observer().onCompositeAggregationSnapshot(List.copyOf(rows));
    } catch (RuntimeException e) {
      LOG.warn("Failed to publish a composite aggregation page snapshot", e);
    }
  }

  public static boolean acceptsCompositeAggregationSnapshots() {
    Captured captured = CURRENT.get();
    return captured != null && captured.observer().acceptsCompositeAggregationSnapshots();
  }

  /**
   * Starts one underlying OpenSearch search operation.
   *
   * @param exactFraction whether this request represents all remaining query work
   */
  public static SearchOperation startSearch(boolean exactFraction) {
    Captured captured = CURRENT.get();
    if (captured == null) {
      return null;
    }
    return new SearchOperation(captured.nextOperationId(), exactFraction, captured.observer());
  }

  /** Handle for one normal {@code _search} request. */
  public static final class SearchOperation {
    private final long id;
    private final boolean exactFraction;
    private final Observer observer;

    private SearchOperation(long id, boolean exactFraction, Observer observer) {
      this.id = id;
      this.exactFraction = exactFraction;
      this.observer = observer;
    }

    public boolean exactFraction() {
      return exactFraction;
    }

    public void registerTask(CancellableTask task) {
      try {
        observer.onSearchTaskStarted(id, () -> task.cancel("PPL asynchronous job cancelled"));
      } catch (RuntimeException e) {
        LOG.warn("Failed to register an OpenSearch search task", e);
      }
    }

    public void publish(QueryProgress progress) {
      try {
        observer.onProgress(progress);
      } catch (RuntimeException e) {
        LOG.warn("Failed to publish OpenSearch search progress", e);
      }
    }

    public void publishAggregationSnapshot(List<ExprValue> rows) {
      try {
        observer.onAggregationSnapshot(rows);
      } catch (RuntimeException e) {
        LOG.warn("Failed to publish an OpenSearch aggregation snapshot", e);
      }
    }

    public void complete() {
      try {
        observer.onSearchTaskFinished(id);
      } catch (RuntimeException e) {
        LOG.warn("Failed to unregister an OpenSearch search task", e);
      }
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
