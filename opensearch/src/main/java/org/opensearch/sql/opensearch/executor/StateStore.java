/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.executor;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import org.opensearch.sql.data.model.ExprValue;

/** Thread-safe store of immutable row snapshots used by a cached Calcite state query plan. */
public final class StateStore {
  public enum UpdateMode {
    APPEND,
    REPLACE
  }

  public record Snapshot(long generation, List<List<ExprValue>> segments, int rowCount)
      implements Iterable<ExprValue> {
    @Override
    public Iterator<ExprValue> iterator() {
      return segments.stream().flatMap(List::stream).iterator();
    }
  }

  private final UpdateMode updateMode;
  private final AtomicLong generation = new AtomicLong();
  private final AtomicReference<Snapshot> current =
      new AtomicReference<>(new Snapshot(0L, List.of(), 0));

  public StateStore(UpdateMode updateMode) {
    this.updateMode = updateMode;
  }

  public UpdateMode updateMode() {
    return updateMode;
  }

  public Snapshot snapshot() {
    return current.get();
  }

  /** Publishes one immutable producer update according to this store's update mode. */
  public synchronized void publish(List<ExprValue> rows) {
    if (updateMode == UpdateMode.APPEND) {
      append(rows);
    } else {
      replace(rows);
    }
  }

  private void append(List<ExprValue> rows) {
    if (rows.isEmpty()) {
      return;
    }
    Snapshot previous = current.get();
    List<List<ExprValue>> segments = new ArrayList<>(previous.segments());
    segments.add(List.copyOf(rows));
    current.set(
        new Snapshot(
            generation.incrementAndGet(),
            List.copyOf(segments),
            Math.addExact(previous.rowCount(), rows.size())));
  }

  private void replace(List<ExprValue> rows) {
    List<ExprValue> immutableRows = List.copyOf(rows);
    current.set(
        new Snapshot(
            generation.incrementAndGet(),
            immutableRows.isEmpty() ? List.of() : List.of(immutableRows),
            immutableRows.size()));
  }
}
