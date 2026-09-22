/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.executor.progressive;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import org.opensearch.sql.data.model.ExprValue;

/** State store for root rows that become visible in append order. */
final class AppendOnlyStateStore implements QueryStateStore {
  private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
  private final List<ExprValue> rows = new ArrayList<>();
  private boolean closed;

  void append(ExprValue row) {
    lock.writeLock().lock();
    try {
      if (!closed) {
        rows.add(row);
      }
    } finally {
      lock.writeLock().unlock();
    }
  }

  @Override
  public List<ExprValue> rows() {
    lock.readLock().lock();
    try {
      return closed ? List.of() : List.copyOf(rows);
    } finally {
      lock.readLock().unlock();
    }
  }

  @Override
  public void close() {
    lock.writeLock().lock();
    try {
      closed = true;
      rows.clear();
    } finally {
      lock.writeLock().unlock();
    }
  }
}
