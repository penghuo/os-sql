/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.distributed.driver;

import com.google.common.util.concurrent.ListenableFuture;
import java.io.Closeable;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import org.opensearch.sql.distributed.context.DriverContext;
import org.opensearch.sql.distributed.data.Page;
import org.opensearch.sql.distributed.operator.Operator;

/**
 * Cooperative multitasking driver that moves Pages between adjacent operators in a pipeline. Each
 * call to process() does a bounded amount of work (one quantum) before yielding. Ported from
 * Trino's io.trino.operator.Driver, adapted for OpenSearch's sql-worker thread pool.
 */
public class Driver implements Closeable {

  private static final long DEFAULT_YIELD_PERIOD_NANOS = TimeUnit.MILLISECONDS.toNanos(10);

  private final DriverContext driverContext;
  private final List<Operator> operators;
  private final DriverYieldSignal yieldSignal;
  private volatile boolean finished;

  public Driver(
      DriverContext driverContext, List<Operator> operators, DriverYieldSignal yieldSignal) {
    this.driverContext = Objects.requireNonNull(driverContext, "driverContext is null");
    this.operators = List.copyOf(Objects.requireNonNull(operators, "operators is null"));
    this.yieldSignal = Objects.requireNonNull(yieldSignal, "yieldSignal is null");
    if (operators.size() < 2) {
      throw new IllegalArgumentException("A Driver requires at least 2 operators (source + sink)");
    }
  }

  public DriverContext getDriverContext() {
    return driverContext;
  }

  /**
   * Processes one quantum of work. Moves Pages between operators until blocked, yielded, or all
   * operators are finished.
   *
   * @return a future that resolves when the driver can make more progress. A resolved future means
   *     the driver can be called again immediately.
   */
  public ListenableFuture<?> process() {
    if (finished) {
      return Operator.NOT_BLOCKED;
    }

    driverContext.setThread(Thread.currentThread());
    yieldSignal.setWithDelay(DEFAULT_YIELD_PERIOD_NANOS);

    try {
      return processInternal();
    } finally {
      yieldSignal.reset();
      driverContext.setThread(null);
    }
  }

  private ListenableFuture<?> processInternal() {
    boolean movedData;
    do {
      movedData = false;

      // Iterate through operator pairs: operators[i] -> operators[i+1]
      for (int i = 0; i < operators.size() - 1; i++) {
        Operator current = operators.get(i);
        Operator next = operators.get(i + 1);

        // Check if current operator is blocked
        ListenableFuture<?> currentBlocked = current.isBlocked();
        if (!currentBlocked.isDone()) {
          return currentBlocked;
        }

        // Check if next operator is blocked
        ListenableFuture<?> nextBlocked = next.isBlocked();
        if (!nextBlocked.isDone()) {
          return nextBlocked;
        }

        // Try to move a Page from current to next
        if (!current.isFinished() && next.needsInput()) {
          Page page = current.getOutput();
          if (page != null && page.getPositionCount() > 0) {
            next.addInput(page);
            movedData = true;
          }
        }

        // Propagate finish signal
        if (current.isFinished() && !next.isFinished()) {
          next.finish();
        }

        // Check yield signal
        if (yieldSignal.isSet()) {
          return Operator.NOT_BLOCKED;
        }
      }

      // Check if the last operator is finished
      if (operators.get(operators.size() - 1).isFinished()) {
        finished = true;
        return Operator.NOT_BLOCKED;
      }
    } while (movedData);

    return Operator.NOT_BLOCKED;
  }

  public boolean isFinished() {
    return finished;
  }

  public List<Operator> getOperators() {
    return operators;
  }

  @Override
  public void close() {
    finished = true;
    for (Operator operator : operators) {
      try {
        operator.close();
      } catch (Exception e) {
        // Log and continue closing remaining operators
      }
    }
    driverContext.close();
  }
}
