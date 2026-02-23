/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dqe.execution.driver;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import org.opensearch.dqe.execution.operator.OperatorContext;
import org.opensearch.threadpool.ThreadPool;

/**
 * Manages driver execution on the dqe_worker thread pool. Submits drivers for execution and handles
 * completion callbacks.
 */
public class DriverRunner {

  private static final String DQE_WORKER_POOL = "dqe_worker";

  private final ThreadPool threadPool;
  private final Map<String, List<Driver>> activeDrivers = new ConcurrentHashMap<>();
  private volatile boolean shutdown = false;

  public DriverRunner(ThreadPool threadPool) {
    this.threadPool = Objects.requireNonNull(threadPool, "threadPool must not be null");
  }

  /**
   * Submits a driver for execution. The driver's process() method is called repeatedly until
   * finished or cancelled.
   */
  public void enqueueDriver(Driver driver, DriverCompletionCallback completionCallback) {
    if (shutdown) {
      completionCallback.onComplete(driver, new IllegalStateException("DriverRunner is shut down"));
      return;
    }

    // Track the driver by query ID
    List<OperatorContext> contexts = driver.getOperatorContexts();
    if (!contexts.isEmpty()) {
      String queryId = contexts.get(0).getQueryId();
      activeDrivers.computeIfAbsent(queryId, k -> new CopyOnWriteArrayList<>()).add(driver);
    }

    threadPool
        .executor(DQE_WORKER_POOL)
        .execute(
            () -> {
              Exception failure = null;
              try {
                while (!driver.isFinished() && !shutdown) {
                  boolean moreWork = driver.process();
                  if (!moreWork) {
                    break;
                  }
                }
              } catch (Exception e) {
                failure = e;
              } finally {
                removeDriver(driver);
                completionCallback.onComplete(driver, failure);
              }
            });
  }

  /** Cancels all running drivers for a given query. */
  public void cancelDrivers(String queryId) {
    List<Driver> drivers = activeDrivers.get(queryId);
    if (drivers != null) {
      for (Driver driver : drivers) {
        // Set interrupt flag on all operator contexts
        for (OperatorContext ctx : driver.getOperatorContexts()) {
          ctx.setInterrupted(true);
        }
      }
    }
  }

  /** Shuts down the runner. */
  public void shutdown() {
    shutdown = true;
    for (List<Driver> drivers : activeDrivers.values()) {
      for (Driver driver : drivers) {
        try {
          driver.close();
        } catch (Exception e) {
          // ignore
        }
      }
    }
    activeDrivers.clear();
  }

  private void removeDriver(Driver driver) {
    List<OperatorContext> contexts = driver.getOperatorContexts();
    if (!contexts.isEmpty()) {
      String queryId = contexts.get(0).getQueryId();
      List<Driver> drivers = activeDrivers.get(queryId);
      if (drivers != null) {
        drivers.remove(driver);
        if (drivers.isEmpty()) {
          activeDrivers.remove(queryId);
        }
      }
    }
  }
}
