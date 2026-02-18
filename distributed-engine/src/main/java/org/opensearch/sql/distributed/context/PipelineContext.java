/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.distributed.context;

import java.io.Closeable;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicLong;
import org.opensearch.sql.distributed.memory.MemoryPool;

/**
 * Context for a pipeline within a task. A pipeline is a chain of operators that runs as a unit.
 * Tracks pipeline lifecycle stats.
 */
public class PipelineContext implements Closeable {

  private final TaskContext taskContext;
  private final int pipelineId;
  private final List<DriverContext> driverContexts = new CopyOnWriteArrayList<>();
  private final AtomicLong inputPositions = new AtomicLong();
  private final AtomicLong outputPositions = new AtomicLong();
  private volatile boolean closed;

  PipelineContext(TaskContext taskContext, int pipelineId) {
    this.taskContext = Objects.requireNonNull(taskContext, "taskContext is null");
    this.pipelineId = pipelineId;
  }

  public TaskContext getTaskContext() {
    return taskContext;
  }

  public int getPipelineId() {
    return pipelineId;
  }

  public MemoryPool getMemoryPool() {
    return taskContext.getMemoryPool();
  }

  public boolean isClosed() {
    return closed;
  }

  /** Creates a new DriverContext under this pipeline. */
  public DriverContext addDriverContext() {
    checkNotClosed();
    DriverContext driverContext = new DriverContext(this, driverContexts.size());
    driverContexts.add(driverContext);
    return driverContext;
  }

  public List<DriverContext> getDriverContexts() {
    return driverContexts;
  }

  public void recordInputPositions(long positions) {
    inputPositions.addAndGet(positions);
  }

  public void recordOutputPositions(long positions) {
    outputPositions.addAndGet(positions);
  }

  public long getInputPositions() {
    return inputPositions.get();
  }

  public long getOutputPositions() {
    return outputPositions.get();
  }

  @Override
  public void close() {
    if (closed) {
      return;
    }
    closed = true;
    for (DriverContext driverContext : driverContexts) {
      driverContext.close();
    }
    driverContexts.clear();
  }

  private void checkNotClosed() {
    if (closed) {
      throw new IllegalStateException("PipelineContext is already closed");
    }
  }
}
