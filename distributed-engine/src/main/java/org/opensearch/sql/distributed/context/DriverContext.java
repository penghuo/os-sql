/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.distributed.context;

import java.io.Closeable;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CopyOnWriteArrayList;
import org.opensearch.sql.distributed.memory.MemoryPool;

/**
 * Context for a single Driver instance. Tracks which thread the driver is bound to and manages
 * child OperatorContexts.
 */
public class DriverContext implements Closeable {

  private final PipelineContext pipelineContext;
  private final int driverId;
  private final List<OperatorContext> operatorContexts = new CopyOnWriteArrayList<>();
  private volatile Thread thread;
  private volatile boolean closed;

  DriverContext(PipelineContext pipelineContext, int driverId) {
    this.pipelineContext = Objects.requireNonNull(pipelineContext, "pipelineContext is null");
    this.driverId = driverId;
  }

  /** Creates a standalone DriverContext for testing (no parent pipeline context). */
  public DriverContext() {
    this.pipelineContext = null;
    this.driverId = 0;
  }

  public PipelineContext getPipelineContext() {
    return pipelineContext;
  }

  public int getDriverId() {
    return driverId;
  }

  public MemoryPool getMemoryPool() {
    return pipelineContext != null ? pipelineContext.getMemoryPool() : null;
  }

  public boolean isClosed() {
    return closed;
  }

  /** Binds this driver to the current thread. */
  public void setThread(Thread thread) {
    this.thread = thread;
  }

  public Thread getThread() {
    return thread;
  }

  /** Creates a new OperatorContext under this driver. */
  public OperatorContext addOperatorContext(int operatorId, String operatorType) {
    checkNotClosed();
    OperatorContext operatorContext = new OperatorContext(this, operatorId, operatorType);
    operatorContexts.add(operatorContext);
    return operatorContext;
  }

  public List<OperatorContext> getOperatorContexts() {
    return operatorContexts;
  }

  @Override
  public void close() {
    if (closed) {
      return;
    }
    closed = true;
    for (OperatorContext operatorContext : operatorContexts) {
      operatorContext.close();
    }
    operatorContexts.clear();
    thread = null;
  }

  private void checkNotClosed() {
    if (closed) {
      throw new IllegalStateException("DriverContext is already closed");
    }
  }
}
