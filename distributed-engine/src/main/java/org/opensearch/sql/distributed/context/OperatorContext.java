/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.distributed.context;

import java.io.Closeable;
import java.util.concurrent.atomic.AtomicLong;
import org.opensearch.sql.distributed.memory.MemoryPool;

/**
 * Per-operator context within the execution runtime. Provides the operator with access to memory
 * tracking, stats collection, and its position in the context hierarchy.
 *
 * <p>Context hierarchy: QueryContext -> TaskContext -> PipelineContext -> DriverContext ->
 * OperatorContext.
 */
public class OperatorContext implements Closeable {

  private final DriverContext driverContext;
  private final int operatorId;
  private final String operatorType;
  private final AtomicLong memoryReservation = new AtomicLong();
  private final AtomicLong inputPositions = new AtomicLong();
  private final AtomicLong outputPositions = new AtomicLong();
  private final AtomicLong wallNanos = new AtomicLong();
  private volatile boolean closed;

  public OperatorContext(DriverContext driverContext, int operatorId, String operatorType) {
    this.driverContext = driverContext;
    this.operatorId = operatorId;
    this.operatorType = operatorType;
  }

  /** Creates a standalone OperatorContext (no parent driver context, no memory tracking). */
  public OperatorContext(int operatorId, String operatorType) {
    this(null, operatorId, operatorType);
  }

  public DriverContext getDriverContext() {
    return driverContext;
  }

  public int getOperatorId() {
    return operatorId;
  }

  public String getOperatorType() {
    return operatorType;
  }

  /** Returns the memory pool, or null if no driver context is set. */
  public MemoryPool getMemoryPool() {
    return driverContext != null ? driverContext.getMemoryPool() : null;
  }

  public boolean isClosed() {
    return closed;
  }

  /** Reserves memory for this operator. Tracked both locally and in the global MemoryPool. */
  public void reserveMemory(long bytes) {
    if (bytes <= 0) {
      return;
    }
    MemoryPool pool = getMemoryPool();
    if (pool != null) {
      pool.reserve(operatorType + "#" + operatorId, bytes);
    }
    memoryReservation.addAndGet(bytes);
  }

  /** Frees memory reserved by this operator. */
  public void freeMemory(long bytes) {
    if (bytes <= 0) {
      return;
    }
    MemoryPool pool = getMemoryPool();
    if (pool != null) {
      pool.free(operatorType + "#" + operatorId, bytes);
    }
    memoryReservation.addAndGet(-bytes);
  }

  public long getMemoryReservation() {
    return memoryReservation.get();
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

  public void addWallNanos(long nanos) {
    wallNanos.addAndGet(nanos);
  }

  public long getWallNanos() {
    return wallNanos.get();
  }

  @Override
  public void close() {
    if (closed) {
      return;
    }
    closed = true;
    long remaining = memoryReservation.getAndSet(0);
    if (remaining > 0) {
      MemoryPool pool = getMemoryPool();
      if (pool != null) {
        pool.free(operatorType + "#" + operatorId, remaining);
      }
    }
  }

  @Override
  public String toString() {
    return "OperatorContext{id=" + operatorId + ", type=" + operatorType + "}";
  }
}
