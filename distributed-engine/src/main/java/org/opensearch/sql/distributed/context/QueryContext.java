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
 * Top-level context for a distributed query. Holds the memory pool reference and manages the
 * lifecycle of all child TaskContexts.
 *
 * <p>Hierarchy: QueryContext -> TaskContext -> PipelineContext -> DriverContext -> OperatorContext
 */
public class QueryContext implements Closeable {

  private final String queryId;
  private final MemoryPool memoryPool;
  private final long queryStartTimeMillis;
  private final long queryTimeoutMillis;
  private final List<TaskContext> taskContexts = new CopyOnWriteArrayList<>();
  private volatile boolean closed;

  public QueryContext(String queryId, MemoryPool memoryPool, long queryTimeoutMillis) {
    this.queryId = Objects.requireNonNull(queryId, "queryId is null");
    this.memoryPool = Objects.requireNonNull(memoryPool, "memoryPool is null");
    this.queryStartTimeMillis = System.currentTimeMillis();
    this.queryTimeoutMillis = queryTimeoutMillis;
  }

  public String getQueryId() {
    return queryId;
  }

  public MemoryPool getMemoryPool() {
    return memoryPool;
  }

  public long getQueryStartTimeMillis() {
    return queryStartTimeMillis;
  }

  public long getQueryTimeoutMillis() {
    return queryTimeoutMillis;
  }

  public boolean isClosed() {
    return closed;
  }

  /** Creates a new TaskContext under this query. */
  public TaskContext addTaskContext(int taskId) {
    checkNotClosed();
    TaskContext taskContext = new TaskContext(this, taskId);
    taskContexts.add(taskContext);
    return taskContext;
  }

  public List<TaskContext> getTaskContexts() {
    return taskContexts;
  }

  @Override
  public void close() {
    if (closed) {
      return;
    }
    closed = true;
    for (TaskContext taskContext : taskContexts) {
      taskContext.close();
    }
    taskContexts.clear();
  }

  private void checkNotClosed() {
    if (closed) {
      throw new IllegalStateException("QueryContext is already closed");
    }
  }
}
