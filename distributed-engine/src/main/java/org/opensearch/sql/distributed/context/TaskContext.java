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
 * Context for a task (one node's portion of a query stage). Each node participating in a query gets
 * a TaskContext.
 */
public class TaskContext implements Closeable {

  private final QueryContext queryContext;
  private final int taskId;
  private final List<PipelineContext> pipelineContexts = new CopyOnWriteArrayList<>();
  private volatile boolean closed;

  TaskContext(QueryContext queryContext, int taskId) {
    this.queryContext = Objects.requireNonNull(queryContext, "queryContext is null");
    this.taskId = taskId;
  }

  public QueryContext getQueryContext() {
    return queryContext;
  }

  public int getTaskId() {
    return taskId;
  }

  public MemoryPool getMemoryPool() {
    return queryContext.getMemoryPool();
  }

  public boolean isClosed() {
    return closed;
  }

  /** Creates a new PipelineContext under this task. */
  public PipelineContext addPipelineContext(int pipelineId) {
    checkNotClosed();
    PipelineContext pipelineContext = new PipelineContext(this, pipelineId);
    pipelineContexts.add(pipelineContext);
    return pipelineContext;
  }

  public List<PipelineContext> getPipelineContexts() {
    return pipelineContexts;
  }

  @Override
  public void close() {
    if (closed) {
      return;
    }
    closed = true;
    for (PipelineContext pipelineContext : pipelineContexts) {
      pipelineContext.close();
    }
    pipelineContexts.clear();
  }

  private void checkNotClosed() {
    if (closed) {
      throw new IllegalStateException("TaskContext is already closed");
    }
  }
}
