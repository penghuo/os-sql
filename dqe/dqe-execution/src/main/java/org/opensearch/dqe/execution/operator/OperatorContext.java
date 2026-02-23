/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dqe.execution.operator;

import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import org.opensearch.dqe.execution.task.DqeQueryCancelledException;
import org.opensearch.dqe.memory.QueryMemoryBudget;

/**
 * Context for a single operator instance within a pipeline. Provides access to memory tracking,
 * cancellation checking, and execution statistics.
 */
public class OperatorContext {

  private final String queryId;
  private final int stageId;
  private final int pipelineId;
  private final int operatorId;
  private final String operatorType;
  private final QueryMemoryBudget memoryBudget;
  private final AtomicBoolean interrupted = new AtomicBoolean(false);
  private final AtomicLong inputPositions = new AtomicLong(0);
  private final AtomicLong outputPositions = new AtomicLong(0);
  private final AtomicLong inputDataSizeBytes = new AtomicLong(0);
  private final AtomicLong outputDataSizeBytes = new AtomicLong(0);
  private final AtomicLong reservedBytes = new AtomicLong(0);

  public OperatorContext(
      String queryId,
      int stageId,
      int pipelineId,
      int operatorId,
      String operatorType,
      QueryMemoryBudget memoryBudget) {
    this.queryId = Objects.requireNonNull(queryId, "queryId must not be null");
    this.stageId = stageId;
    this.pipelineId = pipelineId;
    this.operatorId = operatorId;
    this.operatorType = Objects.requireNonNull(operatorType, "operatorType must not be null");
    this.memoryBudget = Objects.requireNonNull(memoryBudget, "memoryBudget must not be null");
  }

  public String getQueryId() {
    return queryId;
  }

  public int getStageId() {
    return stageId;
  }

  public int getPipelineId() {
    return pipelineId;
  }

  public int getOperatorId() {
    return operatorId;
  }

  public String getOperatorType() {
    return operatorType;
  }

  /** Reserves memory via the per-query memory budget. */
  public void reserveMemory(long bytes) {
    memoryBudget.reserve(bytes, operatorType + "[" + operatorId + "]");
    reservedBytes.addAndGet(bytes);
  }

  /** Releases memory back to the per-query budget. */
  public void releaseMemory(long bytes) {
    memoryBudget.release(bytes, operatorType + "[" + operatorId + "]");
    reservedBytes.addAndGet(-bytes);
  }

  /** Returns bytes currently reserved by this operator. */
  public long getReservedBytes() {
    return reservedBytes.get();
  }

  /** Returns true if the query has been cancelled. */
  public boolean isInterrupted() {
    return interrupted.get();
  }

  /** Sets the interrupt flag. */
  public void setInterrupted(boolean value) {
    interrupted.set(value);
  }

  /** Checks the interrupt flag and throws if the query has been cancelled. */
  public void checkInterrupted() {
    if (interrupted.get()) {
      throw new DqeQueryCancelledException(queryId, "Query cancelled");
    }
  }

  public long getInputPositions() {
    return inputPositions.get();
  }

  public long getOutputPositions() {
    return outputPositions.get();
  }

  public void addInputPositions(long positions) {
    inputPositions.addAndGet(positions);
  }

  public void addOutputPositions(long positions) {
    outputPositions.addAndGet(positions);
  }

  public void addInputDataSizeBytes(long bytes) {
    inputDataSizeBytes.addAndGet(bytes);
  }

  public void addOutputDataSizeBytes(long bytes) {
    outputDataSizeBytes.addAndGet(bytes);
  }
}
