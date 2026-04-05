/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.trino.execution;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.trino.execution.SqlTaskManager;
import io.trino.execution.SqlTaskManager.SqlTaskWithResults;
import io.trino.execution.StageId;
import io.trino.execution.TaskId;
import io.trino.execution.TaskInfo;
import io.trino.execution.buffer.PipelinedOutputBuffers.OutputBufferId;
import io.trino.spi.QueryId;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Tests for {@link OpenSearchSqlTaskManager}.
 *
 * <p>Avoids mocking Trino final classes (Session, OutputBuffers, PlanFragment). Tests focus on:
 * delegation behavior, diagnostic counters, and lifecycle management.
 */
class OpenSearchSqlTaskManagerTest {

  private SqlTaskManager mockDelegate;
  private OpenSearchSqlTaskManager taskManager;
  private TaskId testTaskId;

  @BeforeEach
  void setUp() {
    mockDelegate = mock(SqlTaskManager.class);
    taskManager = new OpenSearchSqlTaskManager(mockDelegate);
    testTaskId = new TaskId(new StageId(new QueryId("test_query"), 0), 0, 0);
  }

  @Test
  void uninitializedManagerThrowsOnGetTaskInfo() {
    OpenSearchSqlTaskManager uninitialized = new OpenSearchSqlTaskManager();
    assertThrows(IllegalStateException.class, () -> uninitialized.getTaskInfo(testTaskId));
  }

  @Test
  void uninitializedManagerThrowsOnCancel() {
    OpenSearchSqlTaskManager uninitialized = new OpenSearchSqlTaskManager();
    assertThrows(IllegalStateException.class, () -> uninitialized.cancelTask(testTaskId));
  }

  @Test
  void initializeAfterConstruction() {
    OpenSearchSqlTaskManager manager = new OpenSearchSqlTaskManager();
    TaskInfo mockInfo = mock(TaskInfo.class);
    when(mockDelegate.getTaskInfo(testTaskId)).thenReturn(mockInfo);

    manager.initialize(mockDelegate);
    TaskInfo result = manager.getTaskInfo(testTaskId);

    assertEquals(mockInfo, result);
  }

  @Test
  void getTaskInfoDelegates() {
    TaskInfo mockInfo = mock(TaskInfo.class);
    when(mockDelegate.getTaskInfo(testTaskId)).thenReturn(mockInfo);

    TaskInfo result = taskManager.getTaskInfo(testTaskId);

    assertEquals(mockInfo, result);
    verify(mockDelegate).getTaskInfo(testTaskId);
  }

  @Test
  void cancelTaskDelegates() {
    TaskInfo mockInfo = mock(TaskInfo.class);
    when(mockDelegate.cancelTask(testTaskId)).thenReturn(mockInfo);

    TaskInfo result = taskManager.cancelTask(testTaskId);

    assertEquals(mockInfo, result);
    verify(mockDelegate).cancelTask(testTaskId);
  }

  @Test
  void abortTaskDelegates() {
    TaskInfo mockInfo = mock(TaskInfo.class);
    when(mockDelegate.abortTask(testTaskId)).thenReturn(mockInfo);

    TaskInfo result = taskManager.abortTask(testTaskId);
    assertEquals(mockInfo, result);
  }

  @Test
  void failTaskDelegates() {
    TaskInfo mockInfo = mock(TaskInfo.class);
    RuntimeException error = new RuntimeException("test failure");
    when(mockDelegate.failTask(testTaskId, error)).thenReturn(mockInfo);

    TaskInfo result = taskManager.failTask(testTaskId, error);
    assertEquals(mockInfo, result);
  }

  @Test
  void acknowledgeTaskResultsDelegates() {
    OutputBufferId bufferId = new OutputBufferId(0);
    taskManager.acknowledgeTaskResults(testTaskId, bufferId, 42L);
    verify(mockDelegate).acknowledgeTaskResults(testTaskId, bufferId, 42L);
  }

  @Test
  void getTaskResultsIncrementsCounter() {
    // SqlTaskWithResults is final — we can't mock it. Instead verify the counter increments
    // and the delegate is called. The delegate will return null (unstubbed), which is fine
    // for testing the counter.
    OutputBufferId bufferId = new OutputBufferId(0);
    assertEquals(0, taskManager.getResultsFetched());

    try {
      taskManager.getTaskResults(
          testTaskId, bufferId, 0L,
          io.airlift.units.DataSize.of(1, io.airlift.units.DataSize.Unit.MEGABYTE));
    } catch (Exception e) {
      // Delegate may throw since it's unstubbed for final return type — that's OK
    }

    assertEquals(1, taskManager.getResultsFetched());
    verify(mockDelegate).getTaskResults(eq(testTaskId), eq(bufferId), eq(0L), any());
  }

  @Test
  void recordPagesProducedUpdatesCounter() {
    assertEquals(0, taskManager.getPagesProduced());
    taskManager.recordPagesProduced(10);
    assertEquals(10, taskManager.getPagesProduced());
    taskManager.recordPagesProduced(5);
    assertEquals(15, taskManager.getPagesProduced());
  }

  @Test
  void resetStatsClearsAllCounters() {
    taskManager.recordPagesProduced(10);
    assertEquals(10, taskManager.getPagesProduced());

    taskManager.resetStats();

    assertEquals(0, taskManager.getTasksReceived());
    assertEquals(0, taskManager.getTaskUpdatesReceived());
    assertEquals(0, taskManager.getSplitsProcessed());
    assertEquals(0, taskManager.getPagesProduced());
    assertEquals(0, taskManager.getResultsFetched());
  }

  @Test
  void closeClosesDelegate() {
    taskManager.close();
    verify(mockDelegate).close();
  }

  @Test
  void closeHandlesNullDelegate() {
    // Should not throw even when uninitialized
    OpenSearchSqlTaskManager uninitialized = new OpenSearchSqlTaskManager();
    uninitialized.close();
  }
}
