/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.executor;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

import org.junit.jupiter.api.Test;
import org.opensearch.sql.executor.ProgressiveQueryResponseListener.QueryProgress;
import org.opensearch.tasks.CancellableTask;

class ProgressiveQueryContextTest {

  @Test
  void restores_nested_and_captured_contexts() {
    ProgressiveQueryContext.Observer outer = mock(ProgressiveQueryContext.Observer.class);
    ProgressiveQueryContext.Observer inner = mock(ProgressiveQueryContext.Observer.class);

    assertFalse(ProgressiveQueryContext.isActive());
    try (ProgressiveQueryContext.Scope ignored = ProgressiveQueryContext.open(outer)) {
      ProgressiveQueryContext.Captured captured = ProgressiveQueryContext.capture();
      assertSame(outer, captured.observer());

      try (ProgressiveQueryContext.Scope nested = ProgressiveQueryContext.open(inner)) {
        assertSame(inner, ProgressiveQueryContext.capture().observer());
      }

      assertTrue(ProgressiveQueryContext.withContext(captured, ProgressiveQueryContext::isActive));
      assertSame(outer, ProgressiveQueryContext.capture().observer());
    }

    assertFalse(ProgressiveQueryContext.isActive());
    assertNull(ProgressiveQueryContext.startSearch(true));
  }

  @Test
  void search_operation_forwards_progress_and_cancellation_lifecycle() {
    ProgressiveQueryContext.Observer observer = mock(ProgressiveQueryContext.Observer.class);
    CancellableTask task = mock(CancellableTask.class);
    QueryProgress progress = new QueryProgress(0.5D, 4, 2);

    try (ProgressiveQueryContext.Scope ignored = ProgressiveQueryContext.open(observer)) {
      ProgressiveQueryContext.SearchOperation operation = ProgressiveQueryContext.startSearch(true);
      assertTrue(operation.exactFraction());
      operation.registerTask(task);
      operation.publish(progress);
      operation.complete();
    }

    verify(observer).onSearchTaskStarted(any(Long.class), any(Runnable.class));
    verify(observer).onProgress(progress);
    verify(observer).onSearchTaskFinished(any(Long.class));
  }

  @Test
  void observer_failure_does_not_fail_query_execution() {
    ProgressiveQueryContext.Observer observer = mock(ProgressiveQueryContext.Observer.class);
    doThrow(new IllegalStateException("listener failed")).when(observer).onProgress(any());
    doThrow(new IllegalStateException("listener failed"))
        .when(observer)
        .onSearchTaskStarted(any(Long.class), any(Runnable.class));
    doThrow(new IllegalStateException("listener failed"))
        .when(observer)
        .onSearchTaskFinished(any(Long.class));

    try (ProgressiveQueryContext.Scope ignored = ProgressiveQueryContext.open(observer)) {
      ProgressiveQueryContext.SearchOperation operation =
          ProgressiveQueryContext.startSearch(false);
      operation.registerTask(mock(CancellableTask.class));
      operation.publish(QueryProgress.UNKNOWN);
      operation.complete();
    }

    assertFalse(ProgressiveQueryContext.isActive());
  }
}
