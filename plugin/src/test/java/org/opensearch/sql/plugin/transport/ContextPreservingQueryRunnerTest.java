/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.plugin.transport;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;

import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.After;
import org.junit.Test;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.concurrent.ThreadContext;
import org.opensearch.core.tasks.TaskId;
import org.opensearch.sql.job.QueryResult;
import org.opensearch.sql.job.QueryRunner;
import org.opensearch.sql.opensearch.executor.OpenSearchQueryManager;
import org.opensearch.tasks.CancellableTask;

public class ContextPreservingQueryRunnerTest {

  private final ThreadContext threadContext = new ThreadContext(Settings.EMPTY);
  private final PPLQueryTask task =
      (PPLQueryTask)
          new TransportPPLQueryRequest("source=test", null, "/_plugins/_ppl")
              .createTask(1, "transport", PPLQueryAction.NAME, TaskId.EMPTY_TASK_ID, Map.of());

  @After
  public void tearDown() {
    OpenSearchQueryManager.clearCancellableTask();
    org.apache.logging.log4j.ThreadContext.clearMap();
  }

  @Test
  public void runRestoresTheSubmittingContextOnAnotherThreadAndPutsBackTheCallers()
      throws Exception {
    CapturingRunner delegate = new CapturingRunner(threadContext);
    QueryRunner runner;
    try (ThreadContext.StoredContext ignored = threadContext.stashContext()) {
      threadContext.putHeader("submitter", "alice");
      org.apache.logging.log4j.ThreadContext.put("request_id", "req-1");
      runner = new ContextPreservingQueryRunner(delegate, threadContext, task);
    }
    org.apache.logging.log4j.ThreadContext.clearMap();

    Thread starter =
        new Thread(
            () -> {
              threadContext.putHeader("submitter", "someone-else");
              runner.run();
              delegate.callerHeaderAfterRun.set(threadContext.getHeader("submitter"));
              delegate.callerTaskAfterRun.set(OpenSearchQueryManager.getCancellableTask());
            });
    starter.start();
    starter.join();

    assertEquals("alice", delegate.header.get());
    assertEquals("req-1", delegate.requestId.get());
    assertSame(task, delegate.task.get());
    assertEquals("someone-else", delegate.callerHeaderAfterRun.get());
    assertNull(delegate.callerTaskAfterRun.get());
  }

  /** Records the context it runs in. */
  private static final class CapturingRunner implements QueryRunner {
    private final ThreadContext threadContext;
    private final AtomicReference<String> header = new AtomicReference<>();
    private final AtomicReference<String> requestId = new AtomicReference<>();
    private final AtomicReference<CancellableTask> task = new AtomicReference<>();
    private final AtomicReference<String> callerHeaderAfterRun = new AtomicReference<>();
    private final AtomicReference<CancellableTask> callerTaskAfterRun = new AtomicReference<>();

    CapturingRunner(ThreadContext threadContext) {
      this.threadContext = threadContext;
    }

    @Override
    public CompletionStage<QueryResult> run() {
      header.set(threadContext.getHeader("submitter"));
      requestId.set(org.apache.logging.log4j.ThreadContext.get("request_id"));
      task.set(OpenSearchQueryManager.getCancellableTask());
      return new CompletableFuture<>();
    }

    @Override
    public void cancel() {}
  }
}
