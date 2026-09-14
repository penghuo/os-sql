/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.executor;

import org.opensearch.sql.common.response.ResponseListener;

/**
 * Opt-in listener for Calcite executions that can publish intermediate query snapshots.
 *
 * <p>The normal {@link ResponseListener} contract remains unchanged. Execution engines should call
 * {@link #onPartial(ExecutionEngine.QueryResponse)} only when the optimized physical plan is known
 * to produce semantically valid preview rows. {@link UpdateMode#APPEND} previews are immutable
 * prefixes; {@link UpdateMode#REPLACE} previews are complete snapshots that may be replaced by a
 * later sequence.
 */
public interface ProgressiveQueryResponseListener
    extends ResponseListener<ExecutionEngine.QueryResponse> {

  /** Whether rows already returned by a running query are immutable. */
  enum UpdateMode {
    APPEND,
    REPLACE
  }

  /**
   * Query-level execution progress.
   *
   * <p>A value of {@code -1} means unknown. In particular, a query implemented by multiple search
   * requests, such as composite aggregation pagination or a PIT scan, has no exact overall shard
   * denominator.
   */
  record QueryProgress(double fractionDone, int shardsTotal, int shardsCompleted) {
    public static final QueryProgress UNKNOWN = new QueryProgress(-1D, -1, -1);

    public QueryProgress {
      if (fractionDone != -1D && (fractionDone < 0D || fractionDone > 1D)) {
        throw new IllegalArgumentException("fractionDone must be -1 or between 0 and 1");
      }
      if (shardsTotal < -1 || shardsCompleted < -1) {
        throw new IllegalArgumentException("shard counters must be -1 or non-negative");
      }
    }
  }

  /** Called once the logical and physical plans establish the fixed update mode for the job. */
  default void onQueryClassified(UpdateMode updateMode) {}

  /** Publishes progress without changing the current result rows. */
  default void onProgress(QueryProgress progress) {}

  /**
   * Registers a currently running OpenSearch search task with the job.
   *
   * <p>The callback is intentionally a {@link Runnable} so the core execution contract does not
   * depend on OpenSearch server task classes.
   */
  default void onSearchTaskStarted(long operationId, Runnable cancelAction) {}

  /** Removes a completed OpenSearch search task from the job cancellation set. */
  default void onSearchTaskFinished(long operationId) {}

  /** Publishes the current rows according to the job's fixed {@link UpdateMode}. */
  void onPartial(ExecutionEngine.QueryResponse response);

  /**
   * Atomically publishes rows and the progress represented by those rows.
   *
   * <p>Implementations that persist asynchronous job snapshots should override this method so one
   * sequence identifies both values. The default preserves compatibility for other listeners.
   */
  default void onPartial(ExecutionEngine.QueryResponse response, QueryProgress progress) {
    onProgress(progress);
    onPartial(response);
  }
}
