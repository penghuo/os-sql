/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.executor;

/**
 * A query task that wants to know when its query begins executing, as opposed to waiting for or
 * being prepared on a worker thread. {@link OpenSearchExecutionEngine} calls it when execution or
 * explain starts, on the thread that does the work.
 */
public interface ExecutionStartListener {

  /** Called when the query begins executing; may be called more than once per query. */
  void onExecutionStarted();
}
