/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.executor;

import org.opensearch.sql.executor.ExecutionEngine.QueryResponse;

/**
 * Job-scoped access to the current result of a running Calcite query.
 *
 * <p>The primary query owns result production. API polling only reads the state already published
 * by that execution and never starts a second primary query.
 */
public interface ProgressiveQueryContext extends AutoCloseable {

  /** Materialize the complete result currently visible to the job. */
  QueryResponse currentResult();

  @Override
  void close();
}
