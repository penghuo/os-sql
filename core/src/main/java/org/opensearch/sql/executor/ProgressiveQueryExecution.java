/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.executor;

import java.util.Optional;
import java.util.concurrent.CompletionStage;
import org.opensearch.sql.executor.ExecutionEngine.QueryResponse;

/**
 * Lifecycle-facing handle for one running progressive query.
 *
 * <p>The execution module owns result production. The lifecycle module owns this handle after
 * submission and uses it to read current results, observe terminal completion, and release
 * execution-owned result resources.
 */
public interface ProgressiveQueryExecution extends AutoCloseable {

  /** Returns the complete result currently visible, or empty before execution is ready. */
  Optional<QueryResponse> currentResult();

  /** Completes normally on query success and exceptionally on query failure. */
  CompletionStage<Void> completion();

  @Override
  void close();
}
