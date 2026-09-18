/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.executor;

import org.opensearch.sql.common.response.ResponseListener;

/**
 * Opt-in listener for intermediate Calcite query results.
 *
 * <p>The ordinary {@link ResponseListener} contract is unchanged. An execution engine invokes
 * {@link #onPartial(ExecutionEngine.QueryResponse)} only when the physical plan has a producer with
 * defined correctness semantics.
 */
public interface PartialResultResponseListener
    extends ResponseListener<ExecutionEngine.QueryResponse> {

  /** How a consumer applies each partial response. */
  enum UpdateMode {
    /** The response contains only newly finalized rows. */
    APPEND,
    /** The response is the complete current snapshot and replaces the previous response. */
    REPLACE
  }

  /** Called before the first partial response establishes the update mode for this execution. */
  default void onPartialResultMode(UpdateMode updateMode) {}

  /** Publishes an intermediate query result. */
  void onPartial(ExecutionEngine.QueryResponse response);
}
