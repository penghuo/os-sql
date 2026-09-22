/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.executor;

import org.opensearch.sql.common.response.ResponseListener;
import org.opensearch.sql.executor.ExecutionEngine.QueryResponse;

/**
 * Response listener used by asynchronous PPL execution.
 *
 * <p>The listener remains the normal terminal result callback and additionally receives the
 * current-result context after the physical plan has been classified and before execution starts.
 */
public interface ProgressiveQueryResponseListener extends ResponseListener<QueryResponse> {

  void onContextReady(ProgressiveQueryContext context);
}
