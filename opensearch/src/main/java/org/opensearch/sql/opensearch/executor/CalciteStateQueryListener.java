/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.executor;

import org.opensearch.sql.common.response.ResponseListener;
import org.opensearch.sql.executor.ExecutionEngine.QueryResponse;

/** Opt-in listener that receives the queryable partial-result state for a Calcite execution. */
public interface CalciteStateQueryListener extends ResponseListener<QueryResponse> {
  void onStateQuery(CalciteStateQuery stateQuery);
}
