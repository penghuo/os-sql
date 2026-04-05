/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.trino.transport;

import org.opensearch.action.ActionType;

/**
 * Action type for fetching TRINO_PAGES binary from an upstream OutputBuffer. This is the
 * performance-critical data plane — every shuffled page flows through here.
 */
public class TrinoTaskResultsAction extends ActionType<TrinoTaskResultsResponse> {

  public static final String NAME = "cluster:internal/trino/task/results";
  public static final TrinoTaskResultsAction INSTANCE = new TrinoTaskResultsAction();

  private TrinoTaskResultsAction() {
    super(NAME, TrinoTaskResultsResponse::new);
  }
}
