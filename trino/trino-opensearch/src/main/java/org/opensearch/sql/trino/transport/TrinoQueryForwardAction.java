/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.trino.transport;

import org.opensearch.action.ActionType;

/**
 * Action type for forwarding a Trino SQL query to another OpenSearch node for execution. The target
 * node executes the full query using its local Trino engine and returns the JSON result.
 */
public class TrinoQueryForwardAction extends ActionType<TrinoQueryForwardResponse> {

  public static final String NAME = "cluster:internal/trino/query/forward";
  public static final TrinoQueryForwardAction INSTANCE = new TrinoQueryForwardAction();

  private TrinoQueryForwardAction() {
    super(NAME, TrinoQueryForwardResponse::new);
  }
}
