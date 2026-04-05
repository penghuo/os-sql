/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.trino.transport;

import org.opensearch.action.ActionType;

/**
 * Action type for acknowledging consumed pages from an OutputBuffer. This is the flow control
 * mechanism — the upstream can release memory after ack.
 */
public class TrinoTaskResultsAckAction extends ActionType<TrinoTaskResultsAckResponse> {

  public static final String NAME = "cluster:internal/trino/task/results/ack";
  public static final TrinoTaskResultsAckAction INSTANCE = new TrinoTaskResultsAckAction();

  private TrinoTaskResultsAckAction() {
    super(NAME, TrinoTaskResultsAckResponse::new);
  }
}
