/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.dqe.coordinator.transport;

import org.opensearch.action.ActionType;

/**
 * Action type for the DQE Trino SQL query endpoint. Registered with the transport layer so REST
 * requests are routed to {@link TransportTrinoSqlAction}.
 */
public class TrinoSqlAction extends ActionType<TrinoSqlResponse> {

  public static final String NAME = "cluster:admin/opensearch/dqe/sql";
  public static final TrinoSqlAction INSTANCE = new TrinoSqlAction();

  private TrinoSqlAction() {
    super(NAME, TrinoSqlResponse::new);
  }
}
