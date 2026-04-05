/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.trino.transport;

import org.opensearch.action.ActionType;

/** Action type for polling Trino task status on a worker node. */
public class TrinoTaskStatusAction extends ActionType<TrinoTaskStatusResponse> {

  public static final String NAME = "cluster:internal/trino/task/status";
  public static final TrinoTaskStatusAction INSTANCE = new TrinoTaskStatusAction();

  private TrinoTaskStatusAction() {
    super(NAME, TrinoTaskStatusResponse::new);
  }
}
