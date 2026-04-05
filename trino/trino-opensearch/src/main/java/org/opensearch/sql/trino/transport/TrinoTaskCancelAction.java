/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.trino.transport;

import org.opensearch.action.ActionType;

/** Action type for cancelling a Trino task on a worker node. */
public class TrinoTaskCancelAction extends ActionType<TrinoTaskCancelResponse> {

  public static final String NAME = "cluster:internal/trino/task/cancel";
  public static final TrinoTaskCancelAction INSTANCE = new TrinoTaskCancelAction();

  private TrinoTaskCancelAction() {
    super(NAME, TrinoTaskCancelResponse::new);
  }
}
