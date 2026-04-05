/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.trino.transport;

import org.opensearch.action.ActionType;

/** Action type for creating or updating a Trino task on a worker node. */
public class TrinoTaskUpdateAction extends ActionType<TrinoTaskUpdateResponse> {

  public static final String NAME = "cluster:internal/trino/task/update";
  public static final TrinoTaskUpdateAction INSTANCE = new TrinoTaskUpdateAction();

  private TrinoTaskUpdateAction() {
    super(NAME, TrinoTaskUpdateResponse::new);
  }
}
