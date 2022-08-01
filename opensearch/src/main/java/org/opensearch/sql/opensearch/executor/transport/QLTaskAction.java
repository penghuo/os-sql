/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.executor.transport;

import org.opensearch.action.ActionType;

public class QLTaskAction extends ActionType<QLTaskResponse> {
  public static final QLTaskAction INSTANCE = new QLTaskAction();
  public static final String NAME = "cluster:opensearch/query/task";

  private QLTaskAction() {
    super(NAME, QLTaskResponse::new);
  }
}
