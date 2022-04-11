/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.plugin.transport;

import org.opensearch.action.ActionType;

public class StreamExpressionAction extends ActionType<StreamExpressionResponse> {
  public static final StreamExpressionAction INSTANCE = new StreamExpressionAction();
  public static final String NAME = "cluster:opensearch/query/stream";

  private StreamExpressionAction() {
    super(NAME, StreamExpressionResponse::new);
  }
}
