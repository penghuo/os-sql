/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.plugin.transport;

import org.opensearch.action.ActionType;

public final class PPLAsyncDeleteAction extends ActionType<TransportPPLQueryResponse> {
  public static final String NAME = "cluster:admin/opensearch/ppl/async_query/delete";
  public static final PPLAsyncDeleteAction INSTANCE = new PPLAsyncDeleteAction();

  private PPLAsyncDeleteAction() {
    super(NAME, TransportPPLQueryResponse::new);
  }
}
