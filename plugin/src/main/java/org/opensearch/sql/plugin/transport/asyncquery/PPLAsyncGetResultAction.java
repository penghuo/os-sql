/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.plugin.transport.asyncquery;

import org.opensearch.action.ActionType;
import org.opensearch.sql.plugin.transport.TransportPPLQueryResponse;

public final class PPLAsyncGetResultAction extends ActionType<TransportPPLQueryResponse> {
  public static final String NAME = "cluster:admin/opensearch/ppl/async_query/result";
  public static final PPLAsyncGetResultAction INSTANCE = new PPLAsyncGetResultAction();

  private PPLAsyncGetResultAction() {
    super(NAME, TransportPPLQueryResponse::new);
  }
}
