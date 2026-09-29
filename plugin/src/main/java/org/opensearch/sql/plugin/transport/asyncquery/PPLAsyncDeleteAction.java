/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.plugin.transport.asyncquery;

import org.opensearch.action.ActionType;
import org.opensearch.sql.plugin.transport.TransportPPLQueryResponse;

/** Transport action definition for cancelling a retained asynchronous PPL query. */
public final class PPLAsyncDeleteAction extends ActionType<TransportPPLQueryResponse> {
  /** Transport action name. */
  public static final String NAME = "cluster:admin/opensearch/ppl/async_query/delete";

  /** Singleton action instance. */
  public static final PPLAsyncDeleteAction INSTANCE = new PPLAsyncDeleteAction();

  private PPLAsyncDeleteAction() {
    super(NAME, TransportPPLQueryResponse::new);
  }
}
