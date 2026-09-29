/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.plugin.transport.asyncquery;

import org.opensearch.action.ActionType;
import org.opensearch.sql.plugin.transport.TransportPPLQueryResponse;

/** Transport action definition for reading a retained asynchronous PPL query result. */
public final class PPLAsyncGetResultAction extends ActionType<TransportPPLQueryResponse> {
  /** Transport action name. */
  public static final String NAME = "cluster:admin/opensearch/ppl/async_query/result";

  /** Singleton action instance. */
  public static final PPLAsyncGetResultAction INSTANCE = new PPLAsyncGetResultAction();

  private PPLAsyncGetResultAction() {
    super(NAME, TransportPPLQueryResponse::new);
  }
}
