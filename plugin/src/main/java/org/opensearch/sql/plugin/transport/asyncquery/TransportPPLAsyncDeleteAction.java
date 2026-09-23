/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.plugin.transport.asyncquery;

import org.opensearch.action.support.ActionFilters;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.inject.Inject;
import org.opensearch.core.action.ActionListener;
import org.opensearch.sql.plugin.transport.TransportPPLQueryResponse;
import org.opensearch.tasks.Task;
import org.opensearch.transport.TransportService;

public final class TransportPPLAsyncDeleteAction
    extends TransportPPLAsyncQueryRoutingAction<PPLAsyncDeleteRequest> {

  private final PPLAsyncQueryService asyncQueryService;
  private final PPLAsyncQueryResponseFormatter responseFormatter;

  @Inject
  public TransportPPLAsyncDeleteAction(
      TransportService transportService,
      ActionFilters actionFilters,
      ClusterService clusterService,
      PPLAsyncQueryService asyncQueryService) {
    super(
        PPLAsyncDeleteAction.NAME,
        transportService,
        actionFilters,
        PPLAsyncDeleteRequest::new,
        clusterService);
    this.asyncQueryService = asyncQueryService;
    this.responseFormatter = new PPLAsyncQueryResponseFormatter();
  }

  @Override
  protected void executeOnOwner(
      Task task,
      PPLAsyncDeleteRequest request,
      ActionListener<TransportPPLQueryResponse> listener) {
    listener.onResponse(
        responseFormatter.format(asyncQueryService.delete(request.id(), currentUser())));
  }
}
