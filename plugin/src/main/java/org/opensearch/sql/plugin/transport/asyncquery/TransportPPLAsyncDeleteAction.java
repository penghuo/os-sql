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
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.TransportService;

/** Routes asynchronous PPL DELETE requests to the job owner node and performs cancellation. */
public final class TransportPPLAsyncDeleteAction
    extends TransportPPLAsyncQueryRoutingAction<PPLAsyncDeleteRequest> {

  private final PPLAsyncQueryResponseFormatter responseFormatter;

  /**
   * Creates the asynchronous PPL DELETE transport action.
   *
   * @param transportService node transport service
   * @param actionFilters configured transport action filters
   * @param clusterService current cluster state service
   * @param asyncQueryService owner-node asynchronous query lifecycle service
   */
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
        clusterService,
        asyncQueryService,
        ThreadPool.Names.SAME);
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
