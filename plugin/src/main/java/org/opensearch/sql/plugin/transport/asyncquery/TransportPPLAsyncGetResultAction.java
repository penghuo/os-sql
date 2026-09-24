/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.plugin.transport.asyncquery;

import static org.opensearch.sql.opensearch.executor.OpenSearchQueryManager.SQL_WORKER_THREAD_POOL_NAME;

import java.util.concurrent.Executor;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.inject.Inject;
import org.opensearch.core.action.ActionListener;
import org.opensearch.sql.plugin.transport.TransportPPLQueryResponse;
import org.opensearch.tasks.Task;
import org.opensearch.transport.TransportService;

/** Routes asynchronous PPL GET requests to the job owner node and returns its current snapshot. */
public final class TransportPPLAsyncGetResultAction
    extends TransportPPLAsyncQueryRoutingAction<PPLAsyncGetResultRequest> {

  private final PPLAsyncQueryService asyncQueryService;
  private final PPLAsyncQueryResponseFormatter responseFormatter;
  private final Executor worker;

  /**
   * Creates the asynchronous PPL GET transport action.
   *
   * @param transportService node transport service
   * @param actionFilters configured transport action filters
   * @param clusterService current cluster state service
   * @param asyncQueryService owner-node asynchronous query lifecycle service
   */
  @Inject
  public TransportPPLAsyncGetResultAction(
      TransportService transportService,
      ActionFilters actionFilters,
      ClusterService clusterService,
      PPLAsyncQueryService asyncQueryService) {
    super(
        PPLAsyncGetResultAction.NAME,
        transportService,
        actionFilters,
        PPLAsyncGetResultRequest::new,
        clusterService,
        asyncQueryService);
    this.asyncQueryService = asyncQueryService;
    this.responseFormatter = new PPLAsyncQueryResponseFormatter();
    this.worker = transportService.getThreadPool().executor(SQL_WORKER_THREAD_POOL_NAME);
  }

  @Override
  protected void executeOnOwner(
      Task task,
      PPLAsyncGetResultRequest request,
      ActionListener<TransportPPLQueryResponse> listener) {
    PPLAsyncQueryUser caller = currentUser();
    try {
      worker.execute(
          () ->
              ActionListener.completeWith(
                  listener,
                  () ->
                      responseFormatter.format(
                          asyncQueryService.get(request.id(), caller, request.keepAlive()))));
    } catch (RuntimeException e) {
      listener.onFailure(e);
    }
  }
}
