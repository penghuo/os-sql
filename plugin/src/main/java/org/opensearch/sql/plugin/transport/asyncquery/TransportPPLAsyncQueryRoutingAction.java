/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.plugin.transport.asyncquery;

import org.opensearch.ExceptionsHelper;
import org.opensearch.ResourceNotFoundException;
import org.opensearch.action.ActionListenerResponseHandler;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.HandledTransportAction;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.common.io.stream.Writeable;
import org.opensearch.node.NodeClosedException;
import org.opensearch.sql.plugin.transport.TransportPPLQueryResponse;
import org.opensearch.tasks.Task;
import org.opensearch.transport.ConnectTransportException;
import org.opensearch.transport.NodeDisconnectedException;
import org.opensearch.transport.NodeNotConnectedException;
import org.opensearch.transport.TransportRequestOptions;
import org.opensearch.transport.TransportService;

abstract class TransportPPLAsyncQueryRoutingAction<Request extends AbstractPPLAsyncQueryRequest>
    extends HandledTransportAction<Request, TransportPPLQueryResponse> {

  private final String actionName;
  private final TransportService transportService;
  private final ClusterService clusterService;
  private final PPLAsyncQuerySecurity asyncQuerySecurity;
  private final PPLAsyncQueryService asyncQueryService;

  TransportPPLAsyncQueryRoutingAction(
      String actionName,
      TransportService transportService,
      ActionFilters actionFilters,
      Writeable.Reader<Request> requestReader,
      ClusterService clusterService,
      PPLAsyncQuerySecurity asyncQuerySecurity,
      PPLAsyncQueryService asyncQueryService) {
    super(actionName, transportService, actionFilters, requestReader);
    this.actionName = actionName;
    this.transportService = transportService;
    this.clusterService = clusterService;
    this.asyncQuerySecurity = asyncQuerySecurity;
    this.asyncQueryService = asyncQueryService;
  }

  @Override
  protected final void doExecute(
      Task task, Request request, ActionListener<TransportPPLQueryResponse> listener) {
    try {
      asyncQueryService.ensurePplEnabled();
    } catch (RuntimeException e) {
      listener.onFailure(e);
      return;
    }
    PPLAsyncQueryJobId jobId = PPLAsyncQueryJobId.parse(request.id());
    if (clusterService.localNode().getId().equals(jobId.ownerNodeId())) {
      executeOnOwner(task, request, listener);
      return;
    }

    DiscoveryNode ownerNode = clusterService.state().nodes().get(jobId.ownerNodeId());
    if (ownerNode == null) {
      listener.onFailure(notFound());
      return;
    }
    transportService.sendRequest(
        ownerNode,
        actionName,
        request,
        TransportRequestOptions.EMPTY,
        new ActionListenerResponseHandler<>(
            ActionListener.wrap(
                listener::onResponse,
                failure -> listener.onFailure(ownerUnavailable(failure) ? notFound() : failure)),
            TransportPPLQueryResponse::new));
  }

  protected final PPLAsyncQueryUser currentUser() {
    return asyncQuerySecurity.currentUser(transportService.getThreadPool().getThreadContext());
  }

  protected abstract void executeOnOwner(
      Task task, Request request, ActionListener<TransportPPLQueryResponse> listener);

  private static ResourceNotFoundException notFound() {
    return new ResourceNotFoundException("PPL asynchronous query not found");
  }

  static boolean ownerUnavailable(Exception failure) {
    Throwable cause = ExceptionsHelper.unwrapCause(failure);
    return cause instanceof ConnectTransportException
        || cause instanceof NodeDisconnectedException
        || cause instanceof NodeNotConnectedException
        || cause instanceof NodeClosedException;
  }
}
