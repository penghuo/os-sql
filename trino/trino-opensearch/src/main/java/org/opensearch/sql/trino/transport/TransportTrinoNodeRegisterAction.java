/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.trino.transport;

import java.net.URI;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.HandledTransportAction;
import org.opensearch.common.inject.Inject;
import org.opensearch.core.action.ActionListener;
import org.opensearch.sql.trino.node.OpenSearchNodeManager;
import org.opensearch.sql.trino.plugin.TrinoServiceHolder;
import org.opensearch.tasks.Task;
import org.opensearch.transport.TransportService;

/**
 * Handles {@code trino:node/register} — stores a remote node's Trino HTTP URL in the local
 * OpenSearchNodeManager. This enables the coordinator to construct correct InternalNode URIs for
 * the exchange mechanism (page fetching via HTTP).
 */
public class TransportTrinoNodeRegisterAction
    extends HandledTransportAction<TrinoNodeRegisterRequest, TrinoNodeRegisterResponse> {

  private static final Logger LOG = LogManager.getLogger(TransportTrinoNodeRegisterAction.class);

  @Inject
  public TransportTrinoNodeRegisterAction(
      TransportService transportService, ActionFilters actionFilters) {
    super(
        TrinoNodeRegisterAction.NAME,
        transportService,
        actionFilters,
        TrinoNodeRegisterRequest::new);
  }

  @Override
  protected void doExecute(
      Task task,
      TrinoNodeRegisterRequest request,
      ActionListener<TrinoNodeRegisterResponse> listener) {
    try {
      if (!TrinoServiceHolder.isInitialized()) {
        listener.onResponse(new TrinoNodeRegisterResponse(false, "", ""));
        return;
      }
      TrinoServiceHolder holder = TrinoServiceHolder.getInstance();
      OpenSearchNodeManager nodeManager = holder.getNodeManager();
      if (nodeManager != null) {
        nodeManager.registerTrinoHttpUrl(
            request.getNodeId(), URI.create(request.getTrinoHttpUrl()));
        LOG.info("Registered remote Trino HTTP URL: node={}, url={}",
            request.getNodeId(), request.getTrinoHttpUrl());
        // Include this node's own URL in the response for bidirectional exchange
        URI myUrl = holder.getTrinoHttpUrl();
        String myNodeId = nodeManager.getCurrentNode().getNodeIdentifier();
        listener.onResponse(new TrinoNodeRegisterResponse(
            true, myNodeId, myUrl != null ? myUrl.toString() : ""));
      } else {
        LOG.warn("NodeManager not available, cannot register Trino HTTP URL for node={}",
            request.getNodeId());
        listener.onResponse(new TrinoNodeRegisterResponse(false, "", ""));
      }
    } catch (Exception e) {
      LOG.error("Failed to register Trino HTTP URL for node={}", request.getNodeId(), e);
      listener.onFailure(e);
    }
  }
}
