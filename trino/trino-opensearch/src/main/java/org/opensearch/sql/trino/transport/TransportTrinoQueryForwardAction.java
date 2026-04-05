/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.trino.transport;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.HandledTransportAction;
import org.opensearch.common.inject.Inject;
import org.opensearch.core.action.ActionListener;
import org.opensearch.sql.trino.bootstrap.TrinoEngine;
import org.opensearch.sql.trino.plugin.TrinoServiceHolder;
import org.opensearch.tasks.Task;
import org.opensearch.transport.TransportService;

/**
 * Handles {@code trino:query/forward} — executes a full SQL query on the local Trino engine. This
 * is the cross-node query distribution mechanism: the coordinator forwards queries to worker nodes
 * via this transport action.
 */
public class TransportTrinoQueryForwardAction
    extends HandledTransportAction<TrinoQueryForwardRequest, TrinoQueryForwardResponse> {

  private static final Logger LOG = LogManager.getLogger(TransportTrinoQueryForwardAction.class);

  @Inject
  public TransportTrinoQueryForwardAction(
      TransportService transportService, ActionFilters actionFilters) {
    super(
        TrinoQueryForwardAction.NAME,
        transportService,
        actionFilters,
        TrinoQueryForwardRequest::new);
    // Make TransportService available to the REST handler for direct node targeting
    org.opensearch.sql.trino.plugin.TrinoServiceHolder.setTransportService(transportService);
  }

  @Override
  protected void doExecute(
      Task task,
      TrinoQueryForwardRequest request,
      ActionListener<TrinoQueryForwardResponse> listener) {
    try {
      if (!TrinoServiceHolder.isInitialized()) {
        listener.onFailure(new IllegalStateException("Trino engine not initialized"));
        return;
      }

      // Execute the query on the LOCAL Trino engine
      TrinoEngine engine = TrinoServiceHolder.getInstance().getEngine();
      if (engine == null) {
        listener.onFailure(new IllegalStateException("TrinoEngine not available"));
        return;
      }

      // Record that this node received a forwarded query
      TrinoServiceHolder.getInstance().getTaskManager().recordPagesProduced(1);

      String resultJson =
          engine.executeAndSerializeJson(request.getSql(), request.getCatalog(), request.getSchema());
      listener.onResponse(new TrinoQueryForwardResponse(resultJson));
    } catch (Exception e) {
      LOG.error("Failed to execute forwarded query: {}", request.getSql(), e);
      listener.onFailure(e);
    }
  }
}
