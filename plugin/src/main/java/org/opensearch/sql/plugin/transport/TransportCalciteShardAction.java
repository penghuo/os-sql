/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.plugin.transport;

import java.util.ArrayList;
import java.util.List;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.HandledTransportAction;
import org.opensearch.common.inject.Inject;
import org.opensearch.core.action.ActionListener;
import org.opensearch.sql.opensearch.storage.scan.ShardCalciteRuntime;
import org.opensearch.tasks.Task;
import org.opensearch.transport.TransportService;
import org.opensearch.transport.client.node.NodeClient;

/**
 * Transport action for executing Calcite plan fragments on data nodes.
 *
 * <p>Receives a serialized Calcite plan fragment via {@link CalciteShardRequest}, deserializes it,
 * binds it to the local shard via {@link ShardCalciteRuntime}, executes the Calcite Enumerable
 * pipeline, and returns results in a {@link CalciteShardResponse}.
 *
 * <p>Security is enforced via:
 *
 * <ul>
 *   <li>ActionFilters on this TransportAction provide RBAC and audit logging
 *   <li>CalciteLocalShardScan uses NodeClient.search(preference=_shards:N|_local) which routes
 *       through the standard search pipeline where DLS/FLS is enforced
 * </ul>
 */
public class TransportCalciteShardAction
        extends HandledTransportAction<CalciteShardRequest, CalciteShardResponse> {

    private static final Logger LOG = LogManager.getLogger(TransportCalciteShardAction.class);

    private final NodeClient client;

    @Inject
    public TransportCalciteShardAction(
            TransportService transportService,
            ActionFilters actionFilters,
            NodeClient client) {
        super(
                CalciteShardAction.NAME,
                transportService,
                actionFilters,
                CalciteShardRequest::new);
        this.client = client;
    }

    @Override
    protected void doExecute(
            Task task,
            CalciteShardRequest request,
            ActionListener<CalciteShardResponse> listener) {
        try {
            ShardCalciteRuntime runtime = new ShardCalciteRuntime(client);
            List<Object[]> rows = runtime.execute(
                    request.getSerializedPlan(),
                    request.getIndexName(),
                    request.getShardId());

            List<String> columnNames = runtime.getLastColumnNames();
            listener.onResponse(
                    new CalciteShardResponse(rows, columnNames, request.getShardId()));
        } catch (Exception e) {
            LOG.error("Failed to execute Calcite plan on shard {}", request.getShardId(), e);
            listener.onResponse(
                    new CalciteShardResponse(request.getShardId(), e.getMessage()));
        }
    }
}
