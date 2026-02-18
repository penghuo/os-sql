/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.plugin.transport;

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
 * Transport action for receiving shuffled data and executing local plan fragments.
 *
 * <p>Receives shuffled row data via {@link CalciteShuffleRequest}, deserializes it, executes the
 * plan fragment specified in the request, and returns results in a {@link CalciteShuffleResponse}.
 *
 * <p>Security is enforced via ActionFilters on this TransportAction which provide RBAC and audit
 * logging.
 */
public class TransportCalciteShuffleAction
        extends HandledTransportAction<CalciteShuffleRequest, CalciteShuffleResponse> {

    private static final Logger LOG = LogManager.getLogger(TransportCalciteShuffleAction.class);

    private final NodeClient client;

    @Inject
    public TransportCalciteShuffleAction(
            TransportService transportService,
            ActionFilters actionFilters,
            NodeClient client) {
        super(
                CalciteShuffleAction.NAME,
                transportService,
                actionFilters,
                CalciteShuffleRequest::new);
        this.client = client;
    }

    @Override
    protected void doExecute(
            Task task,
            CalciteShuffleRequest request,
            ActionListener<CalciteShuffleResponse> listener) {
        try {
            LOG.debug(
                    "Received shuffle request for partition {} with {} bytes of data",
                    request.getPartitionId(),
                    request.getSerializedRows().length);

            ShardCalciteRuntime runtime = new ShardCalciteRuntime(client);

            // TODO: Deserialize shuffledRows from byte[] (request.getSerializedRows())
            // For now, pass empty rows — full binary deserialization will be added when
            // the row serialization format is finalized
            List<Object[]> shuffledRows = List.of();

            List<Object[]> resultRows =
                    runtime.executeShuffleFragment(
                            request.getSerializedPlan(),
                            shuffledRows,
                            request.getColumnNames(),
                            request.getPartitionId());

            List<String> columnNames = runtime.getLastColumnNames();
            listener.onResponse(
                    new CalciteShuffleResponse(
                            resultRows, columnNames, request.getPartitionId()));
        } catch (Exception e) {
            LOG.error(
                    "Failed to process shuffle data for partition {}",
                    request.getPartitionId(),
                    e);
            listener.onResponse(
                    new CalciteShuffleResponse(request.getPartitionId(), e.getMessage()));
        }
    }
}
