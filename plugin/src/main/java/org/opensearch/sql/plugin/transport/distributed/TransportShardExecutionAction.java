/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.plugin.transport.distributed;

import org.opensearch.action.ActionRequest;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.HandledTransportAction;
import org.opensearch.common.inject.Inject;
import org.opensearch.core.action.ActionListener;
import org.opensearch.sql.opensearch.executor.distributed.FragmentExecutor;
import org.opensearch.tasks.Task;
import org.opensearch.transport.TransportService;

/** Transport action handler for shard-level plan fragment execution. */
public class TransportShardExecutionAction
        extends HandledTransportAction<ActionRequest, ShardExecutionResponse> {

    private final FragmentExecutor fragmentExecutor;
    private final TransportService transportService;

    @Inject
    public TransportShardExecutionAction(
            TransportService transportService,
            ActionFilters actionFilters,
            FragmentExecutor fragmentExecutor) {
        super(
                ShardExecutionAction.NAME,
                transportService,
                actionFilters,
                ShardExecutionRequest::new);
        this.fragmentExecutor = fragmentExecutor;
        this.transportService = transportService;
    }

    @Override
    protected void doExecute(
            Task task,
            ActionRequest actionRequest,
            ActionListener<ShardExecutionResponse> listener) {
        ShardExecutionRequest request = (ShardExecutionRequest) actionRequest;
        try {
            FragmentExecutor.FragmentResult result =
                    fragmentExecutor.execute(
                            request.getQueryId(),
                            request.getFragmentId(),
                            request.getSerializedPlan(),
                            request.getIndexName(),
                            request.getShardId(),
                            request.getSettings());
            listener.onResponse(
                    new ShardExecutionResponse(
                            result.getQueryId(),
                            result.getFragmentId(),
                            ShardExecutionResponse.Status.valueOf(result.getStatus().name()),
                            result.getResultRows(),
                            result.isHasMore(),
                            result.getRowsProcessed(),
                            result.getExecutionTimeMs(),
                            result.getErrorMessage(),
                            transportService.getLocalNode().getId()));
        } catch (Exception e) {
            listener.onResponse(
                    new ShardExecutionResponse(
                            request.getQueryId(),
                            request.getFragmentId(),
                            ShardExecutionResponse.Status.FAILED,
                            null,
                            false,
                            0L,
                            0L,
                            e.getMessage(),
                            transportService.getLocalNode().getId()));
        }
    }
}
