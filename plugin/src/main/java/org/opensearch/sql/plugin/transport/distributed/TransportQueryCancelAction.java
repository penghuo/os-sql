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
import org.opensearch.sql.opensearch.executor.distributed.ExchangeService;
import org.opensearch.tasks.Task;
import org.opensearch.transport.TransportService;

/** Transport action handler for cancelling distributed queries. */
public class TransportQueryCancelAction
        extends HandledTransportAction<ActionRequest, QueryCancelResponse> {

    private final ExchangeService exchangeService;

    @Inject
    public TransportQueryCancelAction(
            TransportService transportService,
            ActionFilters actionFilters,
            ExchangeService exchangeService) {
        super(
                QueryCancelAction.NAME,
                transportService,
                actionFilters,
                QueryCancelRequest::new);
        this.exchangeService = exchangeService;
    }

    @Override
    protected void doExecute(
            Task task,
            ActionRequest actionRequest,
            ActionListener<QueryCancelResponse> listener) {
        QueryCancelRequest request = (QueryCancelRequest) actionRequest;
        try {
            exchangeService.cleanup(request.getQueryId());
            listener.onResponse(new QueryCancelResponse(true, 0));
        } catch (Exception e) {
            listener.onResponse(new QueryCancelResponse(false, 0));
        }
    }
}
