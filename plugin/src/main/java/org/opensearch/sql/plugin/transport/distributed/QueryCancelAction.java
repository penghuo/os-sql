/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.plugin.transport.distributed;

import org.opensearch.action.ActionType;

/** Action type for cancelling a distributed query. */
public class QueryCancelAction extends ActionType<QueryCancelResponse> {
    public static final String NAME = "cluster:internal/ppl/query/cancel";
    public static final QueryCancelAction INSTANCE = new QueryCancelAction();

    private QueryCancelAction() {
        super(NAME, QueryCancelResponse::new);
    }
}
