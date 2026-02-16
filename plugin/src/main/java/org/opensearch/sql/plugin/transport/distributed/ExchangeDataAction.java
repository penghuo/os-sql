/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.plugin.transport.distributed;

import org.opensearch.action.ActionType;

/** Action type for exchanging data between plan fragments. */
public class ExchangeDataAction extends ActionType<ExchangeDataResponse> {
    public static final String NAME = "cluster:internal/ppl/exchange/data";
    public static final ExchangeDataAction INSTANCE = new ExchangeDataAction();

    private ExchangeDataAction() {
        super(NAME, ExchangeDataResponse::new);
    }
}
