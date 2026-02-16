/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.plugin.transport.distributed;

import org.opensearch.action.ActionType;

/** Action type for executing a plan fragment on a specific shard. */
public class ShardExecutionAction extends ActionType<ShardExecutionResponse> {
    public static final String NAME = "cluster:internal/ppl/shard/execute";
    public static final ShardExecutionAction INSTANCE = new ShardExecutionAction();

    private ShardExecutionAction() {
        super(NAME, ShardExecutionResponse::new);
    }
}
