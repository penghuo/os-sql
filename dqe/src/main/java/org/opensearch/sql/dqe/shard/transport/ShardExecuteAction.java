/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.dqe.shard.transport;

import org.opensearch.action.ActionType;

/**
 * Action type for shard-level DQE plan fragment execution. Registered with the transport layer so
 * the coordinator can dispatch serialized plan fragments to individual shards.
 */
public class ShardExecuteAction extends ActionType<ShardExecuteResponse> {

  public static final String NAME = "indices:data/read/dqe/shard/execute";
  public static final ShardExecuteAction INSTANCE = new ShardExecuteAction();

  private ShardExecuteAction() {
    super(NAME, ShardExecuteResponse::new);
  }
}
