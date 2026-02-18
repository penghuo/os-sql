/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.distributed.transport;

import org.opensearch.action.ActionType;

/**
 * Action type for executing a plan fragment on a data node. Registered in the plugin's getActions()
 * to allow the coordinator to dispatch shard-level query fragments to data nodes via
 * TransportService.
 */
public class ShardQueryAction extends ActionType<ShardQueryResponse> {

  public static final String NAME = "cluster:internal/opensearch/sql/distributed/shard_query";
  public static final ShardQueryAction INSTANCE = new ShardQueryAction();

  private ShardQueryAction() {
    super(NAME, ShardQueryResponse::new);
  }
}
