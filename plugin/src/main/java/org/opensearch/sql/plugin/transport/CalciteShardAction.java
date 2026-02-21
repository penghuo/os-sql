/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.plugin.transport;

import org.opensearch.action.ActionType;

/** Action type for executing a Calcite plan fragment on a specific shard. */
public class CalciteShardAction extends ActionType<CalciteShardResponse> {

  public static final String NAME = "indices:data/read/opensearch/calcite/shard";
  public static final CalciteShardAction INSTANCE = new CalciteShardAction();

  private CalciteShardAction() {
    super(NAME, CalciteShardResponse::new);
  }
}
