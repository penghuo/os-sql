/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.plugin.transport;

import org.opensearch.core.action.ActionListener;
import org.opensearch.sql.opensearch.dqe.ShardQueryDispatcher;
import org.opensearch.transport.client.node.NodeClient;

/**
 * Plugin-module implementation of {@link ShardQueryDispatcher} that dispatches shard plan fragments
 * via the OpenSearch transport layer using {@link CalciteShardAction}.
 */
public class CalciteShardQueryDispatcher implements ShardQueryDispatcher {

  private final NodeClient client;

  public CalciteShardQueryDispatcher(NodeClient client) {
    this.client = client;
  }

  @Override
  public void dispatch(
      String planJson, String indexName, int shardId, ActionListener<ShardResponse> listener) {
    CalciteShardRequest request = new CalciteShardRequest(planJson, indexName, shardId);
    client.execute(
        CalciteShardAction.INSTANCE,
        request,
        new ActionListener<CalciteShardResponse>() {
          @Override
          public void onResponse(CalciteShardResponse response) {
            if (response.getError() != null) {
              listener.onResponse(new ShardResponse(response.getError()));
            } else {
              listener.onResponse(new ShardResponse(response.getRows()));
            }
          }

          @Override
          public void onFailure(Exception e) {
            listener.onFailure(e);
          }
        });
  }
}
