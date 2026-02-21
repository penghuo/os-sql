/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.plugin.transport;

import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.HandledTransportAction;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.inject.Inject;
import org.opensearch.core.action.ActionListener;
import org.opensearch.sql.common.setting.Settings;
import org.opensearch.sql.opensearch.client.OpenSearchClient;
import org.opensearch.sql.opensearch.client.OpenSearchNodeClient;
import org.opensearch.sql.opensearch.dqe.ShardCalciteRuntime;
import org.opensearch.sql.opensearch.setting.OpenSearchSettings;
import org.opensearch.sql.opensearch.storage.OpenSearchIndex;
import org.opensearch.tasks.Task;
import org.opensearch.transport.TransportService;
import org.opensearch.transport.client.node.NodeClient;

/**
 * Transport action handler that receives a serialized Calcite shard plan and executes it locally.
 * Delegates actual plan execution to {@link ShardCalciteRuntime}.
 */
public class TransportCalciteShardAction
    extends HandledTransportAction<CalciteShardRequest, CalciteShardResponse> {

  private final OpenSearchClient client;
  private final Settings settings;

  @Inject
  public TransportCalciteShardAction(
      TransportService transportService,
      ActionFilters actionFilters,
      NodeClient nodeClient,
      ClusterService clusterService) {
    super(CalciteShardAction.NAME, transportService, actionFilters, CalciteShardRequest::new);
    this.client = new OpenSearchNodeClient(nodeClient);
    this.settings = new OpenSearchSettings(clusterService.getClusterSettings());
  }

  @Override
  protected void doExecute(
      Task task, CalciteShardRequest request, ActionListener<CalciteShardResponse> listener) {
    try {
      OpenSearchIndex osIndex =
          new OpenSearchIndex(client, settings, request.getIndexName());
      ShardCalciteRuntime runtime = new ShardCalciteRuntime();
      ShardCalciteRuntime.Result result =
          runtime.execute(
              request.getPlanJson(), request.getIndexName(), request.getShardId(), osIndex);

      if (result.hasError()) {
        listener.onResponse(new CalciteShardResponse(result.getError()));
      } else {
        listener.onResponse(
            new CalciteShardResponse(
                result.getRows(), result.getColumnNames(), result.getColumnTypes()));
      }
    } catch (Exception e) {
      listener.onResponse(new CalciteShardResponse(e));
    }
  }
}
