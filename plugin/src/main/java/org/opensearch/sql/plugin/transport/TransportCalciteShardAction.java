/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.plugin.transport;

import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.HandledTransportAction;
import org.opensearch.common.inject.Inject;
import org.opensearch.core.action.ActionListener;
import org.opensearch.sql.opensearch.dqe.ShardCalciteRuntime;
import org.opensearch.tasks.Task;
import org.opensearch.transport.TransportService;

/**
 * Transport action handler that receives a serialized Calcite shard plan and executes it locally.
 * Delegates actual plan execution to {@link ShardCalciteRuntime}.
 */
public class TransportCalciteShardAction
    extends HandledTransportAction<CalciteShardRequest, CalciteShardResponse> {

  @Inject
  public TransportCalciteShardAction(
      TransportService transportService, ActionFilters actionFilters) {
    super(CalciteShardAction.NAME, transportService, actionFilters, CalciteShardRequest::new);
  }

  @Override
  protected void doExecute(
      Task task, CalciteShardRequest request, ActionListener<CalciteShardResponse> listener) {
    try {
      ShardCalciteRuntime runtime = new ShardCalciteRuntime();
      ShardCalciteRuntime.Result result =
          runtime.execute(request.getPlanJson(), request.getIndexName(), request.getShardId());

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
