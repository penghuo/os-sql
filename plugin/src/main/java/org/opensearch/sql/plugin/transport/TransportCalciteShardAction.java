/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.plugin.transport;

import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.HandledTransportAction;
import org.opensearch.common.inject.Inject;
import org.opensearch.core.action.ActionListener;
import org.opensearch.tasks.Task;
import org.opensearch.transport.TransportService;

/**
 * Transport action handler that receives a serialized Calcite shard plan and executes it locally.
 * Delegates actual plan execution to ShardCalciteRuntime (T9).
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
      // TODO: Integrate with ShardCalciteRuntime (T9) to execute the plan.
      // ShardCalciteRuntime will deserialize the planJson, bind to the local shard,
      // execute the plan fragment, and return typed rows.
      //
      // Placeholder: once T9 is available, replace with:
      //   ShardCalciteRuntime runtime = ...;
      //   CalciteShardResponse response = runtime.execute(
      //       request.getPlanJson(), request.getIndexName(), request.getShardId());
      //   listener.onResponse(response);

      listener.onFailure(
          new UnsupportedOperationException(
              "ShardCalciteRuntime (T9) not yet integrated"));
    } catch (Exception e) {
      listener.onResponse(new CalciteShardResponse(e));
    }
  }
}
