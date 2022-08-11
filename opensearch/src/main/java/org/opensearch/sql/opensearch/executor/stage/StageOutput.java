/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.executor.stage;

import java.util.ArrayList;
import java.util.List;
import lombok.RequiredArgsConstructor;
import org.opensearch.sql.common.response.ResponseListener;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.executor.ExecutionEngine;

public class StageOutput {
  private final List<ResponseListener<ExecutionEngine.QueryResponse>> listeners;
  private final ExecutionEngine.Schema schema;

  public StageOutput(ExecutionEngine.Schema schema) {
    this.schema = schema;
    this.listeners = new ArrayList<>();
  }


  public void consume(List<ExprValue> result) {
    ExecutionEngine.QueryResponse response = new ExecutionEngine.QueryResponse(schema, result);
    listeners.forEach(listener -> listener.onResponse(response));
  }

  public void addListener(ResponseListener<ExecutionEngine.QueryResponse> listener) {
    listeners.add(listener);
  }
}
