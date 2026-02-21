/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.dqe;

import java.util.List;
import org.opensearch.core.action.ActionListener;

/**
 * Abstraction for dispatching a serialized Calcite shard plan to a specific shard and receiving
 * typed rows back. The plugin module provides the implementation that wires through transport
 * (CalciteShardAction/Request/Response). This avoids a circular dependency between the opensearch
 * and plugin modules.
 */
public interface ShardQueryDispatcher {

  /**
   * Dispatch a serialized plan to a specific shard for execution.
   *
   * @param planJson the serialized Calcite plan JSON
   * @param indexName the OpenSearch index name
   * @param shardId the target shard ID
   * @param listener callback with shard results or error
   */
  void dispatch(
      String planJson, String indexName, int shardId, ActionListener<ShardResponse> listener);

  /** Response from a shard execution. */
  class ShardResponse {
    private final List<Object[]> rows;
    private final Exception error;

    /** Construct a successful response. */
    public ShardResponse(List<Object[]> rows) {
      this.rows = rows;
      this.error = null;
    }

    /** Construct an error response. */
    public ShardResponse(Exception error) {
      this.rows = List.of();
      this.error = error;
    }

    public List<Object[]> getRows() {
      return rows;
    }

    public Exception getError() {
      return error;
    }

    public boolean hasError() {
      return error != null;
    }
  }
}
