/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.executor.progressive;

import java.util.List;
import org.apache.lucene.search.TotalHits;
import org.opensearch.search.aggregations.InternalAggregations;
import org.opensearch.sql.data.model.ExprValue;

/** Publishes each non-composite reduce as the current physical aggregation leaf state. */
final class AggregationStatePublisher implements SearchExecutionObserver {
  private final ReplaceStateStore store;
  private final AggregationResultMapper mapper;

  AggregationStatePublisher(ReplaceStateStore store, AggregationResultMapper mapper) {
    this.store = store;
    this.mapper = mapper;
  }

  @Override
  public boolean needsPartialReduces() {
    return true;
  }

  @Override
  public void onPartialReduce(
      TotalHits totalHits, InternalAggregations aggregations, int reducePhase) {
    publish(totalHits, aggregations);
  }

  @Override
  public void onFinalReduce(
      TotalHits totalHits, InternalAggregations aggregations, int reducePhase) {
    publish(totalHits, aggregations);
  }

  private void publish(TotalHits totalHits, InternalAggregations aggregations) {
    List<ExprValue> rows = mapper.map(totalHits, aggregations);
    if (!rows.isEmpty()) {
      store.replace(rows);
    }
  }
}
