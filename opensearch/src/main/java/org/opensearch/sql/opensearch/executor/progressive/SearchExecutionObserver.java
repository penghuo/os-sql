/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.executor.progressive;

import org.apache.lucene.search.TotalHits;
import org.opensearch.search.aggregations.InternalAggregations;

/** Receives source execution events produced by one OpenSearch search request. */
public interface SearchExecutionObserver {
  SearchExecutionObserver NOOP = new SearchExecutionObserver() {};

  default boolean needsPartialReduces() {
    return false;
  }

  default void onPartialReduce(
      TotalHits totalHits, InternalAggregations aggregations, int reducePhase) {}

  default void onFinalReduce(
      TotalHits totalHits, InternalAggregations aggregations, int reducePhase) {}
}
