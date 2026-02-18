/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.executor;

/**
 * Callback interface for collecting distributed engine metrics. Implemented in the
 * distributed-engine module and set on QueryService via dependency injection.
 */
public interface DistributedEngineMetricsCollector {

  /** Record that a query entered the distributed engine path. */
  void recordQueryTotal();

  /** Record that a query was successfully executed through the distributed engine. */
  void recordQueryDistributed();

  /** Record an expected fallback (unsupported pattern). */
  void recordFallbackExpected();

  /** Record an unexpected fallback (error during distributed execution). */
  void recordFallbackError();

  /** No-op implementation for when distributed engine is not available. */
  DistributedEngineMetricsCollector NOOP =
      new DistributedEngineMetricsCollector() {
        @Override
        public void recordQueryTotal() {}

        @Override
        public void recordQueryDistributed() {}

        @Override
        public void recordFallbackExpected() {}

        @Override
        public void recordFallbackError() {}
      };
}
