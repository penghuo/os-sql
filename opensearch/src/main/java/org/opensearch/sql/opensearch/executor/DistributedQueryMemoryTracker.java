/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.executor;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.core.common.breaker.CircuitBreaker;
import org.opensearch.core.common.breaker.CircuitBreakingException;
import org.opensearch.core.indices.breaker.CircuitBreakerService;

/**
 * Tracks memory usage during distributed query execution and integrates with OpenSearch's circuit
 * breaker to prevent OOM conditions.
 *
 * <p>Each shard result received during distributed execution is estimated for memory usage and
 * tracked against the REQUEST circuit breaker. If the cumulative memory exceeds the breaker limit, a
 * {@link CircuitBreakingException} is thrown to cancel the query.
 */
public class DistributedQueryMemoryTracker implements AutoCloseable {
    private static final Logger LOG = LogManager.getLogger(DistributedQueryMemoryTracker.class);

    /** Average estimated bytes per field value in a result row. */
    private static final long ESTIMATED_BYTES_PER_FIELD = 64L;

    private final CircuitBreaker breaker;
    private long trackedBytes = 0;

    public DistributedQueryMemoryTracker(CircuitBreakerService circuitBreakerService) {
        this.breaker = circuitBreakerService.getBreaker(CircuitBreaker.REQUEST);
    }

    /**
     * Tracks memory for a shard result based on estimated row and column counts.
     *
     * @param rowCount number of rows in the shard result
     * @param columnCount number of columns in each row
     * @param extraBytes additional bytes to track (e.g., binary field sizes)
     * @throws CircuitBreakingException if memory limit is exceeded
     */
    public void trackShardResult(int rowCount, int columnCount, long extraBytes) {
        long estimatedBytes = (long) rowCount * columnCount * ESTIMATED_BYTES_PER_FIELD + extraBytes;
        trackedBytes += estimatedBytes;
        breaker.addEstimateBytesAndMaybeBreak(estimatedBytes, "DQE shard results");
        LOG.debug(
                "Tracked {} bytes for shard result (total: {} bytes)", estimatedBytes, trackedBytes);
    }

    /** Releases all tracked memory back to the circuit breaker. */
    public void release() {
        if (trackedBytes > 0) {
            breaker.addWithoutBreaking(-trackedBytes);
            LOG.debug("Released {} bytes from circuit breaker", trackedBytes);
            trackedBytes = 0;
        }
    }

    @Override
    public void close() {
        release();
    }

    /** Returns the total bytes currently tracked. */
    public long getTrackedBytes() {
        return trackedBytes;
    }
}
