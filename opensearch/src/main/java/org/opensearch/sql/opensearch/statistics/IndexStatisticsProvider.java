/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.statistics;

import org.opensearch.sql.opensearch.client.OpenSearchClient;

/**
 * Provides index statistics for cost-based query optimization decisions. Uses the OpenSearch _stats
 * API to estimate index sizes and document counts.
 */
public class IndexStatisticsProvider {

    /** Default broadcast threshold: indices with fewer docs than this are broadcast. */
    public static final long DEFAULT_BROADCAST_THRESHOLD = 10_000L;

    private final OpenSearchClient client;

    public IndexStatisticsProvider(OpenSearchClient client) {
        this.client = client;
    }

    /**
     * Estimates the number of documents in the given index. Uses the _stats API to get the document
     * count.
     *
     * @param indexName the index name to estimate
     * @return estimated document count, or {@link Long#MAX_VALUE} on failure
     */
    public long estimateRowCount(String indexName) {
        try {
            // TODO: Use the client to call _stats API for actual doc count
            // For now, return a conservative default that won't trigger broadcast
            return Long.MAX_VALUE;
        } catch (Exception e) {
            return Long.MAX_VALUE; // Conservative fallback
        }
    }

    /**
     * Estimates the size in bytes of the given index.
     *
     * @param indexName the index name to estimate
     * @return estimated size in bytes, or {@link Long#MAX_VALUE} on failure
     */
    public long estimateSizeBytes(String indexName) {
        try {
            // TODO: Use _stats API for store.size_in_bytes
            return Long.MAX_VALUE;
        } catch (Exception e) {
            return Long.MAX_VALUE; // Conservative fallback
        }
    }

    /**
     * Determines if the given index is small enough for broadcast join.
     *
     * @param indexName the index name to check
     * @return true if the index has fewer documents than {@link #DEFAULT_BROADCAST_THRESHOLD}
     */
    public boolean isSmallEnoughForBroadcast(String indexName) {
        return estimateRowCount(indexName) < DEFAULT_BROADCAST_THRESHOLD;
    }
}
