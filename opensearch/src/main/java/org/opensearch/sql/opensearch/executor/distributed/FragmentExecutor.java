/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.executor.distributed;

import java.util.Map;
import lombok.Data;

/**
 * Executes a plan fragment on a data node. Responsible for deserializing the plan, setting up the
 * execution context, and running the fragment against local shard data.
 */
public class FragmentExecutor {

    /** Result of a fragment execution. */
    @Data
    public static class FragmentResult {
        /** Execution status. */
        public enum Status {
            SUCCESS,
            FAILED,
            CANCELLED
        }

        private final String queryId;
        private final int fragmentId;
        private final Status status;
        private final byte[] resultRows;
        private final boolean hasMore;
        private final long rowsProcessed;
        private final long executionTimeMs;
        private final String errorMessage;
    }

    /**
     * Execute a plan fragment on a specific shard.
     *
     * @param queryId the query identifier
     * @param fragmentId the fragment identifier
     * @param serializedPlan the serialized physical plan
     * @param indexName the target index
     * @param shardId the target shard
     * @param settings execution settings
     * @return the fragment execution result
     */
    public FragmentResult execute(
            String queryId,
            int fragmentId,
            byte[] serializedPlan,
            String indexName,
            int shardId,
            Map<String, String> settings) {
        long startTime = System.currentTimeMillis();
        try {
            // For now, return empty result since actual plan deserialization
            // requires the full OpenSearch query infrastructure.
            // This will be filled in when the shard-level execution pipeline is complete.
            long executionTimeMs = System.currentTimeMillis() - startTime;
            return new FragmentResult(
                    queryId,
                    fragmentId,
                    FragmentResult.Status.SUCCESS,
                    new byte[0],
                    false,
                    0L,
                    executionTimeMs,
                    null);
        } catch (Exception e) {
            long executionTimeMs = System.currentTimeMillis() - startTime;
            return new FragmentResult(
                    queryId,
                    fragmentId,
                    FragmentResult.Status.FAILED,
                    null,
                    false,
                    0L,
                    executionTimeMs,
                    e.getMessage());
        }
    }
}
