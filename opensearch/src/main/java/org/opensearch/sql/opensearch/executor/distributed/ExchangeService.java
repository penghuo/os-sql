/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.executor.distributed;

import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.planner.physical.distributed.ExchangeBuffer;

/**
 * Manages ExchangeBuffer instances per query and fragment. Provides buffer lifecycle management for
 * the distributed execution engine.
 */
public class ExchangeService {
    private static final long DEFAULT_BUFFER_CAPACITY_BYTES = 32 * 1024 * 1024; // 32 MB

    private final ConcurrentMap<String, ConcurrentMap<Integer, ExchangeBuffer>> buffers;

    public ExchangeService() {
        this.buffers = new ConcurrentHashMap<>();
    }

    /**
     * Creates a consumer-side exchange buffer for receiving data from upstream fragments.
     *
     * @param queryId the query identifier
     * @param fragmentId the fragment that will consume data
     * @param totalSources the number of upstream sources that will feed into this buffer
     * @return the created ExchangeBuffer
     */
    public ExchangeBuffer createConsumerBuffer(String queryId, int fragmentId, int totalSources) {
        ExchangeBuffer buffer = new ExchangeBuffer(DEFAULT_BUFFER_CAPACITY_BYTES, totalSources);
        buffers
                .computeIfAbsent(queryId, k -> new ConcurrentHashMap<>())
                .put(fragmentId, buffer);
        return buffer;
    }

    /**
     * Feeds result rows into an exchange buffer for a specific query/fragment.
     *
     * @param queryId the query identifier
     * @param fragmentId the target fragment's buffer
     * @param rows the data to feed
     * @return true if the data was accepted, false if the buffer is full or doesn't exist
     */
    public boolean feedResults(String queryId, int fragmentId, List<ExprValue> rows) {
        ConcurrentMap<Integer, ExchangeBuffer> queryBuffers = buffers.get(queryId);
        if (queryBuffers == null) {
            return false;
        }
        ExchangeBuffer buffer = queryBuffers.get(fragmentId);
        if (buffer == null) {
            return false;
        }
        return buffer.offer(rows);
    }

    /**
     * Marks all upstream sources as complete for a specific query/fragment buffer.
     *
     * @param queryId the query identifier
     * @param fragmentId the target fragment's buffer
     */
    public void markAllSourcesComplete(String queryId, int fragmentId) {
        ConcurrentMap<Integer, ExchangeBuffer> queryBuffers = buffers.get(queryId);
        if (queryBuffers != null) {
            ExchangeBuffer buffer = queryBuffers.get(fragmentId);
            if (buffer != null) {
                buffer.markAllComplete();
            }
        }
    }

    /**
     * Cleans up all exchange buffers for a query.
     *
     * @param queryId the query identifier
     */
    public void cleanup(String queryId) {
        ConcurrentMap<Integer, ExchangeBuffer> queryBuffers = buffers.remove(queryId);
        if (queryBuffers != null) {
            queryBuffers.values().forEach(ExchangeBuffer::close);
        }
    }
}
