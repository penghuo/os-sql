/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.planner.physical.distributed;

import java.util.List;
import java.util.NoSuchElementException;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import org.opensearch.sql.data.model.ExprValue;

/**
 * Thread-safe bounded buffer for exchanging data between plan fragments. Supports backpressure by
 * rejecting offers when capacity is exceeded.
 */
public class ExchangeBuffer {
    private static final long POLL_TIMEOUT_MS = 100;

    private final long capacityBytes;
    private final int totalSources;
    private final LinkedBlockingQueue<ExprValue> queue;
    private final AtomicInteger completedSources;
    private final AtomicLong usedBytes;
    private final AtomicBoolean closed;

    public ExchangeBuffer(long capacityBytes, int totalSources) {
        this.capacityBytes = capacityBytes;
        this.totalSources = totalSources;
        this.queue = new LinkedBlockingQueue<>();
        this.completedSources = new AtomicInteger(0);
        this.usedBytes = new AtomicLong(0);
        this.closed = new AtomicBoolean(false);
    }

    /**
     * Returns true if there are more rows available. Blocks until either data is available in the
     * queue or all sources have completed. Returns false only when the queue is empty and all
     * sources are done.
     */
    public boolean hasNext() {
        // Fast path: data already available
        if (!queue.isEmpty()) {
            return true;
        }
        // Fast path: all sources complete
        if (completedSources.get() >= totalSources) {
            return !queue.isEmpty();
        }
        // Slow path: wait until data arrives or all sources complete
        while (queue.isEmpty() && completedSources.get() < totalSources) {
            try {
                Thread.sleep(1);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return !queue.isEmpty();
            }
        }
        return !queue.isEmpty();
    }

    /**
     * Retrieves the next row, polling with a timeout. Throws NoSuchElementException if all sources
     * are complete and the queue is empty.
     */
    public ExprValue next() {
        while (true) {
            ExprValue value = null;
            try {
                value = queue.poll(POLL_TIMEOUT_MS, TimeUnit.MILLISECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RuntimeException("Interrupted while waiting for exchange data", e);
            }
            if (value != null) {
                return value;
            }
            if (!hasNext()) {
                throw new NoSuchElementException("Exchange buffer exhausted: all sources complete");
            }
        }
    }

    /**
     * Offers a batch of rows to the buffer. Returns false if the buffer does not have enough
     * capacity to accept the batch.
     */
    public boolean offer(List<ExprValue> rows) {
        if (closed.get()) {
            return false;
        }
        // Estimate size as number of rows (rough estimate, 1 row ~ 256 bytes)
        long estimatedBytes = (long) rows.size() * 256;
        if (usedBytes.get() + estimatedBytes > capacityBytes) {
            return false;
        }
        usedBytes.addAndGet(estimatedBytes);
        queue.addAll(rows);
        return true;
    }

    /** Marks a source as complete. */
    public void markSourceComplete(int sourceId) {
        completedSources.incrementAndGet();
    }

    /** Marks all sources as complete. */
    public void markAllComplete() {
        completedSources.set(totalSources);
    }

    /** Closes the buffer and releases resources. */
    public void close() {
        closed.set(true);
        queue.clear();
        usedBytes.set(0);
    }

    /** Returns the estimated available bytes in the buffer. */
    public long availableBytes() {
        return Math.max(0, capacityBytes - usedBytes.get());
    }
}
