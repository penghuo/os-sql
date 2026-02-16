/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.executor.distributed;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import lombok.Getter;

/** Tracks the lifecycle state of a distributed query across all participating shards. */
public class DistributedQueryContext {

    /** Possible states of a distributed query. */
    public enum QueryState {
        PLANNING,
        DISPATCHING,
        EXECUTING,
        COMPLETE,
        FAILED,
        CANCELLED
    }

    @Getter private final String queryId;
    private final AtomicReference<QueryState> state;
    private final AtomicInteger pendingShards;
    private final AtomicBoolean cancelled;

    public DistributedQueryContext(String queryId, int totalShards) {
        this.queryId = queryId;
        this.state = new AtomicReference<>(QueryState.PLANNING);
        this.pendingShards = new AtomicInteger(totalShards);
        this.cancelled = new AtomicBoolean(false);
    }

    /** Returns the current query state. */
    public QueryState getState() {
        return state.get();
    }

    /** Returns whether the query has been cancelled. */
    public boolean isCancelled() {
        return cancelled.get();
    }

    /** Returns the number of shards still pending completion. */
    public int getPendingShards() {
        return pendingShards.get();
    }

    /**
     * Transition the query state. Only allows forward transitions (no going back to earlier
     * states).
     */
    public boolean setState(QueryState newState) {
        return state.getAndUpdate(
                        current -> {
                            if (current.ordinal() < newState.ordinal()) {
                                return newState;
                            }
                            return current;
                        })
                != newState;
    }

    /** Called when a shard completes successfully. Transitions to COMPLETE if all shards done. */
    public void onShardComplete() {
        int remaining = pendingShards.decrementAndGet();
        if (remaining <= 0) {
            state.compareAndSet(QueryState.EXECUTING, QueryState.COMPLETE);
        }
    }

    /** Called when a shard fails. Transitions the query to FAILED state. */
    public void onShardFailed(String errorMessage) {
        state.set(QueryState.FAILED);
    }

    /** Marks the query as complete. */
    public void complete() {
        state.set(QueryState.COMPLETE);
    }

    /** Marks the query as failed. */
    public void fail(String reason) {
        state.set(QueryState.FAILED);
    }

    /** Cancels the query. */
    public void cancel(String reason) {
        cancelled.set(true);
        state.set(QueryState.CANCELLED);
    }
}
