/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.executor.distributed;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class DistributedQueryContextTest {

    @Test
    @DisplayName("Initial state is PLANNING")
    void initialState() {
        DistributedQueryContext ctx = new DistributedQueryContext("q1", 3);
        assertEquals(DistributedQueryContext.QueryState.PLANNING, ctx.getState());
        assertEquals("q1", ctx.getQueryId());
        assertEquals(3, ctx.getPendingShards());
        assertFalse(ctx.isCancelled());
    }

    @Test
    @DisplayName("setState transitions forward")
    void setStateForward() {
        DistributedQueryContext ctx = new DistributedQueryContext("q1", 1);

        ctx.setState(DistributedQueryContext.QueryState.DISPATCHING);
        assertEquals(DistributedQueryContext.QueryState.DISPATCHING, ctx.getState());

        ctx.setState(DistributedQueryContext.QueryState.EXECUTING);
        assertEquals(DistributedQueryContext.QueryState.EXECUTING, ctx.getState());
    }

    @Test
    @DisplayName("onShardComplete decrements pending and transitions to COMPLETE")
    void onShardComplete() {
        DistributedQueryContext ctx = new DistributedQueryContext("q1", 3);
        ctx.setState(DistributedQueryContext.QueryState.EXECUTING);

        ctx.onShardComplete();
        assertEquals(2, ctx.getPendingShards());
        assertEquals(DistributedQueryContext.QueryState.EXECUTING, ctx.getState());

        ctx.onShardComplete();
        assertEquals(1, ctx.getPendingShards());

        ctx.onShardComplete();
        assertEquals(0, ctx.getPendingShards());
        assertEquals(DistributedQueryContext.QueryState.COMPLETE, ctx.getState());
    }

    @Test
    @DisplayName("onShardFailed transitions to FAILED")
    void onShardFailed() {
        DistributedQueryContext ctx = new DistributedQueryContext("q1", 3);
        ctx.setState(DistributedQueryContext.QueryState.EXECUTING);

        ctx.onShardFailed("shard 0 timeout");
        assertEquals(DistributedQueryContext.QueryState.FAILED, ctx.getState());
    }

    @Test
    @DisplayName("cancel sets cancelled flag and CANCELLED state")
    void cancel() {
        DistributedQueryContext ctx = new DistributedQueryContext("q1", 3);
        ctx.setState(DistributedQueryContext.QueryState.EXECUTING);

        ctx.cancel("user requested");
        assertTrue(ctx.isCancelled());
        assertEquals(DistributedQueryContext.QueryState.CANCELLED, ctx.getState());
    }

    @Test
    @DisplayName("complete sets COMPLETE state")
    void complete() {
        DistributedQueryContext ctx = new DistributedQueryContext("q1", 1);
        ctx.setState(DistributedQueryContext.QueryState.EXECUTING);

        ctx.complete();
        assertEquals(DistributedQueryContext.QueryState.COMPLETE, ctx.getState());
    }

    @Test
    @DisplayName("fail sets FAILED state")
    void fail() {
        DistributedQueryContext ctx = new DistributedQueryContext("q1", 1);
        ctx.fail("internal error");
        assertEquals(DistributedQueryContext.QueryState.FAILED, ctx.getState());
    }
}
