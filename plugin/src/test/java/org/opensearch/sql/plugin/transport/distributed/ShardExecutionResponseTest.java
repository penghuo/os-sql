/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.plugin.transport.distributed;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

import org.junit.Ignore;
import org.junit.Test;

@Ignore("Conflicts with shadow Jar solution of calcite")
public class ShardExecutionResponseTest {

    @Test
    public void testGetters() {
        byte[] rows = new byte[] {10, 20};
        ShardExecutionResponse response =
                new ShardExecutionResponse(
                        "q1",
                        0,
                        ShardExecutionResponse.Status.SUCCESS,
                        rows,
                        false,
                        100L,
                        50L,
                        null,
                        "node-1");

        assertEquals("q1", response.getQueryId());
        assertEquals(0, response.getFragmentId());
        assertEquals(ShardExecutionResponse.Status.SUCCESS, response.getStatus());
        assertArrayEquals(rows, response.getResultRows());
        assertFalse(response.isHasMore());
        assertEquals(100L, response.getRowsProcessed());
        assertEquals(50L, response.getExecutionTimeMs());
        assertEquals(null, response.getErrorMessage());
        assertEquals("node-1", response.getExecutingNodeId());
    }

    @Test
    public void testStatusEnum() {
        assertEquals(3, ShardExecutionResponse.Status.values().length);
        assertEquals(
                ShardExecutionResponse.Status.SUCCESS,
                ShardExecutionResponse.Status.valueOf("SUCCESS"));
        assertEquals(
                ShardExecutionResponse.Status.FAILED,
                ShardExecutionResponse.Status.valueOf("FAILED"));
        assertEquals(
                ShardExecutionResponse.Status.CANCELLED,
                ShardExecutionResponse.Status.valueOf("CANCELLED"));
    }
}
