/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.plugin.transport.distributed;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import java.util.Map;
import org.junit.Ignore;
import org.junit.Test;

@Ignore("Conflicts with shadow Jar solution of calcite")
public class ShardExecutionRequestTest {

    @Test
    public void testGetters() {
        byte[] plan = new byte[] {1, 2, 3};
        Map<String, String> settings = Map.of("timeout", "30s");
        ShardExecutionRequest request =
                new ShardExecutionRequest("q1", 0, plan, "test-index", 5, settings);

        assertEquals("q1", request.getQueryId());
        assertEquals(0, request.getFragmentId());
        assertArrayEquals(plan, request.getSerializedPlan());
        assertEquals("test-index", request.getIndexName());
        assertEquals(5, request.getShardId());
        assertEquals(settings, request.getSettings());
    }

    @Test
    public void testValidateReturnsNull() {
        ShardExecutionRequest request =
                new ShardExecutionRequest("q1", 0, new byte[0], "idx", 0, Map.of());
        assertNull(request.validate());
    }
}
