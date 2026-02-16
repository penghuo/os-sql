/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.plugin.transport.distributed;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import org.junit.Ignore;
import org.junit.Test;

@Ignore("Conflicts with shadow Jar solution of calcite")
public class QueryCancelRequestTest {

    @Test
    public void testGetters() {
        QueryCancelRequest request = new QueryCancelRequest("q1", "timeout exceeded");

        assertEquals("q1", request.getQueryId());
        assertEquals("timeout exceeded", request.getReason());
    }

    @Test
    public void testNullReason() {
        QueryCancelRequest request = new QueryCancelRequest("q1", null);

        assertEquals("q1", request.getQueryId());
        assertNull(request.getReason());
    }

    @Test
    public void testValidateReturnsNull() {
        QueryCancelRequest request = new QueryCancelRequest("q1", "reason");
        assertNull(request.validate());
    }
}
