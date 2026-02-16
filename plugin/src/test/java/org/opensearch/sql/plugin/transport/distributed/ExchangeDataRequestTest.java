/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.plugin.transport.distributed;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import org.junit.Ignore;
import org.junit.Test;

@Ignore("Conflicts with shadow Jar solution of calcite")
public class ExchangeDataRequestTest {

    @Test
    public void testGetters() {
        byte[] rows = new byte[] {1, 2, 3, 4};
        ExchangeDataRequest request = new ExchangeDataRequest("q1", 0, 1, 3, rows, true, 42L);

        assertEquals("q1", request.getQueryId());
        assertEquals(0, request.getSourceFragmentId());
        assertEquals(1, request.getTargetFragmentId());
        assertEquals(3, request.getPartitionId());
        assertArrayEquals(rows, request.getRows());
        assertTrue(request.isLastBatch());
        assertEquals(42L, request.getSequenceNumber());
    }

    @Test
    public void testValidateReturnsNull() {
        ExchangeDataRequest request =
                new ExchangeDataRequest("q1", 0, 1, 0, new byte[0], false, 0L);
        assertNull(request.validate());
    }
}
