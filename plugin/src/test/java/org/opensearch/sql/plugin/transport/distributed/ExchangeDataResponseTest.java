/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.plugin.transport.distributed;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import org.junit.Ignore;
import org.junit.Test;

@Ignore("Conflicts with shadow Jar solution of calcite")
public class ExchangeDataResponseTest {

    @Test
    public void testGetters() {
        ExchangeDataResponse response = new ExchangeDataResponse(true, 1024 * 1024L, 5L);

        assertTrue(response.isAccepted());
        assertEquals(1024 * 1024L, response.getAvailableBufferBytes());
        assertEquals(5L, response.getAcknowledgedSequence());
    }
}
