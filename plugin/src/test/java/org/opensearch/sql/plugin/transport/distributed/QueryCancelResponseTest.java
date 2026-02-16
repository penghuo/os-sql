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
public class QueryCancelResponseTest {

    @Test
    public void testGetters() {
        QueryCancelResponse response = new QueryCancelResponse(true, 5);

        assertTrue(response.isAcknowledged());
        assertEquals(5, response.getFragmentsCancelled());
    }
}
