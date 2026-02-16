/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.executor.distributed;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.opensearch.sql.data.model.ExprIntegerValue;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.planner.physical.distributed.ExchangeBuffer;

class ExchangeServiceTest {

    private ExchangeService service;

    @BeforeEach
    void setUp() {
        service = new ExchangeService();
    }

    @Test
    @DisplayName("createConsumerBuffer creates a usable buffer")
    void createConsumerBuffer() {
        ExchangeBuffer buffer = service.createConsumerBuffer("q1", 0, 1);
        assertNotNull(buffer);
        assertTrue(buffer.hasNext()); // source not yet complete
    }

    @Test
    @DisplayName("feedResults delivers data to the buffer")
    void feedResultsDeliversData() {
        ExchangeBuffer buffer = service.createConsumerBuffer("q1", 0, 1);

        ExprValue val = new ExprIntegerValue(42);
        assertTrue(service.feedResults("q1", 0, List.of(val)));

        assertEquals(val, buffer.next());
    }

    @Test
    @DisplayName("feedResults returns false for unknown queryId")
    void feedResultsReturnsFalseForUnknownQuery() {
        assertFalse(service.feedResults("unknown", 0, List.of(new ExprIntegerValue(1))));
    }

    @Test
    @DisplayName("feedResults returns false for unknown fragmentId")
    void feedResultsReturnsFalseForUnknownFragment() {
        service.createConsumerBuffer("q1", 0, 1);
        assertFalse(service.feedResults("q1", 999, List.of(new ExprIntegerValue(1))));
    }

    @Test
    @DisplayName("markAllSourcesComplete signals buffer exhaustion")
    void markAllSourcesComplete() {
        ExchangeBuffer buffer = service.createConsumerBuffer("q1", 0, 1);
        assertTrue(buffer.hasNext());

        service.markAllSourcesComplete("q1", 0);
        assertFalse(buffer.hasNext());
    }

    @Test
    @DisplayName("cleanup closes buffers and rejects further feeds")
    void cleanupClosesBuffers() {
        service.createConsumerBuffer("q1", 0, 1);
        service.createConsumerBuffer("q1", 1, 2);

        service.cleanup("q1");

        assertFalse(service.feedResults("q1", 0, List.of(new ExprIntegerValue(1))));
        assertFalse(service.feedResults("q1", 1, List.of(new ExprIntegerValue(2))));
    }

    @Test
    @DisplayName("Multiple queries are isolated")
    void multipleQueriesIsolated() {
        ExchangeBuffer buf1 = service.createConsumerBuffer("q1", 0, 1);
        ExchangeBuffer buf2 = service.createConsumerBuffer("q2", 0, 1);

        service.feedResults("q1", 0, List.of(new ExprIntegerValue(10)));
        service.feedResults("q2", 0, List.of(new ExprIntegerValue(20)));

        assertEquals(new ExprIntegerValue(10), buf1.next());
        assertEquals(new ExprIntegerValue(20), buf2.next());

        service.cleanup("q1");
        // q2 should still work
        assertTrue(service.feedResults("q2", 0, List.of(new ExprIntegerValue(30))));
    }
}
