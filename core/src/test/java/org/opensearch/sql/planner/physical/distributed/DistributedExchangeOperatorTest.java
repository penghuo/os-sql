/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.planner.physical.distributed;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import java.util.NoSuchElementException;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.opensearch.sql.data.model.ExprIntegerValue;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.planner.distributed.ExchangeSpec;

class DistributedExchangeOperatorTest {

    @Test
    @DisplayName("Basic open, feed, consume lifecycle")
    void basicLifecycle() {
        DistributedExchangeOperator op =
                new DistributedExchangeOperator(ExchangeSpec.gather(), 1);
        op.open();

        ExchangeBuffer buffer = op.getBuffer();
        assertNotNull(buffer);

        ExprValue val1 = new ExprIntegerValue(10);
        ExprValue val2 = new ExprIntegerValue(20);
        buffer.offer(List.of(val1, val2));
        buffer.markSourceComplete(0);

        assertTrue(op.hasNext());
        assertEquals(val1, op.next());
        assertTrue(op.hasNext());
        assertEquals(val2, op.next());
        assertFalse(op.hasNext());

        op.close();
    }

    @Test
    @DisplayName("Empty exchange with all sources complete")
    void emptyExchange() {
        DistributedExchangeOperator op =
                new DistributedExchangeOperator(ExchangeSpec.gather(), 1);
        op.open();

        op.getBuffer().markAllComplete();
        assertFalse(op.hasNext());

        op.close();
    }

    @Test
    @DisplayName("Multiple sources must all complete before exhaustion")
    void multipleSources() {
        DistributedExchangeOperator op =
                new DistributedExchangeOperator(ExchangeSpec.gather(), 3);
        op.open();

        ExchangeBuffer buffer = op.getBuffer();

        buffer.offer(List.of(new ExprIntegerValue(1)));
        buffer.markSourceComplete(0);

        // One source complete with data, two still pending
        assertTrue(op.hasNext());
        assertEquals(new ExprIntegerValue(1), op.next());

        // Queue empty but 2 sources still pending
        assertTrue(op.hasNext());

        buffer.markSourceComplete(1);
        assertTrue(op.hasNext()); // Still 1 pending

        buffer.markSourceComplete(2);
        assertFalse(op.hasNext()); // All done

        op.close();
    }

    @Test
    @DisplayName("getChild returns empty list (exchange is leaf node)")
    void getChildReturnsEmpty() {
        DistributedExchangeOperator op =
                new DistributedExchangeOperator(ExchangeSpec.gather(), 1);
        assertTrue(op.getChild().isEmpty());
    }

    @Test
    @DisplayName("Throws NoSuchElementException when exhausted")
    void throwsWhenExhausted() {
        DistributedExchangeOperator op =
                new DistributedExchangeOperator(ExchangeSpec.gather(), 1);
        op.open();

        op.getBuffer().markAllComplete();

        assertFalse(op.hasNext());
        assertThrows(NoSuchElementException.class, op::next);

        op.close();
    }
}
