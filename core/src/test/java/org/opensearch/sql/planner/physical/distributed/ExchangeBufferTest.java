/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.planner.physical.distributed;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.opensearch.sql.data.model.ExprIntegerValue;
import org.opensearch.sql.data.model.ExprValue;

class ExchangeBufferTest {

    @Test
    @DisplayName("Basic offer and consume lifecycle")
    void offerAndConsume() {
        ExchangeBuffer buffer = new ExchangeBuffer(1024 * 1024, 1);

        ExprValue val1 = new ExprIntegerValue(1);
        ExprValue val2 = new ExprIntegerValue(2);
        assertTrue(buffer.offer(List.of(val1, val2)));
        assertTrue(buffer.hasNext());
        assertEquals(val1, buffer.next());
        assertEquals(val2, buffer.next());

        // Queue is empty but source not complete, so hasNext is true
        assertTrue(buffer.hasNext());

        buffer.markSourceComplete(0);
        assertFalse(buffer.hasNext());
    }

    @Test
    @DisplayName("NoSuchElementException when buffer exhausted")
    void throwsWhenExhausted() {
        ExchangeBuffer buffer = new ExchangeBuffer(1024 * 1024, 1);
        buffer.markAllComplete();

        assertFalse(buffer.hasNext());
        assertThrows(NoSuchElementException.class, buffer::next);
    }

    @Test
    @DisplayName("Backpressure rejects offers when capacity exceeded")
    void backpressureRejectsOffers() {
        // Capacity for roughly 2 rows (2 * 256 bytes = 512)
        ExchangeBuffer buffer = new ExchangeBuffer(512, 1);

        ExprValue val = new ExprIntegerValue(1);
        assertTrue(buffer.offer(List.of(val, val))); // 2 * 256 = 512, exactly at capacity
        assertFalse(buffer.offer(List.of(val))); // Exceeds capacity
    }

    @Test
    @DisplayName("markSourceComplete tracks multiple sources")
    void markSourceCompleteTracksMultipleSources() {
        ExchangeBuffer buffer = new ExchangeBuffer(1024 * 1024, 3);

        assertTrue(buffer.hasNext()); // 0 of 3 sources complete

        buffer.markSourceComplete(0);
        assertTrue(buffer.hasNext()); // 1 of 3

        buffer.markSourceComplete(1);
        assertTrue(buffer.hasNext()); // 2 of 3

        buffer.markSourceComplete(2);
        assertFalse(buffer.hasNext()); // 3 of 3
    }

    @Test
    @DisplayName("markAllComplete makes buffer immediately exhausted")
    void markAllCompleteExhaustsBuffer() {
        ExchangeBuffer buffer = new ExchangeBuffer(1024 * 1024, 5);
        assertTrue(buffer.hasNext());

        buffer.markAllComplete();
        assertFalse(buffer.hasNext());
    }

    @Test
    @DisplayName("close rejects subsequent offers")
    void closeRejectsOffers() {
        ExchangeBuffer buffer = new ExchangeBuffer(1024 * 1024, 1);
        buffer.close();

        assertFalse(buffer.offer(List.of(new ExprIntegerValue(1))));
    }

    @Test
    @DisplayName("availableBytes decreases as data is added")
    void availableBytesDecreases() {
        ExchangeBuffer buffer = new ExchangeBuffer(1024, 1);
        assertEquals(1024, buffer.availableBytes());

        buffer.offer(List.of(new ExprIntegerValue(1))); // ~256 bytes
        assertEquals(1024 - 256, buffer.availableBytes());
    }

    @Test
    @DisplayName("Concurrent producer and consumer")
    void concurrentProducerConsumer() throws InterruptedException {
        ExchangeBuffer buffer = new ExchangeBuffer(1024 * 1024, 1);
        int totalRows = 100;
        CountDownLatch producerDone = new CountDownLatch(1);
        AtomicInteger consumed = new AtomicInteger(0);

        // Producer thread
        Thread producer =
                new Thread(
                        () -> {
                            for (int i = 0; i < totalRows; i++) {
                                buffer.offer(List.of(new ExprIntegerValue(i)));
                            }
                            buffer.markSourceComplete(0);
                            producerDone.countDown();
                        });

        // Consumer runs on main thread
        producer.start();

        List<ExprValue> results = new ArrayList<>();
        while (buffer.hasNext()) {
            try {
                results.add(buffer.next());
            } catch (NoSuchElementException e) {
                // Race condition: hasNext was true but data consumed between check
                break;
            }
        }

        producerDone.await();
        assertEquals(totalRows, results.size());
    }
}
