/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dqe.exchange.buffer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class BackpressureControllerTests {

  @Test
  @DisplayName("awaitCapacity succeeds when buffer has room")
  void awaitCapacitySucceedsWhenRoom() throws InterruptedException {
    BackpressureController bp = new BackpressureController(1000, 5000);
    assertTrue(bp.awaitCapacity(500));
    assertEquals(500, bp.getBufferedBytes());
  }

  @Test
  @DisplayName("awaitCapacity times out when buffer is full")
  void awaitCapacityTimesOutWhenFull() throws InterruptedException {
    BackpressureController bp = new BackpressureController(100, 200);
    assertTrue(bp.awaitCapacity(100));
    // Buffer is now full; next attempt should timeout
    long start = System.nanoTime();
    assertFalse(bp.awaitCapacity(1));
    long elapsed = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start);
    assertTrue(elapsed >= 150, "Should have waited near 200ms, got " + elapsed);
  }

  @Test
  @DisplayName("notifyConsumed frees capacity for waiting producers")
  void notifyConsumedFreesCapacity() throws Exception {
    BackpressureController bp = new BackpressureController(100, 5000);
    assertTrue(bp.awaitCapacity(100));

    // Start a producer thread that will block
    ExecutorService exec = Executors.newSingleThreadExecutor();
    Future<Boolean> result = exec.submit(() -> bp.awaitCapacity(50));

    // Give the producer time to start waiting
    Thread.sleep(100);

    // Free space
    bp.notifyConsumed(60);

    assertTrue(result.get(5, TimeUnit.SECONDS));
    exec.shutdown();
  }

  @Test
  @DisplayName("isFull returns true when at capacity")
  void isFullAtCapacity() throws InterruptedException {
    BackpressureController bp = new BackpressureController(100, 5000);
    assertFalse(bp.isFull());
    bp.awaitCapacity(100);
    assertTrue(bp.isFull());
    bp.notifyConsumed(1);
    assertFalse(bp.isFull());
  }

  @Test
  @DisplayName("abort wakes up waiting producers with false return")
  void abortWakesProducers() throws Exception {
    BackpressureController bp = new BackpressureController(100, 30_000);
    bp.awaitCapacity(100);

    ExecutorService exec = Executors.newSingleThreadExecutor();
    CountDownLatch started = new CountDownLatch(1);
    AtomicBoolean acquired = new AtomicBoolean(true);

    exec.submit(
        () -> {
          try {
            started.countDown();
            acquired.set(bp.awaitCapacity(50));
          } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
          }
        });

    started.await();
    Thread.sleep(50);
    bp.abort();
    exec.shutdown();
    exec.awaitTermination(5, TimeUnit.SECONDS);

    assertFalse(acquired.get());
    assertTrue(bp.isAborted());
  }

  @Test
  @DisplayName("awaitCapacity returns false immediately when aborted")
  void awaitCapacityReturnsFalseWhenAborted() throws InterruptedException {
    BackpressureController bp = new BackpressureController(100, 5000);
    bp.abort();
    assertFalse(bp.awaitCapacity(10));
  }

  @Test
  @DisplayName("constructor rejects zero maxBufferBytes")
  void constructorRejectsZeroMax() {
    assertThrows(IllegalArgumentException.class, () -> new BackpressureController(0, 1000));
  }

  @Test
  @DisplayName("constructor rejects zero timeout")
  void constructorRejectsZeroTimeout() {
    assertThrows(IllegalArgumentException.class, () -> new BackpressureController(100, 0));
  }

  @Test
  @DisplayName("getMaxBufferBytes returns configured value")
  void getMaxBufferBytes() {
    BackpressureController bp = new BackpressureController(4096, 1000);
    assertEquals(4096, bp.getMaxBufferBytes());
  }

  @Test
  @DisplayName("notifyConsumed with zero or negative is no-op")
  void notifyConsumedZeroIsNoop() throws InterruptedException {
    BackpressureController bp = new BackpressureController(100, 1000);
    bp.awaitCapacity(50);
    bp.notifyConsumed(0);
    assertEquals(50, bp.getBufferedBytes());
    bp.notifyConsumed(-10);
    assertEquals(50, bp.getBufferedBytes());
  }
}
