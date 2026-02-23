/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dqe.exchange.buffer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class SequenceTrackerTests {

  @Test
  @DisplayName("first chunk for a partition is always accepted")
  void firstChunkAccepted() {
    SequenceTracker tracker = new SequenceTracker();
    assertTrue(tracker.acceptIfNew(0, 0));
    assertEquals(0, tracker.getLastReceived(0));
  }

  @Test
  @DisplayName("increasing sequence numbers are accepted")
  void increasingSequenceAccepted() {
    SequenceTracker tracker = new SequenceTracker();
    assertTrue(tracker.acceptIfNew(0, 0));
    assertTrue(tracker.acceptIfNew(0, 1));
    assertTrue(tracker.acceptIfNew(0, 2));
    assertEquals(2, tracker.getLastReceived(0));
  }

  @Test
  @DisplayName("duplicate sequence number is rejected")
  void duplicateRejected() {
    SequenceTracker tracker = new SequenceTracker();
    assertTrue(tracker.acceptIfNew(0, 0));
    assertFalse(tracker.acceptIfNew(0, 0));
    assertEquals(0, tracker.getLastReceived(0));
  }

  @Test
  @DisplayName("out-of-order (lower) sequence number is rejected")
  void outOfOrderRejected() {
    SequenceTracker tracker = new SequenceTracker();
    assertTrue(tracker.acceptIfNew(0, 5));
    assertFalse(tracker.acceptIfNew(0, 3));
    assertEquals(5, tracker.getLastReceived(0));
  }

  @Test
  @DisplayName("different partitions are tracked independently")
  void differentPartitionsIndependent() {
    SequenceTracker tracker = new SequenceTracker();
    assertTrue(tracker.acceptIfNew(0, 0));
    assertTrue(tracker.acceptIfNew(1, 0));
    assertTrue(tracker.acceptIfNew(0, 1));
    assertTrue(tracker.acceptIfNew(1, 1));
    assertEquals(1, tracker.getLastReceived(0));
    assertEquals(1, tracker.getLastReceived(1));
  }

  @Test
  @DisplayName("getLastReceived returns -1 for unknown partition")
  void unknownPartitionReturnsNegativeOne() {
    SequenceTracker tracker = new SequenceTracker();
    assertEquals(-1, tracker.getLastReceived(42));
  }

  @Test
  @DisplayName("reset clears all tracking state")
  void resetClearsState() {
    SequenceTracker tracker = new SequenceTracker();
    tracker.acceptIfNew(0, 5);
    tracker.acceptIfNew(1, 3);
    assertEquals(2, tracker.getTrackedPartitionCount());

    tracker.reset();
    assertEquals(0, tracker.getTrackedPartitionCount());
    assertEquals(-1, tracker.getLastReceived(0));
    assertTrue(tracker.acceptIfNew(0, 0));
  }

  @Test
  @DisplayName("gap in sequence numbers is accepted")
  void gapAccepted() {
    SequenceTracker tracker = new SequenceTracker();
    assertTrue(tracker.acceptIfNew(0, 0));
    assertTrue(tracker.acceptIfNew(0, 10));
    assertEquals(10, tracker.getLastReceived(0));
  }

  @Test
  @DisplayName("concurrent acceptIfNew is thread-safe")
  void concurrentAcceptIfNew() throws InterruptedException {
    SequenceTracker tracker = new SequenceTracker();
    int threadCount = 10;
    int chunksPerThread = 100;
    CountDownLatch startLatch = new CountDownLatch(1);
    CountDownLatch doneLatch = new CountDownLatch(threadCount);
    AtomicInteger totalAccepted = new AtomicInteger(0);

    ExecutorService exec = Executors.newFixedThreadPool(threadCount);
    for (int t = 0; t < threadCount; t++) {
      exec.submit(
          () -> {
            try {
              startLatch.await();
              for (int seq = 0; seq < chunksPerThread; seq++) {
                if (tracker.acceptIfNew(0, seq)) {
                  totalAccepted.incrementAndGet();
                }
              }
            } catch (InterruptedException e) {
              Thread.currentThread().interrupt();
            } finally {
              doneLatch.countDown();
            }
          });
    }

    startLatch.countDown();
    doneLatch.await(10, TimeUnit.SECONDS);
    exec.shutdown();

    // Each sequence number should be accepted exactly once
    assertEquals(chunksPerThread, totalAccepted.get());
    assertEquals(chunksPerThread - 1, tracker.getLastReceived(0));
  }
}
