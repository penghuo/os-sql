/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dqe.memory;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class AdmissionControllerTests {

  @Test
  @DisplayName("tryAcquire succeeds when under capacity")
  void tryAcquireSucceedsUnderCapacity() {
    AdmissionController controller = new AdmissionController(10);
    assertTrue(controller.tryAcquire());
    assertEquals(1, controller.getRunningQueryCount());
  }

  @Test
  @DisplayName("tryAcquire succeeds up to max capacity")
  void tryAcquireSucceedsUpToMaxCapacity() {
    AdmissionController controller = new AdmissionController(3);

    assertTrue(controller.tryAcquire());
    assertTrue(controller.tryAcquire());
    assertTrue(controller.tryAcquire());
    assertEquals(3, controller.getRunningQueryCount());
  }

  @Test
  @DisplayName("tryAcquire fails when at capacity")
  void tryAcquireFailsAtCapacity() {
    AdmissionController controller = new AdmissionController(2);

    assertTrue(controller.tryAcquire());
    assertTrue(controller.tryAcquire());
    assertFalse(controller.tryAcquire());
  }

  @Test
  @DisplayName("release frees a slot for new query")
  void releaseFreesSlot() {
    AdmissionController controller = new AdmissionController(1);

    assertTrue(controller.tryAcquire());
    assertFalse(controller.tryAcquire());

    controller.release();
    assertTrue(controller.tryAcquire());
  }

  @Test
  @DisplayName("getRunningQueryCount reflects current state")
  void getRunningQueryCountReflectsState() {
    AdmissionController controller = new AdmissionController(5);

    assertEquals(0, controller.getRunningQueryCount());

    controller.tryAcquire();
    controller.tryAcquire();
    assertEquals(2, controller.getRunningQueryCount());

    controller.release();
    assertEquals(1, controller.getRunningQueryCount());
  }

  @Test
  @DisplayName("getMaxConcurrentQueries returns configured value")
  void getMaxConcurrentQueriesReturnsConfigured() {
    AdmissionController controller = new AdmissionController(42);
    assertEquals(42, controller.getMaxConcurrentQueries());
  }

  @Test
  @DisplayName("isAtCapacity returns true when full")
  void isAtCapacityReturnsTrueWhenFull() {
    AdmissionController controller = new AdmissionController(1);

    assertFalse(controller.isAtCapacity());
    controller.tryAcquire();
    assertTrue(controller.isAtCapacity());
  }

  @Test
  @DisplayName("isAtCapacity returns false after release")
  void isAtCapacityReturnsFalseAfterRelease() {
    AdmissionController controller = new AdmissionController(1);

    controller.tryAcquire();
    assertTrue(controller.isAtCapacity());

    controller.release();
    assertFalse(controller.isAtCapacity());
  }

  @Test
  @DisplayName("constructor rejects zero max concurrent queries")
  void constructorRejectsZero() {
    assertThrows(IllegalArgumentException.class, () -> new AdmissionController(0));
  }

  @Test
  @DisplayName("constructor rejects negative max concurrent queries")
  void constructorRejectsNegative() {
    assertThrows(IllegalArgumentException.class, () -> new AdmissionController(-1));
  }

  @Test
  @DisplayName("concurrent tryAcquire respects capacity limit")
  void concurrentTryAcquireRespectsLimit() throws Exception {
    int maxConcurrent = 5;
    int totalAttempts = 20;
    AdmissionController controller = new AdmissionController(maxConcurrent);

    ExecutorService executor = Executors.newFixedThreadPool(totalAttempts);
    CountDownLatch startLatch = new CountDownLatch(1);
    AtomicInteger acquired = new AtomicInteger(0);

    List<Future<?>> futures = new ArrayList<>();
    for (int i = 0; i < totalAttempts; i++) {
      futures.add(
          executor.submit(
              () -> {
                try {
                  startLatch.await();
                } catch (InterruptedException e) {
                  Thread.currentThread().interrupt();
                  return;
                }
                if (controller.tryAcquire()) {
                  acquired.incrementAndGet();
                }
              }));
    }

    startLatch.countDown();
    for (Future<?> f : futures) {
      f.get();
    }

    assertEquals(maxConcurrent, acquired.get());
    assertEquals(maxConcurrent, controller.getRunningQueryCount());

    executor.shutdown();
  }

  @Test
  @DisplayName("single capacity controller works correctly")
  void singleCapacityControllerWorks() {
    AdmissionController controller = new AdmissionController(1);

    assertTrue(controller.tryAcquire());
    assertFalse(controller.tryAcquire());
    assertEquals(1, controller.getRunningQueryCount());

    controller.release();
    assertEquals(0, controller.getRunningQueryCount());
    assertFalse(controller.isAtCapacity());
  }
}
