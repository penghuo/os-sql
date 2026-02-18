/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.distributed.memory;

import static org.junit.jupiter.api.Assertions.*;

import com.google.common.util.concurrent.ListenableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class MemoryPoolTest {

  @Test
  @DisplayName("Reserve user memory, check available")
  void testReserve() {
    MemoryPool pool = new MemoryPool("test", 1024);

    ListenableFuture<Void> future = pool.reserve("alloc1", 100);
    assertTrue(future.isDone());
    assertEquals(100, pool.getReservedBytes());
    assertEquals(924, pool.getFreeBytes());
  }

  @Test
  @DisplayName("Reserve then free, available returns to original")
  void testReserveThenFree() {
    MemoryPool pool = new MemoryPool("test", 1024);

    pool.reserve("alloc1", 500);
    assertEquals(500, pool.getReservedBytes());

    pool.free("alloc1", 500);
    assertEquals(0, pool.getReservedBytes());
    assertEquals(1024, pool.getFreeBytes());
  }

  @Test
  @DisplayName("Reserve up to limit returns unresolved future")
  void testReserveBlocking() {
    MemoryPool pool = new MemoryPool("test", 100);

    // Fill up the pool
    ListenableFuture<Void> f1 = pool.reserve("alloc1", 100);
    assertTrue(f1.isDone());

    // Next reservation should be blocked
    ListenableFuture<Void> f2 = pool.reserve("alloc2", 50);
    assertFalse(f2.isDone());
    assertEquals(100, pool.getReservedBytes()); // still 100, the 50 was rejected

    // Free some memory to unblock
    pool.free("alloc1", 50);
    // The blocked future should now be resolved
    assertTrue(f2.isDone());
  }

  @Test
  @DisplayName("Reserve revocable memory")
  void testReserveRevocable() {
    MemoryPool pool = new MemoryPool("test", 1024);

    ListenableFuture<Void> future = pool.reserveRevocable("rev1", 200);
    assertTrue(future.isDone());
    assertEquals(200, pool.getReservedRevocableBytes());
    assertEquals(824, pool.getFreeBytes());

    pool.freeRevocable("rev1", 200);
    assertEquals(0, pool.getReservedRevocableBytes());
    assertEquals(1024, pool.getFreeBytes());
  }

  @Test
  @DisplayName("Revocable + user memory combined limit")
  void testCombinedLimit() {
    MemoryPool pool = new MemoryPool("test", 100);

    pool.reserve("user", 60);
    pool.reserveRevocable("rev", 30);
    assertEquals(60, pool.getReservedBytes());
    assertEquals(30, pool.getReservedRevocableBytes());
    assertEquals(10, pool.getFreeBytes());

    // This should block because 60+30+20 = 110 > 100
    ListenableFuture<Void> f = pool.reserve("user2", 20);
    assertFalse(f.isDone());
  }

  @Test
  @DisplayName("Cannot free more than reserved")
  void testCannotFreeMoreThanReserved() {
    MemoryPool pool = new MemoryPool("test", 1024);
    pool.reserve("alloc", 100);
    assertThrows(IllegalStateException.class, () -> pool.free("alloc", 200));
  }

  @Test
  @DisplayName("Zero-byte reservation succeeds immediately")
  void testZeroReservation() {
    MemoryPool pool = new MemoryPool("test", 1024);
    ListenableFuture<Void> f = pool.reserve("zero", 0);
    assertTrue(f.isDone());
    assertEquals(0, pool.getReservedBytes());
  }

  @Test
  @DisplayName("Negative bytes rejected")
  void testNegativeBytes() {
    MemoryPool pool = new MemoryPool("test", 1024);
    assertThrows(IllegalArgumentException.class, () -> pool.reserve("neg", -1));
    assertThrows(IllegalArgumentException.class, () -> pool.free("neg", -1));
  }

  @Test
  @DisplayName("Multi-threaded reserve/free consistency")
  void testConcurrency() throws Exception {
    MemoryPool pool = new MemoryPool("test", 1_000_000);
    int threadCount = 10;
    int iterations = 1000;
    ExecutorService executor = Executors.newFixedThreadPool(threadCount);
    CountDownLatch latch = new CountDownLatch(threadCount);
    AtomicInteger errors = new AtomicInteger();

    for (int t = 0; t < threadCount; t++) {
      executor.submit(
          () -> {
            try {
              for (int i = 0; i < iterations; i++) {
                pool.reserve("thread", 10);
                pool.free("thread", 10);
              }
            } catch (Exception e) {
              errors.incrementAndGet();
            } finally {
              latch.countDown();
            }
          });
    }

    assertTrue(latch.await(10, TimeUnit.SECONDS));
    executor.shutdown();
    assertEquals(0, errors.get());
    assertEquals(0, pool.getReservedBytes());
  }

  @Test
  @DisplayName("Pool maxBytes must be positive")
  void testInvalidMaxBytes() {
    assertThrows(IllegalArgumentException.class, () -> new MemoryPool("test", 0));
    assertThrows(IllegalArgumentException.class, () -> new MemoryPool("test", -1));
  }

  @Test
  @DisplayName("toString provides useful info")
  void testToString() {
    MemoryPool pool = new MemoryPool("my-pool", 2048);
    pool.reserve("a", 100);
    String str = pool.toString();
    assertTrue(str.contains("my-pool"));
    assertTrue(str.contains("2048"));
    assertTrue(str.contains("100"));
  }
}
