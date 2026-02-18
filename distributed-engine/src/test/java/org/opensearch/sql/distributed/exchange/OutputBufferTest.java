/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.distributed.exchange;

import static org.junit.jupiter.api.Assertions.*;

import com.google.common.util.concurrent.ListenableFuture;
import java.util.Optional;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.opensearch.sql.distributed.data.LongArrayBlock;
import org.opensearch.sql.distributed.data.Page;

class OutputBufferTest {

  @Test
  @DisplayName("Basic enqueue and poll operations")
  void testBasicEnqueueAndPoll() {
    OutputBuffer buffer = new OutputBuffer();
    Page page = createTestPage(100);

    ListenableFuture<Void> future = buffer.enqueue(page);
    assertTrue(future.isDone(), "Should not be blocked on small page");

    Page polled = buffer.poll();
    assertNotNull(polled);
    assertEquals(100, polled.getPositionCount());
  }

  @Test
  @DisplayName("Poll returns null when buffer is empty")
  void testPollEmptyBuffer() {
    OutputBuffer buffer = new OutputBuffer();
    assertNull(buffer.poll());
    assertTrue(buffer.isEmpty());
  }

  @Test
  @DisplayName("Buffer reports correct page count and byte size")
  void testBufferSizeTracking() {
    OutputBuffer buffer = new OutputBuffer();

    buffer.enqueue(createTestPage(100));
    buffer.enqueue(createTestPage(200));

    assertEquals(2, buffer.getBufferedPages());
    assertTrue(buffer.getBufferedBytes() > 0);

    buffer.poll();
    assertEquals(1, buffer.getBufferedPages());
  }

  @Test
  @DisplayName("Backpressure: enqueue blocks when buffer is full")
  void testBackpressure() {
    // Create a tiny buffer (64 bytes)
    OutputBuffer buffer = new OutputBuffer(64);

    // First page should not block
    ListenableFuture<Void> f1 = buffer.enqueue(createTestPage(100));

    // Buffer should now be full (100 longs * 8 bytes > 64 bytes)
    ListenableFuture<Void> f2 = buffer.enqueue(createTestPage(100));
    assertFalse(f2.isDone(), "Second enqueue should block when buffer is full");

    // Drain the buffer
    buffer.poll();
    buffer.poll();

    // Future should be resolved after draining
    assertTrue(f2.isDone(), "Future should resolve after draining");
  }

  @Test
  @DisplayName("isBlocked returns resolved future when pages available")
  void testIsBlockedWithData() {
    OutputBuffer buffer = new OutputBuffer();
    buffer.enqueue(createTestPage(10));

    ListenableFuture<Void> blocked = buffer.isBlocked();
    assertTrue(blocked.isDone());
  }

  @Test
  @DisplayName("isBlocked returns unresolved future when empty")
  void testIsBlockedWhenEmpty() {
    OutputBuffer buffer = new OutputBuffer();

    ListenableFuture<Void> blocked = buffer.isBlocked();
    assertFalse(blocked.isDone(), "Should be blocked when empty");

    // Enqueue should resolve the blocked future
    buffer.enqueue(createTestPage(10));
    assertTrue(blocked.isDone(), "Should unblock after enqueue");
  }

  @Test
  @DisplayName("Finish marks buffer as complete")
  void testFinish() {
    OutputBuffer buffer = new OutputBuffer();
    buffer.enqueue(createTestPage(10));
    buffer.setFinished();

    assertFalse(buffer.isFinished(), "Not finished until drained");
    buffer.poll();
    assertTrue(buffer.isFinished(), "Finished after drain");
  }

  @Test
  @DisplayName("Cannot enqueue to finished buffer")
  void testEnqueueAfterFinish() {
    OutputBuffer buffer = new OutputBuffer();
    buffer.setFinished();

    assertThrows(IllegalStateException.class, () -> buffer.enqueue(createTestPage(10)));
  }

  @Test
  @DisplayName("Finish unblocks waiting consumers")
  void testFinishUnblocksConsumers() {
    OutputBuffer buffer = new OutputBuffer();
    ListenableFuture<Void> blocked = buffer.isBlocked();
    assertFalse(blocked.isDone());

    buffer.setFinished();
    assertTrue(blocked.isDone(), "Should unblock on finish");
  }

  private Page createTestPage(int positions) {
    long[] values = new long[positions];
    for (int i = 0; i < positions; i++) {
      values[i] = i;
    }
    LongArrayBlock block = new LongArrayBlock(positions, Optional.empty(), values);
    return new Page(block);
  }
}
