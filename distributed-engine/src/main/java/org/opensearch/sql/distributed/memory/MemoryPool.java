/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.distributed.memory;

import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Tracks memory usage for distributed query execution. Integrates with OpenSearch's circuit breaker
 * system. Each query gets a MemoryPool to track allocations.
 *
 * <p>When the pool is exhausted, callers receive an unresolved future that is completed when memory
 * becomes available.
 */
public class MemoryPool {

  private final String id;
  private final long maxBytes;
  private final AtomicLong reservedBytes = new AtomicLong();
  private final AtomicLong reservedRevocableBytes = new AtomicLong();
  private final Queue<SettableFuture<Void>> blockedCallers = new ConcurrentLinkedQueue<>();

  public MemoryPool(String id, long maxBytes) {
    if (maxBytes <= 0) {
      throw new IllegalArgumentException("maxBytes must be positive");
    }
    this.id = id;
    this.maxBytes = maxBytes;
  }

  public String getId() {
    return id;
  }

  public long getMaxBytes() {
    return maxBytes;
  }

  public long getReservedBytes() {
    return reservedBytes.get();
  }

  public long getReservedRevocableBytes() {
    return reservedRevocableBytes.get();
  }

  public long getFreeBytes() {
    return maxBytes - reservedBytes.get() - reservedRevocableBytes.get();
  }

  /**
   * Reserves user (non-revocable) memory. Returns a resolved future if the reservation succeeds, or
   * an unresolved future if the pool is exhausted.
   */
  public ListenableFuture<Void> reserve(String allocationTag, long bytes) {
    if (bytes < 0) {
      throw new IllegalArgumentException("bytes must be non-negative");
    }
    if (bytes == 0) {
      return Futures.immediateVoidFuture();
    }
    long newTotal = reservedBytes.addAndGet(bytes);
    if (newTotal + reservedRevocableBytes.get() > maxBytes) {
      reservedBytes.addAndGet(-bytes);
      SettableFuture<Void> future = SettableFuture.create();
      blockedCallers.add(future);
      return future;
    }
    return Futures.immediateVoidFuture();
  }

  /** Frees previously reserved user memory. May unblock waiting callers. */
  public void free(String allocationTag, long bytes) {
    if (bytes < 0) {
      throw new IllegalArgumentException("bytes must be non-negative");
    }
    if (bytes == 0) {
      return;
    }
    long newTotal = reservedBytes.addAndGet(-bytes);
    if (newTotal < 0) {
      reservedBytes.addAndGet(bytes);
      throw new IllegalStateException("Cannot free more memory than was reserved");
    }
    unblockCallers();
  }

  /**
   * Reserves revocable memory. Returns a resolved future if the reservation succeeds, or an
   * unresolved future if the pool is exhausted.
   */
  public ListenableFuture<Void> reserveRevocable(String allocationTag, long bytes) {
    if (bytes < 0) {
      throw new IllegalArgumentException("bytes must be non-negative");
    }
    if (bytes == 0) {
      return Futures.immediateVoidFuture();
    }
    long newTotal = reservedRevocableBytes.addAndGet(bytes);
    if (reservedBytes.get() + newTotal > maxBytes) {
      reservedRevocableBytes.addAndGet(-bytes);
      SettableFuture<Void> future = SettableFuture.create();
      blockedCallers.add(future);
      return future;
    }
    return Futures.immediateVoidFuture();
  }

  /** Frees previously reserved revocable memory. May unblock waiting callers. */
  public void freeRevocable(String allocationTag, long bytes) {
    if (bytes < 0) {
      throw new IllegalArgumentException("bytes must be non-negative");
    }
    if (bytes == 0) {
      return;
    }
    long newTotal = reservedRevocableBytes.addAndGet(-bytes);
    if (newTotal < 0) {
      reservedRevocableBytes.addAndGet(bytes);
      throw new IllegalStateException("Cannot free more revocable memory than was reserved");
    }
    unblockCallers();
  }

  private void unblockCallers() {
    while (!blockedCallers.isEmpty()) {
      SettableFuture<Void> future = blockedCallers.poll();
      if (future != null && !future.isDone()) {
        future.set(null);
      }
    }
  }

  @Override
  public String toString() {
    return String.format(
        "MemoryPool{id='%s', maxBytes=%d, reservedBytes=%d, reservedRevocableBytes=%d}",
        id, maxBytes, reservedBytes.get(), reservedRevocableBytes.get());
  }
}
