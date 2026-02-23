/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dqe.exchange.buffer;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Controls backpressure between producers and consumers in an exchange buffer.
 *
 * <p>When the buffer is full (buffered bytes >= maxBufferBytes), producers block on {@link
 * #awaitCapacity(long)} until consumers free space via {@link #notifyConsumed(long)} or the timeout
 * expires.
 *
 * <p>Thread-safe. Uses a ReentrantLock with a Condition to avoid busy-waiting.
 */
public class BackpressureController {

  private final long maxBufferBytes;
  private final long producerTimeoutMs;
  private final AtomicLong bufferedBytes;
  private final AtomicBoolean aborted;
  private final ReentrantLock lock;
  private final Condition capacityAvailable;

  /**
   * Create a backpressure controller.
   *
   * @param maxBufferBytes maximum buffer capacity in bytes
   * @param producerTimeoutMs how long producers wait for capacity before timing out (milliseconds)
   */
  public BackpressureController(long maxBufferBytes, long producerTimeoutMs) {
    if (maxBufferBytes <= 0) {
      throw new IllegalArgumentException("maxBufferBytes must be > 0, got: " + maxBufferBytes);
    }
    if (producerTimeoutMs <= 0) {
      throw new IllegalArgumentException(
          "producerTimeoutMs must be > 0, got: " + producerTimeoutMs);
    }
    this.maxBufferBytes = maxBufferBytes;
    this.producerTimeoutMs = producerTimeoutMs;
    this.bufferedBytes = new AtomicLong(0);
    this.aborted = new AtomicBoolean(false);
    this.lock = new ReentrantLock();
    this.capacityAvailable = lock.newCondition();
  }

  /**
   * Wait until there is enough capacity to buffer the required bytes.
   *
   * <p>Returns {@code true} if capacity was acquired, {@code false} if the timeout expired or the
   * controller was aborted.
   *
   * @param requiredBytes the number of bytes the producer wants to add
   * @return true if capacity was acquired
   * @throws InterruptedException if the waiting thread is interrupted
   */
  public boolean awaitCapacity(long requiredBytes) throws InterruptedException {
    if (aborted.get()) {
      return false;
    }

    lock.lockInterruptibly();
    try {
      long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(producerTimeoutMs);
      while (bufferedBytes.get() + requiredBytes > maxBufferBytes && !aborted.get()) {
        long remainingNanos = deadline - System.nanoTime();
        if (remainingNanos <= 0) {
          return false;
        }
        capacityAvailable.await(remainingNanos, TimeUnit.NANOSECONDS);
      }

      if (aborted.get()) {
        return false;
      }

      bufferedBytes.addAndGet(requiredBytes);
      return true;
    } finally {
      lock.unlock();
    }
  }

  /**
   * Notify that bytes have been consumed, freeing buffer capacity.
   *
   * @param freedBytes the number of bytes freed
   */
  public void notifyConsumed(long freedBytes) {
    if (freedBytes <= 0) {
      return;
    }
    bufferedBytes.addAndGet(-freedBytes);
    lock.lock();
    try {
      capacityAvailable.signalAll();
    } finally {
      lock.unlock();
    }
  }

  /** Current buffered bytes. */
  public long getBufferedBytes() {
    return bufferedBytes.get();
  }

  /** Whether the buffer is at or above capacity. */
  public boolean isFull() {
    return bufferedBytes.get() >= maxBufferBytes;
  }

  /** Abort the controller — all waiting producers will return false. */
  public void abort() {
    aborted.set(true);
    lock.lock();
    try {
      capacityAvailable.signalAll();
    } finally {
      lock.unlock();
    }
  }

  /** Whether the controller has been aborted. */
  public boolean isAborted() {
    return aborted.get();
  }

  /** Maximum buffer capacity in bytes. */
  public long getMaxBufferBytes() {
    return maxBufferBytes;
  }
}
