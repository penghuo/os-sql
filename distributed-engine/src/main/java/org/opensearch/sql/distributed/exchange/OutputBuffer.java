/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.distributed.exchange;

import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import java.util.ArrayDeque;
import java.util.Queue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import org.opensearch.sql.distributed.data.Page;

/**
 * Bounded buffer for Pages produced by leaf stage operators. Supports backpressure: when the buffer
 * exceeds {@code maxBufferedBytes}, producers are blocked via a {@link ListenableFuture} until
 * consumers drain enough data.
 *
 * <p>Thread-safe: producers and consumers may operate from different threads.
 */
public class OutputBuffer {

  /** Default max buffer size: 8 MB. */
  public static final long DEFAULT_MAX_BUFFERED_BYTES = 8L * 1024 * 1024;

  private final long maxBufferedBytes;
  private final Queue<Page> pages;
  private final AtomicLong bufferedBytes;
  private final AtomicBoolean finished;

  /** Set when buffer is full and a producer is waiting. Resolved when space becomes available. */
  private volatile SettableFuture<Void> notFullFuture;

  /** Set when buffer is empty and a consumer is waiting. Resolved when data becomes available. */
  private volatile SettableFuture<Void> notEmptyFuture;

  private final Object lock = new Object();

  public OutputBuffer() {
    this(DEFAULT_MAX_BUFFERED_BYTES);
  }

  public OutputBuffer(long maxBufferedBytes) {
    if (maxBufferedBytes <= 0) {
      throw new IllegalArgumentException("maxBufferedBytes must be positive");
    }
    this.maxBufferedBytes = maxBufferedBytes;
    this.pages = new ArrayDeque<>();
    this.bufferedBytes = new AtomicLong(0);
    this.finished = new AtomicBoolean(false);
    this.notFullFuture = null;
    this.notEmptyFuture = null;
  }

  /**
   * Enqueue a page into the buffer. If the buffer is full, the returned future will be unresolved
   * until space becomes available.
   *
   * @param page the page to enqueue
   * @return a future that resolves when the buffer has space (or immediately if not full)
   * @throws IllegalStateException if the buffer has been marked as finished
   */
  public ListenableFuture<Void> enqueue(Page page) {
    if (finished.get()) {
      throw new IllegalStateException("Cannot enqueue to a finished OutputBuffer");
    }

    synchronized (lock) {
      pages.add(page);
      long newSize = bufferedBytes.addAndGet(page.getSizeInBytes());

      // Notify any waiting consumer
      if (notEmptyFuture != null) {
        notEmptyFuture.set(null);
        notEmptyFuture = null;
      }

      // Check if buffer is now full
      if (newSize >= maxBufferedBytes) {
        if (notFullFuture == null || notFullFuture.isDone()) {
          notFullFuture = SettableFuture.create();
        }
        return notFullFuture;
      }
    }

    return com.google.common.util.concurrent.Futures.immediateVoidFuture();
  }

  /**
   * Dequeue the next page from the buffer.
   *
   * @return the next page, or null if the buffer is empty
   */
  public Page poll() {
    synchronized (lock) {
      Page page = pages.poll();
      if (page != null) {
        long newSize = bufferedBytes.addAndGet(-page.getSizeInBytes());

        // If buffer was full and now has space, unblock producers
        if (newSize < maxBufferedBytes && notFullFuture != null && !notFullFuture.isDone()) {
          notFullFuture.set(null);
        }
      }
      return page;
    }
  }

  /**
   * Returns a future that resolves when data is available in the buffer or the buffer is finished.
   * If data is already available, returns an immediately resolved future.
   */
  public ListenableFuture<Void> isBlocked() {
    synchronized (lock) {
      if (!pages.isEmpty() || finished.get()) {
        return com.google.common.util.concurrent.Futures.immediateVoidFuture();
      }
      if (notEmptyFuture == null || notEmptyFuture.isDone()) {
        notEmptyFuture = SettableFuture.create();
      }
      return notEmptyFuture;
    }
  }

  /** Mark this buffer as finished. No more pages will be enqueued. */
  public void setFinished() {
    finished.set(true);
    synchronized (lock) {
      // Unblock any waiting consumer
      if (notEmptyFuture != null && !notEmptyFuture.isDone()) {
        notEmptyFuture.set(null);
      }
      // Unblock any waiting producer
      if (notFullFuture != null && !notFullFuture.isDone()) {
        notFullFuture.set(null);
      }
    }
  }

  /** Returns true if the buffer has been marked as finished and is empty. */
  public boolean isFinished() {
    return finished.get() && pages.isEmpty();
  }

  /** Returns true if the buffer is empty (no pages available). */
  public boolean isEmpty() {
    return pages.isEmpty();
  }

  /** Returns the current number of buffered bytes. */
  public long getBufferedBytes() {
    return bufferedBytes.get();
  }

  /** Returns the maximum buffer size in bytes. */
  public long getMaxBufferedBytes() {
    return maxBufferedBytes;
  }

  /** Returns the number of pages currently in the buffer. */
  public int getBufferedPages() {
    return pages.size();
  }
}
