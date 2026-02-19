/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.distributed.spill;

import java.util.concurrent.atomic.AtomicLong;

/** Tracks spill-to-disk metrics for monitoring and diagnostics. Thread-safe via atomic counters. */
public class SpillMetrics {

  private final AtomicLong spillCount = new AtomicLong();
  private final AtomicLong spilledBytes = new AtomicLong();
  private final AtomicLong spilledPages = new AtomicLong();
  private final AtomicLong readBackBytes = new AtomicLong();
  private final AtomicLong readBackPages = new AtomicLong();

  /** Records a single spill operation. */
  public void recordSpill(long bytes, int pages) {
    spillCount.incrementAndGet();
    spilledBytes.addAndGet(bytes);
    spilledPages.addAndGet(pages);
  }

  /** Records a read-back operation during merge. */
  public void recordReadBack(long bytes, int pages) {
    readBackBytes.addAndGet(bytes);
    readBackPages.addAndGet(pages);
  }

  public long getSpillCount() {
    return spillCount.get();
  }

  public long getSpilledBytes() {
    return spilledBytes.get();
  }

  public long getSpilledPages() {
    return spilledPages.get();
  }

  public long getReadBackBytes() {
    return readBackBytes.get();
  }

  public long getReadBackPages() {
    return readBackPages.get();
  }

  @Override
  public String toString() {
    return String.format(
        "SpillMetrics{spillCount=%d, spilledBytes=%d, spilledPages=%d, "
            + "readBackBytes=%d, readBackPages=%d}",
        spillCount.get(),
        spilledBytes.get(),
        spilledPages.get(),
        readBackBytes.get(),
        readBackPages.get());
  }
}
