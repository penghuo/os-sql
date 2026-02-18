/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.distributed.driver;

/**
 * Signal used by Drivers for cooperative scheduling. The yield signal ensures that a Driver does
 * not monopolize a thread beyond a time quantum. Ported from Trino's
 * io.trino.operator.DriverYieldSignal.
 */
public class DriverYieldSignal {

  private volatile boolean yield;
  private volatile long yieldForceTimeNanos;

  /**
   * Arms the yield signal to trigger after the specified duration. The signal will fire when
   * System.nanoTime() exceeds the deadline.
   */
  public void setWithDelay(long maxRunTimeNanos) {
    this.yieldForceTimeNanos = System.nanoTime() + maxRunTimeNanos;
    this.yield = false;
  }

  /** Returns true if the driver should yield control (either forced or time-based). */
  public boolean isSet() {
    return yield || (yieldForceTimeNanos > 0 && System.nanoTime() >= yieldForceTimeNanos);
  }

  /** Explicitly sets the yield signal. */
  public void set() {
    this.yield = true;
  }

  /** Resets the yield signal. */
  public void reset() {
    this.yield = false;
    this.yieldForceTimeNanos = 0;
  }

  /** Forces yield for testing purposes. */
  public void forceYieldForTesting() {
    this.yield = true;
  }
}
