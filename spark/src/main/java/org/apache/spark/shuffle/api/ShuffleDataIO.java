/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.apache.spark.shuffle.api;

import org.apache.spark.annotation.Private;

/**
 * :: Private ::
 * An interface for plugging in modules for storing and reading temporary shuffle data.
 * <p>
 * This is the root of a plugin system for storing shuffle bytes to arbitrary storage
 * backends in the sort-based shuffle algorithm implemented by the
 * {@link org.apache.spark.shuffle.sort.SortShuffleManager}. If another shuffle algorithm is
 * needed instead of sort-based shuffle, one should implement
 * {@link org.apache.spark.shuffle.ShuffleManager} instead.
 * <p>
 * A single instance of this module is loaded per process in the Spark application.
 * The default implementation reads and writes shuffle data from the local disks of
 * the executor, and is the implementation of shuffle file storage that has remained
 * consistent throughout most of Spark's history.
 * <p>
 * Alternative implementations of shuffle data storage can be loaded via setting
 * <code>spark.shuffle.sort.io.plugin.class</code>.
 * @since 3.0.0
 */
@Private
public interface ShuffleDataIO {

  /**
   * Called once on executor processes to bootstrap the shuffle data storage modules that
   * are only invoked on the executors.
   */
  ShuffleExecutorComponents executor();

  /**
   * Called once on driver process to bootstrap the shuffle metadata modules that
   * are maintained by the driver.
   */
  ShuffleDriverComponents driver();
}
