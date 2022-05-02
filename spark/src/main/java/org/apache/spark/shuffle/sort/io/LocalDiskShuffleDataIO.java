/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.apache.spark.shuffle.sort.io;

import org.apache.spark.SparkConf;
import org.apache.spark.shuffle.api.ShuffleDataIO;
import org.apache.spark.shuffle.api.ShuffleDriverComponents;
import org.apache.spark.shuffle.api.ShuffleExecutorComponents;

/**
 * Implementation of the {@link ShuffleDataIO} plugin system that replicates the local shuffle
 * storage and index file functionality that has historically been used from Spark 2.4 and earlier.
 */
public class LocalDiskShuffleDataIO implements ShuffleDataIO {

  private final SparkConf sparkConf;

  public LocalDiskShuffleDataIO(SparkConf sparkConf) {
    this.sparkConf = sparkConf;
  }

  @Override
  public ShuffleExecutorComponents executor() {
    return new LocalDiskShuffleExecutorComponents(sparkConf);
  }

  @Override
  public ShuffleDriverComponents driver() {
    return new LocalDiskShuffleDriverComponents();
  }
}
