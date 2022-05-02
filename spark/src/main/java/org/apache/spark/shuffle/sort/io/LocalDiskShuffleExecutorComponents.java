/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.apache.spark.shuffle.sort.io;

import com.google.common.annotations.VisibleForTesting;
import java.util.Map;
import java.util.Optional;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkEnv;
import org.apache.spark.shuffle.IndexShuffleBlockResolver;
import org.apache.spark.shuffle.api.ShuffleExecutorComponents;
import org.apache.spark.shuffle.api.ShuffleMapOutputWriter;
import org.apache.spark.shuffle.api.SingleSpillShuffleMapOutputWriter;
import org.apache.spark.storage.BlockManager;

public class LocalDiskShuffleExecutorComponents implements ShuffleExecutorComponents {

  private final SparkConf sparkConf;
  private BlockManager blockManager;
  private IndexShuffleBlockResolver blockResolver;

  public LocalDiskShuffleExecutorComponents(SparkConf sparkConf) {
    this.sparkConf = sparkConf;
  }

  @VisibleForTesting
  public LocalDiskShuffleExecutorComponents(
      SparkConf sparkConf,
      BlockManager blockManager,
      IndexShuffleBlockResolver blockResolver) {
    this.sparkConf = sparkConf;
    this.blockManager = blockManager;
    this.blockResolver = blockResolver;
  }

  @Override
  public void initializeExecutor(String appId, String execId, Map<String, String> extraConfigs) {
    blockManager = SparkEnv.get().blockManager();
    if (blockManager == null) {
      throw new IllegalStateException("No blockManager available from the SparkEnv.");
    }
    blockResolver = new IndexShuffleBlockResolver(sparkConf, blockManager);
  }

  @Override
  public ShuffleMapOutputWriter createMapOutputWriter(
      int shuffleId,
      long mapTaskId,
      int numPartitions) {
    if (blockResolver == null) {
      throw new IllegalStateException(
          "Executor components must be initialized before getting writers.");
    }
    return new LocalDiskShuffleMapOutputWriter(
        shuffleId, mapTaskId, numPartitions, blockResolver, sparkConf);
  }

  @Override
  public Optional<SingleSpillShuffleMapOutputWriter> createSingleFileMapOutputWriter(
      int shuffleId,
      long mapId) {
    if (blockResolver == null) {
      throw new IllegalStateException(
          "Executor components must be initialized before getting writers.");
    }
    return Optional.of(new LocalDiskSingleSpillMapOutputWriter(shuffleId, mapId, blockResolver));
  }
}
