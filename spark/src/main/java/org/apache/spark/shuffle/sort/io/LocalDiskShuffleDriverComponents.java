/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.apache.spark.shuffle.sort.io;

import java.util.Collections;
import java.util.Map;
import org.apache.spark.SparkEnv;
import org.apache.spark.shuffle.api.ShuffleDriverComponents;
import org.apache.spark.storage.BlockManagerMaster;

public class LocalDiskShuffleDriverComponents implements ShuffleDriverComponents {

  private BlockManagerMaster blockManagerMaster;

  @Override
  public Map<String, String> initializeApplication() {
    blockManagerMaster = SparkEnv.get().blockManager().master();
    return Collections.emptyMap();
  }

  @Override
  public void cleanupApplication() {
    // nothing to clean up
  }

  @Override
  public void removeShuffle(int shuffleId, boolean blocking) {
    if (blockManagerMaster == null) {
      throw new IllegalStateException("Driver components must be initialized before using");
    }
    blockManagerMaster.removeShuffle(shuffleId, blocking);
  }
}
