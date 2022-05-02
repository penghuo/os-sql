/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.apache.spark.shuffle.api;

import java.io.File;
import java.io.IOException;
import org.apache.spark.annotation.Private;

/**
 * Optional extension for partition writing that is optimized for transferring a single
 * file to the backing store.
 */
@Private
public interface SingleSpillShuffleMapOutputWriter {

  /**
   * Transfer a file that contains the bytes of all the partitions written by this map task.
   */
  void transferMapSpillFile(
      File mapOutputFile,
      long[] partitionLengths,
      long[] checksums) throws IOException;
}
