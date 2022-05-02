/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.apache.spark.shuffle.sort.io;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import org.apache.spark.shuffle.IndexShuffleBlockResolver;
import org.apache.spark.shuffle.api.SingleSpillShuffleMapOutputWriter;
import org.apache.spark.util.Utils;

public class LocalDiskSingleSpillMapOutputWriter
    implements SingleSpillShuffleMapOutputWriter {

  private final int shuffleId;
  private final long mapId;
  private final IndexShuffleBlockResolver blockResolver;

  public LocalDiskSingleSpillMapOutputWriter(
      int shuffleId,
      long mapId,
      IndexShuffleBlockResolver blockResolver) {
    this.shuffleId = shuffleId;
    this.mapId = mapId;
    this.blockResolver = blockResolver;
  }

  @Override
  public void transferMapSpillFile(
      File mapSpillFile,
      long[] partitionLengths,
      long[] checksums) throws IOException {
    // The map spill file already has the proper format, and it contains all of the partition data.
    // So just transfer it directly to the destination without any merging.
    File outputFile = blockResolver.getDataFile(shuffleId, mapId);
    File tempFile = Utils.tempFileWith(outputFile);
    Files.move(mapSpillFile.toPath(), tempFile.toPath());
    blockResolver
      .writeMetadataFileAndCommit(shuffleId, mapId, partitionLengths, checksums, tempFile);
  }
}
