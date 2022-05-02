/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.apache.spark.shuffle.api.metadata;

import java.util.Optional;
import org.apache.spark.annotation.Private;

/**
 * :: Private ::
 * Represents the result of writing map outputs for a shuffle map task.
 * <p>
 * Partition lengths represents the length of each block written in the map task. This can
 * be used for downstream readers to allocate resources, such as in-memory buffers.
 * <p>
 * Map output writers can choose to attach arbitrary metadata tags to register with a
 * shuffle output tracker (a module that is currently yet to be built in a future
 * iteration of the shuffle storage APIs).
 */
@Private
public final class MapOutputCommitMessage {

  private final long[] partitionLengths;
  private final Optional<MapOutputMetadata> mapOutputMetadata;

  private MapOutputCommitMessage(
      long[] partitionLengths, Optional<MapOutputMetadata> mapOutputMetadata) {
    this.partitionLengths = partitionLengths;
    this.mapOutputMetadata = mapOutputMetadata;
  }

  public static MapOutputCommitMessage of(long[] partitionLengths) {
    return new MapOutputCommitMessage(partitionLengths, Optional.empty());
  }

  public static MapOutputCommitMessage of(
      long[] partitionLengths, MapOutputMetadata mapOutputMetadata) {
    return new MapOutputCommitMessage(partitionLengths, Optional.of(mapOutputMetadata));
  }

  public long[] getPartitionLengths() {
    return partitionLengths;
  }

  public Optional<MapOutputMetadata> getMapOutputMetadata() {
    return mapOutputMetadata;
  }
}
