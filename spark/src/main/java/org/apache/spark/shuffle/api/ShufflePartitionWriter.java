/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.apache.spark.shuffle.api;

import java.io.IOException;
import java.io.OutputStream;
import java.util.Optional;
import org.apache.spark.annotation.Private;

/**
 * :: Private ::
 * An interface for opening streams to persist partition bytes to a backing data store.
 * <p>
 * This writer stores bytes for one (mapper, reducer) pair, corresponding to one shuffle
 * block.
 *
 * @since 3.0.0
 */
@Private
public interface ShufflePartitionWriter {

  /**
   * Open and return an {@link OutputStream} that can write bytes to the underlying
   * data store.
   * <p>
   * This method will only be called once on this partition writer in the map task, to write the
   * bytes to the partition. The output stream will only be used to write the bytes for this
   * partition. The map task closes this output stream upon writing all the bytes for this
   * block, or if the write fails for any reason.
   * <p>
   * Implementations that intend on combining the bytes for all the partitions written by this
   * map task should reuse the same OutputStream instance across all the partition writers provided
   * by the parent {@link ShuffleMapOutputWriter}. If one does so, ensure that
   * {@link OutputStream#close()} does not close the resource, since it will be reused across
   * partition writes. The underlying resources should be cleaned up in
   * {@link ShuffleMapOutputWriter#commitAllPartitions(long[])} and
   * {@link ShuffleMapOutputWriter#abort(Throwable)}.
   */
  OutputStream openStream() throws IOException;

  /**
   * Opens and returns a {@link WritableByteChannelWrapper} for transferring bytes from
   * input byte channels to the underlying shuffle data store.
   * <p>
   * This method will only be called once on this partition writer in the map task, to write the
   * bytes to the partition. The channel will only be used to write the bytes for this
   * partition. The map task closes this channel upon writing all the bytes for this
   * block, or if the write fails for any reason.
   * <p>
   * Implementations that intend on combining the bytes for all the partitions written by this
   * map task should reuse the same channel instance across all the partition writers provided
   * by the parent {@link ShuffleMapOutputWriter}. If one does so, ensure that
   * {@link WritableByteChannelWrapper#close()} does not close the resource, since the channel
   * will be reused across partition writes. The underlying resources should be cleaned up in
   * {@link ShuffleMapOutputWriter#commitAllPartitions(long[])} and
   * {@link ShuffleMapOutputWriter#abort(Throwable)}.
   * <p>
   * This method is primarily for advanced optimizations where bytes can be copied from the input
   * spill files to the output channel without copying data into memory. If such optimizations are
   * not supported, the implementation should return {@link Optional#empty()}. By default, the
   * implementation returns {@link Optional#empty()}.
   * <p>
   * Note that the returned {@link WritableByteChannelWrapper} itself is closed, but not the
   * underlying channel that is returned by {@link WritableByteChannelWrapper#channel()}. Ensure
   * that the underlying channel is cleaned up in {@link WritableByteChannelWrapper#close()},
   * {@link ShuffleMapOutputWriter#commitAllPartitions(long[])}, or
   * {@link ShuffleMapOutputWriter#abort(Throwable)}.
   */
  default Optional<WritableByteChannelWrapper> openChannelWrapper() throws IOException {
    return Optional.empty();
  }

  /**
   * Returns the number of bytes written either by this writer's output stream opened by
   * {@link #openStream()} or the byte channel opened by {@link #openChannelWrapper()}.
   * <p>
   * This can be different from the number of bytes given by the caller. For example, the
   * stream might compress or encrypt the bytes before persisting the data to the backing
   * data store.
   */
  long getNumBytesWritten();
}
