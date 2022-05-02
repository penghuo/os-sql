/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.apache.spark.shuffle.api;

import java.io.Closeable;
import java.nio.channels.WritableByteChannel;
import org.apache.spark.annotation.Private;

/**
 * :: Private ::
 * A thin wrapper around a {@link WritableByteChannel}.
 * <p>
 * This is primarily provided for the local disk shuffle implementation to provide a
 * {@link java.nio.channels.FileChannel} that keeps the channel open across partition writes.
 *
 * @since 3.0.0
 */
@Private
public interface WritableByteChannelWrapper extends Closeable {

  /**
   * The underlying channel to write bytes into.
   */
  WritableByteChannel channel();
}
