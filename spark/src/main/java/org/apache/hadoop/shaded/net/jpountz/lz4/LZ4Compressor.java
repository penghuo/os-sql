/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.apache.hadoop.shaded.net.jpountz.lz4;

/**
 * TODO(SPARK-36679): A temporary workaround for SPARK-36669. We should remove this after
 * Hadoop 3.3.2 release which fixes the LZ4 relocation in shaded Hadoop client libraries.
 * This does not need implement all net.jpountz.lz4.LZ4Compressor API, just the ones used
 * by Hadoop Lz4Compressor.
 */
public final class LZ4Compressor {

  private net.jpountz.lz4.LZ4Compressor lz4Compressor;

  public LZ4Compressor(net.jpountz.lz4.LZ4Compressor lz4Compressor) {
    this.lz4Compressor = lz4Compressor;
  }

  public void compress(java.nio.ByteBuffer src, java.nio.ByteBuffer dest) {
    lz4Compressor.compress(src, dest);
  }
}
