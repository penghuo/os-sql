/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.apache.hadoop.shaded.net.jpountz.lz4;

/**
 * TODO(SPARK-36679): A temporary workaround for SPARK-36669. We should remove this after
 * Hadoop 3.3.2 release which fixes the LZ4 relocation in shaded Hadoop client libraries.
 * This does not need implement all net.jpountz.lz4.LZ4SafeDecompressor API, just the ones
 * used by Hadoop Lz4Decompressor.
 */
public final class LZ4SafeDecompressor {
  private net.jpountz.lz4.LZ4SafeDecompressor lz4Decompressor;

  public LZ4SafeDecompressor(net.jpountz.lz4.LZ4SafeDecompressor lz4Decompressor) {
    this.lz4Decompressor = lz4Decompressor;
  }

  public void decompress(java.nio.ByteBuffer src, java.nio.ByteBuffer dest) {
    lz4Decompressor.decompress(src, dest);
  }
}
