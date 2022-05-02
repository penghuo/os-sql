/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.apache.hadoop.shaded.net.jpountz.lz4;

/**
 * TODO(SPARK-36679): A temporary workaround for SPARK-36669. We should remove this after
 * Hadoop 3.3.2 release which fixes the LZ4 relocation in shaded Hadoop client libraries.
 * This does not need implement all net.jpountz.lz4.LZ4Factory API, just the ones used by
 * Hadoop Lz4Compressor.
 */
public final class LZ4Factory {

  private net.jpountz.lz4.LZ4Factory lz4Factory;

  public LZ4Factory(net.jpountz.lz4.LZ4Factory lz4Factory) {
    this.lz4Factory = lz4Factory;
  }

  public static LZ4Factory fastestInstance() {
    return new LZ4Factory(net.jpountz.lz4.LZ4Factory.fastestInstance());
  }

  public LZ4Compressor highCompressor() {
    return new LZ4Compressor(lz4Factory.highCompressor());
  }

  public LZ4Compressor fastCompressor() {
    return new LZ4Compressor(lz4Factory.fastCompressor());
  }

  public LZ4SafeDecompressor safeDecompressor() {
    return new LZ4SafeDecompressor(lz4Factory.safeDecompressor());
  }
}
