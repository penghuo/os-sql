/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.distributed.exchange;

import com.google.common.hash.HashFunction;
import com.google.common.hash.Hasher;
import com.google.common.hash.Hashing;
import java.util.List;
import org.opensearch.sql.distributed.data.Block;
import org.opensearch.sql.distributed.data.BooleanArrayBlock;
import org.opensearch.sql.distributed.data.ByteArrayBlock;
import org.opensearch.sql.distributed.data.DictionaryBlock;
import org.opensearch.sql.distributed.data.DoubleArrayBlock;
import org.opensearch.sql.distributed.data.IntArrayBlock;
import org.opensearch.sql.distributed.data.LongArrayBlock;
import org.opensearch.sql.distributed.data.Page;
import org.opensearch.sql.distributed.data.ShortArrayBlock;
import org.opensearch.sql.distributed.data.VariableWidthBlock;

/**
 * Computes partition assignment for each row in a Page based on key columns using murmur3 hash.
 * Used by {@link HashExchange} to determine which target node should receive each row.
 */
public class HashPartitioner {

  @SuppressWarnings("deprecation")
  private static final HashFunction MURMUR3 = Hashing.murmur3_128();

  private final List<Integer> partitionKeyChannels;
  private final int partitionCount;

  /**
   * Creates a HashPartitioner.
   *
   * @param partitionKeyChannels the column indices to hash on
   * @param partitionCount the number of partitions (target nodes)
   */
  public HashPartitioner(List<Integer> partitionKeyChannels, int partitionCount) {
    if (partitionKeyChannels == null || partitionKeyChannels.isEmpty()) {
      throw new IllegalArgumentException("partitionKeyChannels must not be empty");
    }
    if (partitionCount <= 0) {
      throw new IllegalArgumentException("partitionCount must be positive");
    }
    this.partitionKeyChannels = List.copyOf(partitionKeyChannels);
    this.partitionCount = partitionCount;
  }

  /**
   * Computes the partition assignment for each position in the page.
   *
   * @param page the input page
   * @return array of partition IDs (0..partitionCount-1), one per position
   */
  public int[] partition(Page page) {
    int positionCount = page.getPositionCount();
    int[] partitions = new int[positionCount];

    for (int position = 0; position < positionCount; position++) {
      partitions[position] = getPartition(page, position);
    }

    return partitions;
  }

  /**
   * Computes the partition for a single position.
   *
   * @param page the input page
   * @param position the row position
   * @return partition ID (0..partitionCount-1)
   */
  public int getPartition(Page page, int position) {
    Hasher hasher = MURMUR3.newHasher();

    for (int channel : partitionKeyChannels) {
      Block block = page.getBlock(channel);
      hashBlock(hasher, block, position);
    }

    // Use absolute value and modulo to get partition
    int hash = hasher.hash().asInt();
    return Math.floorMod(hash, partitionCount);
  }

  private static void hashBlock(Hasher hasher, Block block, int position) {
    if (block.isNull(position)) {
      hasher.putInt(0);
      return;
    }

    // Unwrap dictionary/RLE blocks to get to the value
    if (block instanceof DictionaryBlock dict) {
      hashBlock(hasher, dict.getDictionary(), dict.getId(position));
      return;
    }
    if (block instanceof org.opensearch.sql.distributed.data.RunLengthEncodedBlock rle) {
      hashBlock(hasher, rle.getValue(), 0);
      return;
    }

    // Hash based on value type
    if (block instanceof LongArrayBlock longBlock) {
      hasher.putLong(longBlock.getLong(position));
    } else if (block instanceof IntArrayBlock intBlock) {
      hasher.putInt(intBlock.getInt(position));
    } else if (block instanceof DoubleArrayBlock doubleBlock) {
      hasher.putDouble(doubleBlock.getDouble(position));
    } else if (block instanceof ByteArrayBlock byteBlock) {
      hasher.putByte(byteBlock.getByte(position));
    } else if (block instanceof ShortArrayBlock shortBlock) {
      hasher.putShort(shortBlock.getShort(position));
    } else if (block instanceof BooleanArrayBlock boolBlock) {
      hasher.putBoolean(boolBlock.getBoolean(position));
    } else if (block instanceof VariableWidthBlock varBlock) {
      byte[] data = varBlock.getSlice(position);
      hasher.putInt(data.length);
      hasher.putBytes(data);
    } else {
      throw new UnsupportedOperationException(
          "Cannot hash block type: " + block.getClass().getSimpleName());
    }
  }

  public List<Integer> getPartitionKeyChannels() {
    return partitionKeyChannels;
  }

  public int getPartitionCount() {
    return partitionCount;
  }
}
