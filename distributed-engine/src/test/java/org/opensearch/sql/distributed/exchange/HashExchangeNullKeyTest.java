/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.distributed.exchange;

import static org.junit.jupiter.api.Assertions.*;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.opensearch.sql.distributed.data.LongArrayBlock;
import org.opensearch.sql.distributed.data.Page;

/** Tests hash exchange partitioning with null keys. */
class HashExchangeNullKeyTest {

  @Test
  @DisplayName("Null keys are partitioned deterministically")
  void nullKeysPartitionDeterministically() {
    HashPartitioner partitioner = new HashPartitioner(List.of(0), 4);

    // 3 rows: key=10, key=null, key=30
    long[] values = {10, 0, 30};
    boolean[] nulls = {false, true, false};
    Page page = new Page(new LongArrayBlock(3, Optional.of(nulls), values));

    int[] partitions = partitioner.partition(page);
    assertEquals(3, partitions.length);

    for (int p : partitions) {
      assertTrue(p >= 0 && p < 4, "All partitions should be in [0, 4)");
    }

    // Same input must produce same result
    int[] partitions2 = partitioner.partition(page);
    assertArrayEquals(partitions, partitions2, "Null key partitioning must be deterministic");
  }

  @Test
  @DisplayName("All null keys map to the same partition")
  void allNullKeysSamePartition() {
    HashPartitioner partitioner = new HashPartitioner(List.of(0), 8);

    // 5 rows all with null keys
    long[] values = new long[5];
    boolean[] nulls = {true, true, true, true, true};
    Page page = new Page(new LongArrayBlock(5, Optional.of(nulls), values));

    int[] partitions = partitioner.partition(page);

    // All null keys should hash to the same partition
    int expected = partitions[0];
    for (int i = 1; i < partitions.length; i++) {
      assertEquals(expected, partitions[i], "All null keys should hash to the same partition");
    }
  }

  @Test
  @DisplayName("Null keys included in extractPartition results")
  void nullKeysIncludedInExtractPartition() {
    HashPartitioner partitioner = new HashPartitioner(List.of(0), 4);

    // Mix of null and non-null keys
    long[] values = {10, 0, 30, 0, 50};
    boolean[] nulls = {false, true, false, true, false};
    Page page = new Page(new LongArrayBlock(5, Optional.of(nulls), values));

    // Extract all partitions and count total
    int totalExtracted = 0;
    for (int p = 0; p < 4; p++) {
      Page extracted = HashExchange.extractPartition(page, partitioner, p);
      if (extracted != null) {
        totalExtracted += extracted.getPositionCount();
      }
    }

    assertEquals(5, totalExtracted, "All rows including nulls should be partitioned");
  }

  @Test
  @DisplayName("Null keys included in partitionPage results")
  void nullKeysIncludedInPartitionPage() {
    HashPartitioner partitioner = new HashPartitioner(List.of(0), 4);

    // Mix of null and non-null keys
    long[] values = {10, 0, 30, 0, 50};
    boolean[] nulls = {false, true, false, true, false};
    Page page = new Page(new LongArrayBlock(5, Optional.of(nulls), values));

    Map<Integer, Page> partitioned = HashExchange.partitionPage(page, partitioner);

    int totalRows = partitioned.values().stream().mapToInt(Page::getPositionCount).sum();
    assertEquals(5, totalRows, "All rows including nulls should appear in partition map");
  }

  @Test
  @DisplayName("Multi-column key with one null column")
  void multiColumnKeyWithPartialNull() {
    HashPartitioner partitioner = new HashPartitioner(List.of(0, 1), 4);

    // col0: non-null, col1: has nulls
    long[] col0 = {1, 2, 3};
    long[] col1 = {10, 0, 30};
    boolean[] col1Nulls = {false, true, false};
    Page page =
        new Page(
            new LongArrayBlock(3, Optional.empty(), col0),
            new LongArrayBlock(3, Optional.of(col1Nulls), col1));

    int[] partitions = partitioner.partition(page);
    assertEquals(3, partitions.length);

    for (int p : partitions) {
      assertTrue(p >= 0 && p < 4);
    }
  }

  @Test
  @DisplayName("Multi-column key with all columns null")
  void multiColumnKeyAllNull() {
    HashPartitioner partitioner = new HashPartitioner(List.of(0, 1), 4);

    long[] col0 = {0, 0};
    long[] col1 = {0, 0};
    boolean[] allNulls = {true, true};
    Page page =
        new Page(
            new LongArrayBlock(2, Optional.of(allNulls), col0),
            new LongArrayBlock(2, Optional.of(allNulls), col1));

    int[] partitions = partitioner.partition(page);

    // Both rows have identical (null, null) keys — same partition
    assertEquals(partitions[0], partitions[1]);
  }
}
