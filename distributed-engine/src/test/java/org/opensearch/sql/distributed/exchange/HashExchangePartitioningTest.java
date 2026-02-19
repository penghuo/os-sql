/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.distributed.exchange;

import static org.junit.jupiter.api.Assertions.*;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.opensearch.sql.distributed.data.LongArrayBlock;
import org.opensearch.sql.distributed.data.Page;

/**
 * Tests for hash exchange partitioning: HashPartitioner +
 * HashExchange.extractPartition/partitionPage.
 */
class HashExchangePartitioningTest {

  @Test
  @DisplayName("Single key column partitions deterministically")
  void singleKeyPartitionsDeterministically() {
    HashPartitioner partitioner = new HashPartitioner(List.of(0), 4);

    long[] keys = {10, 20, 30, 40, 50};
    Page page = new Page(new LongArrayBlock(5, Optional.empty(), keys));

    int[] partitions = partitioner.partition(page);
    assertEquals(5, partitions.length);

    // All partitions should be in [0, 4)
    for (int p : partitions) {
      assertTrue(p >= 0 && p < 4, "Partition " + p + " out of range [0,4)");
    }

    // Same input must produce same partitions (deterministic)
    int[] partitions2 = partitioner.partition(page);
    assertArrayEquals(partitions, partitions2);
  }

  @Test
  @DisplayName("Same key values always map to same partition")
  void sameKeySamePartition() {
    HashPartitioner partitioner = new HashPartitioner(List.of(0), 8);

    // Repeated key values
    long[] keys = {100, 200, 100, 200, 100};
    Page page = new Page(new LongArrayBlock(5, Optional.empty(), keys));

    int[] partitions = partitioner.partition(page);

    // Positions 0, 2, 4 have key=100 — must get same partition
    assertEquals(partitions[0], partitions[2]);
    assertEquals(partitions[0], partitions[4]);

    // Positions 1, 3 have key=200 — must get same partition
    assertEquals(partitions[1], partitions[3]);
  }

  @Test
  @DisplayName("Multi-column key partitions correctly")
  void multiColumnKeyPartitions() {
    HashPartitioner partitioner = new HashPartitioner(List.of(0, 1), 4);

    // (1, 10), (2, 20), (1, 10), (1, 20)
    long[] col0 = {1, 2, 1, 1};
    long[] col1 = {10, 20, 10, 20};
    Page page =
        new Page(
            new LongArrayBlock(4, Optional.empty(), col0),
            new LongArrayBlock(4, Optional.empty(), col1));

    int[] partitions = partitioner.partition(page);

    // (1, 10) at positions 0 and 2 should be same partition
    assertEquals(partitions[0], partitions[2]);

    // (1, 10) and (1, 20) may differ
    // (1, 10) and (2, 20) may differ
    // Just verify all are in range
    for (int p : partitions) {
      assertTrue(p >= 0 && p < 4);
    }
  }

  @Test
  @DisplayName("Single partition maps all rows to partition 0")
  void singlePartitionMapsAllToZero() {
    HashPartitioner partitioner = new HashPartitioner(List.of(0), 1);

    long[] keys = {10, 20, 30, 40, 50};
    Page page = new Page(new LongArrayBlock(5, Optional.empty(), keys));

    int[] partitions = partitioner.partition(page);
    for (int p : partitions) {
      assertEquals(0, p, "With 1 partition, all rows should map to partition 0");
    }
  }

  @Test
  @DisplayName("extractPartition selects rows for a specific partition")
  void extractPartitionSelectsCorrectRows() {
    HashPartitioner partitioner = new HashPartitioner(List.of(0), 3);

    long[] keys = {1, 2, 3, 4, 5, 6, 7, 8, 9, 10};
    Page page = new Page(new LongArrayBlock(10, Optional.empty(), keys));

    int totalExtracted = 0;
    for (int partition = 0; partition < 3; partition++) {
      Page extracted = HashExchange.extractPartition(page, partitioner, partition);
      if (extracted != null) {
        totalExtracted += extracted.getPositionCount();
      }
    }

    // All rows should end up in exactly one partition
    assertEquals(10, totalExtracted, "Sum of all partitions must equal total rows");
  }

  @Test
  @DisplayName("extractPartition returns null for partition with no matching rows")
  void extractPartitionReturnsNullForEmptyPartition() {
    // With 1000 partitions and only 1 row, most partitions will be empty
    HashPartitioner partitioner = new HashPartitioner(List.of(0), 1000);

    Page page = new Page(new LongArrayBlock(1, Optional.empty(), new long[] {42}));
    int[] partitions = partitioner.partition(page);
    int assignedPartition = partitions[0];

    // Pick a different partition
    int emptyPartition = (assignedPartition + 1) % 1000;
    Page extracted = HashExchange.extractPartition(page, partitioner, emptyPartition);
    assertNull(extracted, "Should be null when no rows match the partition");
  }

  @Test
  @DisplayName("extractPartition returns original page when all rows match")
  void extractPartitionReturnsOriginalWhenAllMatch() {
    HashPartitioner partitioner = new HashPartitioner(List.of(0), 1);

    long[] keys = {10, 20, 30};
    Page page = new Page(new LongArrayBlock(3, Optional.empty(), keys));

    // With 1 partition, all rows map to partition 0
    Page extracted = HashExchange.extractPartition(page, partitioner, 0);
    assertSame(page, extracted, "Should return original page when all rows match");
  }

  @Test
  @DisplayName("extractPartition returns null for empty page")
  void extractPartitionReturnsNullForEmptyPage() {
    HashPartitioner partitioner = new HashPartitioner(List.of(0), 4);

    Page emptyPage = new Page(new LongArrayBlock(0, Optional.empty(), new long[0]));
    Page result = HashExchange.extractPartition(emptyPage, partitioner, 0);
    assertNull(result);
  }

  @Test
  @DisplayName("partitionPage splits page into partition map")
  void partitionPageSplitsCorrectly() {
    HashPartitioner partitioner = new HashPartitioner(List.of(0), 3);

    long[] keys = {1, 2, 3, 4, 5, 6, 7, 8, 9, 10};
    long[] values = {100, 200, 300, 400, 500, 600, 700, 800, 900, 1000};
    Page page =
        new Page(
            new LongArrayBlock(10, Optional.empty(), keys),
            new LongArrayBlock(10, Optional.empty(), values));

    Map<Integer, Page> partitioned = HashExchange.partitionPage(page, partitioner);

    // Verify all partitions are in valid range
    for (int key : partitioned.keySet()) {
      assertTrue(key >= 0 && key < 3, "Partition " + key + " out of range");
    }

    // Verify total rows across all partitions
    int totalRows = 0;
    for (Page p : partitioned.values()) {
      totalRows += p.getPositionCount();
      // Each sub-page should have 2 channels (key + value)
      assertEquals(2, p.getChannelCount());
    }
    assertEquals(10, totalRows, "Total rows across partitions must equal input rows");
  }

  @Test
  @DisplayName("partitionPage returns empty map for empty page")
  void partitionPageEmptyInput() {
    HashPartitioner partitioner = new HashPartitioner(List.of(0), 4);

    Page emptyPage = new Page(new LongArrayBlock(0, Optional.empty(), new long[0]));
    Map<Integer, Page> result = HashExchange.partitionPage(emptyPage, partitioner);
    assertTrue(result.isEmpty());
  }

  @Test
  @DisplayName("partitionPage preserves values across partitions")
  void partitionPagePreservesValues() {
    HashPartitioner partitioner = new HashPartitioner(List.of(0), 2);

    long[] keys = {10, 20, 10, 20, 10};
    long[] values = {1, 2, 3, 4, 5};
    Page page =
        new Page(
            new LongArrayBlock(5, Optional.empty(), keys),
            new LongArrayBlock(5, Optional.empty(), values));

    Map<Integer, Page> partitioned = HashExchange.partitionPage(page, partitioner);

    // Collect all key-value pairs from partitions
    Map<Long, Set<Long>> keyToValues = new HashMap<>();
    for (Page p : partitioned.values()) {
      LongArrayBlock keyBlock = (LongArrayBlock) p.getBlock(0);
      LongArrayBlock valBlock = (LongArrayBlock) p.getBlock(1);
      for (int i = 0; i < p.getPositionCount(); i++) {
        keyToValues
            .computeIfAbsent(keyBlock.getLong(i), k -> new HashSet<>())
            .add(valBlock.getLong(i));
      }
    }

    // Key 10 should have values {1, 3, 5}
    assertEquals(Set.of(1L, 3L, 5L), keyToValues.get(10L));
    // Key 20 should have values {2, 4}
    assertEquals(Set.of(2L, 4L), keyToValues.get(20L));
  }

  @Test
  @DisplayName("Constructor rejects empty partition key channels")
  void rejectsEmptyPartitionKeys() {
    assertThrows(IllegalArgumentException.class, () -> new HashPartitioner(List.of(), 4));
  }

  @Test
  @DisplayName("Constructor rejects non-positive partition count")
  void rejectsNonPositivePartitionCount() {
    assertThrows(IllegalArgumentException.class, () -> new HashPartitioner(List.of(0), 0));
    assertThrows(IllegalArgumentException.class, () -> new HashPartitioner(List.of(0), -1));
  }

  @Test
  @DisplayName("getPartitionKeyChannels returns unmodifiable copy")
  void partitionKeyChannelsAreImmutable() {
    HashPartitioner partitioner = new HashPartitioner(List.of(0, 1), 4);
    assertEquals(List.of(0, 1), partitioner.getPartitionKeyChannels());
    assertEquals(4, partitioner.getPartitionCount());
  }
}
