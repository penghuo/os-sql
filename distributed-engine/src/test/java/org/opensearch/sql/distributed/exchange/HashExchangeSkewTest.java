/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.distributed.exchange;

import static org.junit.jupiter.api.Assertions.*;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.opensearch.sql.distributed.data.LongArrayBlock;
import org.opensearch.sql.distributed.data.Page;

/** Tests hash exchange behavior with skewed key distributions. */
class HashExchangeSkewTest {

  @Test
  @DisplayName("Heavily skewed distribution: 90% of rows have same key")
  void heavilySkewedDistribution() {
    HashPartitioner partitioner = new HashPartitioner(List.of(0), 4);

    // 90 rows with key=1, 10 rows with key=2..11
    long[] keys = new long[100];
    for (int i = 0; i < 90; i++) {
      keys[i] = 1;
    }
    for (int i = 90; i < 100; i++) {
      keys[i] = i - 88; // 2..11
    }
    Page page = new Page(new LongArrayBlock(100, Optional.empty(), keys));

    Map<Integer, Page> partitioned = HashExchange.partitionPage(page, partitioner);

    // All partitions should receive data (at least the skewed key's partition + others)
    int totalRows = 0;
    for (Page p : partitioned.values()) {
      assertTrue(p.getPositionCount() > 0);
      totalRows += p.getPositionCount();
    }
    assertEquals(100, totalRows);
  }

  @Test
  @DisplayName("All rows have the same key: all go to one partition")
  void allSameKey() {
    HashPartitioner partitioner = new HashPartitioner(List.of(0), 4);

    long[] keys = new long[50];
    for (int i = 0; i < 50; i++) {
      keys[i] = 42;
    }
    Page page = new Page(new LongArrayBlock(50, Optional.empty(), keys));

    Map<Integer, Page> partitioned = HashExchange.partitionPage(page, partitioner);

    // Only one partition should have data
    assertEquals(1, partitioned.size(), "All same keys should map to exactly one partition");
    Page onlyPartition = partitioned.values().iterator().next();
    assertEquals(50, onlyPartition.getPositionCount());
  }

  @Test
  @DisplayName("Uniform distribution spreads across partitions")
  void uniformDistribution() {
    HashPartitioner partitioner = new HashPartitioner(List.of(0), 4);

    // 1000 distinct keys should spread somewhat evenly
    long[] keys = new long[1000];
    for (int i = 0; i < 1000; i++) {
      keys[i] = i;
    }
    Page page = new Page(new LongArrayBlock(1000, Optional.empty(), keys));

    Map<Integer, Page> partitioned = HashExchange.partitionPage(page, partitioner);

    // All 4 partitions should have data
    assertEquals(
        4, partitioned.size(), "All partitions should receive data with 1000 distinct keys");

    // Each partition should have roughly 250 rows (allow variance)
    for (Map.Entry<Integer, Page> entry : partitioned.entrySet()) {
      int count = entry.getValue().getPositionCount();
      assertTrue(count > 100, "Partition " + entry.getKey() + " has too few rows: " + count);
      assertTrue(count < 500, "Partition " + entry.getKey() + " has too many rows: " + count);
    }
  }

  @Test
  @DisplayName("Two distinct keys distribute to at most 2 partitions")
  void twoDistinctKeys() {
    HashPartitioner partitioner = new HashPartitioner(List.of(0), 8);

    long[] keys = new long[100];
    for (int i = 0; i < 100; i++) {
      keys[i] = i % 2 == 0 ? 100 : 200;
    }
    Page page = new Page(new LongArrayBlock(100, Optional.empty(), keys));

    Map<Integer, Page> partitioned = HashExchange.partitionPage(page, partitioner);

    // At most 2 partitions should have data (could be 1 if both hash to same partition)
    assertTrue(partitioned.size() <= 2, "Two keys should use at most 2 partitions");

    int totalRows = partitioned.values().stream().mapToInt(Page::getPositionCount).sum();
    assertEquals(100, totalRows);
  }

  @Test
  @DisplayName("Large page with many partitions distributes all rows")
  void largePageManyPartitions() {
    HashPartitioner partitioner = new HashPartitioner(List.of(0), 16);

    long[] keys = new long[10_000];
    for (int i = 0; i < 10_000; i++) {
      keys[i] = i;
    }
    Page page = new Page(new LongArrayBlock(10_000, Optional.empty(), keys));

    Map<Integer, Page> partitioned = HashExchange.partitionPage(page, partitioner);

    // All 16 partitions should receive data with 10K distinct keys
    assertEquals(16, partitioned.size());

    int totalRows = partitioned.values().stream().mapToInt(Page::getPositionCount).sum();
    assertEquals(10_000, totalRows);
  }

  @Test
  @DisplayName("extractPartition with skewed data produces correct counts")
  void extractPartitionSkewedData() {
    HashPartitioner partitioner = new HashPartitioner(List.of(0), 4);

    // All rows have key=7
    long[] keys = new long[200];
    for (int i = 0; i < 200; i++) {
      keys[i] = 7;
    }
    Page page = new Page(new LongArrayBlock(200, Optional.empty(), keys));

    // Only one partition should get all 200 rows
    Map<Integer, Integer> partitionCounts = new HashMap<>();
    for (int p = 0; p < 4; p++) {
      Page extracted = HashExchange.extractPartition(page, partitioner, p);
      partitionCounts.put(p, extracted == null ? 0 : extracted.getPositionCount());
    }

    int nonEmpty = 0;
    int totalRows = 0;
    for (int count : partitionCounts.values()) {
      if (count > 0) nonEmpty++;
      totalRows += count;
    }
    assertEquals(1, nonEmpty, "All same keys should land in exactly one partition");
    assertEquals(200, totalRows);
  }
}
