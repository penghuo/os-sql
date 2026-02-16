/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.planner.distributed;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class ExchangeSpecTest {

  private static final long DEFAULT_BUFFER_SIZE = 10L * 1024 * 1024;

  @Test
  @DisplayName("gather() should create a GATHER exchange with no keys or sort fields")
  void gatherCreatesCorrectSpec() {
    ExchangeSpec spec = ExchangeSpec.gather();
    assertEquals(ExchangeType.GATHER, spec.getType());
    assertTrue(spec.getOrderByFields().isEmpty());
    assertTrue(spec.getPartitionKeys().isEmpty());
    assertEquals(DEFAULT_BUFFER_SIZE, spec.getBufferSizeBytes());
  }

  @Test
  @DisplayName("hash() should create a HASH exchange with partition keys")
  void hashCreatesCorrectSpec() {
    List<String> keys = Arrays.asList("user_id", "region");
    ExchangeSpec spec = ExchangeSpec.hash(keys);
    assertEquals(ExchangeType.HASH, spec.getType());
    assertTrue(spec.getOrderByFields().isEmpty());
    assertEquals(keys, spec.getPartitionKeys());
    assertEquals(DEFAULT_BUFFER_SIZE, spec.getBufferSizeBytes());
  }

  @Test
  @DisplayName("orderedGather() should create a GATHER exchange with sort fields")
  void orderedGatherCreatesCorrectSpec() {
    List<String> sortFields = Arrays.asList("timestamp", "id");
    ExchangeSpec spec = ExchangeSpec.orderedGather(sortFields);
    assertEquals(ExchangeType.GATHER, spec.getType());
    assertEquals(sortFields, spec.getOrderByFields());
    assertTrue(spec.getPartitionKeys().isEmpty());
    assertEquals(DEFAULT_BUFFER_SIZE, spec.getBufferSizeBytes());
  }

  @Test
  @DisplayName("broadcast() should create a BROADCAST exchange with no keys")
  void broadcastCreatesCorrectSpec() {
    ExchangeSpec spec = ExchangeSpec.broadcast();
    assertEquals(ExchangeType.BROADCAST, spec.getType());
    assertTrue(spec.getOrderByFields().isEmpty());
    assertTrue(spec.getPartitionKeys().isEmpty());
    assertEquals(DEFAULT_BUFFER_SIZE, spec.getBufferSizeBytes());
  }

  @Test
  @DisplayName("custom ExchangeSpec should preserve all fields")
  void customSpecPreservesFields() {
    List<String> orderByFields = Collections.singletonList("ts");
    List<String> partitionKeys = Collections.singletonList("key");
    long bufferSize = 5L * 1024 * 1024;
    ExchangeSpec spec = new ExchangeSpec(ExchangeType.HASH, orderByFields, partitionKeys, bufferSize);
    assertEquals(ExchangeType.HASH, spec.getType());
    assertEquals(orderByFields, spec.getOrderByFields());
    assertEquals(partitionKeys, spec.getPartitionKeys());
    assertEquals(bufferSize, spec.getBufferSizeBytes());
  }

  @Test
  @DisplayName("equals and hashCode should work correctly for identical specs")
  void equalsAndHashCode() {
    ExchangeSpec spec1 = ExchangeSpec.gather();
    ExchangeSpec spec2 = ExchangeSpec.gather();
    assertEquals(spec1, spec2);
    assertEquals(spec1.hashCode(), spec2.hashCode());
  }
}
