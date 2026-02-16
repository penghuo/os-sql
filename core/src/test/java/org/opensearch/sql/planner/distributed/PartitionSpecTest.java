/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.planner.distributed;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.Arrays;
import java.util.List;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class PartitionSpecTest {

  @Test
  @DisplayName("PartitionSpec should preserve partition keys and count")
  void constructorPreservesFields() {
    List<String> keys = Arrays.asList("user_id", "region");
    PartitionSpec spec = new PartitionSpec(keys, 8);
    assertEquals(keys, spec.getPartitionKeys());
    assertEquals(8, spec.getNumPartitions());
  }

  @Test
  @DisplayName("equals and hashCode should work correctly")
  void equalsAndHashCode() {
    List<String> keys = Arrays.asList("a", "b");
    PartitionSpec spec1 = new PartitionSpec(keys, 4);
    PartitionSpec spec2 = new PartitionSpec(keys, 4);
    assertEquals(spec1, spec2);
    assertEquals(spec1.hashCode(), spec2.hashCode());
  }

  @Test
  @DisplayName("toString should contain field values")
  void toStringContainsFields() {
    PartitionSpec spec = new PartitionSpec(Arrays.asList("key1"), 3);
    String str = spec.toString();
    assertEquals(true, str.contains("key1"));
    assertEquals(true, str.contains("3"));
  }
}
