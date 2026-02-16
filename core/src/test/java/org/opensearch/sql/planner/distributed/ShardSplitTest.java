/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.planner.distributed;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.opensearch.sql.storage.split.Split;

class ShardSplitTest {

  @Test
  @DisplayName("ShardSplit should implement Split interface")
  void implementsSplitInterface() {
    ShardSplit split = createSplit("test_index", 0, "node1", true);
    assertTrue(split instanceof Split);
  }

  @Test
  @DisplayName("getSplitId should return indexName:shardId format")
  void getSplitIdFormat() {
    ShardSplit split = createSplit("my_index", 3, "node1", false);
    assertEquals("my_index:3", split.getSplitId());
  }

  @Test
  @DisplayName("constructor should preserve all fields")
  void constructorPreservesFields() {
    List<String> replicas = Arrays.asList("node2", "node3");
    ShardSplit split = new ShardSplit("orders", 5, "node1", replicas, true);
    assertEquals("orders", split.getIndexName());
    assertEquals(5, split.getShardId());
    assertEquals("node1", split.getPreferredNodeId());
    assertEquals(replicas, split.getReplicaNodeIds());
    assertTrue(split.isLocal());
  }

  @Test
  @DisplayName("isLocal should be false when shard is on a remote node")
  void isLocalFalseForRemoteShard() {
    ShardSplit split = createSplit("test", 0, "remote_node", false);
    assertFalse(split.isLocal());
  }

  @Test
  @DisplayName("equals should compare all fields")
  void equalsComparesAllFields() {
    ShardSplit split1 = createSplit("idx", 1, "node1", true);
    ShardSplit split2 = createSplit("idx", 1, "node1", true);
    ShardSplit split3 = createSplit("idx", 2, "node1", true);
    assertEquals(split1, split2);
    assertNotEquals(split1, split3);
  }

  @Test
  @DisplayName("hashCode should be consistent with equals")
  void hashCodeConsistentWithEquals() {
    ShardSplit split1 = createSplit("idx", 1, "node1", true);
    ShardSplit split2 = createSplit("idx", 1, "node1", true);
    assertEquals(split1.hashCode(), split2.hashCode());
  }

  @Test
  @DisplayName("toString should contain meaningful information")
  void toStringContainsFields() {
    ShardSplit split = createSplit("products", 7, "nodeA", false);
    String str = split.toString();
    assertTrue(str.contains("products"));
    assertTrue(str.contains("7"));
    assertTrue(str.contains("nodeA"));
  }

  private ShardSplit createSplit(String index, int shard, String preferredNode, boolean local) {
    return new ShardSplit(index, shard, preferredNode, Collections.emptyList(), local);
  }
}
