/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dqe.exchange.action;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.core.common.io.stream.StreamInput;

class DqeStageExecuteRequestTests {

  @Test
  @DisplayName("serialization round-trip preserves all fields")
  void serializationRoundTrip() throws IOException {
    byte[] planFragment = {10, 20, 30};
    List<DqeStageExecuteRequest.ShardSplitInfo> splits =
        Arrays.asList(
            new DqeStageExecuteRequest.ShardSplitInfo(0, "node-1", "my_index", true),
            new DqeStageExecuteRequest.ShardSplitInfo(1, "node-1", "my_index", false),
            new DqeStageExecuteRequest.ShardSplitInfo(2, "node-2", "my_index", true));

    DqeStageExecuteRequest original =
        new DqeStageExecuteRequest(
            "query-789", 0, planFragment, splits, "coordinator-node", 256L * 1024 * 1024);

    BytesStreamOutput out = new BytesStreamOutput();
    original.writeTo(out);

    StreamInput in = out.bytes().streamInput();
    DqeStageExecuteRequest deserialized = new DqeStageExecuteRequest(in);

    assertEquals("query-789", deserialized.getQueryId());
    assertEquals(0, deserialized.getStageId());
    assertArrayEquals(planFragment, deserialized.getSerializedPlanFragment());
    assertEquals("coordinator-node", deserialized.getCoordinatorNodeId());
    assertEquals(256L * 1024 * 1024, deserialized.getQueryMemoryBudgetBytes());

    List<DqeStageExecuteRequest.ShardSplitInfo> deserializedSplits =
        deserialized.getShardSplitInfos();
    assertEquals(3, deserializedSplits.size());

    assertEquals(0, deserializedSplits.get(0).getShardId());
    assertEquals("node-1", deserializedSplits.get(0).getNodeId());
    assertEquals("my_index", deserializedSplits.get(0).getIndexName());
    assertTrue(deserializedSplits.get(0).isPrimary());

    assertEquals(1, deserializedSplits.get(1).getShardId());
    assertFalse(deserializedSplits.get(1).isPrimary());

    assertEquals(2, deserializedSplits.get(2).getShardId());
    assertEquals("node-2", deserializedSplits.get(2).getNodeId());
  }

  @Test
  @DisplayName("serialization round-trip with empty shard splits")
  void serializationRoundTripEmptySplits() throws IOException {
    DqeStageExecuteRequest original =
        new DqeStageExecuteRequest(
            "query-empty", 1, new byte[] {1}, Collections.emptyList(), "coord", 1024L);

    BytesStreamOutput out = new BytesStreamOutput();
    original.writeTo(out);

    StreamInput in = out.bytes().streamInput();
    DqeStageExecuteRequest deserialized = new DqeStageExecuteRequest(in);

    assertEquals(0, deserialized.getShardSplitInfos().size());
  }

  @Test
  @DisplayName("shard split list is unmodifiable")
  void shardSplitListIsUnmodifiable() {
    DqeStageExecuteRequest request =
        new DqeStageExecuteRequest(
            "q",
            0,
            new byte[0],
            Arrays.asList(new DqeStageExecuteRequest.ShardSplitInfo(0, "n", "i", true)),
            "c",
            1024L);

    assertThrows(
        UnsupportedOperationException.class,
        () ->
            request
                .getShardSplitInfos()
                .add(new DqeStageExecuteRequest.ShardSplitInfo(1, "n2", "i2", false)));
  }

  @Test
  @DisplayName("constructor rejects null queryId")
  void constructorRejectsNullQueryId() {
    assertThrows(
        NullPointerException.class,
        () ->
            new DqeStageExecuteRequest(null, 0, new byte[0], Collections.emptyList(), "c", 1024L));
  }

  @Test
  @DisplayName("constructor rejects null plan fragment")
  void constructorRejectsNullPlanFragment() {
    assertThrows(
        NullPointerException.class,
        () -> new DqeStageExecuteRequest("q", 0, null, Collections.emptyList(), "c", 1024L));
  }

  @Test
  @DisplayName("constructor rejects null shard splits")
  void constructorRejectsNullShardSplits() {
    assertThrows(
        NullPointerException.class,
        () -> new DqeStageExecuteRequest("q", 0, new byte[0], null, "c", 1024L));
  }

  @Test
  @DisplayName("constructor rejects null coordinator node")
  void constructorRejectsNullCoordinatorNode() {
    assertThrows(
        NullPointerException.class,
        () ->
            new DqeStageExecuteRequest("q", 0, new byte[0], Collections.emptyList(), null, 1024L));
  }

  @Test
  @DisplayName("ShardSplitInfo serialization round-trip")
  void shardSplitInfoSerializationRoundTrip() throws IOException {
    DqeStageExecuteRequest.ShardSplitInfo original =
        new DqeStageExecuteRequest.ShardSplitInfo(5, "data-node-3", "logs-2024", false);

    BytesStreamOutput out = new BytesStreamOutput();
    original.writeTo(out);

    StreamInput in = out.bytes().streamInput();
    DqeStageExecuteRequest.ShardSplitInfo deserialized =
        new DqeStageExecuteRequest.ShardSplitInfo(in);

    assertEquals(5, deserialized.getShardId());
    assertEquals("data-node-3", deserialized.getNodeId());
    assertEquals("logs-2024", deserialized.getIndexName());
    assertFalse(deserialized.isPrimary());
  }
}
