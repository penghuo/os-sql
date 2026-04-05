/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.trino.transport;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.IOException;
import org.junit.jupiter.api.Test;
import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.core.common.io.stream.StreamInput;

/** Verifies transport request/response types survive serialization round-trip. */
class TrinoTransportSerializationTest {

  // --- TrinoTaskUpdateRequest ---

  @Test
  void taskUpdateRequestRoundTrip() throws IOException {
    byte[] fragment = {0x01, 0x02, 0x03};
    byte[] splits = {0x04, 0x05};
    byte[] buffers = {0x06};
    byte[] session = {0x07, 0x08};

    TrinoTaskUpdateRequest original =
        new TrinoTaskUpdateRequest("query.0.0.0", fragment, splits, buffers, session);

    BytesStreamOutput out = new BytesStreamOutput();
    original.writeTo(out);
    StreamInput in = out.bytes().streamInput();
    TrinoTaskUpdateRequest deserialized = new TrinoTaskUpdateRequest(in);

    assertEquals("query.0.0.0", deserialized.getTaskId());
    assertArrayEquals(fragment, deserialized.getPlanFragmentJson());
    assertArrayEquals(splits, deserialized.getSplitAssignmentsJson());
    assertArrayEquals(buffers, deserialized.getOutputBuffersJson());
    assertArrayEquals(session, deserialized.getSessionJson());
  }

  @Test
  void taskUpdateRequestWithEmptyArrays() throws IOException {
    TrinoTaskUpdateRequest original =
        new TrinoTaskUpdateRequest("q.1.2.3", new byte[0], new byte[0], new byte[0], new byte[0]);

    BytesStreamOutput out = new BytesStreamOutput();
    original.writeTo(out);
    TrinoTaskUpdateRequest deserialized = new TrinoTaskUpdateRequest(out.bytes().streamInput());

    assertEquals("q.1.2.3", deserialized.getTaskId());
    assertEquals(0, deserialized.getPlanFragmentJson().length);
    assertEquals(0, deserialized.getSplitAssignmentsJson().length);
  }

  @Test
  void taskUpdateRequestWithLargePayload() throws IOException {
    // Simulate a realistic PlanFragment size (100KB)
    byte[] largeFragment = new byte[100 * 1024];
    for (int i = 0; i < largeFragment.length; i++) {
      largeFragment[i] = (byte) (i % 256);
    }

    TrinoTaskUpdateRequest original =
        new TrinoTaskUpdateRequest(
            "query.0.0.0", largeFragment, new byte[0], new byte[0], new byte[0]);

    BytesStreamOutput out = new BytesStreamOutput();
    original.writeTo(out);
    TrinoTaskUpdateRequest deserialized = new TrinoTaskUpdateRequest(out.bytes().streamInput());

    assertArrayEquals(largeFragment, deserialized.getPlanFragmentJson());
  }

  // --- TrinoTaskUpdateResponse ---

  @Test
  void taskUpdateResponseRoundTrip() throws IOException {
    byte[] taskInfo = "{\"state\":\"RUNNING\"}".getBytes();
    TrinoTaskUpdateResponse original = new TrinoTaskUpdateResponse(taskInfo);

    BytesStreamOutput out = new BytesStreamOutput();
    original.writeTo(out);
    TrinoTaskUpdateResponse deserialized = new TrinoTaskUpdateResponse(out.bytes().streamInput());

    assertArrayEquals(taskInfo, deserialized.getTaskInfoJson());
  }

  // --- TrinoTaskStatusRequest ---

  @Test
  void taskStatusRequestRoundTrip() throws IOException {
    TrinoTaskStatusRequest original = new TrinoTaskStatusRequest("query.0.1.0");

    BytesStreamOutput out = new BytesStreamOutput();
    original.writeTo(out);
    TrinoTaskStatusRequest deserialized = new TrinoTaskStatusRequest(out.bytes().streamInput());

    assertEquals("query.0.1.0", deserialized.getTaskId());
  }

  // --- TrinoTaskStatusResponse ---

  @Test
  void taskStatusResponseRoundTrip() throws IOException {
    byte[] taskInfo = "{\"state\":\"FINISHED\"}".getBytes();
    TrinoTaskStatusResponse original = new TrinoTaskStatusResponse(taskInfo);

    BytesStreamOutput out = new BytesStreamOutput();
    original.writeTo(out);
    TrinoTaskStatusResponse deserialized = new TrinoTaskStatusResponse(out.bytes().streamInput());

    assertArrayEquals(taskInfo, deserialized.getTaskInfoJson());
  }

  // --- TrinoTaskCancelRequest ---

  @Test
  void taskCancelRequestRoundTrip() throws IOException {
    TrinoTaskCancelRequest original = new TrinoTaskCancelRequest("query.0.2.0");

    BytesStreamOutput out = new BytesStreamOutput();
    original.writeTo(out);
    TrinoTaskCancelRequest deserialized = new TrinoTaskCancelRequest(out.bytes().streamInput());

    assertEquals("query.0.2.0", deserialized.getTaskId());
  }

  // --- TrinoTaskCancelResponse ---

  @Test
  void taskCancelResponseRoundTrip() throws IOException {
    byte[] taskInfo = "{\"state\":\"CANCELED\"}".getBytes();
    TrinoTaskCancelResponse original = new TrinoTaskCancelResponse(taskInfo);

    BytesStreamOutput out = new BytesStreamOutput();
    original.writeTo(out);
    TrinoTaskCancelResponse deserialized = new TrinoTaskCancelResponse(out.bytes().streamInput());

    assertArrayEquals(taskInfo, deserialized.getTaskInfoJson());
  }
}
