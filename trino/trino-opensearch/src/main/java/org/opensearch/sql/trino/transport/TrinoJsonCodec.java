/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.trino.transport;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.trino.execution.SplitAssignment;
import io.trino.execution.TaskInfo;
import io.trino.execution.buffer.OutputBuffers;
import io.trino.sql.planner.PlanFragment;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.List;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * Centralized JSON serialization/deserialization for Trino objects transported between OpenSearch
 * nodes.
 *
 * <p>Uses Trino's own {@link ObjectMapper} with all registered type serializers. This ensures
 * PlanFragment, SplitAssignment, OutputBuffers, and TaskInfo survive round-trip serialization
 * correctly — including all internal Trino types (expressions, types, operators, etc.).
 *
 * <p>The ObjectMapper must be obtained from the Trino engine's Guice injector AFTER shadow jar
 * relocation. Do NOT construct a plain ObjectMapper — it will lack Trino's type serializers.
 */
public class TrinoJsonCodec {

  private static final Logger LOG = LogManager.getLogger(TrinoJsonCodec.class);

  private final ObjectMapper objectMapper;

  /** Create with a provided ObjectMapper (must have Trino type serializers registered). */
  public TrinoJsonCodec(ObjectMapper objectMapper) {
    this.objectMapper = objectMapper;
  }

  /** Create with a default ObjectMapper. Suitable for basic serialization. */
  public TrinoJsonCodec() {
    this.objectMapper = new ObjectMapper();
  }

  public byte[] serializePlanFragment(PlanFragment fragment) {
    return serialize(fragment, "PlanFragment");
  }

  public PlanFragment deserializePlanFragment(byte[] json) {
    return deserialize(json, PlanFragment.class, "PlanFragment");
  }

  public byte[] serializeSplitAssignments(List<SplitAssignment> splits) {
    return serialize(splits, "SplitAssignments");
  }

  @SuppressWarnings("unchecked")
  public List<SplitAssignment> deserializeSplitAssignments(byte[] json) {
    try {
      return objectMapper.readValue(
          json,
          objectMapper
              .getTypeFactory()
              .constructCollectionType(List.class, SplitAssignment.class));
    } catch (IOException e) {
      throw new UncheckedIOException("Failed to deserialize SplitAssignments", e);
    }
  }

  public byte[] serializeOutputBuffers(OutputBuffers outputBuffers) {
    return serialize(outputBuffers, "OutputBuffers");
  }

  public OutputBuffers deserializeOutputBuffers(byte[] json) {
    return deserialize(json, OutputBuffers.class, "OutputBuffers");
  }

  public byte[] serializeTaskInfo(TaskInfo taskInfo) {
    return serialize(taskInfo, "TaskInfo");
  }

  public TaskInfo deserializeTaskInfo(byte[] json) {
    return deserialize(json, TaskInfo.class, "TaskInfo");
  }

  private byte[] serialize(Object obj, String typeName) {
    try {
      return objectMapper.writeValueAsBytes(obj);
    } catch (IOException e) {
      throw new UncheckedIOException("Failed to serialize " + typeName, e);
    }
  }

  private <T> T deserialize(byte[] json, Class<T> type, String typeName) {
    try {
      return objectMapper.readValue(json, type);
    } catch (IOException e) {
      throw new UncheckedIOException("Failed to deserialize " + typeName, e);
    }
  }
}
