/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dqe.execution.plan;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Objects;
import org.opensearch.dqe.parser.DqeErrorCode;
import org.opensearch.dqe.parser.DqeException;

/**
 * Serializable plan fragment sent from coordinator to data nodes. Contains the information needed
 * for a data node to re-parse and execute its portion of a query.
 */
public class PlanFragment {

  private static final int SERIALIZATION_VERSION = 1;

  private final String queryText;
  private final String queryId;
  private final long pitKeepAliveSeconds;
  private final int batchSize;

  /**
   * Create a plan fragment.
   *
   * @param queryText SQL text for data nodes to re-parse
   * @param queryId query identifier for tracking/logging
   * @param pitKeepAliveSeconds PIT keep-alive duration in seconds
   * @param batchSize scan batch size
   */
  public PlanFragment(String queryText, String queryId, long pitKeepAliveSeconds, int batchSize) {
    this.queryText = Objects.requireNonNull(queryText, "queryText must not be null");
    this.queryId = Objects.requireNonNull(queryId, "queryId must not be null");
    this.pitKeepAliveSeconds = pitKeepAliveSeconds;
    this.batchSize = batchSize;
  }

  public String getQueryText() {
    return queryText;
  }

  public String getQueryId() {
    return queryId;
  }

  public long getPitKeepAliveSeconds() {
    return pitKeepAliveSeconds;
  }

  public int getBatchSize() {
    return batchSize;
  }

  /** Serialize this plan fragment to a byte array. */
  public byte[] serialize() {
    try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DataOutputStream out = new DataOutputStream(baos)) {
      out.writeInt(SERIALIZATION_VERSION);
      out.writeUTF(queryText);
      out.writeUTF(queryId);
      out.writeLong(pitKeepAliveSeconds);
      out.writeInt(batchSize);
      out.flush();
      return baos.toByteArray();
    } catch (IOException e) {
      throw new DqeException(
          "Failed to serialize PlanFragment: " + e.getMessage(), DqeErrorCode.EXECUTION_ERROR, e);
    }
  }

  /**
   * Deserialize a plan fragment from a byte array.
   *
   * @param data the serialized bytes
   * @return the deserialized PlanFragment
   */
  public static PlanFragment deserialize(byte[] data) {
    Objects.requireNonNull(data, "data must not be null");
    try (ByteArrayInputStream bais = new ByteArrayInputStream(data);
        DataInputStream in = new DataInputStream(bais)) {
      int version = in.readInt();
      if (version != SERIALIZATION_VERSION) {
        throw new DqeException(
            "Unsupported PlanFragment version: " + version, DqeErrorCode.EXECUTION_ERROR);
      }
      String queryText = in.readUTF();
      String queryId = in.readUTF();
      long pitKeepAliveSeconds = in.readLong();
      int batchSize = in.readInt();
      return new PlanFragment(queryText, queryId, pitKeepAliveSeconds, batchSize);
    } catch (IOException e) {
      throw new DqeException(
          "Failed to deserialize PlanFragment: " + e.getMessage(), DqeErrorCode.EXECUTION_ERROR, e);
    }
  }
}
