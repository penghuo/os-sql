/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.trino.transport;

import java.io.IOException;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.action.ActionRequest;
import org.opensearch.action.ActionRequestValidationException;

/**
 * Request to register a node's Trino HTTP URL. Sent from a starting node to all other nodes so
 * they can construct correct InternalNode URIs for exchange.
 */
public class TrinoNodeRegisterRequest extends ActionRequest {

  @Override
  public ActionRequestValidationException validate() {
    return null;
  }

  private final String nodeId;
  private final String trinoHttpUrl;

  public TrinoNodeRegisterRequest(String nodeId, String trinoHttpUrl) {
    this.nodeId = nodeId;
    this.trinoHttpUrl = trinoHttpUrl;
  }

  public TrinoNodeRegisterRequest(StreamInput in) throws IOException {
    super(in);
    this.nodeId = in.readString();
    this.trinoHttpUrl = in.readString();
  }

  @Override
  public void writeTo(StreamOutput out) throws IOException {
    super.writeTo(out);
    out.writeString(nodeId);
    out.writeString(trinoHttpUrl);
  }

  public String getNodeId() {
    return nodeId;
  }

  public String getTrinoHttpUrl() {
    return trinoHttpUrl;
  }
}
