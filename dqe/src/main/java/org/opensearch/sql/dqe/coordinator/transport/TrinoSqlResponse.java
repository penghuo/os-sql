/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.dqe.coordinator.transport;

import java.io.IOException;
import lombok.Getter;
import org.opensearch.core.action.ActionResponse;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;

/**
 * Transport response carrying the result of a DQE Trino SQL query execution. The result is a
 * formatted string (typically JSON) along with the appropriate content type.
 */
@Getter
public class TrinoSqlResponse extends ActionResponse {

  /** The formatted query result (JSON string). */
  private final String result;

  /** The content type of the result. */
  private final String contentType;

  /** Create a response with the default JSON content type. */
  public TrinoSqlResponse(String result) {
    this.result = result;
    this.contentType = "application/json; charset=UTF-8";
  }

  /** Create a response with a custom content type. */
  public TrinoSqlResponse(String result, String contentType) {
    this.result = result;
    this.contentType = contentType;
  }

  /** Deserialize from a stream. */
  public TrinoSqlResponse(StreamInput in) throws IOException {
    super(in);
    this.result = in.readString();
    this.contentType = in.readString();
  }

  @Override
  public void writeTo(StreamOutput out) throws IOException {
    out.writeString(result);
    out.writeString(contentType);
  }
}
