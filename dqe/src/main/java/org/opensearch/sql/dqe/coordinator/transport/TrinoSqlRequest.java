/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.dqe.coordinator.transport;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import lombok.Getter;
import org.opensearch.action.ActionRequest;
import org.opensearch.action.ActionRequestValidationException;
import org.opensearch.core.common.io.stream.InputStreamStreamInput;
import org.opensearch.core.common.io.stream.OutputStreamStreamOutput;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;

/**
 * Transport request carrying a Trino SQL query string and a flag indicating whether the request is
 * an explain-only request. Serializable via the OpenSearch Writeable protocol.
 */
@Getter
public class TrinoSqlRequest extends ActionRequest {

  /** The SQL query text. */
  private final String query;

  /** Whether this is an explain request (returns the plan rather than executing). */
  private final boolean explain;

  public TrinoSqlRequest(String query, boolean explain) {
    this.query = query;
    this.explain = explain;
  }

  /** Deserialize from a stream. */
  public TrinoSqlRequest(StreamInput in) throws IOException {
    super(in);
    this.query = in.readString();
    this.explain = in.readBoolean();
  }

  @Override
  public void writeTo(StreamOutput out) throws IOException {
    super.writeTo(out);
    out.writeString(query);
    out.writeBoolean(explain);
  }

  @Override
  public ActionRequestValidationException validate() {
    return null;
  }

  /**
   * Re-create a {@link TrinoSqlRequest} from a generic {@link ActionRequest}. If the request is
   * already an instance of this class, it is returned directly. Otherwise, the request is
   * round-tripped through stream serialization.
   */
  public static TrinoSqlRequest fromActionRequest(ActionRequest actionRequest) {
    if (actionRequest instanceof TrinoSqlRequest) {
      return (TrinoSqlRequest) actionRequest;
    }
    try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
        OutputStreamStreamOutput osso = new OutputStreamStreamOutput(baos)) {
      actionRequest.writeTo(osso);
      try (InputStreamStreamInput input =
          new InputStreamStreamInput(new ByteArrayInputStream(baos.toByteArray()))) {
        return new TrinoSqlRequest(input);
      }
    } catch (IOException e) {
      throw new IllegalArgumentException("failed to parse ActionRequest into TrinoSqlRequest", e);
    }
  }
}
