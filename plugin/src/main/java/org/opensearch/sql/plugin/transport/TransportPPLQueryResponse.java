/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.plugin.transport;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import lombok.Getter;
import org.opensearch.core.action.ActionResponse;
import org.opensearch.core.common.io.stream.InputStreamStreamInput;
import org.opensearch.core.common.io.stream.OutputStreamStreamOutput;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;

public class TransportPPLQueryResponse extends ActionResponse {
  @Getter private final String result;
  @Getter private final String contentType;

  /** True when the transport action selected the asynchronous lifecycle API. */
  @Getter private final boolean asyncQueryResponse;

  public TransportPPLQueryResponse(String result) {
    this(result, "application/json; charset=UTF-8");
  }

  public TransportPPLQueryResponse(String result, String contentType) {
    this(result, contentType, false);
  }

  private TransportPPLQueryResponse(String result, String contentType, boolean asyncQueryResponse) {
    this.result = result;
    this.contentType = contentType;
    this.asyncQueryResponse = asyncQueryResponse;
  }

  /** Creates a response produced by the asynchronous query lifecycle API. */
  static TransportPPLQueryResponse asyncQueryResponse(String result) {
    return new TransportPPLQueryResponse(result, "application/json; charset=UTF-8", true);
  }

  public TransportPPLQueryResponse(StreamInput in) throws IOException {
    super(in);
    result = in.readString();
    contentType = in.readString();
    asyncQueryResponse = in.readBoolean();
  }

  @Override
  public void writeTo(StreamOutput out) throws IOException {
    out.writeString(result);
    out.writeString(contentType);
    out.writeBoolean(asyncQueryResponse);
  }

  public static TransportPPLQueryResponse fromActionResponse(ActionResponse actionResponse) {
    if (actionResponse instanceof TransportPPLQueryResponse) {
      return (TransportPPLQueryResponse) actionResponse;
    }

    try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
        OutputStreamStreamOutput osso = new OutputStreamStreamOutput(baos)) {
      actionResponse.writeTo(osso);
      try (StreamInput input =
          new InputStreamStreamInput(new ByteArrayInputStream(baos.toByteArray()))) {
        return new TransportPPLQueryResponse(input);
      }
    } catch (IOException e) {
      throw new UncheckedIOException(
          "failed to parse ActionResponse into TransportPPLQueryResponse", e);
    }
  }
}
