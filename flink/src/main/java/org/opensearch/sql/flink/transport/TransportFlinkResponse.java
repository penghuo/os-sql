package org.opensearch.sql.flink.transport;

import java.io.IOException;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import org.opensearch.action.ActionResponse;
import org.opensearch.common.io.stream.StreamInput;
import org.opensearch.common.io.stream.StreamOutput;

@RequiredArgsConstructor
public class TransportFlinkResponse extends ActionResponse {
  @Getter
  private final String result;

  public TransportFlinkResponse(StreamInput in) throws IOException {
    super(in);
    result = in.readString();
  }

  @Override
  public void writeTo(StreamOutput out) throws IOException {
    out.writeString(result);
  }
}
