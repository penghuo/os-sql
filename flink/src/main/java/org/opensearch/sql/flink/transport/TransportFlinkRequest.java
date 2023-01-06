package org.opensearch.sql.flink.transport;

import java.io.IOException;
import org.opensearch.action.ActionRequest;
import org.opensearch.action.ActionRequestValidationException;
import org.opensearch.common.io.stream.StreamInput;

public class TransportFlinkRequest extends ActionRequest {

  public TransportFlinkRequest(StreamInput in) throws IOException {

  }

  @Override
  public ActionRequestValidationException validate() {
    return null;
  }
}
