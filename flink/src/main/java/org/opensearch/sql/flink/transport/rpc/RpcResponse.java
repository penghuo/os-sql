package org.opensearch.sql.flink.transport.rpc;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import lombok.Getter;
import lombok.SneakyThrows;
import org.opensearch.common.io.stream.StreamInput;
import org.opensearch.common.io.stream.StreamOutput;
import org.opensearch.transport.TransportResponse;

public class RpcResponse extends TransportResponse {

  @Getter
  private final Object resp;

  public RpcResponse(Object resp) {
    this.resp = resp;
  }

  @SneakyThrows(ClassNotFoundException.class)
  public RpcResponse(StreamInput in) throws IOException {
    super(in);

    ObjectInputStream inputStream = new ObjectInputStream(in);
    this.resp = inputStream.readObject();
  }


  @Override
  public void writeTo(StreamOutput out) throws IOException {
    ObjectOutputStream outputStream = new ObjectOutputStream(out);
    outputStream.writeObject(resp);
  }
}
