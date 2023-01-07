package org.opensearch.sql.flink.transport.rpc;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import lombok.Getter;
import lombok.SneakyThrows;
import org.apache.flink.runtime.rpc.messages.Message;
import org.opensearch.common.io.stream.StreamInput;
import org.opensearch.common.io.stream.StreamOutput;
import org.opensearch.transport.TransportRequest;

public class RpcRequest extends TransportRequest {

  @Getter
  private final Object obj;

  public RpcRequest(Object obj) {
    this.obj = obj;
  }

  @SneakyThrows(ClassNotFoundException.class)
  public RpcRequest(StreamInput in) throws IOException {
    super(in);

    ObjectInputStream inputStream = new ObjectInputStream(in);
    this.obj = inputStream.readObject();
  }

  @Override
  public void writeTo(StreamOutput out) throws IOException {
    ObjectOutputStream outputStream = new ObjectOutputStream(out);
    outputStream.writeObject(obj);
  }
}
