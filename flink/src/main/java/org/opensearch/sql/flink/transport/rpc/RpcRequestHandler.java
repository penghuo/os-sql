package org.opensearch.sql.flink.transport.rpc;

import org.opensearch.tasks.Task;
import org.opensearch.transport.TransportChannel;
import org.opensearch.transport.TransportRequestHandler;

public class RpcRequestHandler implements TransportRequestHandler<RpcRequest> {
  @Override
  public void messageReceived(RpcRequest rpcRequest, TransportChannel transportChannel, Task task)
      throws Exception {


  }
}
