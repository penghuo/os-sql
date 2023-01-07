package org.opensearch.sql.flink.bear;

import org.apache.flink.runtime.rpc.RpcEndpoint;
import org.apache.flink.runtime.rpc.RpcService;

public class BearService extends RpcEndpoint implements BearGateway {

  public BearService(RpcService rpcService) {
    super(rpcService);
  }

  @Override
  public String echo(String message) {
    return message;
  }

  @Override
  public String getRpcClassName() {
    return BearGateway.class.getSimpleName();
  }
}
