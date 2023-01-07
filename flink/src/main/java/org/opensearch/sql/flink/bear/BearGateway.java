package org.opensearch.sql.flink.bear;

import org.apache.flink.runtime.rpc.RpcGateway;

public interface BearGateway extends RpcGateway {
  String echo(String message);
}
