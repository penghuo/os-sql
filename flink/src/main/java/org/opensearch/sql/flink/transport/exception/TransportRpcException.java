package org.opensearch.sql.flink.transport.exception;

import org.apache.flink.runtime.rpc.exceptions.RpcException;

public class TransportRpcException extends RpcException {

  private static final long serialVersionUID = -3796329968494146418L;

  public TransportRpcException(String message) {
    super(message);
  }

  public TransportRpcException(String message, Throwable cause) {
    super(message, cause);
  }

  public TransportRpcException(Throwable cause) {
    super(cause);
  }
}
