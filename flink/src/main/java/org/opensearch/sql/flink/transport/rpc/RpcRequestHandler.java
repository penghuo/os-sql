package org.opensearch.sql.flink.transport.rpc;

import static org.opensearch.sql.flink.transport.concurrent.ClassLoadingUtils.runWithContextClassLoader;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import lombok.SneakyThrows;
import org.apache.flink.runtime.rpc.Local;
import org.apache.flink.runtime.rpc.RpcEndpoint;
import org.apache.flink.runtime.rpc.exceptions.RpcConnectionException;
import org.apache.flink.runtime.rpc.messages.HandshakeSuccessMessage;
import org.apache.flink.runtime.rpc.messages.Message;
import org.apache.flink.runtime.rpc.messages.RemoteHandshakeMessage;
import org.apache.flink.runtime.rpc.messages.RpcInvocation;
import org.apache.flink.types.Either;
import org.apache.flink.util.concurrent.FutureUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.sql.flink.transport.exception.TransportRpcException;
import org.opensearch.tasks.Task;
import org.opensearch.transport.TransportChannel;
import org.opensearch.transport.TransportRequestHandler;

public class RpcRequestHandler implements TransportRequestHandler<RpcRequest> {
  protected final Logger log = LogManager.getLogger(getClass());

  private final Map<String, RpcEndpoint> endpointMap = new ConcurrentHashMap<>();

  private final ClassLoader flinkClassLoader;

  public RpcRequestHandler(ClassLoader flinkClassLoader) {
    this.flinkClassLoader = flinkClassLoader;
  }

  public void register(RpcEndpoint rpcEndpoint) {
    endpointMap.put(rpcEndpoint.getRpcClassName(), rpcEndpoint);
  }

  @Override
  public void messageReceived(RpcRequest req, TransportChannel channel, Task task)
      throws Exception {

    Message message = (Message) req.getObj();

    if (message instanceof RemoteHandshakeMessage) {
      channel.sendResponse(new RpcResponse(HandshakeSuccessMessage.INSTANCE));
    } else if (message instanceof RpcInvocation) {
      handleRpcInvocation(channel, (RpcInvocation)message);
    }
  }

  @SneakyThrows(IOException.class)
  private void handleRpcInvocation(TransportChannel channel, RpcInvocation rpcInvocation) {
    Method rpcMethod = null;

    RpcEndpoint rpcEndpoint = endpointMap.get(rpcInvocation.getDeclaringClassName());

    try {
      String methodName = rpcInvocation.getMethodName();
      Class<?>[] parameterTypes = rpcInvocation.getParameterTypes();

      rpcMethod = lookupRpcMethod(rpcEndpoint, methodName, parameterTypes);
    } catch (final NoSuchMethodException e) {
      log.error("Could not find rpc method for rpc invocation.", e);

      RpcConnectionException rpcException =
          new RpcConnectionException("Could not find rpc method for rpc invocation.", e);
      channel.sendResponse(rpcException);
    }

    if (rpcMethod != null) {
      try {
        // this supports declaration of anonymous classes
        rpcMethod.setAccessible(true);

        final Method capturedRpcMethod = rpcMethod;
        if (rpcMethod.getReturnType().equals(Void.TYPE)) {
          // No return value to send back
          runWithContextClassLoader(
              () -> capturedRpcMethod.invoke(rpcEndpoint, rpcInvocation.getArgs()),
              flinkClassLoader);
        } else {
          final Object result;
          try {
            result =
                runWithContextClassLoader(
                    () ->
                        capturedRpcMethod.invoke(
                            rpcEndpoint, rpcInvocation.getArgs()),
                    flinkClassLoader);
          } catch (InvocationTargetException e) {
            log.debug(
                "Reporting back error thrown in remote procedure {}", rpcMethod, e);

            // tell the sender about the failure
            channel.sendResponse(e);
            return;
          }

          final String methodName = rpcMethod.getName();
          final boolean isLocalRpcInvocation =
              rpcMethod.getAnnotation(Local.class) != null;

          if (result instanceof CompletableFuture) {
            final CompletableFuture<?> responseFuture = (CompletableFuture<?>) result;
            sendAsyncResponse(channel, responseFuture, methodName, isLocalRpcInvocation);
          } else {
            sendSyncResponse(channel, result, methodName, isLocalRpcInvocation);
          }
        }
      } catch (Throwable e) {
        log.error("Error while executing remote procedure call {}.", rpcMethod, e);
        // tell the sender about the failure
        channel.sendResponse(new RuntimeException(e));
      }
    }
  }

  @SneakyThrows(IOException.class)
  private void sendSyncResponse(
      TransportChannel channel, Object response, String methodName, boolean isLocalRpcInvocation) {
      Either<TransportRpcSerializedValue, TransportRpcException> serializedResult =
          serializeRemoteResultAndVerifySize(response, methodName);
    if(!isLocalRpcInvocation) {
      if (serializedResult.isLeft()) {
        channel.sendResponse(new RpcResponse(serializedResult.left()));
      } else {
        channel.sendResponse(serializedResult.right());
      }
    } else {
      channel.sendResponse(new RpcResponse(response));
    }
  }

  private void sendAsyncResponse(
      TransportChannel channel, CompletableFuture<?> asyncResponse, String methodName,
      boolean isLocalRpcInvocation) {
    FutureUtils.assertNoException(
        asyncResponse.handle(
            (value, throwable) -> {
              try {
                if (throwable != null) {
                  channel.sendResponse(new RuntimeException(throwable));
                } else {
                  if (!isLocalRpcInvocation) {
                    Either<TransportRpcSerializedValue, TransportRpcException>
                        serializedResult =
                        serializeRemoteResultAndVerifySize(
                            value, methodName);

                    if (serializedResult.isLeft()) {
                      channel.sendResponse(new RpcResponse(serializedResult.left()));
                    } else {
                      channel.sendResponse(serializedResult.right());
                    }
                  } else {
                    channel.sendResponse(new RpcResponse(value));
                  }
                }
              } catch (IOException e) {
                throw new RuntimeException(e);
              }
              // consume the provided throwable
              return null;
            }));
  }

  /**
   * Look up the rpc method on the given {@link RpcEndpoint} instance.
   *
   * @param methodName Name of the method
   * @param parameterTypes Parameter types of the method
   * @return Method of the rpc endpoint
   * @throws NoSuchMethodException Thrown if the method with the given name and parameter types
   *     cannot be found at the rpc endpoint
   */
  private Method lookupRpcMethod(final RpcEndpoint rpcEndpoint, final String methodName,
                                 final Class<?>[] parameterTypes)
      throws NoSuchMethodException {
    return rpcEndpoint.getClass().getMethod(methodName, parameterTypes);
  }

  private Either<TransportRpcSerializedValue, TransportRpcException> serializeRemoteResultAndVerifySize(
      Object result, String methodName) {
    try {
      TransportRpcSerializedValue serializedResult = TransportRpcSerializedValue.valueOf(result);

      long resultSize = serializedResult.getSerializedDataLength();
      long maximumFramesize = 10485760;

      if (resultSize > maximumFramesize) {
        return Either.Right(
            new TransportRpcException(
                "The method "
                    + methodName
                    + "'s result size "
                    + resultSize
                    + " exceeds the maximum size "
                    + maximumFramesize
                    + " ."));
      } else {
        return Either.Left(serializedResult);
      }
    } catch (IOException e) {
      return Either.Right(
          new TransportRpcException(
              "Failed to serialize the result for RPC call : " + methodName + '.',
              e));
    }
  }

}
