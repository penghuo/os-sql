package org.opensearch.sql.flink.transport.rpc;

import static org.opensearch.sql.flink.transport.TransportFlinkAction.RPC_ACTION_NAME;
import static org.opensearch.sql.flink.transport.concurrent.ClassLoadingUtils.guardCompletionWithContextClassLoader;

import java.io.IOException;
import java.lang.annotation.Annotation;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.time.Duration;
import java.util.Objects;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.runtime.rpc.Local;
import org.apache.flink.runtime.rpc.MainThreadExecutable;
import org.apache.flink.runtime.rpc.RpcGateway;
import org.apache.flink.runtime.rpc.RpcServer;
import org.apache.flink.runtime.rpc.RpcTimeout;
import org.apache.flink.runtime.rpc.StartStoppable;
import org.apache.flink.runtime.rpc.exceptions.RecipientUnreachableException;
import org.apache.flink.runtime.rpc.exceptions.RpcException;
import org.apache.flink.runtime.rpc.messages.LocalRpcInvocation;
import org.apache.flink.runtime.rpc.messages.RemoteRpcInvocation;
import org.apache.flink.runtime.rpc.messages.RpcInvocation;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.TimeUtils;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.common.io.stream.StreamInput;
import org.opensearch.transport.TransportException;
import org.opensearch.transport.TransportResponseHandler;
import org.opensearch.transport.TransportService;

class TransportInvocationHandler implements InvocationHandler, RpcGateway, RpcServer {

  private final DiscoveryNode node;

  private final TransportService service;

  protected final boolean isLocal;

  private final Duration timeout;

  private final ClassLoader flinkClassLoader;

  public TransportInvocationHandler(DiscoveryNode node,
                                    TransportService service, boolean isLocal,
                                    ClassLoader flinkClassLoader) {
    this.node = node;
    this.service = service;
    this.isLocal = isLocal;
    this.timeout = Duration.ofSeconds(30);
    this.flinkClassLoader = flinkClassLoader;
  }

  @Override
  public void start() {
//    tell(ControlMessages.START);
  }

  @Override
  public void stop() {
//    tell(ControlMessages.STOP);
  }

  @Override
  public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
    Class<?> declaringClass = method.getDeclaringClass();

    Object result;

    if (declaringClass.equals(Object.class)
        || declaringClass.equals(RpcGateway.class)
        || declaringClass.equals(StartStoppable.class)
        || declaringClass.equals(MainThreadExecutable.class)
        || declaringClass.equals(RpcServer.class)) {
      result = method.invoke(this, args);
    } else {
      result = invokeRpc(method, args);
    }

    return result;
  }

  @Override
  public DiscoveryNode getDiscoveryNode() {
    return node;
  }

  protected void tell(Object message) {
    service.sendRequest(node, RPC_ACTION_NAME, new RpcRequest(message),
        new TransportResponseHandler<RpcResponse>() {
          @Override
          public RpcResponse read(StreamInput streamInput) throws IOException {
            throw new IllegalStateException("tell does not expected response");
          }

          @Override
          public void handleException(TransportException e) {
            throw new IllegalStateException("tell does not expected response");
          }

          @Override
          public String executor() {
            return "sql-worker";
          }

          @Override
          public void handleResponse(RpcResponse rpcResponse) {
            throw new IllegalStateException("tell does not expected response");
          }
        });
  }

  protected CompletableFuture<?> ask(Object message, Duration timeout) {
    CompletableFuture<Object> response = new CompletableFuture<>();
    service.sendRequest(node, RPC_ACTION_NAME, new RpcRequest(message),
        new TransportResponseHandler<RpcResponse>() {
          @Override
          public void handleResponse(RpcResponse resp) {
            response.complete(resp.getResp());
          }

          @Override
          public void handleException(TransportException e) {

          }

          @Override
          public String executor() {
            return "sql-worker";
          }

          @Override
          public RpcResponse read(StreamInput in) throws IOException {
            return new RpcResponse(in);
          }
        });
    return guardCompletionWithContextClassLoader(response, flinkClassLoader);
  }

  // ------------------------------------------------------------------------
  //  Private methods
  // ------------------------------------------------------------------------

  /**
   * Invokes a RPC method by sending the RPC invocation details to the rpc endpoint.
   *
   * @param method to call
   * @param args   of the method call
   * @return result of the RPC; the result future is completed with a {@link TimeoutException} if
   * the requests times out; if the recipient is not reachable, then the result future is
   * completed with a {@link RecipientUnreachableException}.
   * @throws Exception if the RPC invocation fails
   */
  private Object invokeRpc(Method method, Object[] args) throws Exception {
    String methodName = method.getName();
    Class<?>[] parameterTypes = method.getParameterTypes();
    final boolean isLocalRpcInvocation = method.getAnnotation(Local.class) != null;
    Annotation[][] parameterAnnotations = method.getParameterAnnotations();
    Duration futureTimeout = extractRpcTimeout(parameterAnnotations, args, timeout);

    final RpcInvocation rpcInvocation =
        createRpcInvocationMessage(
            method.getDeclaringClass().getSimpleName(),
            methodName,
            isLocalRpcInvocation,
            parameterTypes,
            args);

    Class<?> returnType = method.getReturnType();

    final Object result;

    if (Objects.equals(returnType, Void.TYPE)) {
      tell(rpcInvocation);

      result = null;
    } else {
      // Capture the call stack. It is significantly faster to do that via an exception than
      // via Thread.getStackTrace(), because exceptions lazily initialize the stack trace,
      // initially only
      // capture a lightweight native pointer, and convert that into the stack trace lazily
      // when needed.
      final Throwable callStackCapture = null;

      // execute an asynchronous call
      final CompletableFuture<?> resultFuture =
          ask(rpcInvocation, futureTimeout)
              .thenApply(
                  resultValue ->
                      deserializeValueIfNeeded(
                          resultValue, method, flinkClassLoader));

      final CompletableFuture<Object> completableFuture = new CompletableFuture<>();
      resultFuture.whenComplete(
          (resultValue, failure) -> {
            if (failure != null) {
              completableFuture.completeExceptionally(failure);
            } else {
              completableFuture.complete(resultValue);
            }
          });

      if (Objects.equals(returnType, CompletableFuture.class)) {
        result = completableFuture;
      } else {
        try {
          result = completableFuture.get(futureTimeout.toMillis(), TimeUnit.MILLISECONDS);
        } catch (ExecutionException ee) {
          throw new RpcException(
              "Failure while obtaining synchronous RPC result.",
              ExceptionUtils.stripExecutionException(ee));
        }
      }
    }

    return result;
  }

  /**
   * Create the RpcInvocation message for the given RPC.
   *
   * @param declaringClassName   of the RPC
   * @param methodName           of the RPC
   * @param isLocalRpcInvocation whether the RPC must be sent as a local message
   * @param parameterTypes       of the RPC
   * @param args                 of the RPC
   * @return RpcInvocation message which encapsulates the RPC details
   * @throws IOException if we cannot serialize the RPC invocation parameters
   */
  private RpcInvocation createRpcInvocationMessage(
      final String declaringClassName,
      final String methodName,
      final boolean isLocalRpcInvocation,
      final Class<?>[] parameterTypes,
      final Object[] args)
      throws IOException {
    final RpcInvocation rpcInvocation;

    if (isLocal || isLocalRpcInvocation) {
      rpcInvocation =
          new LocalRpcInvocation(declaringClassName, methodName, parameterTypes, args);
    } else {
      rpcInvocation =
          new RemoteRpcInvocation(declaringClassName, methodName, parameterTypes, args);
    }

    return rpcInvocation;
  }

  private static Object deserializeValueIfNeeded(
      Object o, Method method, ClassLoader flinkClassLoader) {
    if (o instanceof TransportRpcSerializedValue) {
      try {
        return ((TransportRpcSerializedValue) o).deserializeValue(flinkClassLoader);
      } catch (IOException | ClassNotFoundException e) {
        throw new CompletionException(
            new RpcException(
                "Could not deserialize the serialized payload of RPC method : "
                    + method.getName(),
                e));
      }
    } else {
      return o;
    }
  }

  // ------------------------------------------------------------------------
  //  Helper methods
  // ------------------------------------------------------------------------

  /**
   * Extracts the {@link RpcTimeout} annotated rpc timeout value from the list of given method
   * arguments. If no {@link RpcTimeout} annotated parameter could be found, then the default
   * timeout is returned.
   *
   * @param parameterAnnotations Parameter annotations
   * @param args                 Array of arguments
   * @param defaultTimeout       Default timeout to return if no {@link RpcTimeout} annotated parameter
   *                             has been found
   * @return Timeout extracted from the array of arguments or the default timeout
   */
  private static Duration extractRpcTimeout(
      Annotation[][] parameterAnnotations, Object[] args, Duration defaultTimeout) {
    if (args != null) {
      Preconditions.checkArgument(parameterAnnotations.length == args.length);

      for (int i = 0; i < parameterAnnotations.length; i++) {
        if (isRpcTimeout(parameterAnnotations[i])) {
          if (args[i] instanceof Time) {
            return TimeUtils.toDuration((Time) args[i]);
          } else if (args[i] instanceof Duration) {
            return (Duration) args[i];
          } else {
            throw new RuntimeException(
                "The rpc timeout parameter must be of type "
                    + Time.class.getName()
                    + " or "
                    + Duration.class.getName()
                    + ". The type "
                    + args[i].getClass().getName()
                    + " is not supported.");
          }
        }
      }
    }

    return defaultTimeout;
  }


  /**
   * Checks whether any of the annotations is of type {@link RpcTimeout}.
   *
   * @param annotations Array of annotations
   * @return True if {@link RpcTimeout} was found; otherwise false
   */
  private static boolean isRpcTimeout(Annotation[] annotations) {
    for (Annotation annotation : annotations) {
      if (annotation.annotationType().equals(RpcTimeout.class)) {
        return true;
      }
    }

    return false;
  }

  @Override
  public void runAsync(Runnable runnable) {
    throw new IllegalStateException("runAsync");
  }

  @Override
  public <V> CompletableFuture<V> callAsync(Callable<V> callable, Duration callTimeout) {
    throw new IllegalStateException("callAsync");
  }

  @Override
  public void scheduleRunAsync(Runnable runnable, long delay) {
    throw new IllegalStateException("scheduleRunAsync");
  }

  @Override
  public CompletableFuture<Void> getTerminationFuture() {
    throw new IllegalStateException("getTerminationFuture");
  }
}
