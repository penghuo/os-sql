package org.opensearch.sql.flink.transport.rpc;

import static org.opensearch.sql.flink.transport.TransportFlinkAction.RPC_ACTION_NAME;
import static org.opensearch.sql.flink.transport.concurrent.ClassLoadingUtils.guardCompletionWithContextClassLoader;

import java.io.IOException;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Proxy;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import org.apache.flink.runtime.rpc.RpcEndpoint;
import org.apache.flink.runtime.rpc.RpcGateway;
import org.apache.flink.runtime.rpc.RpcServer;
import org.apache.flink.runtime.rpc.RpcService;
import org.apache.flink.runtime.rpc.RpcUtils;
import org.apache.flink.runtime.rpc.messages.RemoteHandshakeMessage;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.common.io.stream.StreamInput;
import org.opensearch.transport.TransportException;
import org.opensearch.transport.TransportResponseHandler;
import org.opensearch.transport.TransportService;

public class TransportRpcService implements RpcService {

  private static final Logger LOG = LogManager.getLogger(TransportRpcService.class);

  static final int VERSION = 1;

  private final DiscoveryNode node;

  private final TransportService service;

  protected final boolean isLocal;

  private final ClassLoader flinkClassLoader;

  private final RpcRequestHandler handler;

  public TransportRpcService(DiscoveryNode node, TransportService service, boolean isLocal,
                             ClassLoader flinkClassLoader, RpcRequestHandler handler) {
    this.node = node;
    this.service = service;
    this.isLocal = isLocal;
    this.flinkClassLoader = flinkClassLoader;
    this.handler = handler;
  }

  @Override
  public <C extends RpcEndpoint & RpcGateway> RpcServer startServer(C rpcEndpoint) {
    handler.register(rpcEndpoint);

    LOG.info(
        "Starting RPC endpoint for {}.",
        rpcEndpoint.getClass().getName());

    Set<Class<?>> implementedRpcGateways =
        new HashSet<>(RpcUtils.extractImplementedRpcGateways(rpcEndpoint.getClass()));

    implementedRpcGateways.add(RpcServer.class);

    final InvocationHandler invocationHandler = new TransportInvocationHandler(node, service,
        isLocal, TransportService.class.getClassLoader());


    // Rather than using the System ClassLoader directly, we derive the ClassLoader
    // from this class . That works better in cases where Flink runs embedded and all Flink
    // code is loaded dynamically (for example from an OSGI bundle) through a custom ClassLoader
    ClassLoader classLoader = getClass().getClassLoader();

    @SuppressWarnings("unchecked")
    RpcServer server =
        (RpcServer)
            Proxy.newProxyInstance(
                classLoader,
                implementedRpcGateways.toArray(
                    new Class<?>[implementedRpcGateways.size()]),
                invocationHandler);
    return server;
  }

  @Override
  public <C extends RpcGateway> CompletableFuture<C> connect(DiscoveryNode node,
                                                             final boolean local, Class<C> clazz) {
    return connectInternal(node, local, clazz);
  }

  private <C extends RpcGateway> CompletableFuture<C> connectInternal(
      final DiscoveryNode node,
      final boolean local,
      final Class<C> clazz) {
    LOG.debug(
        "Try to connect to remote RPC endpoint with address {}. Returning a {} gateway.",
        node.getName(),
        clazz.getName());


    final CompletableFuture<C> gatewayFuture =
        ask(node, new RemoteHandshakeMessage(clazz, getVersion())).thenApply(ignored -> {
          InvocationHandler invocationHandler = new TransportInvocationHandler(node, service,
              local, flinkClassLoader);

          // Rather than using the System ClassLoader directly, we derive the
          // ClassLoader from this class.
          // That works better in cases where Flink runs embedded and
          // all Flink code is loaded dynamically
          // (for example from an OSGI bundle) through a custom ClassLoader
          ClassLoader classLoader = getClass().getClassLoader();

          @SuppressWarnings("unchecked")
          C proxy =
              (C)
                  Proxy.newProxyInstance(
                      classLoader,
                      new Class<?>[] {clazz},
                      invocationHandler);

          return proxy;
        });
    return guardCompletionWithContextClassLoader(gatewayFuture, flinkClassLoader);
  }

  protected int getVersion() {
    return VERSION;
  }

  protected CompletableFuture<Object> ask(DiscoveryNode node, Object message) {
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
}
