package org.opensearch.sql.flink.transport.rpc;

import static org.apache.flink.util.Preconditions.checkState;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Proxy;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;
import org.apache.flink.runtime.rpc.RpcEndpoint;
import org.apache.flink.runtime.rpc.RpcGateway;
import org.apache.flink.runtime.rpc.RpcServer;
import org.apache.flink.runtime.rpc.RpcService;
import org.apache.flink.runtime.rpc.RpcUtils;
import org.apache.flink.runtime.rpc.messages.HandshakeSuccessMessage;
import org.apache.flink.runtime.rpc.messages.RemoteHandshakeMessage;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.transport.TransportService;

public class TransportRpcService implements RpcService {

  private static final Logger LOG = LogManager.getLogger(TransportRpcService.class);

  private final DiscoveryNode node;

  private final TransportService service;

  protected final boolean isLocal;

  public TransportRpcService(DiscoveryNode node, TransportService service, boolean isLocal) {
    this.node = node;
    this.service = service;
    this.isLocal = isLocal;
  }

  @Override
  public <C extends RpcEndpoint & RpcGateway> RpcServer startServer(C rpcEndpoint) {

    LOG.info(
        "Starting RPC endpoint for {} at {} .",
        rpcEndpoint.getClass().getName(),
        node.getName());


    Set<Class<?>> implementedRpcGateways =
        new HashSet<>(RpcUtils.extractImplementedRpcGateways(rpcEndpoint.getClass()));

    implementedRpcGateways.add(RpcServer.class);

    final InvocationHandler akkaInvocationHandler = new TransportInvocationHandler(node, service,
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
                akkaInvocationHandler);
    return server;
  }

  @Override
  public <C extends RpcGateway> CompletableFuture<C> connect(DiscoveryNode node, Class<C> clazz) {
    return null;
  }

  private <C extends RpcGateway> CompletableFuture<C> connectInternal(
      final String address,
      final Class<C> clazz,
      Function<ActorRef, InvocationHandler> invocationHandlerFactory) {
    checkState(!stopped, "RpcService is stopped");

    LOG.debug(
        "Try to connect to remote RPC endpoint with address {}. Returning a {} gateway.",
        address,
        clazz.getName());

    final CompletableFuture<ActorRef> actorRefFuture = resolveActorAddress(address);

    final CompletableFuture<HandshakeSuccessMessage> handshakeFuture =
        actorRefFuture.thenCompose(
            (ActorRef actorRef) ->
                AkkaFutureUtils.toJava(
                    Patterns.ask(
                        actorRef,
                        new RemoteHandshakeMessage(
                            clazz, getVersion()),
                        configuration.getTimeout().toMillis())
                        .<HandshakeSuccessMessage>mapTo(
                            ClassTag$.MODULE$
                                .<HandshakeSuccessMessage>apply(
                                    HandshakeSuccessMessage
                                        .class))));

    final CompletableFuture<C> gatewayFuture =
        actorRefFuture.thenCombineAsync(
            handshakeFuture,
            (ActorRef actorRef, HandshakeSuccessMessage ignored) -> {
              InvocationHandler invocationHandler =
                  invocationHandlerFactory.apply(actorRef);

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
            },
            actorSystem.dispatcher());

    return guardCompletionWithContextClassLoader(gatewayFuture, flinkClassLoader);
  }
}
