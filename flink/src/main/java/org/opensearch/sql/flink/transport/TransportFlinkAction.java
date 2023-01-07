package org.opensearch.sql.flink.transport;

import java.util.concurrent.CompletableFuture;
import lombok.SneakyThrows;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.action.ActionListener;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.HandledTransportAction;
import org.opensearch.client.node.NodeClient;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.inject.Inject;
import org.opensearch.common.settings.Settings;
import org.opensearch.sql.flink.bear.BearGateway;
import org.opensearch.sql.flink.bear.BearService;
import org.opensearch.sql.flink.transport.rpc.RpcRequest;
import org.opensearch.sql.flink.transport.rpc.RpcRequestHandler;
import org.opensearch.sql.flink.transport.rpc.TransportRpcService;
import org.opensearch.tasks.Task;
import org.opensearch.transport.TransportService;

public class TransportFlinkAction extends HandledTransportAction<TransportFlinkRequest, TransportFlinkResponse> {

  private static final Logger LOG = LogManager.getLogger(TransportRpcService.class);

  public static String RPC_ACTION_NAME = FlinkAction.NAME + "[n]";

  private final ClusterService clusterService;

  private final TransportService transportService;

  private final RpcRequestHandler rpcRequestHandler;

  private TransportRpcService rpcService;

  private BearService bearService;


  /** Constructor of TransportPPLQueryAction. */
  @Inject
  public TransportFlinkAction(
      TransportService transportService,
      ActionFilters actionFilters,
      NodeClient client,
      ClusterService clusterService,
      Settings clusterSettings,
      String executor) {
    super(FlinkAction.NAME, transportService, actionFilters, TransportFlinkRequest::new);

    this.transportService = transportService;
    this.clusterService = clusterService;

    rpcRequestHandler =
        new RpcRequestHandler(TransportRpcService.class.getClassLoader());
    transportService
        .registerRequestHandler(RPC_ACTION_NAME, "sql-worker", RpcRequest::new, rpcRequestHandler);
    rpcService = new TransportRpcService(transportService.getLocalNode(), transportService, true,
        TransportRpcService.class.getClassLoader(), rpcRequestHandler);

    bearService = new BearService(rpcService);

    try {
      bearService.start();
    } catch (Exception e) {
      LOG.error(e);
      throw e;
    }
  }

  @Override
  protected void doExecute(Task task, TransportFlinkRequest request,
                           ActionListener<TransportFlinkResponse> actionListener) {
    CompletableFuture<BearGateway> connect =
        rpcService.connect(clusterService.localNode(), true, BearGateway.class);

    String req = "hello world";
    System.out.println("send request ===> " + req);

    try {
      String rsp = connect.get().echo(req);
      System.out.println("get response <=== " + rsp);
      actionListener.onResponse(new TransportFlinkResponse(rsp));
    } catch (Exception e) {
      LOG.error(e);
      throw new RuntimeException(e);
    }
  }
}
