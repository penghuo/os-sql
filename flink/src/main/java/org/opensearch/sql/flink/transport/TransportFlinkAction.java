package org.opensearch.sql.flink.transport;

import org.opensearch.action.ActionListener;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.HandledTransportAction;
import org.opensearch.client.node.NodeClient;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.inject.Inject;
import org.opensearch.common.settings.Settings;
import org.opensearch.sql.flink.transport.rpc.RpcRequest;
import org.opensearch.tasks.Task;
import org.opensearch.transport.TransportChannel;
import org.opensearch.transport.TransportRequestHandler;
import org.opensearch.transport.TransportService;

public class TransportFlinkAction extends HandledTransportAction<TransportFlinkRequest, TransportFlinkResponse> {

  public static String RPC_ACTION_NAME = FlinkAction.NAME + "[n]";

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

    transportService.registerRequestHandler(RPC_ACTION_NAME, executor, RpcRequest::new,
        new TransportRequestHandler<RpcRequest>() {
          @Override
          public void messageReceived(RpcRequest rpcRequest, TransportChannel transportChannel,
                                      Task task) throws Exception {

          }
        });
  }

  @Override
  protected void doExecute(Task task, TransportFlinkRequest request,
                           ActionListener<TransportFlinkResponse> actionListener) {

  }
}
