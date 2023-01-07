package org.opensearch.sql.flink.transport;

import org.opensearch.action.ActionType;

public class FlinkAction extends ActionType<TransportFlinkResponse> {
  public static final String NAME = "cluster:admin/opensearch/query";
  public static final FlinkAction INSTANCE = new FlinkAction();

  private FlinkAction() {
    super(NAME, TransportFlinkResponse::new);
  }
}
