/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.plugin.rest;

import static org.opensearch.rest.RestStatus.SERVICE_UNAVAILABLE;

import com.google.common.collect.ImmutableList;
import java.util.List;
import org.opensearch.action.ActionListener;
import org.opensearch.client.node.NodeClient;
import org.opensearch.rest.BaseRestHandler;
import org.opensearch.rest.BytesRestResponse;
import org.opensearch.rest.RestRequest;
import org.opensearch.rest.RestStatus;
import org.opensearch.sql.flink.transport.FlinkAction;
import org.opensearch.sql.flink.transport.TransportFlinkRequest;
import org.opensearch.sql.flink.transport.TransportFlinkResponse;

public class RestFlinkAction extends BaseRestHandler {

  @Override
  public List<Route> routes() {
    return ImmutableList.of(new Route(RestRequest.Method.POST, "/_plugins/_flink"));
  }

  @Override
  public String getName() {
    return "flink";
  }

  @Override
  protected RestChannelConsumer prepareRequest(RestRequest restRequest, NodeClient nodeClient) {
    return channel -> {
      nodeClient.execute(FlinkAction.INSTANCE, new TransportFlinkRequest(),
          new ActionListener<>() {
            @Override
            public void onResponse(TransportFlinkResponse transportFlinkResponse) {
              channel.sendResponse(new BytesRestResponse(RestStatus.OK, "OK"));
            }

            @Override
            public void onFailure(Exception e) {
              channel.sendResponse(new BytesRestResponse(SERVICE_UNAVAILABLE, e.getMessage()));
            }
          });
    };
  }
}
