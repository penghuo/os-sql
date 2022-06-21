/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.plugin.rest;

import static org.opensearch.rest.RestStatus.OK;
import static org.opensearch.rest.RestStatus.SERVICE_UNAVAILABLE;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.action.ActionListener;
import org.opensearch.client.node.NodeClient;
import org.opensearch.rest.BaseRestHandler;
import org.opensearch.rest.BytesRestResponse;
import org.opensearch.rest.RestRequest;
import org.opensearch.sql.opensearch.s3.transport.CreateViewAction;
import org.opensearch.sql.opensearch.s3.transport.CreateViewRequest;
import org.opensearch.sql.opensearch.s3.transport.CreateViewResponse;

public class RestS3QueryAction extends BaseRestHandler {
  public static final String STREAM_API_ENDPOINT = "/_plugins/_s3";

  private static final Logger LOG = LogManager.getLogger();

  /**
   * Constructor of RestS3QueryAction.
   */
  public RestS3QueryAction() {
    super();
  }

  @Override
  public List<Route> routes() {
    return ImmutableList.of(new Route(RestRequest.Method.POST, STREAM_API_ENDPOINT));
  }

  @Override
  public String getName() {
    return "stream_expression_action";
  }

  @Override
  protected Set<String> responseParams() {
    Set<String> responseParams = new HashSet<>(super.responseParams());
    responseParams.addAll(Arrays.asList("format", "sanitize"));
    return responseParams;
  }

  @Override
  protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) {
    return channel -> client.execute(
        CreateViewAction.INSTANCE,
        new CreateViewRequest(
            "s3://arion.dev.us-west-2/logs/documents-181998-1k.json",
            "s3_00001",
            new ImmutableMap.Builder<String, Object>()
                .put("@timestamp", "date")
                .put("clientip", "keyword")
                .put("request", "text")
                .put("size", "integer")
                .put("status", "integer")
                .build()
        ),
        new ActionListener<>() {
          @Override
          public void onResponse(CreateViewResponse response) {
            channel.sendResponse(new BytesRestResponse(OK, "application/json; charset=UTF-8",
                "ok"));
          }

          @Override
          public void onFailure(Exception e) {
            channel.sendResponse(new BytesRestResponse(SERVICE_UNAVAILABLE, "application/json; "
                + "charset=UTF-8", e.getLocalizedMessage()));
          }
        });
  }
}
