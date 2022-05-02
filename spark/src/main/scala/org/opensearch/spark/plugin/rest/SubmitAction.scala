/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.spark.plugin.rest

import org.opensearch.client.node.NodeClient
import org.opensearch.rest.RestHandler.Route
import org.opensearch.rest.{BaseRestHandler, BytesRestResponse, RestHandler, RestRequest, RestStatus}

import java.util

class SubmitAction extends BaseRestHandler {
  val QUERY_API_ENDPOINT = "/_plugins/_spark"

  override def getName: String = "spark_submit_action"

  override def prepareRequest(restRequest: RestRequest, nodeClient: NodeClient): BaseRestHandler.RestChannelConsumer = {
    channel => channel.sendResponse(
      new BytesRestResponse(RestStatus.OK, "application/json; charset=UTF-8", "Hello World"))
  }

  override def routes(): util.List[RestHandler.Route] = {
    util.Arrays.asList(new Route(RestRequest.Method.POST, QUERY_API_ENDPOINT))
  }
}
