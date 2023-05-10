/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.storage

import java.io.IOException

import org.apache.http.HttpHost
import org.opensearch.client.{RequestOptions, RestClient, RestHighLevelClient}
import org.opensearch.client.indices.{GetMappingsRequest, GetMappingsResponse}
import org.opensearch.flint.storage.opensearch.FlintOpenSearchReaderBuilder

trait FlintClient {
  def getMappings(indexName: String): String

  def createReaderBuilder(indexName: String): FlintReaderBuilder
}

object FlintClient {
  def create(options: FlintOptions): FlintClient = {
    val client = new RestHighLevelClient(
      RestClient.builder(new HttpHost(options.getHost, options.getPort, "http")))
    new OpenSearchRestHighLevelClient(client)
  }
}

class OpenSearchRestHighLevelClient(client: RestHighLevelClient)
    extends FlintClient
    with Serializable {
  override def getMappings(indexName: String): String = {
    val request: GetMappingsRequest = new GetMappingsRequest().indices(indexName)
    try {
      val response: GetMappingsResponse =
        client.indices().getMapping(request, RequestOptions.DEFAULT)
      response
        .mappings()
        .get(indexName)
        .source()
        .string()
    } catch {
      case e: IOException =>
        throw new IllegalStateException(s"Failed to get index mappings for $indexName}", e)
    }
  }

  override def createReaderBuilder(indexName: String): FlintReaderBuilder = {
    new FlintOpenSearchReaderBuilder(indexName, client)
  }

}
