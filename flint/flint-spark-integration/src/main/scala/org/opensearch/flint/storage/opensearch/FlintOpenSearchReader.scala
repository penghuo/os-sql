/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.storage.opensearch

import java.io.IOException
import java.util

import scala.collection.JavaConverters._

import org.opensearch.action.search.{SearchRequest, SearchResponse}
import org.opensearch.client.{RequestOptions, RestHighLevelClient}
import org.opensearch.flint.storage.FlintReader
import org.opensearch.search.SearchHit
import org.opensearch.search.builder.SearchSourceBuilder

class FlintOpenSearchReader(
    indexName: String,
    client: RestHighLevelClient,
    sourceBuilder: SearchSourceBuilder)
    extends FlintReader {
  private var iterator: Iterator[SearchHit] = _

  def open(): Unit = None

  def hasNext(): Boolean = {
    try {
      if (iterator == null) {
        val response: SearchResponse = client.search(
          new SearchRequest().indices(indexName).source(sourceBuilder),
          RequestOptions.DEFAULT)
        iterator = util.Arrays.asList(response.getHits.getHits: _*).asScala.iterator
      }
      iterator.hasNext
    } catch {
      case e: IOException =>
        // todo, log.error
        throw new RuntimeException(e)
    }
  }

  /** Return each hit doc. */
  def next(): String = iterator.next().getSourceAsString

  def close(): Unit = {
    try {
      if (client != null) {
        client.close()
      }
    } catch {
      case e: Exception =>
        // todo, log.error
        throw new RuntimeException(e)
    }
  }
}
