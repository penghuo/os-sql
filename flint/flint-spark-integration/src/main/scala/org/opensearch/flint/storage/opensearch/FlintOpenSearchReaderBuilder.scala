/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.storage.opensearch

import java.util

import org.opensearch.client.RestHighLevelClient
import org.opensearch.common.settings.Settings
import org.opensearch.common.xcontent.{NamedXContentRegistry, XContentParser, XContentType}
import org.opensearch.common.xcontent.DeprecationHandler.IGNORE_DEPRECATIONS
import org.opensearch.flint.storage.FlintReaderBuilder
import org.opensearch.index.query.AbstractQueryBuilder
import org.opensearch.search.SearchModule
import org.opensearch.search.builder.SearchSourceBuilder

import org.apache.spark.sql.connector.expressions.filter.Predicate
import org.apache.spark.sql.flint.FlintQueryBuilder

class FlintOpenSearchReaderBuilder(indexName: String, client: RestHighLevelClient)
    extends FlintReaderBuilder {

  lazy val searchSourceBuilder = new SearchSourceBuilder()

  lazy val XContentRegistry = new NamedXContentRegistry(
    new SearchModule(Settings.builder().build(), new util.ArrayList).getNamedXContents)

  def build(): FlintOpenSearchReader = {
    new FlintOpenSearchReader(indexName, client, searchSourceBuilder)
  }

  def pushPredicates(predicates: Array[Predicate]): FlintOpenSearchReaderBuilder = {
    val query = FlintQueryBuilder.compilePredicates(predicates)
    query.map(q => {
      val parser: XContentParser =
        XContentType.JSON.xContent.createParser(XContentRegistry, IGNORE_DEPRECATIONS, q)
      searchSourceBuilder.query(AbstractQueryBuilder.parseInnerQueryBuilder(parser))
    })

    this
  }
}
