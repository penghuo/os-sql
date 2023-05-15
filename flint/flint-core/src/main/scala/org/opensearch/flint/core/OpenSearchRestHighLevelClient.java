/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.core;

import org.opensearch.action.search.SearchRequest;
import org.opensearch.client.RequestOptions;
import org.opensearch.client.RestHighLevelClient;
import org.opensearch.client.indices.GetMappingsRequest;
import org.opensearch.client.indices.GetMappingsResponse;
import org.opensearch.common.Strings;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.xcontent.NamedXContentRegistry;
import org.opensearch.common.xcontent.XContentParser;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.flint.core.storage.FlintReader;
import org.opensearch.flint.core.storage.OpenSearchScrollReader;
import org.opensearch.index.query.AbstractQueryBuilder;
import org.opensearch.index.query.MatchAllQueryBuilder;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.search.SearchModule;
import org.opensearch.search.builder.SearchSourceBuilder;

import java.io.IOException;
import java.util.ArrayList;

import static org.opensearch.common.xcontent.DeprecationHandler.IGNORE_DEPRECATIONS;

public class OpenSearchRestHighLevelClient implements FlintClient {

  private static NamedXContentRegistry
      repository =
      new NamedXContentRegistry(new SearchModule(Settings.builder().build(), new ArrayList<>()).getNamedXContents());

  private RestHighLevelClient client;

  public OpenSearchRestHighLevelClient(RestHighLevelClient client) {
    this.client = client;
  }

  @Override public String getMappings(String indexName) {
    GetMappingsRequest request = new GetMappingsRequest().indices(indexName);
    try {
      GetMappingsResponse response = client.indices().getMapping(request, RequestOptions.DEFAULT);
      return response.mappings().get(indexName).source().string();
    } catch (IOException e) {
      throw new IllegalStateException("Failed to get index mappings for " + indexName, e);
    }
  }

  /**
   * Create {@link FlintReader}.
   *
   * @param indexName index name.
   * @param query DSL query. DSL query is null means match_all
   * @return {@link FlintReader}.
   */
  @Override public FlintReader createReader(String indexName, String query) {
    try {
      QueryBuilder queryBuilder = new MatchAllQueryBuilder();
      if (!Strings.isNullOrEmpty(query)) {
        XContentParser parser = XContentType.JSON.xContent().createParser(repository, IGNORE_DEPRECATIONS, query);
        queryBuilder = AbstractQueryBuilder.parseInnerQueryBuilder(parser);
      }
      SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder().query(queryBuilder);
      return new OpenSearchScrollReader(client, new SearchRequest().indices(indexName).source(searchSourceBuilder));
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }
}
