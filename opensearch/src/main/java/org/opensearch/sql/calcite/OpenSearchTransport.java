/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.opensearch.sql.calcite;

import static java.util.Objects.requireNonNull;
import static org.opensearch.core.xcontent.DeprecationHandler.IGNORE_DEPRECATIONS;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.collect.ImmutableMap;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Locale;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.StreamSupport;
import org.apache.calcite.runtime.Hook;
import org.opensearch.action.admin.indices.mapping.get.GetMappingsResponse;
import org.opensearch.action.search.ClearScrollRequest;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.action.search.SearchScrollRequest;
import org.opensearch.client.node.NodeClient;
import org.opensearch.cluster.metadata.MappingMetadata;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.search.SearchModule;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Set of predefined functions for REST interaction with elastic search API. Performs
 * HTTP requests and JSON (de)serialization.
 */
final class OpenSearchTransport {

  private static final Logger LOGGER = LoggerFactory.getLogger(OpenSearchTable.class);

  static final int DEFAULT_FETCH_SIZE = 5196;

  private final ObjectMapper mapper;
  private final NodeClient nodeClient;

  final String indexName;

  final OpenSearchVersion version;

  final OpenSearchMapping mapping;

  private final NamedXContentRegistry xContentRegistry = new NamedXContentRegistry(
      new SearchModule(Settings.EMPTY, new ArrayList<>()).getNamedXContents());

  /**
   * Default batch size.
   *
   * @see <a href="https://www.elastic.co/guide/en/OpenSearch/reference/current/search-request-scroll.html">Scrolling API</a>
   */
  final int fetchSize;

  OpenSearchTransport(final NodeClient nodeClient,
                      final ObjectMapper mapper,
                      final String indexName,
                      final int fetchSize) {
    this.mapper = requireNonNull(mapper, "mapper");
    this.nodeClient = requireNonNull(nodeClient, "restClient");
    this.indexName = requireNonNull(indexName, "indexName");
    this.fetchSize = fetchSize;
    this.version = version(); // cache version
    this.mapping = fetchAndCreateMapping(); // cache mapping
  }

  /**
   * Detects current Elastic Search version by connecting to a existing instance.
   * It is a {@code GET} request to {@code /}. Returned JSON has server information
   * (including version).
   *
   * @return parsed version from ES, or {@link OpenSearchVersion#UNKNOWN}
   */
  private OpenSearchVersion version() {
    // FIXME. detect OS version
    return OpenSearchVersion.OS;
  }

  /**
   * Build index mapping returning new instance of {@link OpenSearchMapping}.
   */
  private OpenSearchMapping fetchAndCreateMapping() {
    GetMappingsResponse mappingsResponse =
        nodeClient.admin().indices().prepareGetMappings(indexName).setLocal(true).get();
    MappingMetadata mappingMetadata = mappingsResponse.mappings().get(indexName);
    ObjectNode properties = mapper.convertValue(mappingMetadata.getSourceAsMap(), ObjectNode.class);
    ImmutableMap.Builder<String, String> builder = ImmutableMap.builder();
    OpenSearchJson.visitMappingProperties(properties, builder::put);
    return new OpenSearchMapping(indexName, builder.build());
  }

  ObjectMapper mapper() {
    return mapper;
  }

  /**
   * Fetches search results given a scrollId.
   */
  Function<String, OpenSearchJson.Result> scroll() {
    return scrollId -> {
      try {
        SearchResponse searchResponse =
            nodeClient.searchScroll(new SearchScrollRequest().scroll(scrollId).scroll("1m"))
                .actionGet();
        return mapper.readValue(searchResponse.toString(), OpenSearchJson.Result.class);
      } catch (IOException e) {
        String message = String.format(Locale.ROOT, "Couldn't fetch next scroll %s", scrollId);
        throw new UncheckedIOException(message, e);
      }
    };
  }

  void closeScroll(Iterable<String> scrollIds) {
    requireNonNull(scrollIds, "scrollIds");
    try {

      ClearScrollRequest clearScrollRequest = new ClearScrollRequest();
      clearScrollRequest.setScrollIds(StreamSupport.stream(scrollIds.spliterator(), false).toList());
      nodeClient.clearScroll(clearScrollRequest);
    } catch ( UncheckedIOException e) {
      LOGGER.warn("Failed to close scroll(s): {}", scrollIds, e);
    }
  }

  Function<ObjectNode, OpenSearchJson.Result> search() {
    return search(Collections.emptyMap());
  }

  /**
   * Search request using HTTP post.
   */
  Function<ObjectNode, OpenSearchJson.Result> search(final Map<String, String> httpParams) {
    requireNonNull(httpParams, "httpParams");
    return query -> {
      Hook.QUERY_PLAN.run(query);
      final SearchResponse searchResponse;
      try {
        final String json = mapper.writeValueAsString(query);
        LOGGER.debug("OpenSearch Query: {}", json);
        SearchRequest searchRequest = new SearchRequest()
            .indices(indexName)
            .source(parseQuery(json))
            .scroll(httpParams.get("scoll"));
        searchResponse = nodeClient.search(searchRequest).actionGet();
        return mapper.readValue(searchResponse.toString(), OpenSearchJson.Result.class);
      } catch (JsonProcessingException e) {
        throw new UncheckedIOException(e);
      }
    };
  }

  SearchSourceBuilder parseQuery(String query) {
    try {
      XContentParser parser =
          XContentType.JSON.xContent().createParser(xContentRegistry, IGNORE_DEPRECATIONS, query);
      return SearchSourceBuilder.fromXContent(parser);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }
}
