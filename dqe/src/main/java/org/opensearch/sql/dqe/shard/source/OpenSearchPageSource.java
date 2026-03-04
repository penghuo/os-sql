/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.dqe.shard.source;

import io.trino.spi.Page;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.opensearch.action.search.ClearScrollRequest;
import org.opensearch.action.search.ClearScrollResponse;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.action.search.SearchScrollRequest;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.search.SearchHit;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.opensearch.sql.dqe.operator.Operator;

/**
 * Leaf data source operator that executes a {@link SearchRequest} scoped to a single shard via
 * {@code _only_local} preference, converts {@link SearchHit}s to Trino {@link Page}s using {@link
 * PageBuilder}.
 *
 * <p>Uses the scroll API for batched retrieval. Each call to {@link #processNextBatch()} returns
 * one page of data or {@code null} when exhausted. On {@link #close()}, the scroll context is
 * cleared.
 */
public class OpenSearchPageSource implements Operator {

  /**
   * Abstraction over OpenSearch client search operations. This allows the class to be tested
   * without depending directly on {@code NodeClient}, which has many transitive dependencies and
   * can be difficult to mock.
   */
  public interface SearchClient {
    /** Execute a search request and return the response. */
    SearchResponse search(SearchRequest request);

    /** Continue scrolling with an existing scroll context. */
    SearchResponse searchScroll(SearchScrollRequest request);

    /** Clear a scroll context to free server-side resources. */
    ClearScrollResponse clearScroll(ClearScrollRequest request);
  }

  private static final TimeValue SCROLL_TIMEOUT = TimeValue.timeValueMinutes(1);

  private final SearchClient client;
  private final String indexName;
  private final int shardId;
  private final SearchSourceBuilder dslQuery;
  private final List<ColumnHandle> columns;
  private final int batchSize;

  private String scrollId;
  private boolean exhausted = false;

  /**
   * Create a new OpenSearchPageSource.
   *
   * @param client search client for executing queries
   * @param indexName target index name
   * @param shardId target shard (for routing)
   * @param dslQuery filter and projection pushed down to DSL
   * @param columns columns to extract from hits
   * @param batchSize number of rows per page
   */
  public OpenSearchPageSource(
      SearchClient client,
      String indexName,
      int shardId,
      SearchSourceBuilder dslQuery,
      List<ColumnHandle> columns,
      int batchSize) {
    this.client = client;
    this.indexName = indexName;
    this.shardId = shardId;
    this.dslQuery = dslQuery;
    this.columns = columns;
    this.batchSize = batchSize;
  }

  @Override
  public Page processNextBatch() {
    if (exhausted) {
      return null;
    }

    SearchResponse response;
    if (scrollId == null) {
      // First request: issue a new search with scroll
      SearchRequest request = new SearchRequest(indexName);
      request.preference("_only_local");
      request.source(new SearchSourceBuilder().query(dslQuery.query()).size(batchSize));
      request.scroll(SCROLL_TIMEOUT);
      response = client.search(request);
      scrollId = response.getScrollId();
    } else {
      // Subsequent requests: continue scrolling
      SearchScrollRequest scrollRequest = new SearchScrollRequest(scrollId);
      scrollRequest.scroll(SCROLL_TIMEOUT);
      response = client.searchScroll(scrollRequest);
    }

    SearchHit[] hits = response.getHits().getHits();
    if (hits.length == 0) {
      exhausted = true;
      return null;
    }

    List<Map<String, Object>> rows = new ArrayList<>(hits.length);
    for (SearchHit hit : hits) {
      rows.add(hit.getSourceAsMap());
    }
    return PageBuilder.build(columns, rows);
  }

  @Override
  public void close() {
    if (scrollId != null) {
      ClearScrollRequest clearRequest = new ClearScrollRequest();
      clearRequest.addScrollId(scrollId);
      client.clearScroll(clearRequest);
    }
  }
}
