/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dqe.execution.operator.scan;

import java.util.List;
import java.util.Objects;
import javax.annotation.Nullable;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.dqe.execution.pit.PitHandle;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.search.builder.PointInTimeBuilder;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.opensearch.search.sort.SortBuilder;
import org.opensearch.search.sort.SortBuilders;

/**
 * Builds OpenSearch SearchRequest objects for shard-level scan operations. Handles PIT integration,
 * search_after pagination, shard targeting via preference, column pruning, and predicate pushdown.
 */
public class SearchRequestBuilder {

  private final String indexName;
  private final int shardId;
  private final List<String> requiredColumns;
  @Nullable private final QueryBuilder pushedDownQuery;
  @Nullable private final List<SortBuilder<?>> sortBuilders;
  private final int batchSize;

  public SearchRequestBuilder(
      String indexName,
      int shardId,
      List<String> requiredColumns,
      @Nullable QueryBuilder pushedDownQuery,
      @Nullable List<SortBuilder<?>> sortBuilders,
      int batchSize) {
    this.indexName = Objects.requireNonNull(indexName, "indexName must not be null");
    this.shardId = shardId;
    this.requiredColumns =
        List.copyOf(Objects.requireNonNull(requiredColumns, "requiredColumns must not be null"));
    this.pushedDownQuery = pushedDownQuery;
    this.sortBuilders = sortBuilders;
    this.batchSize = batchSize;
  }

  /** Builds the initial SearchRequest (first page). */
  public SearchRequest buildInitialRequest(PitHandle pitHandle) {
    SearchSourceBuilder source = buildCommonSource(pitHandle);
    return buildSearchRequest(source, pitHandle);
  }

  /** Builds a subsequent SearchRequest using search_after from the previous response. */
  public SearchRequest buildSearchAfterRequest(PitHandle pitHandle, Object[] searchAfterValues) {
    SearchSourceBuilder source = buildCommonSource(pitHandle);
    source.searchAfter(searchAfterValues);
    return buildSearchRequest(source, pitHandle);
  }

  private SearchSourceBuilder buildCommonSource(PitHandle pitHandle) {
    SearchSourceBuilder source = new SearchSourceBuilder();
    source.size(batchSize);

    // Column pruning
    if (!requiredColumns.isEmpty()) {
      source.fetchSource(requiredColumns.toArray(new String[0]), null);
    }

    // Pushed-down predicate
    if (pushedDownQuery != null) {
      source.query(pushedDownQuery);
    }

    // Sort -- use explicit sorts if provided, otherwise _doc for efficient iteration
    if (sortBuilders != null && !sortBuilders.isEmpty()) {
      for (SortBuilder<?> sb : sortBuilders) {
        source.sort(sb);
      }
    } else {
      source.sort(SortBuilders.fieldSort("_doc"));
    }

    // PIT configuration
    PointInTimeBuilder pitBuilder = new PointInTimeBuilder(pitHandle.getPitId());
    pitBuilder.setKeepAlive(pitHandle.getKeepAlive());
    source.pointInTimeBuilder(pitBuilder);

    return source;
  }

  private SearchRequest buildSearchRequest(SearchSourceBuilder source, PitHandle pitHandle) {
    SearchRequest request = new SearchRequest();
    // When using PIT, do not set index or preference — PIT already targets the index
    // and OpenSearch rejects preference with PIT.
    // For non-PIT requests (future), shard targeting via preference would go here.
    request.source(source);
    return request;
  }

  public String getIndexName() {
    return indexName;
  }

  public int getShardId() {
    return shardId;
  }
}
