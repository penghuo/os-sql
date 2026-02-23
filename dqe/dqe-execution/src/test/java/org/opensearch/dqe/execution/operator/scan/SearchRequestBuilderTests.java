/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dqe.execution.operator.scan;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import java.util.List;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.dqe.execution.pit.PitHandle;
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.index.query.TermQueryBuilder;
import org.opensearch.search.sort.SortBuilder;
import org.opensearch.search.sort.SortBuilders;

class SearchRequestBuilderTests {

  private PitHandle pit() {
    return new PitHandle("pit-abc", "test-index", TimeValue.timeValueMinutes(5));
  }

  @Test
  @DisplayName("initial request sets PIT, shard preference, and batch size")
  void initialRequestSetsPitAndPreference() {
    SearchRequestBuilder builder =
        new SearchRequestBuilder("test-index", 2, List.of("name", "age"), null, null, 100);

    SearchRequest request = builder.buildInitialRequest(pit());

    assertNotNull(request);
    assertEquals("_shards:2|_local", request.preference());
    assertNotNull(request.source());
    assertEquals(100, request.source().size());
    assertNotNull(request.source().pointInTimeBuilder());
  }

  @Test
  @DisplayName("fetchSource includes required columns")
  void fetchSourceIncludesColumns() {
    SearchRequestBuilder builder =
        new SearchRequestBuilder("test-index", 0, List.of("name", "age"), null, null, 50);

    SearchRequest request = builder.buildInitialRequest(pit());
    assertNotNull(request.source().fetchSource());
  }

  @Test
  @DisplayName("pushed-down query is included")
  void pushedDownQueryIncluded() {
    TermQueryBuilder query = QueryBuilders.termQuery("status", "active");
    SearchRequestBuilder builder =
        new SearchRequestBuilder("test-index", 0, List.of("name"), query, null, 50);

    SearchRequest request = builder.buildInitialRequest(pit());
    assertNotNull(request.source().query());
  }

  @Test
  @DisplayName("search_after request includes sort values")
  void searchAfterRequestIncludesSortValues() {
    SearchRequestBuilder builder =
        new SearchRequestBuilder("test-index", 0, List.of("name"), null, null, 50);

    Object[] sortValues = new Object[] {"value1", 42L};
    SearchRequest request = builder.buildSearchAfterRequest(pit(), sortValues);

    assertNotNull(request.source().searchAfter());
    assertEquals(2, request.source().searchAfter().length);
  }

  @Test
  @DisplayName("default sort is _doc when no sort builders provided")
  void defaultSortIsDoc() {
    SearchRequestBuilder builder =
        new SearchRequestBuilder("test-index", 0, List.of("name"), null, null, 50);

    SearchRequest request = builder.buildInitialRequest(pit());
    assertNotNull(request.source().sorts());
    assertEquals(1, request.source().sorts().size());
  }

  @Test
  @DisplayName("custom sort builders are used when provided")
  void customSortBuildersUsed() {
    List<SortBuilder<?>> sorts = List.of(SortBuilders.fieldSort("age"));
    SearchRequestBuilder builder =
        new SearchRequestBuilder("test-index", 0, List.of("name"), null, sorts, 50);

    SearchRequest request = builder.buildInitialRequest(pit());
    assertNotNull(request.source().sorts());
    assertEquals(1, request.source().sorts().size());
  }

  @Test
  @DisplayName("getIndexName and getShardId return constructor values")
  void gettersReturnValues() {
    SearchRequestBuilder builder =
        new SearchRequestBuilder("my-index", 3, List.of("col1"), null, null, 100);

    assertEquals("my-index", builder.getIndexName());
    assertEquals(3, builder.getShardId());
  }

  @Test
  @DisplayName("PIT index is not set on SearchRequest (PIT handles index)")
  void pitIndexNotSetOnRequest() {
    SearchRequestBuilder builder =
        new SearchRequestBuilder("test-index", 0, List.of("name"), null, null, 50);

    SearchRequest request = builder.buildInitialRequest(pit());
    // When using PIT, indices should not be set on the request
    assertEquals(0, request.indices().length);
  }
}
