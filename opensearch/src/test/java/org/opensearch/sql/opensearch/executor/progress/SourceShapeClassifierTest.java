/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.executor.progress;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;

import java.util.List;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.search.aggregations.bucket.composite.CompositeAggregationBuilder;
import org.opensearch.search.aggregations.bucket.composite.TermsValuesSourceBuilder;
import org.opensearch.search.aggregations.metrics.MaxAggregationBuilder;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.opensearch.sql.executor.progress.ProgressUnit;
import org.opensearch.sql.opensearch.data.value.OpenSearchExprValueFactory;
import org.opensearch.sql.opensearch.request.OpenSearchQueryRequest;
import org.opensearch.sql.opensearch.request.OpenSearchRequest;
import org.opensearch.sql.opensearch.request.OpenSearchScrollRequest;

/**
 * Shape classification, which has to be right before a scan's first search: the calculator reads
 * "every shard reported" as "source done" for a single request and as "one page done" for a paged
 * one, and a paged source misread as single-request pins at full coverage after page one.
 */
class SourceShapeClassifierTest {

  private static final OpenSearchRequest.IndexName INDEX =
      new OpenSearchRequest.IndexName("accounts");

  @Test
  @DisplayName("a plain search within the result window is a single request")
  void plainSearchIsSingleRequest() {
    SearchSourceBuilder source = new SearchSourceBuilder().size(200);
    SourceShapeClassifier.Shape shape =
        SourceShapeClassifier.classify(
            OpenSearchQueryRequest.of(INDEX, source, factory(), List.of()));

    assertEquals(ProgressUnit.SINGLE_REQUEST, shape.unit());
    assertEquals(200L, shape.pageSize());
  }

  @Test
  @DisplayName("a point-in-time search is paged and carries its page size")
  void pointInTimeSearchIsPaged() {
    SearchSourceBuilder source = new SearchSourceBuilder().size(1_000);
    SourceShapeClassifier.Shape shape =
        SourceShapeClassifier.classify(
            OpenSearchQueryRequest.pitOf(
                INDEX, source, factory(), List.of(), TimeValue.timeValueMinutes(1), "pit-id"));

    assertEquals(ProgressUnit.PAGED_ROWS, shape.unit());
    assertEquals(1_000L, shape.pageSize());
  }

  @Test
  @DisplayName("a composite aggregation is measured in bucket coverage, not pages")
  void compositeAggregationIsCoverage() {
    SearchSourceBuilder source = new SearchSourceBuilder().size(0);
    source.aggregation(
        new CompositeAggregationBuilder(
            "composite", List.of(new TermsValuesSourceBuilder("state").field("state"))));

    SourceShapeClassifier.Shape shape =
        SourceShapeClassifier.classify(
            OpenSearchQueryRequest.of(INDEX, source, factory(), List.of()));

    assertEquals(ProgressUnit.BUCKET_COVERAGE, shape.unit());
    // Deliberately no page size: a composite page's coverage is comparable to the index estimate
    // directly, and
    // the request's size is 0 for an aggregation-only search.
    assertEquals(0L, shape.pageSize());
  }

  @Test
  @DisplayName("a non-composite aggregation resolves in one round trip")
  void metricAggregationIsSingleRequest() {
    SearchSourceBuilder source = new SearchSourceBuilder().size(0);
    source.aggregation(new MaxAggregationBuilder("max_balance").field("balance"));

    SourceShapeClassifier.Shape shape =
        SourceShapeClassifier.classify(
            OpenSearchQueryRequest.of(INDEX, source, factory(), List.of()));

    assertEquals(ProgressUnit.SINGLE_REQUEST, shape.unit());
  }

  @Test
  @DisplayName("a scroll request is paged")
  void scrollRequestIsPaged() {
    SearchSourceBuilder source = new SearchSourceBuilder().size(500);
    SourceShapeClassifier.Shape shape =
        SourceShapeClassifier.classify(
            new OpenSearchScrollRequest(
                INDEX, TimeValue.timeValueMinutes(1), source, factory(), List.of()));

    assertEquals(ProgressUnit.PAGED_ROWS, shape.unit());
    assertEquals(500L, shape.pageSize());
  }

  @Test
  @DisplayName("an unrecognised request shape falls back to single request")
  void unknownRequestFallsBack() {
    SourceShapeClassifier.Shape shape =
        SourceShapeClassifier.classify(mock(OpenSearchRequest.class));

    assertEquals(ProgressUnit.SINGLE_REQUEST, shape.unit());
    assertEquals(0L, shape.pageSize());
  }

  private static OpenSearchExprValueFactory factory() {
    return mock(OpenSearchExprValueFactory.class);
  }
}
