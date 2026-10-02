/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.executor.progress;

import org.opensearch.search.aggregations.AggregatorFactories;
import org.opensearch.search.aggregations.bucket.composite.CompositeAggregationBuilder;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.opensearch.sql.executor.progress.ProgressUnit;
import org.opensearch.sql.opensearch.request.OpenSearchQueryRequest;
import org.opensearch.sql.opensearch.request.OpenSearchRequest;
import org.opensearch.sql.opensearch.request.OpenSearchScrollRequest;

/**
 * Decides, before a scan's first search, how its responses should be counted.
 *
 * <p>Shape has to be settled up front. The calculator reads "every shard reported" as "the source
 * is done" for a single-request source and as "one page landed" for a paged one; getting that wrong
 * in the paged direction pins the source at 1.0 after its first page, and the monotonic latch then
 * holds the wrong value for the whole query. Everything needed is already on the built request: a
 * Composite aggregation in the source builder, a point-in-time id, or neither.
 */
public final class SourceShapeClassifier {

  private SourceShapeClassifier() {
    throw new AssertionError(
        SourceShapeClassifier.class.getCanonicalName()
            + " is a utility class and must not be initialized");
  }

  /**
   * Counting shape of a source's responses.
   *
   * @param unit what each response's units mean
   * @param pageSize {@code size} on the physical search request, or {@code 0} when the shape has
   *     none
   */
  public record Shape(ProgressUnit unit, long pageSize) {}

  /** Classifies a built request. Defaults to {@link ProgressUnit#SINGLE_REQUEST} when unsure. */
  public static Shape classify(OpenSearchRequest request) {
    SearchSourceBuilder source = sourceBuilderOf(request);
    if (source == null) {
      return new Shape(ProgressUnit.SINGLE_REQUEST, 0L);
    }
    if (hasCompositeAggregation(source)) {
      // Composite pages are measured in document coverage, so the request's `size` (0 for an
      // aggregation-only search) is not a useful denominator.
      return new Shape(ProgressUnit.BUCKET_COVERAGE, 0L);
    }
    if (isPaged(request)) {
      return new Shape(ProgressUnit.PAGED_ROWS, Math.max(source.size(), 0));
    }
    return new Shape(ProgressUnit.SINGLE_REQUEST, Math.max(source.size(), 0));
  }

  private static SearchSourceBuilder sourceBuilderOf(OpenSearchRequest request) {
    return switch (request) {
      case OpenSearchQueryRequest query -> query.getSourceBuilder();
      // A scroll holds its source builder only on the opening request; later pages carry a scroll
      // id.
      case OpenSearchScrollRequest scroll ->
          scroll.getInitialSearchRequest() == null
              ? null
              : scroll.getInitialSearchRequest().source();
      default -> null;
    };
  }

  /**
   * Whether the request will be answered over several round trips.
   *
   * <p>A point-in-time id means the engine could not satisfy the query inside one result window and
   * will page with {@code search_after}; a scroll request pages by definition.
   */
  private static boolean isPaged(OpenSearchRequest request) {
    return switch (request) {
      case OpenSearchQueryRequest query -> query.getPitId() != null;
      case OpenSearchScrollRequest ignored -> true;
      default -> false;
    };
  }

  private static boolean hasCompositeAggregation(SearchSourceBuilder source) {
    AggregatorFactories.Builder aggregations = source.aggregations();
    if (aggregations == null) {
      return false;
    }
    return aggregations.getAggregatorFactories().stream()
        .anyMatch(CompositeAggregationBuilder.class::isInstance);
  }
}
