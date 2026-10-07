/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.executor.progress;

import org.apache.lucene.search.TotalHits;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.search.aggregations.Aggregation;
import org.opensearch.search.aggregations.Aggregations;
import org.opensearch.search.aggregations.bucket.composite.CompositeAggregation;
import org.opensearch.sql.executor.progress.ProgressUnit;

/**
 * Numbers one search response contributed, read under the source's already-declared shape.
 *
 * <p>Deliberately carries no unit. The shape is a property of the source, declared before its first
 * search, and a response cannot recover it: a point-in-time page and a single request that returned
 * rows look identical from the response side. Inferring one here would reclassify a finished
 * single-request source as paged exactly when its response arrives, collapsing a complete source to
 * one page of an estimated many.
 *
 * @param completedUnits units this response contributed, counted per the declared shape
 * @param pageSize {@code size} requested on the physical search request
 * @param observedTotal {@code TotalHits} value present in the response, or {@code 0}. Under the
 *     default {@code track_total_hits} this may be a lower bound rather than a count, so progress
 *     derived from it is best effort; the relation itself is not carried.
 */
public record SearchProgressReport(long completedUnits, long pageSize, long observedTotal) {

  /**
   * Reads one response. Never inspects or retains document contents.
   *
   * @param declaredUnit the source's declared shape, which selects what to count
   */
  public static SearchProgressReport of(
      SearchRequest request, SearchResponse response, ProgressUnit declaredUnit) {
    if (declaredUnit == ProgressUnit.BUCKET_COVERAGE) {
      // Coverage, not buckets: a bucket's doc_count is the number of source documents it accounts
      // for, which
      // is what the index-size estimate is comparable to. No page size — the request's `size` is 0
      // for an
      // aggregation-only search.
      return new SearchProgressReport(Math.max(compositeCoverage(response), 0L), 0L, 0L);
    }
    long rows = response.getHits() == null ? 0L : hitCount(response);
    TotalHits totalHits = response.getHits() == null ? null : response.getHits().getTotalHits();
    long observedTotal = totalHits == null ? 0L : Math.max(totalHits.value(), 0L);
    return new SearchProgressReport(rows, pageSizeOf(request), observedTotal);
  }

  private static long hitCount(SearchResponse response) {
    return response.getHits().getHits() == null ? 0L : response.getHits().getHits().length;
  }

  private static long pageSizeOf(SearchRequest request) {
    if (request.source() == null) {
      return 0L;
    }
    int size = request.source().size();
    return Math.max(size, 0);
  }

  /**
   * Sum of {@code doc_count} across the first Composite aggregation's buckets, or {@code -1} when
   * the response carries no Composite aggregation.
   *
   * <p>One document can land in several buckets when a group key is multi-valued, and documents
   * missing a key land in none, so coverage is an approximation in both directions. The per-source
   * clamp and the public ceiling keep that from escaping the published range.
   */
  private static long compositeCoverage(SearchResponse response) {
    Aggregations aggregations = response.getAggregations();
    if (aggregations == null) {
      return -1L;
    }
    for (Aggregation aggregation : aggregations.asList()) {
      if (aggregation instanceof CompositeAggregation composite) {
        long coverage = 0L;
        for (CompositeAggregation.Bucket bucket : composite.getBuckets()) {
          coverage += bucket.getDocCount();
        }
        return coverage;
      }
    }
    return -1L;
  }
}
