/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.executor.progress;

/**
 * What a {@link SourceProgressEvent.RowsObserved} counts.
 *
 * <p>The two paged source shapes measure themselves against different denominators, so the unit has
 * to travel with the event. Mixing them would compare a page's row count against an index's
 * document count and report a paged scan as essentially complete after its first page.
 */
public enum ProgressUnit {

  /**
   * One round trip answers the whole source: a hit search inside the result window, or an
   * aggregation reduced on the coordinator. There is no page arithmetic to do, so progress comes
   * entirely from the search's shard callbacks and the source is complete when its response lands.
   *
   * <p>Separating this from {@link #PAGED_ROWS} matters: for a single request, "every shard
   * reported" and "the source is done" are the same event, while for a paged search the first is
   * only the end of one page. Conflating them would let a ten-page scan report its source complete
   * after page one.
   */
  SINGLE_REQUEST,

  /**
   * Rows returned by one page of a paged hit search. Progress is computed over pages: the page size
   * is known, so an index-size estimate converts to an expected page count.
   */
  PAGED_ROWS,

  /**
   * Documents covered by the buckets in one Composite aggregation response, summed from bucket
   * {@code doc_count}. Comparable to the index's document count directly, with no page arithmetic.
   */
  BUCKET_COVERAGE
}
