/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.dqe.shard.source;

import io.trino.spi.Page;
import io.trino.spi.block.Block;
import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.Collector;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.LeafCollector;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.Scorable;
import org.apache.lucene.search.ScoreMode;
import org.opensearch.index.shard.IndexShard;
import org.opensearch.sql.dqe.operator.Operator;

/**
 * Leaf data source operator that reads directly from Lucene doc values instead of using the
 * OpenSearch scroll API. Acquires an {@link IndexSearcher}, collects matching doc IDs per segment,
 * and reads doc values via {@link DocValuesReader} to produce Trino {@link Page}s.
 *
 * <p>Uses a segment-streaming approach: processes one segment at a time, emitting each segment as a
 * single Page. For multi-column reads, uses interleaved DocValues reading to open each column's
 * iterator once per segment and advance all iterators together, avoiding repeated seek overhead.
 */
public class LucenePageSource implements Operator {

  private final IndexSearcher searcher;
  private final Closeable searcherCloseable;
  private final List<ColumnHandle> columns;
  private final int batchSize;
  private final Query query;

  /** Collected doc IDs grouped by segment. */
  private List<SegmentDocs> segments;

  private int currentSegment;
  private int currentOffset;
  private boolean initialized;

  /**
   * Test constructor: accepts an IndexReader directly.
   *
   * @param reader index reader to search
   * @param query Lucene query for filtering
   * @param columns columns to read from doc values
   * @param batchSize number of rows per page
   */
  public LucenePageSource(
      IndexReader reader, Query query, List<ColumnHandle> columns, int batchSize) {
    this.searcher = new IndexSearcher(reader);
    this.searcherCloseable = null;
    this.query = query;
    this.columns = columns;
    this.batchSize = batchSize;
  }

  /**
   * Production constructor: acquires a searcher from IndexShard.
   *
   * @param shard the index shard to read from
   * @param query Lucene query for filtering
   * @param columns columns to read from doc values
   * @param batchSize number of rows per page
   */
  public LucenePageSource(
      IndexShard shard, Query query, List<ColumnHandle> columns, int batchSize) {
    // Engine.Searcher extends IndexSearcher and is Closeable
    org.opensearch.index.engine.Engine.Searcher engineSearcher =
        shard.acquireSearcher("dqe-lucene-reader");
    this.searcher = engineSearcher;
    this.searcherCloseable = engineSearcher;
    this.query = query;
    this.columns = columns;
    this.batchSize = batchSize;
  }

  @Override
  public Page processNextBatch() {
    try {
      if (!initialized) {
        initialize();
        initialized = true;
      }

      // COUNT(*) fast path: no columns needed, return total count
      if (segments == null) {
        return null; // fast path already returned
      }

      while (currentSegment < segments.size()) {
        SegmentDocs seg = segments.get(currentSegment);
        if (currentOffset >= seg.count) {
          currentSegment++;
          currentOffset = 0;
          continue;
        }

        int remaining = seg.count - currentOffset;
        int batchCount = Math.min(remaining, batchSize);

        if (columns.isEmpty()) {
          // COUNT(*)-like: no doc values to read, just advance
          currentOffset += batchCount;
          return new Page(batchCount);
        }

        // Use interleaved reading for multi-column reads: opens each column's
        // DocValues iterator once and advances all together per doc ID.
        // For single-column reads, interleaved is equivalent to column-at-a-time.
        Block[] blocks =
            DocValuesReader.readColumnsInterleaved(
                seg.leaf, columns, seg.docIds, currentOffset, batchCount);

        currentOffset += batchCount;
        return new Page(blocks);
      }

      return null; // all segments exhausted
    } catch (IOException e) {
      throw new RuntimeException("Failed to read doc values", e);
    }
  }

  private void initialize() throws IOException {
    if (columns.isEmpty()) {
      // COUNT(*) fast path: just count matching docs
      int count = searcher.count(query);
      if (count > 0) {
        // Create a single "segment" with the count
        segments = new ArrayList<>();
        segments.add(new SegmentDocs(null, null, count));
      }
      return;
    }

    segments = new ArrayList<>();
    searcher.search(
        query,
        new Collector() {
          @Override
          public LeafCollector getLeafCollector(LeafReaderContext context) {
            // Use a growable int array to avoid Integer boxing overhead.
            // Start with a reasonable initial capacity based on segment size.
            int initialCapacity = Math.min(context.reader().maxDoc(), 65536);
            return new LeafCollector() {
              private int[] docIds = new int[initialCapacity];
              private int size = 0;

              @Override
              public void setScorer(Scorable scorer) {}

              @Override
              public void collect(int doc) {
                if (size == docIds.length) {
                  // Grow by 50%
                  int[] newArray = new int[docIds.length + (docIds.length >> 1) + 1];
                  System.arraycopy(docIds, 0, newArray, 0, size);
                  docIds = newArray;
                }
                docIds[size++] = doc;
              }

              @Override
              public void finish() {
                if (size > 0) {
                  // Trim to exact size if significantly oversized
                  if (docIds.length > size + (size >> 2)) {
                    int[] trimmed = new int[size];
                    System.arraycopy(docIds, 0, trimmed, 0, size);
                    docIds = trimmed;
                  }
                  segments.add(new SegmentDocs(context, docIds, size));
                }
              }
            };
          }

          @Override
          public ScoreMode scoreMode() {
            return ScoreMode.COMPLETE_NO_SCORES;
          }
        });
  }

  @Override
  public void close() {
    if (searcherCloseable != null) {
      try {
        searcherCloseable.close();
      } catch (IOException e) {
        // Best effort close
      }
    }
  }

  /** Holds doc IDs for one segment along with the leaf reader context. */
  private static class SegmentDocs {
    final LeafReaderContext leaf;
    final int[] docIds;
    final int count;

    SegmentDocs(LeafReaderContext leaf, int[] docIds, int count) {
      this.leaf = leaf;
      this.docIds = docIds;
      this.count = count;
    }
  }
}
