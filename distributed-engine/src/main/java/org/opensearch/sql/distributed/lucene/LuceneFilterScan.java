/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.distributed.lucene;

import com.google.common.util.concurrent.ListenableFuture;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.List;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.Weight;
import org.apache.lucene.util.Bits;
import org.opensearch.sql.distributed.context.OperatorContext;
import org.opensearch.sql.distributed.data.Page;
import org.opensearch.sql.distributed.operator.Operator;
import org.opensearch.sql.distributed.operator.OperatorFactory;
import org.opensearch.sql.distributed.operator.SourceOperator;

/**
 * A source operator that scans a Lucene index with a pushed-down filter predicate. Only documents
 * matching the query are read through DocValues and returned as Pages.
 *
 * <p>This operator:
 *
 * <ul>
 *   <li>Creates a {@link Weight} from the pushed {@link Query}
 *   <li>For each segment, obtains a {@link Scorer} and iterates its {@link DocIdSetIterator}
 *   <li>Collects matching doc IDs in batches and reads their DocValues
 *   <li>Converts DocValues to Blocks via {@link DocValuesToBlockConverter}
 * </ul>
 *
 * <p>Performance advantage: Lucene's posting lists and BKD trees efficiently skip non-matching
 * documents, so this operator avoids reading DocValues for filtered-out rows.
 */
public class LuceneFilterScan implements SourceOperator {

  private final OperatorContext operatorContext;
  private final IndexSearcher searcher;
  private final Query query;
  private final List<ColumnMapping> columns;
  private final int batchSize;

  /** Pre-computed Weight for the query (reused across segments). */
  private Weight weight;

  /** Segment iteration state. */
  private final List<LeafReaderContext> leaves;

  private int currentLeafIndex;
  private DocIdSetIterator currentIterator;
  private boolean finished;

  /** Reusable buffer for collecting matching document IDs within a batch. */
  private final int[] docIdBuffer;

  public LuceneFilterScan(
      OperatorContext operatorContext,
      IndexSearcher searcher,
      Query query,
      List<ColumnMapping> columns,
      int batchSize) {
    this.operatorContext = operatorContext;
    this.searcher = searcher;
    this.query = query;
    this.columns = List.copyOf(columns);
    this.batchSize = batchSize;
    this.docIdBuffer = new int[batchSize];
    this.leaves = searcher.getIndexReader().leaves();
    this.currentLeafIndex = 0;
    this.currentIterator = null;
    this.finished = leaves.isEmpty();
  }

  public LuceneFilterScan(
      OperatorContext operatorContext,
      IndexSearcher searcher,
      Query query,
      List<ColumnMapping> columns) {
    this(operatorContext, searcher, query, columns, DocValuesToBlockConverter.DEFAULT_BATCH_SIZE);
  }

  @Override
  public OperatorContext getOperatorContext() {
    return operatorContext;
  }

  @Override
  public ListenableFuture<?> isBlocked() {
    return NOT_BLOCKED;
  }

  /**
   * Produces the next Page of filtered data. Iterates the Scorer's DocIdSetIterator to collect
   * matching doc IDs, then reads DocValues for those IDs.
   *
   * @return a Page with matching documents, or null if no more data
   */
  @Override
  public Page getOutput() {
    if (finished) {
      return null;
    }

    try {
      ensureWeight();

      while (currentLeafIndex < leaves.size()) {
        LeafReaderContext leafCtx = leaves.get(currentLeafIndex);

        if (currentIterator == null) {
          currentIterator = createIterator(leafCtx);
          if (currentIterator == null) {
            currentLeafIndex++;
            continue;
          }
        }

        int count = collectMatchingDocs(docIdBuffer);

        // Check if the iterator exhausted this segment (reached NO_MORE_DOCS).
        // We must advance to the next segment to avoid calling nextDoc() on an
        // exhausted iterator, which causes integer overflow in RangeDocIdSetIterator.
        boolean segmentExhausted = currentIterator.docID() == DocIdSetIterator.NO_MORE_DOCS;
        if (segmentExhausted) {
          currentLeafIndex++;
          currentIterator = null;
        }

        if (count == 0) {
          continue;
        }

        Page page = DocValuesToBlockConverter.readDocValues(leafCtx, columns, docIdBuffer, count);
        if (page.getPositionCount() > 0) {
          return page;
        }
      }

      finished = true;
      return null;
    } catch (IOException e) {
      throw new UncheckedIOException("Failed during filtered Lucene scan", e);
    }
  }

  /** Lazily creates the Weight from the query. Rewrites the query through the IndexSearcher. */
  private void ensureWeight() throws IOException {
    if (weight == null) {
      Query rewritten = searcher.rewrite(query);
      weight = searcher.createWeight(rewritten, ScoreMode.COMPLETE_NO_SCORES, 1.0f);
    }
  }

  /**
   * Creates a DocIdSetIterator for the given segment.
   *
   * @return the iterator, or null if no documents match in this segment
   */
  private DocIdSetIterator createIterator(LeafReaderContext leafCtx) throws IOException {
    Scorer scorer = weight.scorer(leafCtx);
    if (scorer == null) {
      return null;
    }
    return scorer.iterator();
  }

  /**
   * Collects up to batchSize matching document IDs from the current iterator, skipping
   * soft-deleted documents that still appear in the posting lists.
   *
   * @param docIds output array to fill with matching doc IDs
   * @return number of matching docs collected
   */
  private int collectMatchingDocs(int[] docIds) throws IOException {
    Bits liveDocs = leaves.get(currentLeafIndex).reader().getLiveDocs();
    int count = 0;
    while (count < batchSize) {
      int docId = currentIterator.nextDoc();
      if (docId == DocIdSetIterator.NO_MORE_DOCS) {
        break;
      }
      // Skip soft-deleted documents
      if (liveDocs == null || liveDocs.get(docId)) {
        docIds[count++] = docId;
      }
    }
    return count;
  }

  @Override
  public void finish() {
    finished = true;
  }

  @Override
  public boolean isFinished() {
    return finished;
  }

  @Override
  public void close() throws Exception {
    finished = true;
    currentIterator = null;
    weight = null;
  }

  /** Factory for creating LuceneFilterScan operators. */
  public static class Factory implements OperatorFactory {

    private final IndexSearcher searcher;
    private final Query query;
    private final List<ColumnMapping> columns;
    private final int batchSize;

    public Factory(IndexSearcher searcher, Query query, List<ColumnMapping> columns) {
      this(searcher, query, columns, DocValuesToBlockConverter.DEFAULT_BATCH_SIZE);
    }

    public Factory(
        IndexSearcher searcher, Query query, List<ColumnMapping> columns, int batchSize) {
      this.searcher = searcher;
      this.query = query;
      this.columns = List.copyOf(columns);
      this.batchSize = batchSize;
    }

    @Override
    public Operator createOperator(OperatorContext operatorContext) {
      return new LuceneFilterScan(operatorContext, searcher, query, columns, batchSize);
    }

    @Override
    public void noMoreOperators() {
      // No cleanup needed
    }

    public Query getQuery() {
      return query;
    }
  }
}
